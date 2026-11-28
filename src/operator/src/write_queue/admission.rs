// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use crate::write_queue::completion::{CompletionEntry, CompletionError, CompletionHandle};
use crate::write_queue::ledger::{AdmissionCharges, ChargeGuard, MemoryLedger, RejectReason};
use crate::write_queue::lock;
use crate::write_queue::record::Record;
use crate::write_queue::settlement::FinalSettlement;
use crate::write_queue::state::{QueueState, QueueStateError, RecordState};

/// The admission boundary for the process-local write queue.
#[derive(Clone)]
pub struct Admission {
    inner: Arc<AdmissionInner>,
}

/// A published record plus the charges and observer owned by its caller.
///
/// Dropping this value releases the payload, record-slot, and, if still
/// attached, completion charges, but it does not settle the completion entry.
/// Waiters then observe `Timeout`. The queue owner must call [`Self::settle`];
/// PR2's worker and coordinator failure paths are responsible for settling as
/// `FinalSettlement::IssueUnknown`.
pub struct AdmittedRecord {
    record: Record,
    entry: Arc<CompletionEntry>,
    record_slot: Arc<ChargeGuard>,
    payload: ChargeGuard,
    completion: Option<CompletionHandle>,
}

struct AdmissionInner {
    state: Mutex<QueueState>,
    ledger: MemoryLedger,
    next_entry_id: AtomicU64,
}

impl Admission {
    /// Creates an open admission boundary over the given ledger.
    pub fn new(ledger: MemoryLedger) -> Self {
        Self {
            inner: Arc::new(AdmissionInner {
                state: Mutex::new(QueueState::Open),
                ledger,
                next_entry_id: AtomicU64::new(0),
            }),
        }
    }

    /// Applies one queue lifecycle transition.
    pub fn transition(&self, next: QueueState) -> Result<(), QueueStateError> {
        let mut state = lock(&self.inner.state);
        let next = state.transition(next)?;
        *state = next;
        Ok(())
    }

    /// Atomically charges the ledger, allocates the entry id and publishes the record.
    pub fn publish(&self, mut record: Record) -> Result<AdmittedRecord, RejectReason> {
        let state = lock(&self.inner.state);
        match *state {
            QueueState::Open => {}
            QueueState::Draining => return Err(RejectReason::Unavailable),
            QueueState::Closed => return Err(RejectReason::Closed),
        }
        if record.state() != RecordState::Admitted {
            return Err(RejectReason::Unavailable);
        }
        record.start_admission_window(Instant::now());

        let exclusive_bytes = record.exclusive_bytes().ok_or(RejectReason::Unmeasurable)?;
        let charges = self.inner.ledger.reserve_admission(exclusive_bytes)?;
        let entry_id = self.inner.next_entry_id.fetch_add(1, Ordering::Relaxed);
        let entry = Arc::new(CompletionEntry::new());

        record.assign_entry_id(entry_id);
        if record.transition(RecordState::Runnable).is_err() {
            return Err(RejectReason::Unavailable);
        }

        let AdmissionCharges {
            payload,
            completion,
            record_slot,
        } = charges;
        let record_slot = Arc::new(record_slot);
        let handle =
            CompletionHandle::new(Arc::clone(&entry), Arc::clone(&record_slot), completion);

        Ok(AdmittedRecord {
            record,
            entry,
            record_slot,
            payload,
            completion: Some(handle),
        })
    }
}

impl AdmittedRecord {
    /// Returns the published record.
    pub fn record(&self) -> &Record {
        &self.record
    }

    /// Moves the caller completion observer out of the queue owner.
    pub fn take_completion(&mut self) -> Option<CompletionHandle> {
        self.completion.take()
    }

    /// Settles once, notifies waiters, releases every record-owned charge, and
    /// drops the record (including its retained request and sidecar).
    ///
    /// Extract bounded metadata with [`Self::record`] before calling this method.
    pub fn settle(self, settlement: FinalSettlement) -> Result<(), CompletionError> {
        let mut this = self;
        this.record.transition(RecordState::Settled)?;
        this.entry.settle(settlement)?;
        let _ = this.record.take_charge();

        let AdmittedRecord {
            record,
            entry: _,
            record_slot,
            payload,
            completion,
        } = this;
        drop(record);
        drop(payload);
        drop(record_slot);
        drop(completion);

        Ok(())
    }
}
