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

use std::fmt;
use std::sync::{Arc, Mutex};

use crate::write_queue::lock;

/// Fixed overhead charged for the record and its queue ownership.
const PER_RECORD_OVERHEAD: u64 = 256;

/// Fixed overhead charged while a caller can still wait for completion.
const PER_COMPLETION_OVERHEAD: u64 = 128;

/// Why a candidate record was not admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RejectReason {
    Closed,
    Unavailable,
    FullRecords,
    FullBytes,
    RecordTooLarge,
    Unmeasurable,
}

/// Tracks retained bytes and records for the process-local write queue.
#[derive(Clone)]
pub struct MemoryLedger {
    max_bytes: u64,
    max_records: u64,
    state: Arc<Mutex<LedgerState>>,
}

#[derive(Debug)]
struct LedgerState {
    retained_bytes: u64,
    retained_records: u64,
}

/// Releases one ledger charge when the last owner drops it.
#[derive(Debug)]
pub struct ChargeGuard {
    ledger: Arc<Mutex<LedgerState>>,
    charge: Option<Charge>,
}

#[derive(Debug)]
enum Charge {
    Bytes(u64),
    Record,
}

/// Charges acquired atomically for one record publication.
pub(crate) struct AdmissionCharges {
    pub payload: ChargeGuard,
    pub completion: ChargeGuard,
    pub record_slot: ChargeGuard,
}

impl MemoryLedger {
    /// Creates a ledger with the given retained-byte and record limits.
    pub fn new(max_bytes: u64, max_records: u64) -> Self {
        Self {
            max_bytes,
            max_records,
            state: Arc::new(Mutex::new(LedgerState {
                retained_bytes: 0,
                retained_records: 0,
            })),
        }
    }

    /// Reserves bytes for a shared or already-materialized allocation.
    pub fn reserve(&self, bytes: u64) -> Result<ChargeGuard, RejectReason> {
        let mut state = lock(&self.state);
        let retained_bytes = state
            .retained_bytes
            .checked_add(bytes)
            .ok_or(RejectReason::FullBytes)?;
        if retained_bytes > self.max_bytes {
            return Err(RejectReason::FullBytes);
        }

        state.retained_bytes = retained_bytes;
        Ok(self.charge(Charge::Bytes(bytes)))
    }

    /// Reserves the completion observer's fixed state.
    pub fn reserve_completion(&self) -> Result<ChargeGuard, RejectReason> {
        self.reserve(PER_COMPLETION_OVERHEAD)
    }

    /// Reserves payload, completion state and one record slot without overcommitting.
    pub(crate) fn reserve_admission(
        &self,
        exclusive_bytes: u64,
    ) -> Result<AdmissionCharges, RejectReason> {
        let payload_bytes = exclusive_bytes
            .checked_add(PER_RECORD_OVERHEAD)
            .ok_or(RejectReason::RecordTooLarge)?;
        if payload_bytes > self.max_bytes {
            return Err(RejectReason::RecordTooLarge);
        }

        let total_bytes = payload_bytes
            .checked_add(PER_COMPLETION_OVERHEAD)
            .ok_or(RejectReason::FullBytes)?;
        let mut state = lock(&self.state);
        let retained_records = state
            .retained_records
            .checked_add(1)
            .ok_or(RejectReason::FullRecords)?;
        if retained_records > self.max_records {
            return Err(RejectReason::FullRecords);
        }

        let retained_bytes = state
            .retained_bytes
            .checked_add(total_bytes)
            .ok_or(RejectReason::FullBytes)?;
        if retained_bytes > self.max_bytes {
            return Err(RejectReason::FullBytes);
        }

        state.retained_records = retained_records;
        state.retained_bytes = retained_bytes;
        drop(state);

        Ok(AdmissionCharges {
            payload: self.charge(Charge::Bytes(payload_bytes)),
            completion: self.charge(Charge::Bytes(PER_COMPLETION_OVERHEAD)),
            record_slot: self.charge(Charge::Record),
        })
    }

    /// Returns the currently retained byte charge.
    pub fn retained_bytes(&self) -> u64 {
        lock(&self.state).retained_bytes
    }

    /// Returns the currently retained record count.
    pub fn retained_records(&self) -> u64 {
        lock(&self.state).retained_records
    }

    fn charge(&self, charge: Charge) -> ChargeGuard {
        ChargeGuard {
            ledger: Arc::clone(&self.state),
            charge: Some(charge),
        }
    }
}

impl ChargeGuard {
    /// Returns the byte charge held by this guard, or zero for a record slot.
    pub fn bytes(&self) -> u64 {
        match self.charge.as_ref() {
            Some(Charge::Bytes(bytes)) => *bytes,
            Some(Charge::Record) | None => 0,
        }
    }

    /// Releases this charge explicitly.
    pub fn release(self) {
        drop(self);
    }
}

impl Drop for ChargeGuard {
    fn drop(&mut self) {
        let Some(charge) = self.charge.take() else {
            return;
        };
        let mut state = lock(&self.ledger);
        match charge {
            Charge::Bytes(bytes) => {
                state.retained_bytes = state.retained_bytes.saturating_sub(bytes);
            }
            Charge::Record => {
                state.retained_records = state.retained_records.saturating_sub(1);
            }
        }
    }
}

impl fmt::Display for RejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let reason = match self {
            Self::Closed => "queue is closed",
            Self::Unavailable => "queue is unavailable",
            Self::FullRecords => "record budget is full",
            Self::FullBytes => "byte budget is full",
            Self::RecordTooLarge => "record exceeds the byte budget",
            Self::Unmeasurable => "record charge cannot be measured",
        };
        f.write_str(reason)
    }
}

impl std::error::Error for RejectReason {}
