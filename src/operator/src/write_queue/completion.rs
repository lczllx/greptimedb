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
use std::time::Instant;

use tokio::sync::Notify;

use crate::write_queue::ledger::ChargeGuard;
use crate::write_queue::lock;
use crate::write_queue::settlement::FinalSettlement;
use crate::write_queue::state::{RecordState, RecordStateError};

/// A failure while settling a record or observing its completion state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionError {
    AlreadySettled,
    InvalidRecordState { from: RecordState, to: RecordState },
}

/// The result of waiting for a record settlement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompletionWait {
    Applied,
    NonApplied(FinalSettlement),
    Timeout,
}

/// The single terminal settlement observed by all waiters for one entry.
pub(crate) struct CompletionEntry {
    state: Mutex<Option<FinalSettlement>>,
    notify: Notify,
}

/// Keeps one caller attached to a record's completion state.
pub struct CompletionHandle {
    entry: Arc<CompletionEntry>,
    /// Keeps the record slot charged while the caller can still wait.
    _record_slot: Arc<ChargeGuard>,
    /// Keeps the completion state charged until the caller detaches.
    _completion_charge: ChargeGuard,
}

impl CompletionEntry {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    pub(crate) fn settle(&self, settlement: FinalSettlement) -> Result<(), CompletionError> {
        let mut state = lock(&self.state);
        if state.is_some() {
            return Err(CompletionError::AlreadySettled);
        }
        *state = Some(settlement);
        drop(state);
        self.notify.notify_waiters();
        Ok(())
    }
}

impl CompletionHandle {
    pub(crate) fn new(
        entry: Arc<CompletionEntry>,
        record_slot: Arc<ChargeGuard>,
        completion_charge: ChargeGuard,
    ) -> Self {
        Self {
            entry,
            _record_slot: record_slot,
            _completion_charge: completion_charge,
        }
    }

    /// Waits for the record's settlement without cancelling the record.
    pub async fn wait(&self, deadline: Instant) -> CompletionWait {
        loop {
            let notified = self.entry.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let settlement = *lock(&self.entry.state);
            if let Some(settlement) = settlement {
                return match settlement {
                    FinalSettlement::Applied => CompletionWait::Applied,
                    other => CompletionWait::NonApplied(other),
                };
            }

            if Instant::now() >= deadline {
                return CompletionWait::Timeout;
            }

            let sleep = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline));
            tokio::select! {
                _ = notified.as_mut() => {}
                _ = sleep => return CompletionWait::Timeout,
            }
        }
    }

    /// Detaches only this observer, leaving the record eligible for replay.
    ///
    /// Dropping the handle releases its charges; the record is not cancelled.
    pub fn detach(self) {}
}

impl fmt::Display for CompletionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadySettled => f.write_str("record was already settled"),
            Self::InvalidRecordState { from, to } => {
                write!(f, "cannot settle record in state {from:?} for {to:?}")
            }
        }
    }
}

impl std::error::Error for CompletionError {}

impl From<RecordStateError> for CompletionError {
    fn from(error: RecordStateError) -> Self {
        Self::InvalidRecordState {
            from: error.from,
            to: error.to,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completion_entry_settles_once() {
        let entry = Arc::new(CompletionEntry::new());

        entry.settle(FinalSettlement::Applied).unwrap();
        assert_eq!(
            entry.settle(FinalSettlement::IssueUnknown),
            Err(CompletionError::AlreadySettled)
        );
    }
}
