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

//! Process-local write retry queue core.
//!
//! This module provides the record shape, bounded ledger, admission boundary,
//! and completion/settlement foundations. Replay execution, RPC transport,
//! capture, and Frontend integration are added by later PRs.

mod admission;
mod completion;
mod ledger;
mod record;
mod settlement;
mod state;
#[cfg(test)]
mod tests;

use std::sync::{Mutex, MutexGuard, PoisonError};

pub use admission::{Admission, AdmittedRecord};
pub use completion::{CompletionError, CompletionHandle, CompletionWait};
pub use ledger::{ChargeGuard, MemoryLedger, RejectReason};
pub use record::{
    ColumnCompatibility, CompatibilitySidecar, EntryId, FirstRoundDisposition, FlowDecision,
    Record, RecordCharge, RecordInit, ReplayPolicy, SharedCharge,
};
pub use settlement::{FinalSettlement, GiveUpReason, SettlementEvidence, final_settlement};
pub use state::{QueueState, QueueStateError, RecordState, RecordStateError};

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}
