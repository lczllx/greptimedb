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

/// Lifecycle of the process-local queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueState {
    /// New records may be admitted.
    Open,
    /// Admission is stopped while already-published records are drained.
    Draining,
    /// No record may be admitted and the queue is terminal.
    Closed,
}

/// Lifecycle of one queued region request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordState {
    /// The record exists but is not yet visible to the replay worker.
    Admitted,
    /// The record is visible and may be selected for replay.
    Runnable,
    /// A replay invocation is in flight.
    InFlight,
    /// The record reached a terminal settlement.
    Settled,
    /// The record was cancelled before it became visible.
    CancelledBeforePublication,
}

/// An invalid queue lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QueueStateError {
    pub from: QueueState,
    pub to: QueueState,
}

/// An invalid record lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecordStateError {
    pub from: RecordState,
    pub to: RecordState,
}

impl QueueState {
    pub fn transition(self, next: Self) -> Result<Self, QueueStateError> {
        let allowed = matches!(
            (self, next),
            (Self::Open, Self::Draining) | (Self::Draining, Self::Closed)
        );

        if allowed {
            Ok(next)
        } else {
            Err(QueueStateError {
                from: self,
                to: next,
            })
        }
    }
}

impl RecordState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Settled | Self::CancelledBeforePublication)
    }

    pub fn transition(self, next: Self) -> Result<Self, RecordStateError> {
        let allowed = matches!(
            (self, next),
            (Self::Admitted, Self::Runnable)
                | (Self::Admitted, Self::CancelledBeforePublication)
                | (Self::Runnable, Self::InFlight)
                | (Self::InFlight, Self::Runnable)
                | (Self::Runnable, Self::Settled)
                | (Self::InFlight, Self::Settled)
        );

        if allowed {
            Ok(next)
        } else {
            Err(RecordStateError {
                from: self,
                to: next,
            })
        }
    }
}

impl fmt::Display for QueueStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid queue state transition: {:?} -> {:?}",
            self.from, self.to
        )
    }
}

impl std::error::Error for QueueStateError {}

impl fmt::Display for RecordStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid record state transition: {:?} -> {:?}",
            self.from, self.to
        )
    }
}

impl std::error::Error for RecordStateError {}
