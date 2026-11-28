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

/// The terminal outcome of a queued region request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FinalSettlement {
    Applied,
    IssueUnknown,
    ObservedFailure,
    StaleTarget,
    ReplayNotSent,
}

/// Why the queue stopped replaying a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GiveUpReason {
    Deadline,
    AttemptBudgetExhausted,
    TerminalError,
    StaleTarget,
    Shutdown,
    CoordinatorFailure,
}

/// Typed facts used to derive a settlement.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SettlementEvidence {
    pub applied: bool,
    pub issue_unknown: bool,
    pub stale_target: bool,
    pub observed_failure: bool,
}

/// Applies the settlement precedence required by the RFC.
pub fn final_settlement(evidence: SettlementEvidence) -> FinalSettlement {
    if evidence.applied {
        FinalSettlement::Applied
    } else if evidence.issue_unknown {
        FinalSettlement::IssueUnknown
    } else if evidence.stale_target {
        FinalSettlement::StaleTarget
    } else if evidence.observed_failure {
        FinalSettlement::ObservedFailure
    } else {
        FinalSettlement::ReplayNotSent
    }
}
