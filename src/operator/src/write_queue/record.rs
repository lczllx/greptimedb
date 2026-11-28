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

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use api::v1::region::RegionRequest;
use common_error::ext::RetryHint;
use common_meta::peer::Peer;
use datatypes::schema::ColumnSchema;
use store_api::codec::PrimaryKeyEncoding;
use store_api::storage::{ColumnId, RegionId};
use table::metadata::{TableId, TableIdent};

use crate::write_dispatcher::WriteAttribution;
use crate::write_queue::ledger::ChargeGuard;
use crate::write_queue::state::{RecordState, RecordStateError};

/// Identifies one queued record for the lifetime of the frontend process.
pub type EntryId = u64;

/// The first-round outcome that makes a region request eligible for the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FirstRoundDisposition {
    /// The request was definitely submitted and failed with a retryable error.
    ObservedRetryableFailure,
    /// The request was definitely not submitted.
    FirstRoundNotSubmitted,
    /// The request may have been submitted, but its outcome is unknown.
    FirstRoundUnknown,
}

/// The planning-time flow decision retained by a queued record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlowDecision {
    /// The region request does not belong to a flow.
    NonFlow,
    /// The region request belongs to a flow source.
    FlowSource,
}

/// Immutable planning decisions needed to replay a region request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayPolicy {
    /// The region engine selected at planning time.
    pub engine: String,
    /// Whether the region uses append mode.
    pub append_mode: bool,
    /// Whether the table has instant TTL enabled.
    pub instant_ttl: bool,
    /// The primary-key encoding used by the planned request.
    pub primary_key_encoding: PrimaryKeyEncoding,
    /// Whether the request belongs to a flow source.
    pub flow_decision: FlowDecision,
}

/// The planning-time view of one column used by replay compatibility checks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnCompatibility {
    /// The column id observed at planning time.
    pub column_id: ColumnId,
    /// The planning-time column schema.
    pub schema: ColumnSchema,
    /// The semantic type retained separately from [`ColumnSchema`].
    pub semantic_type: SemanticType,
}

/// Planning-time schema and route identity retained for same-region replay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompatibilitySidecar {
    /// The table identity observed at planning time.
    pub table: TableIdent,
    /// The route version used to build the original region request.
    pub route_version: u64,
    /// The partition-expression version used at planning time.
    pub partition_expr_version: u64,
    /// Planning-time column compatibility keyed by column name.
    pub columns: BTreeMap<String, ColumnCompatibility>,
}

/// A charge reserved by the ledger and possibly shared by several records.
pub type SharedCharge = Arc<ChargeGuard>;

/// The retained-charge estimate supplied when a record is constructed.
#[derive(Debug)]
pub enum RecordCharge {
    /// The queue can account for all allocations owned by this record.
    Measured {
        /// Retained bytes owned exclusively by this record.
        exclusive_bytes: u64,
        /// Guards for allocations shared with other queue records.
        shared: Vec<SharedCharge>,
    },
    /// The payload cannot be measured safely; admission must fail closed.
    Unmeasurable,
}

impl RecordCharge {
    fn exclusive_bytes(&self) -> Option<u64> {
        match self {
            Self::Measured {
                exclusive_bytes, ..
            } => Some(*exclusive_bytes),
            Self::Unmeasurable => None,
        }
    }
}

/// One original region request and the state needed to replay it.
pub struct Record {
    /// The table that owns the region.
    pub table_id: TableId,
    /// The region targeted by the retained request.
    pub region_id: RegionId,
    /// The peer that received the original attempt.
    pub first_peer: Peer,
    /// The ordinal of the original region attempt within the call.
    pub first_round_ordinal: u32,
    /// The slot of the original attempt within its peer batch.
    pub peer_batch_slot: u32,
    /// The immutable first-round outcome.
    pub disposition: FirstRoundDisposition,
    /// The retry hint observed during the first round.
    pub retry_hint: RetryHint,
    /// Row ownership retained for replay acknowledgement.
    pub attribution: WriteAttribution,
    /// The original materialized region request.
    pub request: Arc<RegionRequest>,
    /// Planning-time identity used to validate same-region replay.
    pub sidecar: CompatibilitySidecar,
    /// Planning decisions that replay must not recompute.
    pub planned_policy: ReplayPolicy,
    /// The monotonic time at which the retry window starts. `Record::new` writes
    /// a provisional value; `Admission::publish` resets it at admission.
    pub admitted_at: Instant,
    /// The absolute deadline after which no replay may be authorized.
    pub deadline: Instant,
    charge: Option<RecordCharge>,
    entry_id: Option<EntryId>,
    retry_budget: Duration,
    state: RecordState,
}

impl fmt::Debug for Record {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Record")
            .field("table_id", &self.table_id)
            .field("region_id", &self.region_id)
            .field("first_peer_id", &self.first_peer.id)
            .field("first_round_ordinal", &self.first_round_ordinal)
            .field("peer_batch_slot", &self.peer_batch_slot)
            .field("disposition", &self.disposition)
            .field("retry_hint", &self.retry_hint)
            .field("attribution", &self.attribution)
            .field("sidecar", &self.sidecar)
            .field("planned_policy", &self.planned_policy)
            .field("admitted_at", &self.admitted_at)
            .field("deadline", &self.deadline)
            .field("charge", &self.charge)
            .field("entry_id", &self.entry_id)
            .field("state", &self.state)
            .field("request_retained", &true)
            .finish()
    }
}

/// The inputs that are fixed when a region request becomes a queue candidate.
pub struct RecordInit {
    /// The table that owns the region.
    pub table_id: TableId,
    /// The region targeted by the retained request.
    pub region_id: RegionId,
    /// The peer that received the original attempt.
    pub first_peer: Peer,
    /// The ordinal of the original region attempt within the call.
    pub first_round_ordinal: u32,
    /// The slot of the original attempt within its peer batch.
    pub peer_batch_slot: u32,
    /// The immutable first-round outcome.
    pub disposition: FirstRoundDisposition,
    /// The retry hint observed during the first round.
    pub retry_hint: RetryHint,
    /// Row ownership retained for replay acknowledgement.
    pub attribution: WriteAttribution,
    /// The original materialized region request.
    pub request: Arc<RegionRequest>,
    /// Planning-time identity used to validate same-region replay.
    pub sidecar: CompatibilitySidecar,
    /// Planning decisions that replay must not recompute.
    pub planned_policy: ReplayPolicy,
    /// The retry lifetime measured from publication, not record construction.
    pub retry_budget: Duration,
    /// The retained-charge estimate for this record.
    pub charge: RecordCharge,
}

impl Record {
    /// Creates a record in the admitted, not-yet-published state.
    pub fn new(init: RecordInit) -> Self {
        let RecordInit {
            table_id,
            region_id,
            first_peer,
            first_round_ordinal,
            peer_batch_slot,
            disposition,
            retry_hint,
            attribution,
            request,
            sidecar,
            planned_policy,
            retry_budget,
            charge,
        } = init;
        let admitted_at = Instant::now();
        let deadline = admitted_at.checked_add(retry_budget).unwrap_or(admitted_at);

        Self {
            table_id,
            region_id,
            first_peer,
            first_round_ordinal,
            peer_batch_slot,
            disposition,
            retry_hint,
            attribution,
            request,
            sidecar,
            planned_policy,
            admitted_at,
            deadline,
            charge: Some(charge),
            entry_id: None,
            retry_budget,
            state: RecordState::Admitted,
        }
    }

    /// Starts the retry window at publication, the queue's admission point.
    pub(crate) fn start_admission_window(&mut self, now: Instant) {
        self.admitted_at = now;
        self.deadline = now.checked_add(self.retry_budget).unwrap_or(now);
    }

    /// Returns the current record lifecycle state.
    pub fn state(&self) -> RecordState {
        self.state
    }

    /// Returns the queue entry id assigned at publication.
    pub fn entry_id(&self) -> Option<EntryId> {
        self.entry_id
    }

    /// Returns the exclusive retained bytes, or `None` if unmeasurable.
    pub fn exclusive_bytes(&self) -> Option<u64> {
        self.charge.as_ref().and_then(RecordCharge::exclusive_bytes)
    }

    /// Assigns the queue entry id before the record becomes runnable.
    pub fn assign_entry_id(&mut self, entry_id: EntryId) {
        self.entry_id = Some(entry_id);
    }

    /// Removes and returns the retained charge owned by this record.
    pub fn take_charge(&mut self) -> Option<RecordCharge> {
        self.charge.take()
    }

    /// Applies one lifecycle transition.
    pub fn transition(&mut self, next: RecordState) -> Result<(), RecordStateError> {
        self.state = self.state.transition(next)?;
        Ok(())
    }

    /// Marks an unpublished record as cancelled.
    pub fn cancel_before_publication(&mut self) -> Result<(), RecordStateError> {
        self.transition(RecordState::CancelledBeforePublication)
    }
}
