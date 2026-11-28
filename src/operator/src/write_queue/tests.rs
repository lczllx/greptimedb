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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant};

use api::v1::region::RegionRequest;
use common_error::ext::RetryHint;
use common_meta::peer::Peer;
use store_api::codec::PrimaryKeyEncoding;
use store_api::storage::RegionId;

use super::*;

struct CountingWaker(AtomicBool);

impl Wake for CountingWaker {
    fn wake(self: Arc<Self>) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn record(exclusive_bytes: u64) -> Record {
    record_with_charge(RecordCharge::Measured {
        exclusive_bytes,
        shared: Vec::new(),
    })
}

fn record_with_shared(exclusive_bytes: u64, shared: Vec<SharedCharge>) -> Record {
    record_with_charge(RecordCharge::Measured {
        exclusive_bytes,
        shared,
    })
}

fn record_with_charge(charge: RecordCharge) -> Record {
    let region_id = RegionId::new(1024, 1);
    Record::new(RecordInit {
        table_id: region_id.table_id(),
        region_id,
        first_peer: Peer {
            id: 1,
            addr: "127.0.0.1:4001".to_string(),
        },
        first_round_ordinal: 7,
        peer_batch_slot: 3,
        disposition: FirstRoundDisposition::ObservedRetryableFailure,
        retry_hint: RetryHint::Retryable,
        attribution: Default::default(),
        request: Arc::new(RegionRequest::default()),
        sidecar: CompatibilitySidecar {
            table: Default::default(),
            route_version: 1,
            partition_expr_version: 2,
            columns: BTreeMap::new(),
        },
        planned_policy: ReplayPolicy {
            engine: "mito".to_string(),
            append_mode: false,
            instant_ttl: false,
            primary_key_encoding: PrimaryKeyEncoding::Dense,
            flow_decision: FlowDecision::NonFlow,
        },
        retry_budget: Duration::from_secs(60),
        charge,
    })
}

fn record_total_bytes(exclusive_bytes: u64) -> u64 {
    let ledger = MemoryLedger::new(u64::MAX, 1);
    let admission = Admission::new(ledger.clone());
    let admitted = admission.publish(record(exclusive_bytes)).unwrap();
    let bytes = ledger.retained_bytes();
    drop(admitted);
    bytes
}

#[test]
fn test_record_keeps_first_round_shape() {
    let record = record(10);

    assert_eq!(record.region_id.table_id(), record.table_id);
    assert_eq!(record.first_peer.id, 1);
    assert_eq!(record.first_round_ordinal, 7);
    assert_eq!(record.peer_batch_slot, 3);
    assert_eq!(
        record.disposition,
        FirstRoundDisposition::ObservedRetryableFailure
    );
    assert_eq!(record.sidecar.route_version, 1);
    assert_eq!(
        record.planned_policy.primary_key_encoding,
        PrimaryKeyEncoding::Dense
    );
    assert!(!record.attribution.is_tracked());
    assert_eq!(record.state(), RecordState::Admitted);
    assert!(record.entry_id().is_none());
}

#[test]
fn test_queue_state_transitions() {
    assert_eq!(
        QueueState::Open.transition(QueueState::Draining),
        Ok(QueueState::Draining)
    );
    assert_eq!(
        QueueState::Draining.transition(QueueState::Closed),
        Ok(QueueState::Closed)
    );
    assert!(QueueState::Open.transition(QueueState::Closed).is_err());
    assert!(QueueState::Draining.transition(QueueState::Open).is_err());
    assert!(QueueState::Closed.transition(QueueState::Draining).is_err());
}

#[test]
fn test_record_state_transitions_and_cancellation() {
    let mut runnable = record(10);
    assert!(runnable.transition(RecordState::InFlight).is_err());
    runnable.transition(RecordState::Runnable).unwrap();
    assert!(runnable.cancel_before_publication().is_err());
    runnable.transition(RecordState::InFlight).unwrap();
    runnable.transition(RecordState::Runnable).unwrap();
    runnable.transition(RecordState::InFlight).unwrap();
    runnable.transition(RecordState::Settled).unwrap();
    assert!(runnable.state().is_terminal());
    assert!(runnable.transition(RecordState::Runnable).is_err());

    let mut cancelled = record(10);
    cancelled.cancel_before_publication().unwrap();
    assert_eq!(cancelled.state(), RecordState::CancelledBeforePublication);
    assert!(cancelled.transition(RecordState::Runnable).is_err());
}

#[test]
fn test_publication_starts_retry_window() {
    let ledger = MemoryLedger::new(10_000, 10);
    let admission = Admission::new(ledger);
    let mut stale_record = record(10);
    let stale = Instant::now() - Duration::from_secs(3_600);
    stale_record.admitted_at = stale;
    stale_record.deadline = stale;

    let mut first = admission.publish(stale_record).unwrap();
    let first_id = {
        let published = first.record();
        assert!(published.admitted_at > stale);
        assert!(published.deadline > stale);
        assert_eq!(
            published.deadline.duration_since(published.admitted_at),
            Duration::from_secs(60)
        );
        assert_eq!(published.state(), RecordState::Runnable);
        published
            .entry_id()
            .expect("published record has an entry id")
    };

    let second = admission.publish(record(10)).unwrap();
    assert_eq!(second.record().state(), RecordState::Runnable);
    assert!(
        second
            .record()
            .entry_id()
            .expect("published record has an entry id")
            > first_id
    );

    assert!(first.take_completion().is_some());
    assert!(first.take_completion().is_none());
}

#[test]
fn test_admission_rejects_closed_and_draining() {
    let ledger = MemoryLedger::new(10_000, 10);
    let admission = Admission::new(ledger.clone());
    admission.transition(QueueState::Draining).unwrap();

    match admission.publish(record(10)) {
        Err(reason) => assert_eq!(reason, RejectReason::Unavailable),
        Ok(_) => panic!("expected Draining to reject admission"),
    }
    assert_eq!(ledger.retained_bytes(), 0);

    admission.transition(QueueState::Closed).unwrap();
    match admission.publish(record(10)) {
        Err(reason) => assert_eq!(reason, RejectReason::Closed),
        Ok(_) => panic!("expected Closed to reject admission"),
    }
    assert_eq!(ledger.retained_bytes(), 0);
}

#[test]
fn test_cancel_before_publication_is_charge_neutral() {
    let ledger = MemoryLedger::new(10_000, 10);
    let admission = Admission::new(ledger.clone());
    let mut cancelled = record(10);
    cancelled.cancel_before_publication().unwrap();

    match admission.publish(cancelled) {
        Err(reason) => assert_eq!(reason, RejectReason::Unavailable),
        Ok(_) => panic!("expected a cancelled record to be rejected"),
    }
    assert_eq!(ledger.retained_bytes(), 0);
    assert_eq!(ledger.retained_records(), 0);
}

#[test]
fn test_admission_rejects_unmeasurable_and_oversized_records() {
    let ledger = MemoryLedger::new(10_000, 10);
    let admission = Admission::new(ledger.clone());

    match admission.publish(record_with_charge(RecordCharge::Unmeasurable)) {
        Err(reason) => assert_eq!(reason, RejectReason::Unmeasurable),
        Ok(_) => panic!("expected an unmeasurable record to be rejected"),
    }
    assert_eq!(ledger.retained_bytes(), 0);

    let ledger = MemoryLedger::new(1, 10);
    let admission = Admission::new(ledger.clone());
    match admission.publish(record(0)) {
        Err(reason) => assert_eq!(reason, RejectReason::RecordTooLarge),
        Ok(_) => panic!("expected a record larger than the ledger to be rejected"),
    }
    assert_eq!(ledger.retained_bytes(), 0);
    assert_eq!(ledger.retained_records(), 0);
}

#[test]
fn test_byte_budget_is_exact_and_rejection_restores_reservation() {
    let total = record_total_bytes(100);
    let ledger = MemoryLedger::new(total, 10);
    let admission = Admission::new(ledger.clone());

    let first = admission.publish(record(100)).unwrap();
    assert_eq!(ledger.retained_bytes(), total);
    assert_eq!(ledger.retained_records(), 1);

    match admission.publish(record(100)) {
        Err(reason) => assert_eq!(reason, RejectReason::FullBytes),
        Ok(_) => panic!("expected the byte budget to reject the second record"),
    }
    assert_eq!(ledger.retained_bytes(), total);
    assert_eq!(ledger.retained_records(), 1);

    drop(first);
    assert_eq!(ledger.retained_bytes(), 0);
    assert_eq!(ledger.retained_records(), 0);

    let second = admission.publish(record(100)).unwrap();
    drop(second);
}

#[test]
fn test_record_budget_is_exact_and_rejection_restores_reservation() {
    let total = record_total_bytes(100);
    let ledger = MemoryLedger::new(total * 10, 1);
    let admission = Admission::new(ledger.clone());

    let first = admission.publish(record(100)).unwrap();
    assert_eq!(ledger.retained_records(), 1);

    match admission.publish(record(100)) {
        Err(reason) => assert_eq!(reason, RejectReason::FullRecords),
        Ok(_) => panic!("expected the record budget to reject the second record"),
    }
    assert_eq!(ledger.retained_bytes(), total);
    assert_eq!(ledger.retained_records(), 1);

    drop(first);
    assert_eq!(ledger.retained_bytes(), 0);
    assert_eq!(ledger.retained_records(), 0);

    let second = admission.publish(record(100)).unwrap();
    drop(second);
}

#[test]
fn test_charge_guard_releases_exactly_once() {
    let ledger = MemoryLedger::new(10_000, 10);
    let guard = ledger.reserve(100).unwrap();
    assert_eq!(guard.bytes(), 100);
    assert_eq!(ledger.retained_bytes(), 100);
    guard.release();
    assert_eq!(ledger.retained_bytes(), 0);

    let completion = ledger.reserve_completion().unwrap();
    assert!(completion.bytes() > 0);
    assert!(ledger.retained_bytes() > 0);
    drop(completion);
    assert_eq!(ledger.retained_bytes(), 0);
}

#[test]
fn test_shared_charge_is_counted_once_and_released_after_last_record() {
    let total = record_total_bytes(10);
    let ledger = MemoryLedger::new(u64::MAX, 10);
    let admission = Admission::new(ledger.clone());

    let shared = Arc::new(ledger.reserve(50).unwrap());
    let first = admission
        .publish(record_with_shared(10, vec![Arc::clone(&shared)]))
        .unwrap();
    let second = admission
        .publish(record_with_shared(10, vec![Arc::clone(&shared)]))
        .unwrap();
    assert_eq!(ledger.retained_bytes(), 50 + total * 2);

    drop(shared);
    assert_eq!(ledger.retained_bytes(), 50 + total * 2);

    first.settle(FinalSettlement::Applied).unwrap();
    assert_eq!(ledger.retained_bytes(), 50 + total);

    second.settle(FinalSettlement::Applied).unwrap();
    assert_eq!(ledger.retained_bytes(), 0);
    assert_eq!(ledger.retained_records(), 0);
}

#[test]
fn test_completion_only_record_remains_counted_until_detach() {
    let total = record_total_bytes(10);
    let ledger = MemoryLedger::new(u64::MAX, 1);
    let admission = Admission::new(ledger.clone());

    let mut first = admission.publish(record(10)).unwrap();
    let completion = first.take_completion().unwrap();
    first.settle(FinalSettlement::Applied).unwrap();
    assert_eq!(ledger.retained_records(), 1);
    assert!(ledger.retained_bytes() > 0);
    assert!(ledger.retained_bytes() < total);

    match admission.publish(record(10)) {
        Err(reason) => assert_eq!(reason, RejectReason::FullRecords),
        Ok(_) => panic!("completion-only record must still consume the record budget"),
    }

    completion.detach();
    assert_eq!(ledger.retained_records(), 0);
    assert_eq!(ledger.retained_bytes(), 0);

    let second = admission.publish(record(10)).unwrap();
    drop(second);
}

#[test]
fn test_detach_releases_completion_charge_only() {
    let total = record_total_bytes(10);
    let ledger = MemoryLedger::new(u64::MAX, 2);
    let admission = Admission::new(ledger.clone());

    let mut admitted = admission.publish(record(10)).unwrap();
    let completion = admitted.take_completion().unwrap();
    completion.detach();

    assert!(ledger.retained_bytes() > 0);
    assert!(ledger.retained_bytes() < total);
    assert_eq!(ledger.retained_records(), 1);

    admitted.settle(FinalSettlement::Applied).unwrap();
    assert_eq!(ledger.retained_bytes(), 0);
    assert_eq!(ledger.retained_records(), 0);
}

#[tokio::test]
async fn test_settle_before_wait_is_observed() {
    let ledger = MemoryLedger::new(u64::MAX, 10);
    let admission = Admission::new(ledger);
    let mut admitted = admission.publish(record(10)).unwrap();
    let completion = admitted.take_completion().unwrap();

    admitted.settle(FinalSettlement::Applied).unwrap();
    assert_eq!(
        completion
            .wait(Instant::now() + Duration::from_secs(60))
            .await,
        CompletionWait::Applied
    );
}

#[tokio::test]
async fn test_wait_observes_non_applied_settlement() {
    let ledger = MemoryLedger::new(u64::MAX, 10);
    let admission = Admission::new(ledger);
    let mut admitted = admission.publish(record(10)).unwrap();
    let completion = admitted.take_completion().unwrap();

    admitted.settle(FinalSettlement::IssueUnknown).unwrap();
    assert_eq!(
        completion
            .wait(Instant::now() + Duration::from_secs(60))
            .await,
        CompletionWait::NonApplied(FinalSettlement::IssueUnknown)
    );
}

#[tokio::test]
async fn test_concurrent_waiters_all_observe_settlement() {
    let ledger = MemoryLedger::new(u64::MAX, 10);
    let admission = Admission::new(ledger);
    let mut admitted = admission.publish(record(10)).unwrap();
    let completion = admitted.take_completion().unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);

    let settle = tokio::spawn(async move { admitted.settle(FinalSettlement::IssueUnknown) });
    let (first, second) = tokio::join!(completion.wait(deadline), completion.wait(deadline),);
    settle.await.unwrap().unwrap();

    let expected = CompletionWait::NonApplied(FinalSettlement::IssueUnknown);
    assert_eq!(first, expected);
    assert_eq!(second, expected);
}

#[tokio::test]
async fn test_wait_timeout_does_not_cancel_record() {
    let ledger = MemoryLedger::new(u64::MAX, 10);
    let admission = Admission::new(ledger.clone());
    let mut admitted = admission.publish(record(10)).unwrap();
    let completion = admitted.take_completion().unwrap();

    assert_eq!(
        completion.wait(Instant::now()).await,
        CompletionWait::Timeout
    );
    assert_eq!(ledger.retained_records(), 1);

    admitted.settle(FinalSettlement::Applied).unwrap();
    assert_eq!(ledger.retained_records(), 1);
    drop(completion);
    assert_eq!(ledger.retained_bytes(), 0);
    assert_eq!(ledger.retained_records(), 0);
}

#[tokio::test]
async fn test_wait_wakes_from_pending_after_settlement() {
    let ledger = MemoryLedger::new(u64::MAX, 10);
    let admission = Admission::new(ledger);
    let mut admitted = admission.publish(record(10)).unwrap();
    let completion = admitted.take_completion().unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);

    let mut wait = Box::pin(completion.wait(deadline));
    let wake_flag = Arc::new(CountingWaker(AtomicBool::new(false)));
    let waker = Waker::from(Arc::clone(&wake_flag));
    let mut context = Context::from_waker(&waker);

    assert!(wait.as_mut().poll(&mut context).is_pending());
    assert!(!wake_flag.0.load(Ordering::SeqCst));

    admitted.settle(FinalSettlement::Applied).unwrap();
    assert!(wake_flag.0.load(Ordering::SeqCst));
    assert_eq!(
        wait.as_mut().poll(&mut context),
        Poll::Ready(CompletionWait::Applied)
    );
}

#[test]
fn test_concurrent_admission_never_overcommits() {
    let total = record_total_bytes(100);
    let capacity = total * 2;
    let ledger = MemoryLedger::new(capacity, 8);
    let admission = Admission::new(ledger.clone());
    let barrier = Arc::new(Barrier::new(8));

    let handles = (0..8)
        .map(|_| {
            let admission = admission.clone();
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                admission.publish(record(100))
            })
        })
        .collect::<Vec<_>>();

    let mut admitted = Vec::new();
    for handle in handles {
        match handle.join().unwrap() {
            Ok(record) => admitted.push(record),
            Err(reason) => assert_eq!(reason, RejectReason::FullBytes),
        }
    }

    assert_eq!(admitted.len(), 2);
    assert_eq!(ledger.retained_bytes(), capacity);
    assert_eq!(ledger.retained_records(), 2);

    drop(admitted);
    assert_eq!(ledger.retained_bytes(), 0);
    assert_eq!(ledger.retained_records(), 0);
}

#[test]
fn test_settlement_priority_table() {
    let cases = [
        (false, false, false, false, FinalSettlement::ReplayNotSent),
        (true, false, false, false, FinalSettlement::Applied),
        (false, true, false, false, FinalSettlement::IssueUnknown),
        (true, true, false, false, FinalSettlement::Applied),
        (false, false, true, false, FinalSettlement::StaleTarget),
        (true, false, true, false, FinalSettlement::Applied),
        (false, true, true, false, FinalSettlement::IssueUnknown),
        (true, true, true, false, FinalSettlement::Applied),
        (false, false, false, true, FinalSettlement::ObservedFailure),
        (true, false, false, true, FinalSettlement::Applied),
        (false, true, false, true, FinalSettlement::IssueUnknown),
        (true, true, false, true, FinalSettlement::Applied),
        (false, false, true, true, FinalSettlement::StaleTarget),
        (true, false, true, true, FinalSettlement::Applied),
        (false, true, true, true, FinalSettlement::IssueUnknown),
        (true, true, true, true, FinalSettlement::Applied),
    ];

    for (applied, issue_unknown, stale_target, observed_failure, expected) in cases {
        let evidence = SettlementEvidence {
            applied,
            issue_unknown,
            stale_target,
            observed_failure,
        };
        assert_eq!(final_settlement(evidence), expected, "{evidence:?}");
    }
}
