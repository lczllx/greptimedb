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

//! **S5 —— 孤儿日志段：提交点不是「写成功」。**
//!
//! 假设：日志段 PUT 上去了、但 Manifest 的水位没过它 ⇒ 对读者**不存在**。
//! 恢复必须由 Manifest 驱动，**不能靠列举 bucket**。
//!
//! 怎么证：造一个孤儿段（写段、不推水位），然后**用两种恢复姿势各跑一遍**。
//! 关键在于这不是"我声称列举法会错"，而是**同一份落盘状态、两种读法、两个结果**。
//!
//! 判定：`按 Manifest 恢复 ⇒ 孤儿不生效` **且** `按列举恢复 ⇒ 孤儿生效` ⇒ PASS。
//! 后一条不是"顺带一提"，它是这个实验的另一半 —— 没有它，前一条只是"我的代码没读它"。
//!
//! ## 写这个场景时发现的第二件事（比原假设更值钱）
//!
//! 日志段的写**不能用 `if_not_exists`**。段不是提交点，Manifest 才是；
//! 把段用 `if_not_exists` 锁起来，等于**把一次崩溃变成永久卡死** ——
//! 崩在「段写了、水位没推」之间的写者，会让**同一个 lsn 的每一次重试都恒 Conflict**。
//! 这一条是实测出来的，不是推的，见下面第二幕。

use crate::commit::{batch, CommitLog};
use crate::kv::{CasStore, StorageError};

const EPOCH: u64 = 1;

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    let log = CommitLog::new("toy/s5");
    log.wipe(store, EPOCH).await?;
    log.init(store, EPOCH).await?;

    // ================= 第一幕：孤儿段存在但不生效 =================

    // 一个正常提交，把水位推到 1
    let c1 = log.commit(store, EPOCH, 1, &batch(&[("k1", "v1")])).await?;
    let first_committed = matches!(c1, crate::commit::CommitOutcome::Committed { .. });

    // 孤儿：**只做步骤①**（写段），然后"客户端放弃了"—— 水位永不推过它
    log.write_batch_idempotent(store, EPOCH, 2, &batch(&[("k2", "ORPHAN-未提交")]))
        .await?;

    let (m, _) = log.read_manifest(store).await?.expect("manifest 已建");
    let in_bucket = store.list(&log.batch_prefix(EPOCH)).await?;
    let by_manifest = log.recover_by_manifest(store).await?;
    let by_listing = log.recover_by_listing(store, EPOCH).await?;

    let orphan_invisible_by_manifest = !by_manifest.state.contains_key("k2");
    let committed_still_applied = by_manifest.state.contains_key("k1");
    let listing_would_apply_orphan = by_listing.state.contains_key("k2");
    let bucket_has_more_than_live = in_bucket.len() > by_manifest.applied.len();

    // ================= 第二幕：孤儿段会不会把同一个 lsn 卡死 =================

    // (a) 正确姿势：幂等覆盖写 ⇒ 孤儿被**接管**（同一个 lsn 重写一次即可）
    let adopt = log
        .commit(store, EPOCH, 2, &batch(&[("k2", "v2-已接管")]))
        .await?;
    let adopted = matches!(adopt, crate::commit::CommitOutcome::Committed { .. });
    let (m_after_adopt, _) = log.read_manifest(store).await?.expect("manifest 已建");
    let after_adopt = log.recover_by_manifest(store).await?;
    let adopted_value_applied = after_adopt
        .state
        .get("k2")
        .is_some_and(|v| v == "v2-已接管");

    // (b) 错误姿势：`if_not_exists` —— 先模拟"写者写了段然后崩了"，
    //     再模拟"重试同一个 lsn"。重试会**恒 Conflict ⇒ 永久卡死**。
    log.write_batch_if_absent(store, EPOCH, 3, &batch(&[("k3", "v3")]))
        .await?; // ← 崩溃的写者留下的段
    let retry_with_if_absent = log
        .write_batch_if_absent(store, EPOCH, 3, &batch(&[("k3", "v3")]))
        .await;
    let if_absent_blocks_retry = matches!(retry_with_if_absent, Err(StorageError::Conflict(_)));

    // 同样的情况下，幂等写就能继续（同一份段内容，重写一次）
    let retry_via_commit = log.commit(store, EPOCH, 3, &batch(&[("k3", "v3")])).await?;
    let idempotent_retry_ok = matches!(
        retry_via_commit,
        crate::commit::CommitOutcome::Committed { .. }
    );

    // ================= 收尾：完整恢复一次 =================
    let (m_final, _) = log.read_manifest(store).await?.expect("manifest 已建");
    let final_recover = log.recover_by_manifest(store).await?;
    let all_three_applied = ["k1", "k2", "k3"]
        .iter()
        .all(|k| final_recover.state.contains_key(*k));
    let no_missing_segments = final_recover.missing.is_empty();

    let pass = first_committed
        && orphan_invisible_by_manifest
        && committed_still_applied
        && listing_would_apply_orphan
        && bucket_has_more_than_live
        && adopted
        && adopted_value_applied
        && if_absent_blocks_retry
        && idempotent_retry_ok
        && all_three_applied
        && no_missing_segments;

    let verdict = if pass { "PASS" } else { "FAIL" };
    println!(
        "[{verdict}] s5 孤儿日志段: \
         桶里{}个段/生效{}个 | 按Manifest恢复含k2(孤儿)={} 按列举恢复含k2={} | \
         幂等写接管同一lsn={} if_not_exists重试被拒={} | 最终水位committed={} 恢复={:?} 缺失段={:?}",
        in_bucket.len(),
        by_manifest.applied.len(),
        !orphan_invisible_by_manifest,
        listing_would_apply_orphan,
        adopted,
        if_absent_blocks_retry,
        m_final.committed_lsn,
        final_recover.state,
        final_recover.missing,
    );

    if pass {
        println!(
            "        ⇒ 结论一：**「bucket 里有哪些对象」≠「哪些事务生效了」**。\n\
             \x20         同一份落盘状态，两种读法两个结果：\n\
             \x20           按 Manifest 恢复（只重放 (applied, committed]）⇒ 孤儿段**不进状态**；\n\
             \x20           按列举恢复（把看得见的段全重放）⇒ 孤儿段**进了状态** ← 这就是错的那个。\n\
             \x20         ⇒ 提交点**只能是 Manifest 的 CAS**，不是「日志段写成功」。\n\
             \x20         顺带：`恢复`里那个 `missing` 字段值得留着 —— 水位说有、段却不见了，\n\
             \x20         那才是真丢数据，和「多出个孤儿」是完全相反的两种故障，别混在一起看。"
        );
        println!(
            "        ⇒ 结论二（写场景时才发现）：**日志段的写必须幂等，不能用 `if_not_exists`**。\n\
             \x20         段不是提交点，Manifest 才是 —— 给段加 `if_not_exists` 等于把一个**非权威对象**\n\
             \x20         当锁用，代价是：崩在「段写了、水位没推」之间的写者，会让**同一个 lsn 的\n\
             \x20         每一次重试都恒 Conflict** ⇒ 崩溃从「可恢复」变成「永久卡死」。\n\
             \x20         改成幂等覆盖后，孤儿被**接管**（重写一次即可继续），实测已通。\n\
             \x20         ⚠️ 但这有前提：**同一个 (epoch, lsn) 的内容必须唯一** ——\n\
             \x20            否则并发写者会互相覆盖内容。前提违反的后果见 S6 的构造性演示。"
        );
    }
    if !listing_would_apply_orphan {
        println!(
            "        ⚠️ 按列举恢复竟然**没有**应用孤儿段 —— 那说明这一半的对照没跑起来，\n\
             \x20         别拿它当「列举法也安全」的证据（大概率是列举没列到）。"
        );
    }

    let _ = m;
    let _ = m_after_adopt;
    Ok(())
}
