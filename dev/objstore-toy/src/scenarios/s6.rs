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

//! **S6 —— 提交点并发：Manifest CAS。**
//!
//! 假设：两个写者拿**同一个 etag** 并发 CAS Manifest ⇒ 恰好一个成功；
//! **失败方必须不 ack**。
//!
//! 怎么证：先让两个写者都把日志段落好（同样的 lsn、同样的内容），
//! 然后让它们**持同一份 etag 同时**去 CAS —— 这样被测的就是**提交点本身**，
//! 不掺杂"谁先写段"的时序。两个 `tokio::spawn`，真并发。
//!
//! 判定：一个 `Committed`、一个 `LostRace` ⇒ PASS。
//!
//! 接 S5：输了竞争的那个事务**不能生效** —— 这不是额外的断言，
//! 而是 S5 那条"水位决定生效"的**并发版**。S5 说"孤儿段不生效"，
//! S6 说"输家的段**就是**孤儿段"。
//!
//! 接 S1：Manifest 的 `committed_lsn` **只增不减** ⇒ 值不重复 ⇒ ABA 不可能。
//! 这正是它敢用 ETag CAS 保护自己的原因（见 S1 的承重不变量）。

use crate::commit::{batch, CommitLog, CommitOutcome};
use crate::kv::{CasStore, StorageError};

const EPOCH: u64 = 1;

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    // ================= 第一幕：真并发 CAS 提交点 =================

    let log = CommitLog::new("toy/s6");
    log.wipe(store, EPOCH).await?;
    log.init(store, EPOCH).await?;

    // 基线：先提交一个，让水位非零，才能看清"只增"
    log.commit(store, EPOCH, 1, &batch(&[("base", "v0")]))
        .await?;

    // 两个写者从**同一份** Manifest 出发（同一个 etag），都想提交"下一个"事务。
    // 段先落好、内容相同 —— 把被测对象**隔离成提交点本身**。
    let (base_manifest, base_etag) = log.read_manifest(store).await?.expect("manifest 已建");
    let lsn = base_manifest.next_lsn();
    let payload = batch(&[("k", "共同内容")]);
    log.write_batch_idempotent(store, EPOCH, lsn, &payload)
        .await?;

    let (log_a, log_b) = (CommitLog::new("toy/s6"), CommitLog::new("toy/s6"));
    let (store_a, store_b) = (store.clone(), store.clone());
    let (m_a, m_b) = (base_manifest.clone(), base_manifest.clone());
    let (e_a, e_b) = (base_etag.clone(), base_etag.clone());

    let task_a =
        tokio::spawn(async move { log_a.cas_commit(&store_a, &m_a, &e_a, EPOCH, lsn).await });
    let task_b =
        tokio::spawn(async move { log_b.cas_commit(&store_b, &m_b, &e_b, EPOCH, lsn).await });

    let r_a = task_a.await.expect("写者 A 的任务没 panic")?;
    let r_b = task_b.await.expect("写者 B 的任务没 panic")?;

    let wins = [&r_a, &r_b]
        .iter()
        .filter(|r| matches!(r, CommitOutcome::Committed { .. }))
        .count();
    let losses = [&r_a, &r_b]
        .iter()
        .filter(|r| matches!(r, CommitOutcome::LostRace))
        .count();
    let exactly_one_winner = wins == 1 && losses == 1;

    // 水位：等于胜者的 lsn，且**严格大于**基线 ⇒ 单调（S1 不变量的落地点）
    let (after, _) = log.read_manifest(store).await?.expect("manifest 已建");
    let watermark_is_winner = after.committed_lsn == lsn;
    let watermark_monotonic = after.committed_lsn > base_manifest.committed_lsn;

    // 接 S5：生效的事务**只有一个**（输家没有 ack，它的东西也没生效）
    //
    // ⚠️ 这里踩过一个坑，值得留着当例子：最初写的是 `applied == vec![lsn]`，
    //    它 FAIL 了 —— 而且**每个分项布尔都是 true**，是断言自己错的，不是系统错的。
    //    因为上面为了「让水位非零」先提交了一个 lsn=1 的基线，
    //    重放 (applied, committed] = (0, 2] 本来就该得到 [1, 2]。
    //    `== vec![lsn]` 相当于**偷偷要求基线不存在** —— 这就是"测量本身要有意义"
    //    那条纪律的又一次实例：断言本身也可能是错的。
    //    正确的断言是「恰好**多出**一个」：基线 + 被测事务，两个，不多不少。
    let recovered = log.recover_by_manifest(store).await?;
    let exactly_one_new_transaction = recovered.applied == vec![base_manifest.committed_lsn, lsn];
    let base_still_there = recovered.state.contains_key("base");

    // ================= 第二幕：S5 那条"幂等写"的前提，违反会怎样 =================
    //
    // S5 的结论是"日志段用幂等覆盖写"。它的前提是
    // **同一个 (epoch, lsn) 对应的内容必须唯一**。
    //
    // ⚠️ 下面这段是**构造出来的交错**（顺序写死的），**不是**我观察到的竞态 ——
    //    真跑出来的竞态是随机的，没法拿来当稳定证据。这样构造是为了把
    //    "前提不成立时会怎样"问清楚。别把它读成"我实测到了这个竞态"。
    let log2 = CommitLog::new("toy/s6/precondition");
    log2.wipe(store, EPOCH).await?;
    log2.init(store, EPOCH).await?;
    let (m2, etag2) = log2.read_manifest(store).await?.expect("manifest 已建");
    let lsn2 = m2.next_lsn();

    // 写者 A 写它的内容（准备提交「A 的内容」）
    log2.write_batch_idempotent(store, EPOCH, lsn2, &batch(&[("ta", "A 的内容")]))
        .await?;
    // ⚠️ 在 A 做 CAS **之前**，另一个写者 B 用**同一个 lsn** 覆盖了段
    log2.write_batch_idempotent(store, EPOCH, lsn2, &batch(&[("tb", "B 的内容")]))
        .await?;
    // 现在 A 的 CAS 成功 —— 它"提交"了，但它以为的内容已经不在盘上了
    let a_acked = log2.cas_commit(store, &m2, &etag2, EPOCH, lsn2).await?;
    let a_acked = matches!(a_acked, CommitOutcome::Committed { .. });
    let after_precond = log2.recover_by_manifest(store).await?;
    // 期望：A 的内容**没**生效，B 的内容生效了 ⇒ 与 A 的 ack 矛盾
    let ack_content_mismatch =
        !after_precond.state.contains_key("ta") && after_precond.state.contains_key("tb");

    let pass = exactly_one_winner
        && watermark_is_winner
        && watermark_monotonic
        && exactly_one_new_transaction
        && base_still_there
        && a_acked
        && ack_content_mismatch;

    let verdict = if pass { "PASS" } else { "FAIL" };
    println!(
        "[{verdict}] s6 提交点并发: 真并发CAS 胜={wins} 败={losses} \
         | 水位={} == 胜者lsn({lsn})={} 单调递增={} | 生效事务={:?} 只多了一个={} 基线仍在={} \
         | 前提违反演示: A已ack={a_acked} 但生效内容是B的={ack_content_mismatch}",
        after.committed_lsn,
        watermark_is_winner,
        watermark_monotonic,
        recovered.applied,
        exactly_one_new_transaction,
        base_still_there,
    );

    if pass {
        println!(
            "        ⇒ 结论一：**提交点的互斥由 Manifest 的 CAS 提供**，真并发下恰好一胜一败。\n\
             \x20         败者拿到的是 `LostRace` —— 它是个**独立变体**而不是 `Err`，\n\
             \x20         因为「输了竞争」是预期内的正常结果（重试即可），不是错误；\n\
             \x20         但**绝不能**被当成 ack。设计上不提供 `is_success()` 这种会把两者\n\
             \x20         混起来的方法，就是为了不给这个出口。\n\
             \x20         接 S5：输家的日志段**就是** S5 里那个孤儿段 —— 水位不动它，它就不生效。\n\
             \x20         接 S1：水位**只增不减** ⇒ 值永不重复 ⇒ ABA 不可能，所以 ETag CAS 在这里安全。"
        );
        println!(
            "        ⇒ 结论二（S5 那条结论的**前提**，必须一起记住）：\n\
             \x20         「日志段用幂等覆盖写」的安全性**依赖「同 (epoch, lsn) 的内容唯一」**。\n\
             \x20         上面的构造性演示说明：一旦两个写者能用**同一个 lsn** 写入**不同内容**，\n\
             \x20         就会出现「A 拿到了 ack、Manifest 也确实提交了，但**实际生效的是 B 的内容**」。\n\
             \x20         投到状态机上是**静默的数据错误**，而且 A 那边看起来一切正常。\n\
             \x20         ⇒ 设计上必须保证 lsn **由唯一的定序者分配**（不能各写者自己算），\n\
             \x20            或者让段的内容由 lsn 唯一决定（内容可复算）——二选一，别两样都不做。"
        );
    }
    if !exactly_one_winner {
        println!(
            "        ⚠️ 没有出现「恰好一胜一败」（胜={wins} 败={losses}）——\n\
             \x20         要么两个都成功（提交点没互斥），要么两个都失败（etag 用错了），\n\
             \x20         无论哪种，S6 的核心假设都被证伪，别往下写结论。"
        );
    }

    Ok(())
}
