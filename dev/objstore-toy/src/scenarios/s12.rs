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

//! **S12 —— CAS 失败之后：分类够不够？**
//!
//! 假设：CAS 失败后重读 Manifest、比对 `epoch`，**只能分出「被 fenced」**；
//! 「我赢了但 ack 丢了」与「输给并发」在 Manifest 上**完全同形**，分不开。
//!
//! 为什么这条要紧：设计里 `LostRace` 是**独立变体**（S6 的结论），
//! 而「日志记结果不记意图」要求**幂等重放** —— 两者都要求调用方能说出自己是哪种失败：
//!
//! - 「被 fenced」⇒ 本次任期作废，**不能**重试；
//! - 「输给并发」⇒ 换个 lsn 重试；
//! - 「我其实赢了、只是没收到 ack」⇒ **重试是错的**，应该直接 ack。
//!
//! 后两种若不可分，幂等重放就做不出来：你会把一次**已经成功**的提交当成失败重做，
//! 或者反过来，把别人的结果当成自己的。
//!
//! ## 五幕（第四幕是假设，第五幕是出路）
//!
//! - **幕一 · 被 fenced**：换主把 `epoch` 推了上去 ⇒ 重读认得出来（这类**分得开**）。
//! - **幕二 · 被 applier 挤了一下**：水位**没过**我这一格（别人只动了 `applied_lsn`）
//!   ⇒ 重试即可，也不是坏消息。
//! - **幕三 · 输给并发**：别人抢先提交了**同一个 lsn**。
//! - **幕四 · 我赢了但 ack 丢**：CAS 成功了、返回没收到；调用方拿旧快照重试 ⇒ 被拒。
//!   ⇒ **与幕三在 Manifest 上逐字段相同**。
//! - **幕五**：给 Manifest 加一个「水位的最后一个推手是谁」 ⇒ 幕三/幕四分开。
//!
//! 判定：前三幕各自归位 **且 幕三 == 幕四** ⇒ 假设被证实 ⇒ PASS。
//! （这一格的 PASS 是**坏消息被证实**，与 S1 同一种读法，见 `mod.rs`。）

use serde::{Deserialize, Serialize};

use crate::commit::{batch, CommitLog, CommitOutcome, Manifest};
use crate::kv::{CasStore, StorageError};

const EPOCH: u64 = 1;

/// 我在带归属的 Manifest 里用的名字（幕五）。
const ME: &str = "self";

/// 只有 Manifest 现有的三个字段（`epoch` / `committed_lsn` / `applied_lsn`）时，
/// 重读之后**能做出**的判定。
///
/// 注意它只有三个分支 —— 而"我该重试吗"这个问题有**四个**答案。
/// 缺的那个正是本场景要暴露的东西。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Resolution {
    /// `epoch` 变了 ⇒ 我被 fenced，此次提交连同本任期一并作废。
    Fenced,
    /// `epoch` 没变、水位也没过我这一格 ⇒ 只是被人（applier）挤了一下，**重试即可**。
    Retryable,
    /// `epoch` 没变、水位已越过我这一格 —— 但**说不出是谁推的**。
    /// 「别人抢先」与「我自己赢了但没收到 ack」都落在这里。
    Ambiguous,
}

impl Resolution {
    fn label(self) -> &'static str {
        match self {
            Resolution::Fenced => "被 fenced（本任期作废）",
            Resolution::Retryable => "重试即可",
            Resolution::Ambiguous => "★ 分不清：别人抢先 / 我自己赢了但 ack 丢",
        }
    }
}

/// **纯函数**：给定重读到的 Manifest 和我自己的 `(epoch, lsn)`，做出判定。
///
/// 做成纯函数是为了让它可对照 —— 幕五给 Manifest 加了归属字段之后，
/// 改的只有这个函数与它的输入结构，别的一律不动。
fn classify(m: &Manifest, my_epoch: u64, my_lsn: u64) -> Resolution {
    if m.epoch != my_epoch {
        Resolution::Fenced
    } else if m.committed_lsn >= my_lsn {
        Resolution::Ambiguous
    } else {
        Resolution::Retryable
    }
}

// ============================================================================
// 幕五用的：给 Manifest 加一个归属字段
// ============================================================================

/// 带归属的 Manifest —— 与 [`Manifest`] 只差最后一个字段。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct OwnedManifest {
    epoch: u64,
    committed_lsn: u64,
    applied_lsn: u64,
    /// **水位的最后一个推手**。缺了它，「我赢但 ack 丢」与「别人赢」
    /// 在对象上逐字节都可能相同（水位都是那个 lsn）。
    last_writer: String,
}

impl OwnedManifest {
    fn new(last_writer: &str) -> Self {
        Self {
            epoch: EPOCH,
            committed_lsn: 0,
            applied_lsn: 0,
            last_writer: last_writer.to_string(),
        }
    }

    fn push_to(&self, lsn: u64, last_writer: &str) -> Self {
        Self {
            epoch: self.epoch,
            committed_lsn: lsn,
            applied_lsn: self.applied_lsn,
            last_writer: last_writer.to_string(),
        }
    }
}

/// 加上归属字段之后才做得出的判定 —— [`Resolution::Ambiguous`] 被一分为二。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Attributed {
    Fenced,
    Retryable,
    /// 那一格是**我**写的 ⇒ 我其实已经成功了，ack 丢了而已 ⇒ **应当直接确认，不要重放**。
    Applied,
    /// 那一格是**别人**写的 ⇒ 我没生效 ⇒ 换 lsn 重试。
    LostRace,
}

fn classify_owned(m: &OwnedManifest, my_epoch: u64, my_lsn: u64, me: &str) -> Attributed {
    if m.epoch != my_epoch {
        Attributed::Fenced
    } else if m.committed_lsn >= my_lsn {
        if m.last_writer == me {
            Attributed::Applied
        } else {
            Attributed::LostRace
        }
    } else {
        Attributed::Retryable
    }
}

// ============================================================================

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    // ---------------------------------------------------------------- 幕一
    // 被 fenced：我读了一份快照、写好段，还没提交时集群换主了。
    let log = CommitLog::new("toy/s12/fenced");
    let (m, etag) = fresh(&log, store).await?;
    let lsn = m.next_lsn();
    log.write_batch_idempotent(store, EPOCH, lsn, &batch(&[("mine", "只属于我")]))
        .await?;
    // 新主上任的第一次 Manifest CAS 把 epoch 推上去 —— 这正是设计里
    // 「epoch 切换 = 一次 Manifest CAS」的意思（它不额外收费，因为换主本来就要写）。
    let (cur, cur_etag) = log.read_manifest(store).await?.expect("manifest 在");
    let new_epoch = EPOCH + 1;
    log.cas_commit(store, &cur, &cur_etag, new_epoch, cur.committed_lsn)
        .await?;
    // 我拿**旧 etag、旧 epoch** 去提交 ⇒ 必然被拒
    let stale = log.cas_commit(store, &m, &etag, EPOCH, lsn).await?;
    let stale_rejected = matches!(stale, CommitOutcome::LostRace);
    let (after, _) = log.read_manifest(store).await?.expect("manifest 在");
    let fenced = classify(&after, EPOCH, lsn);

    // ---------------------------------------------------------------- 幕二
    // 被 applier 挤了一下：水位没过我这一格 ⇒ 重试即可。
    let log = CommitLog::new("toy/s12/applier");
    let (_, _) = fresh(&log, store).await?;
    log.commit(store, EPOCH, 1, &batch(&[("base", "v0")]))
        .await?;
    let (m, etag) = log.read_manifest(store).await?.expect("manifest 在");
    let lsn = m.next_lsn(); // 2
    log.write_batch_idempotent(store, EPOCH, lsn, &batch(&[("mine", "只属于我")]))
        .await?;
    // 另一个写者（applier）只把 `applied_lsn` 往前推，`committed_lsn` 不动。
    let (cur, cur_etag) = log.read_manifest(store).await?.expect("manifest 在");
    let pushed = Manifest {
        epoch: cur.epoch,
        committed_lsn: cur.committed_lsn,
        applied_lsn: cur.applied_lsn + 1,
    };
    log.cas_manifest(store, &pushed, &cur_etag).await?;
    let squashed = log.cas_commit(store, &m, &etag, EPOCH, lsn).await?;
    let squashed_rejected = matches!(squashed, CommitOutcome::LostRace);
    let (after, _) = log.read_manifest(store).await?.expect("manifest 在");
    let retryable = classify(&after, EPOCH, lsn);

    // ---------------------------------------------------------------- 幕三
    // 输给并发：别人抢先提交了**同一个 lsn**（同一个 (epoch, lsn) 槽位）。
    let log = CommitLog::new("toy/s12/race");
    let (m, etag) = fresh(&log, store).await?;
    let lsn = m.next_lsn(); // 1
    log.write_batch_idempotent(store, EPOCH, lsn, &batch(&[("mine", "只属于我")]))
        .await?;
    // 对手也想要这一格，并且赢了。
    let other = log.cas_commit(store, &m, &etag, EPOCH, lsn).await?;
    let other_won = matches!(other, CommitOutcome::Committed { .. });
    // 我拿**同一份** (值, etag) 去提交 ⇒ 被拒。
    let mine = log.cas_commit(store, &m, &etag, EPOCH, lsn).await?;
    let rejected = matches!(mine, CommitOutcome::LostRace);
    let (after_race, _) = log.read_manifest(store).await?.expect("manifest 在");
    let raced = classify(&after_race, EPOCH, lsn);

    // ---------------------------------------------------------------- 幕四
    // 我赢了但 ack 丢了：CAS 成功、返回没收到；调用方手里还是**旧账**，
    // 于是"重放"同一笔 —— 这正是幂等重放要处理的场景。
    let log = CommitLog::new("toy/s12/replay");
    let (m, etag) = fresh(&log, store).await?;
    let lsn = m.next_lsn(); // 1
    log.write_batch_idempotent(store, EPOCH, lsn, &batch(&[("mine", "只属于我")]))
        .await?;
    let first = log.cas_commit(store, &m, &etag, EPOCH, lsn).await?;
    let i_won = matches!(first, CommitOutcome::Committed { .. });
    // ↑ 这个返回值就是"ack"。丢掉它 = 调用方**没收到**。
    let _lost_ack = first;
    // 调用方重试：它没拿到新 etag，手里还是那份 (m, etag)。
    let replay = log.cas_commit(store, &m, &etag, EPOCH, lsn).await?;
    let replay_rejected = matches!(replay, CommitOutcome::LostRace);
    let (after_replay, _) = log.read_manifest(store).await?.expect("manifest 在");
    let replayed = classify(&after_replay, EPOCH, lsn);

    // 核心断言：幕四与幕三**在同一条判定分支上**。
    let indistinguishable = raced == replayed;
    // 附带：两份 Manifest 连"水位"都相同（逐字段比一眼）。
    let same_watermark = after_race.committed_lsn == after_replay.committed_lsn
        && after_race.epoch == after_replay.epoch
        && after_race.applied_lsn == after_replay.applied_lsn;

    // ---------------------------------------------------------------- 幕五
    // 出路：给 Manifest 加"谁是最后一个推手" ⇒ 幕三/幕四分开。
    let (loser_seen, winner_seen) = (
        race_with_owner(store, "other").await?,
        race_with_owner(store, ME).await?,
    );
    let owner_discriminates = classify_owned(&loser_seen, EPOCH, 1, ME) == Attributed::LostRace
        && classify_owned(&winner_seen, EPOCH, 1, ME) == Attributed::Applied;

    // ---------------------------------------------------------------- 判定
    let acts_ok =
        stale_rejected && squashed_rejected && other_won && rejected && i_won && replay_rejected;
    let pass = acts_ok
        && fenced == Resolution::Fenced
        && retryable == Resolution::Retryable
        && raced == Resolution::Ambiguous
        && indistinguishable
        && same_watermark
        // 幕五也在判定里：若"加归属"分不开，那本场景给出的出路是假的，
        // 结论就不能照写（照 S6 的惯例，一个场景的 pass 可以合取多幕的断言）。
        && owner_discriminates;

    let verdict = if pass { "PASS" } else { "FAIL" };
    println!(
        "[{verdict}] s12 CAS 失败分类: \
         幕一→{} | 幕二→{} | 幕三→{} | 幕四→{} | 幕三==幕四={indistinguishable} 水位逐字段相同={same_watermark} \
         | 各幕的CAS确实被拒: 幕一={stale_rejected} 幕二={squashed_rejected} 幕三={rejected} 幕四={replay_rejected} \
         | 对手赢={other_won} 我第一发赢={i_won} \
         | 加归属后可分={owner_discriminates}",
        fenced.label(),
        retryable.label(),
        raced.label(),
        replayed.label(),
    );

    if pass {
        println!(
            "        ⇒ 结论：重读 Manifest 能**可靠分辨**的只有前两类 ——\n\
             \x20         「被 fenced」（epoch 变了）与「重试即可」（水位没到我这一格）。\n\
             \x20         而「输给并发」与「我赢了但 ack 丢了」落在**同一个分支**上：\n\
             \x20         两种情况下 epoch 都没变、committed_lsn 都 >= 我的 lsn，逐字段相同。\n\
             \x20         ⇒ `LostRace` 这个二分类**不足以支撑幂等重放** ——\n\
             \x20            调用方无法回答「我该重试，还是我其实已经成功了」。\n\
             \x20            把成功的提交当成失败重放会重复执行；反过来会把别人的结果认成自己的。"
        );
        println!(
            "        ⇒ 出路（幕五实测可行，二选一）：\n\
             \x20         ① Manifest 多记一个「水位最后被谁推的」（本场景用法：`last_writer`）；\n\
             \x20         ② **让 lsn 由唯一定序者分配** —— 那样某一格只可能属于一个人，\n\
             \x20            水位越过我这一格就等于「我赢了」。\n\
             \x20         注意 ② 与 S6 第二幕的结论是**同一条**（同一个 (epoch, lsn) 的内容必须唯一，\n\
             \x20         否则段会被互相覆盖）；设计里的 Q11「lsn 由谁分配」正是问这个。\n\
             \x20         ⇒ 在此之前，「重试一次提交」在对象存储后端上**不是幂等**的。"
        );
    }
    if !indistinguishable {
        println!(
            "        ⚠️ 幕三与幕四落在了**不同**分支（{:?} vs {:?}）——\n\
             \x20         说明现有字段其实分得开，假设被证伪，别按上面的结论往下写。",
            raced, replayed
        );
    }
    if !owner_discriminates {
        println!(
            "        ⚠️ 加归属字段之后两种情况**仍然**没分开 —— 出路①不成立，\n\
             \x20         那就只剩「lsn 由唯一定序者分配」这一条路。"
        );
    }

    cleanup(store).await?;
    Ok(())
}

/// 开场：清场 → 建 Manifest → 读一份快照。
async fn fresh(log: &CommitLog, store: &CasStore) -> Result<(Manifest, String), StorageError> {
    log.wipe(store, EPOCH).await?;
    log.init(store, EPOCH).await?;
    log.read_manifest(store)
        .await?
        .ok_or_else(|| StorageError::NotFound("刚建的 Manifest 不见了".to_string()))
}

/// 用"带归属的 Manifest"跑一次两人争同一格，返回**重读**之后看到的那一份。
///
/// `winner` 先写并成功；另一方的 CAS 必然被拒（etag 已变）。这与幕三/幕四的
/// 客户端时序是同一件事，只是对象上多了一个 `last_writer`。
async fn race_with_owner(store: &CasStore, winner: &str) -> Result<OwnedManifest, StorageError> {
    let key = "toy/s12/owned/manifest";
    let _ = store.delete(key).await;
    let initial = serde_json::to_vec(&OwnedManifest::new("init")).expect("可序列化");
    let e0 = store.write_if_not_exists(key, initial).await?;

    let theirs = OwnedManifest::new("other").push_to(1, "other");
    let mine = OwnedManifest::new(ME).push_to(1, ME);
    let (first, second) = if winner == ME {
        (&mine, &theirs)
    } else {
        (&theirs, &mine)
    };

    store
        .write_if_match(key, serde_json::to_vec(first).expect("可序列化"), &e0)
        .await?;
    // 输的那个：同一个 e0 ⇒ 必被拒（这就是"我也试过、但没成"）
    let _ = store
        .write_if_match(key, serde_json::to_vec(second).expect("可序列化"), &e0)
        .await;

    let bytes = store.read(key).await?;
    Ok(serde_json::from_slice(&bytes).expect("合法 OwnedManifest"))
}

/// 自清（只清本场景的前缀）。
async fn cleanup(store: &CasStore) -> Result<(), StorageError> {
    for prefix in [
        "toy/s12/fenced",
        "toy/s12/applier",
        "toy/s12/race",
        "toy/s12/replay",
    ] {
        let log = CommitLog::new(prefix);
        log.wipe(store, EPOCH).await?;
        // 幕一里换主把 epoch 推到了 2，那下面不会有段，但 manifest 同属这个前缀 —— wipe 已删。
    }
    let _ = store.delete("toy/s12/owned/manifest").await;
    Ok(())
}
