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

//! **S8 —— 并发读写 + 并发分裂竞争。**
//!
//! S2/S3 验的是**串行构造出来的**中间态（"我把它摆成这个样子，读是对的"）。
//! 本场景补另一半：**真实并发下自然出现的**中间态。
//!
//! 两幕：
//!
//! - **第一幕 · 并发读 + 单写者分裂**：写者持续追加并分裂，**故意不修目录**
//!   （目录永远指向节点 0 ⇒ 读者每次都从最陈旧的入口出发，必须走完整条右兄弟链，
//!   因而必然经过每一个正在分裂的节点）。4 个读者并发查**已确认插入**的 key。
//!   断言：**0 次 `Unreachable`、0 次查不到**。
//! - **第二幕 · 并发分裂竞争**：两个写者持**同一份 etag** 同时分裂同一个满节点。
//!   断言：恰好一胜一败；**败者重试时能发现"已经被别人分裂了"而收敛**（不是死路）；
//!   并且复现 S3 的孤儿 —— 败者造出的那个右兄弟没人指向。
//!
//! ## 为什么第一幕只有一个写者
//!
//! 单层链只能**尾插** —— 没有内部节点，就没有「这个 key 该插到哪个叶节点」的定位。
//! 两个写者并发追加**不同的 key** 会把链里的 key 顺序弄乱，那是**本 toy 结构退化**
//! 导致的，不是设计的性质。真实 B-link 靠内部节点从根定位叶节点，天然支持并发插入，
//! 这里不假装验了它。⇒ 并发的**写**单独放在第二幕，且只针对「同一个节点的分裂」。
//!
//! ## 对照分工（这个场景的证明力在哪）
//!
//! - S2 用**串行构造 + 反序对照**证明「顺序承重」—— 强因果、覆盖窄；
//! - S8 用**真实并发**证明「正确顺序在任意交织下都不坏」—— 覆盖宽、**没有反序变体**。
//! - ⇒ 第一幕没有对照组是**故意的**：它的价值是**覆盖**（随机交织撞出各种中间态），
//!   因果由 S2 负责。
//!
//! ## 为什么要注入延迟（以及它买到了什么）
//!
//! 第一幕**确实**是变异检查的靶子，但**不注入延迟就几乎打不中**，这点必须写下来：
//!
//! 把分裂两步改反之后，「左节点指向一个还不存在的右兄弟」这个坏状态的存活时间
//! 只有**一次往返**（~1.5ms）。读者要撞上它，得恰好在这 1.5ms 里走到那个节点、
//! 而且查的还是被搬到上半区的那两个 key。**实测：不注入延迟时 6 次跑只红了 1 次。**
//!
//! ⇒ 在两步之间注入 [`SPLIT_GAP_MS`]，把窗口从 ~1.5ms 拉宽到 25ms。
//! 这个延迟**对正确实现是语义中性的** —— 正确顺序下这个窗口里状态是
//! 「右兄弟已存在、左节点还没改」，读者看到的是完整的旧节点，本来就安全；
//! 它**只**让错误顺序下的坏状态活得更久。
//! ⇒ 于是「读者 0 次不可达」从「窗口太短没撞上」变成「窗口够宽也没撞上」，
//! 证据强度完全不同：注入延迟后再做变异检查，连跑 6 次、6 次全红。
//!
//! **顺带得到一条关于并发测试本身的结论**：一次往返宽的竞态窗口，用几百次随机读
//! 去采样是**采样不足**的。要么把窗口撑开，要么别指望这类测试能当回归网。

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::kv::{CasStore, StorageError};
use crate::tree::{BLinkTree, Directory, Node, Search};

/// 第一幕的 key 前缀。
const PREFIX: &str = "toy/s8/concurrent";
/// 第二幕的 key 前缀。
const RACE_PREFIX: &str = "toy/s8/race";

/// 并发读者个数。
const READERS: usize = 4;
/// 写者追加多少个 key（每节点 4 个 key ⇒ 会触发多次分裂）。
const WRITES: usize = 24;
/// 每个读者最多查多少次。
const READS_PER_READER: usize = 50;
/// 至少要发生几次分裂 —— 否则这个并发测试根本没覆盖到中间态。
const MIN_SPLITS: usize = 3;

/// 分裂「落右兄弟」与「CAS 改左节点」两步之间注入的延迟（毫秒）。
///
/// **这个延迟不改变语义，只是把中间态窗口从一次往返撑到可被采样**（理由见模块头）。
/// 设 0 = 关掉注入（跑出来就是那个 1/6 检出率的版本）。
const SPLIT_GAP_MS: u64 = 25;

/// 分裂用的初始满节点（和 S2 同一组 key，便于对照）。
const RACE_KEYS: [u64; 4] = [10, 20, 30, 40];
/// 分裂后会挪到右兄弟的 key（上半区）。
const UPPER_KEY: u64 = 30;
/// 分裂后留在左节点的 key（下半区）。
const LOWER_KEY: u64 = 10;

/// xorshift64 —— 读者挑 key 用。刻意不引依赖：toy 只需要"看起来乱"。
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

/// 写者的累计统计。
#[derive(Default, Clone, Copy)]
struct WriteStat {
    /// 真正插进去的 key 数。
    inserted: usize,
    /// 成功的分裂次数。
    splits: usize,
    /// 撞上的 CAS 冲突次数（重试次数）。
    conflicts: usize,
}

/// 读者的累计统计（跨任务汇总）。
#[derive(Default, Clone, Copy)]
struct ReaderStat {
    /// 实际发出的查询次数。
    reads: usize,
    /// 拿到 `Unreachable` 的次数 —— **必须为 0**。
    unreachable: usize,
    /// 查一个**已确认插入**的 key 却查不到的次数 —— **必须为 0**。
    missing: usize,
}

/// 往链尾追加一个 key，满了就分裂。**带重试** —— 这是 S8 相对 S4 新增的东西：
/// S4 是单写者、不会冲突，它的 `build_chain` 里一次重试都没有。
///
/// 重试的关键是**每轮重新走到最右**，不缓存 tail —— 别人可能已经把它分裂了，
/// 缓存下来的 `(node, etag)` 在下一轮就是过期的。
///
/// `next_id` 是个原子计数器：只有**右兄弟真的落盘成功**才递增，所以节点 id
/// 始终连续（`BLinkTree::wipe` 靠"扫到第一个不存在的 id 就停"来清场，有空洞会漏删）。
async fn append_with_retry(
    tree: &BLinkTree,
    store: &CasStore,
    key: u64,
    next_id: &AtomicU64,
    stat: &mut WriteStat,
) -> Result<bool, StorageError> {
    /// 重试上限。单写者下根本用不到；第二幕那种真竞争下它是"不许无限自旋"的护栏。
    const MAX_ATTEMPTS: usize = 16;

    for _ in 0..MAX_ATTEMPTS {
        let (node, etag) = tree.rightmost(store, 0).await?;

        if node.is_full() {
            // ⚠️ 只在**创建成功后**才消耗 id，否则重试会烧掉 id 留下空洞。
            let id = next_id.load(Ordering::Relaxed);
            let (new_left, new_right) = node.plan_split(id);

            // ---- 步骤 ① 落右兄弟（if_not_exists）----
            match tree.create_node(store, &new_right).await {
                Ok(_) => {
                    next_id.fetch_add(1, Ordering::Relaxed);
                }
                Err(StorageError::Conflict(_)) => {
                    // 同一个 id 被别人抢造了 ⇒ 换一个 id 重来
                    stat.conflicts += 1;
                    continue;
                }
                Err(e) => return Err(e),
            }

            // ⏸ 把中间态窗口撑开（语义中性，见模块头「为什么要注入延迟」）。
            //    放这儿而不是别处，是因为「右兄弟已落盘、左节点还没改」正是
            //    读者最可能被误导的那一段。
            if SPLIT_GAP_MS > 0 {
                tokio::time::sleep(Duration::from_millis(SPLIT_GAP_MS)).await;
            }

            // ---- 步骤 ② CAS 改左节点 ----
            match tree.cas_node(store, &new_left, &etag).await {
                Ok(_) => {
                    // 分裂成功。这一轮不插 key，下一轮重新走一遍再插。
                    stat.splits += 1;
                    continue;
                }
                Err(StorageError::Conflict(_)) => {
                    // 别人先把它分裂了。已经消耗掉的那个右兄弟成了孤儿（呼应 S3）。
                    stat.conflicts += 1;
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        let mut grown = node.clone();
        grown.keys.push(key);
        match tree.cas_node(store, &grown, &etag).await {
            Ok(_) => return Ok(true),
            Err(StorageError::Conflict(_)) => stat.conflicts += 1,
            Err(e) => return Err(e),
        }
    }
    Ok(false)
}

/// **第一幕**：一个写者持续分裂（不修目录），4 个读者并发查已确认插入的 key。
async fn act1(store: &CasStore) -> Result<(ReaderStat, WriteStat), StorageError> {
    let tree = BLinkTree::new(PREFIX);
    tree.wipe(store, 4096).await?;
    tree.create_node(store, &Node::new(0, vec![1])).await?;
    tree.put_dir(store, &Directory::single(0)).await?;
    // ⚠️ 目录到此为止**再也不会更新**。所有读者永远从节点 0 出发，
    //    必须自己沿右兄弟链走到底 —— 也就必然穿过每一个正在分裂的节点。

    let inserted = Arc::new(Mutex::new(Vec::<u64>::new()));
    let done = Arc::new(AtomicBool::new(false));
    let reads = Arc::new(AtomicUsize::new(0));
    let unreachable = Arc::new(AtomicUsize::new(0));
    let missing = Arc::new(AtomicUsize::new(0));

    let mut readers = Vec::with_capacity(READERS);
    for i in 0..READERS {
        let store = store.clone();
        let inserted = Arc::clone(&inserted);
        let done = Arc::clone(&done);
        let reads = Arc::clone(&reads);
        let unreachable = Arc::clone(&unreachable);
        let missing = Arc::clone(&missing);

        readers.push(tokio::spawn(async move {
            let tree = BLinkTree::new(PREFIX);
            let dir = Directory::single(0);
            // 每个读者一个不同的种子
            let mut rng = 0x9E37_79B9_7F4A_7C15u64.wrapping_mul(i as u64 + 1) | 1;
            let mut mine = 0usize;

            loop {
                // 写者收工 + 自己已经查够 ⇒ 退出（保证至少 MIN 次是并发窗口内发生的）
                if mine >= READS_PER_READER || (done.load(Ordering::Relaxed) && mine >= 16) {
                    break;
                }

                // 挑一个**已确认插入**的 key。锁只在这一小段持有，**不跨 await**。
                let key = {
                    let g = inserted.lock().expect("读者互斥锁没被毒化");
                    if g.is_empty() {
                        None
                    } else {
                        Some(g[(xorshift(&mut rng) as usize) % g.len()])
                    }
                };
                let Some(key) = key else {
                    // 写者还没插进第一个 key：让出一次，下一轮再看
                    tokio::task::yield_now().await;
                    continue;
                };

                mine += 1;
                reads.fetch_add(1, Ordering::Relaxed);
                match tree.search(&store, &dir, key).await? {
                    Search::Found { .. } => {}
                    Search::Unreachable { .. } => {
                        unreachable.fetch_add(1, Ordering::Relaxed);
                    }
                    // 这个 key 是**确认插进去过**的，查不到就是丢数据
                    Search::Absent { .. } => {
                        missing.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
            Ok::<(), StorageError>(())
        }));
    }

    // ---- 写者 ----
    let next_id = AtomicU64::new(1);
    let mut stat = WriteStat::default();
    let mut key = 1u64;
    for _ in 0..WRITES {
        key += 10;
        if append_with_retry(&tree, store, key, &next_id, &mut stat).await? {
            stat.inserted += 1;
            inserted.lock().expect("写者互斥锁没被毒化").push(key);
        }
        // 让出一次，好让读者真的插进写者的批次之间（不是"写完了才轮到读"）
        tokio::task::yield_now().await;
    }
    done.store(true, Ordering::Relaxed);

    for h in readers {
        h.await.expect("读者任务没 panic")?;
    }

    Ok((
        ReaderStat {
            reads: reads.load(Ordering::Relaxed),
            unreachable: unreachable.load(Ordering::Relaxed),
            missing: missing.load(Ordering::Relaxed),
        },
        stat,
    ))
}

/// 第二幕里一个写者的结果。
struct SplitRace {
    /// 自己那个右兄弟对象造出来了吗。
    created: bool,
    /// CAS 赢了吗。
    won: bool,
    /// 输的时候，**重试**看到的是「已经被别人分裂过了」吗（收敛，而不是死路）。
    converged: bool,
    /// 输的时候重试读到的左节点。
    ///
    /// 用来做一条**比 `converged` 更强**的断言：输家看到的应该正是**赢家那次分裂的结果**
    /// （`right == Some(赢家的 right_id)`），而不只是"某个变化发生了"。
    /// 这条成立是因为我们那次 CAS 之所以失败，唯一可能就是赢家先落了盘。
    left_after: Option<Node>,
}

/// 分裂一个满节点：① 落右兄弟 → ② CAS 改左节点。**两个写者持同一份 etag 并发跑它。**
///
/// 输家在 CAS 冲突之后**不是直接放弃**，而是重读左节点 —— 这一步就是"重试"：
/// 发现 `high_key` 已经有值（说明已经分裂过了），于是收敛、不再覆盖。
async fn split_race(
    store: &CasStore,
    right_id: u64,
    etag: &str,
) -> Result<SplitRace, StorageError> {
    let tree = BLinkTree::new(RACE_PREFIX);
    let (node, _) = tree
        .get_node(store, 0)
        .await?
        .ok_or_else(|| StorageError::NotFound(format!("{RACE_PREFIX}/node/0")))?;
    let (new_left, new_right) = node.plan_split(right_id);

    // ---- 步骤 ① ----
    let created = match tree.create_node(store, &new_right).await {
        Ok(_) => true,
        Err(StorageError::Conflict(_)) => false,
        Err(e) => return Err(e),
    };

    // ---- 步骤 ② ----
    match tree.cas_node(store, &new_left, etag).await {
        Ok(_) => Ok(SplitRace {
            created,
            won: true,
            converged: false,
            left_after: None,
        }),
        Err(StorageError::Conflict(_)) => {
            let (now, _) = tree
                .get_node(store, 0)
                .await?
                .ok_or_else(|| StorageError::NotFound(format!("{RACE_PREFIX}/node/0")))?;
            // 「已经被别人分裂过」= 左节点的 high_key 有值了（原来是 None，最右节点）
            let converged = now.high_key.is_some();
            Ok(SplitRace {
                created,
                won: false,
                converged,
                left_after: Some(now),
            })
        }
        Err(e) => Err(e),
    }
}

/// 第二幕的观察结果。
struct RaceObservation {
    /// 两个写者各自的结果。
    a: SplitRace,
    b: SplitRace,
    /// 最终左节点（分裂后那个）。
    final_left: Node,
    /// 上半区 key 的查找结果。
    upper: Search,
    /// 下半区 key 的查找结果。
    lower: Search,
    /// 从节点 0 出发能走到的节点 id（右兄弟链），用来判孤儿。
    reachable: Vec<u64>,
}

/// **第二幕**：两个写者持同一份 etag 并发分裂同一个满节点。
async fn act2(store: &CasStore) -> Result<RaceObservation, StorageError> {
    let tree = BLinkTree::new(RACE_PREFIX);
    tree.wipe(store, 64).await?;
    let left = Node::new(0, RACE_KEYS.to_vec());
    let etag = tree.create_node(store, &left).await?;
    tree.put_dir(store, &Directory::single(0)).await?;

    // 两个写者各持一份 store 和**同一份 etag**，用各自不同的 right_id
    let (s_a, s_b) = (store.clone(), store.clone());
    let (e_a, e_b) = (etag.clone(), etag.clone());
    let h_a = tokio::spawn(async move { split_race(&s_a, 1, &e_a).await });
    let h_b = tokio::spawn(async move { split_race(&s_b, 2, &e_b).await });
    let a = h_a.await.expect("写者 A 没 panic")?;
    let b = h_b.await.expect("写者 B 没 panic")?;

    let final_left = tree
        .read_node(store, 0)
        .await?
        .ok_or_else(|| StorageError::NotFound(format!("{RACE_PREFIX}/node/0")))?;

    Ok(RaceObservation {
        a,
        b,
        final_left,
        upper: tree.search_from(store, 0, UPPER_KEY).await?,
        lower: tree.search_from(store, 0, LOWER_KEY).await?,
        reachable: tree.reachable(store, 0).await?,
    })
}

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    // ================= 第一幕 =================
    let (rd, wr) = act1(store).await?;

    let no_unreachable = rd.unreachable == 0;
    let no_missing = rd.missing == 0;
    let readers_ran = rd.reads >= 16;
    let did_split = wr.splits >= MIN_SPLITS;
    let act1_pass = no_unreachable && no_missing && readers_ran && did_split;

    let v1 = if act1_pass { "PASS" } else { "FAIL" };
    println!(
        "[{v1}] s8 第一幕 并发读+分裂: 读者{}个共查{}次 | 不可达={} 已插入却查不到={} \
         | 写者插入={} 分裂={} 冲突={}",
        READERS, rd.reads, rd.unreachable, rd.missing, wr.inserted, wr.splits, wr.conflicts
    );
    if !did_split {
        println!(
            "        ⚠️ 只分裂了 {} 次（要 >= {MIN_SPLITS}）—— 中间态没被覆盖到，这轮不算数。",
            wr.splits
        );
    }
    if !readers_ran {
        println!(
            "        ⚠️ 读者只查了 {} 次 —— 并发窗口太短，没测到东西。",
            rd.reads
        );
    }

    // ================= 第二幕 =================
    let race = act2(store).await?;

    let wins = [race.a.won, race.b.won].iter().filter(|w| **w).count();
    let conflicts = 2 - wins;
    let exactly_one_winner = wins == 1 && conflicts == 1;

    let (winner_right, loser) = if race.a.won {
        (1u64, &race.b)
    } else {
        (2u64, &race.a)
    };
    let loser_right = if race.a.won { 2u64 } else { 1u64 };

    let left_points_to_winner = race.final_left.right == Some(winner_right);
    let loser_converged = loser.converged;
    // 比 converged 更强：输家看到的是**赢家那次分裂的结果**，不只是"有变化"
    let loser_sees_winner = loser
        .left_after
        .as_ref()
        .is_some_and(|n| n.right == Some(winner_right) && n.high_key.is_some());
    // 败者造出的右兄弟**没人指向** —— 就是 S3 那个孤儿
    let loser_is_orphan = loser.created && !race.reachable.contains(&loser_right);
    let searchable = race.upper.is_found() && race.lower.is_found();

    let act2_pass = exactly_one_winner
        && left_points_to_winner
        && loser_converged
        && loser_sees_winner
        && loser_is_orphan
        && searchable;

    let v2 = if act2_pass { "PASS" } else { "FAIL" };
    println!(
        "[{v2}] s8 第二幕 并发分裂: 一胜一败={exactly_one_winner}(胜{wins}/败{conflicts}) \
         | 左节点指向胜者={left_points_to_winner} 败者重试收敛={loser_converged} \
         | 败者看到的是胜者的结果={loser_sees_winner} \
         | 败者的右兄弟是孤儿={loser_is_orphan} 树仍可搜={searchable}",
    );

    // S3 传输层为了吸收瞬态错误重试了几次（进程累计，`all` 下含前面场景的）。这是证据不是噪音。
    let retries = crate::cfg::transient_retries();

    let pass = act1_pass && act2_pass;
    let verdict = if pass { "PASS" } else { "FAIL" };
    println!(
        "[{verdict}] s8 并发读写与并发分裂: \
         第一幕(并发读+分裂)={v1} 第二幕(并发分裂竞争)={v2} | S3瞬态重试={retries}次"
    );

    if pass {
        println!(
            "        ⇒ 结论（三条，都直接约束设计）：\n\
             \x20         ① **「先落右兄弟、再 CAS 改左节点」在真实并发下也成立** —— \n\
             \x20            读者{}次查询里 **0 次不可达、0 次丢 key**，而写者同时分裂了 {} 次。\n\
             \x20            读者每次都被迫从最陈旧的入口（节点 0）出发走完整条右兄弟链，\n\
             \x20            所以它必然穿过每一个正在分裂的节点 —— 这不是「碰巧没撞上」。\n\
             \x20            ⚠️ 本幕**没有反序对照**（那是 S2 的活）：它的价值是**覆盖**不是**因果**。\n\
             \x20         ② **分裂冲突是可恢复的，不是死路** —— 输家拿到 Conflict 之后\n\
             \x20            重读左节点就能发现「已经分裂过了」，于是收敛而不是反复覆盖。\n\
             \x20            这一步在任何并发写路径里都是**必须**的，而 S2/S3/S4 的单写者代码里没有。\n\
             \x20         ⇒ 附带复现了 S3 的孤儿：败者造出的那个右兄弟（节点 {}）**没人指向**，\n\
             \x20            可达链只有 {:?} —— 再一次说明 **GC 是必需项**，且判据只能是可达性遍历。\n\
             \x20         ③ **瞬态传输错误是真实存在的，客户端必须自带重试层** —— 关掉分裂延迟后\n\
             \x20            实测 10 次里有 9 次触发一次 S3 瞬态重试（`Unexpected (temporary):\n\
             \x20            connection closed before message completed`）；而**没挂重试层**时\n\
             \x20            同一个位置 10 次里失败 7 次。本次运行累计触发 {} 次。\n\
             \x20            而 **CAS 冲突不是 transient，不会被这一层吞掉** —— 这正是 S1/S7 和本幕\n\
             \x20            那些**故意制造**的竞争仍能照常冒出来的原因。",
            rd.reads,
            wr.splits,
            loser_right,
            race.reachable,
            retries
        );
    } else {
        if !exactly_one_winner {
            println!(
                "        ⚠️ 并发分裂不是一胜一败（胜{wins}）—— 说明这两个写者**没有真的并发**，\n\
                 \x20          这轮结果不能当证据（同 S0 踩过的坑：顺序调两次不是并发）。"
            );
        }
        if !loser_is_orphan && loser.created {
            println!("        ⚠️ 败者造出的右兄弟竟然可达 —— 分裂竞争的时序跟预期不符。");
        }
    }

    Ok(())
}
