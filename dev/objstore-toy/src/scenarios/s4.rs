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

//! **S4 —— "父指针可以延后修"的代价，量化。**
//!
//! 假设：B-link 树的父指针（这里是目录对象）**可以延后修，但不能永远不修** ——
//! 不修不会丢数据，但每次查找的成本会退化成 O(链长)。
//!
//! 怎么证：造两棵**完全一样**的树（同样的 key、同样的分裂次数、同样的顺序），
//! 唯一的差别是**每次分裂后要不要更新目录**；然后各查一次最大的 key，数走了几个节点。
//! 两棵树的查找结果必须**一样正确**（都找得到）—— 差别只能体现在**跳数**上。
//!
//! 判定：跳数随分裂次数**线性增长**（不修）vs **恒定**（修）⇒ PASS。
//! 要给出具体数字，不能只说"会更慢"。
//!
//! ## 顺带量出来的第二笔账
//!
//! 修目录**不是免费的**：目录是**一个对象**，每次分裂都要重写它 ⇒
//! ① 每次分裂多一次写；② 所有分裂者争同一个对象（热点的来源）。
//! 所以"延后修"不是偷懒，是**用查找的跳数换写入的争用** —— 一个真实的权衡，
//! 而不是一个可以忽略的优化。
//!
//! ⚠️ **诚实边界**：这里量的是**单层链**（没有内部节点）。
//! 真实 B-link 树里陈旧父指针会把查找带到**目标附近**而不是链头，
//! 所以常数会比这里小；但**形状**（随分裂线性增长 vs 恒定）是同一回事。
//! 别把这里的具体数字当成"生产环境就是走 N 跳"。

use crate::kv::{CasStore, StorageError};
use crate::tree::{BLinkTree, Directory, Node};

/// 分裂次数。取 4 的幂次递进，看趋势而不是看单点。
const SPLIT_COUNTS: [usize; 3] = [4, 16, 64];

/// 一次实验的观测值。
struct ChainStat {
    /// 树里一共几个节点（= 分裂次数 + 1）。
    nodes: u64,
    /// 查"最大 key"走了几个节点。
    hops: usize,
    /// 查找结果是否仍然正确（两棵树都必须是 true）。
    found: bool,
}

/// 造一条链：不断往最右边追加 key，满了就分裂。
///
/// `fix_dir` 决定**每次分裂后要不要重写目录** —— 这是本场景唯一的自变量。
/// 注意构造路径**始终**用内存里跟踪的 `tail` 走到最右，
/// 不依赖目录（否则"不修目录"会把构造成本也拖成 O(N²)，量到的就不是同一个东西了）。
async fn build_chain(
    store: &CasStore,
    prefix: &str,
    splits: usize,
    fix_dir: bool,
) -> Result<ChainStat, StorageError> {
    let tree = BLinkTree::new(prefix);
    tree.wipe(store, 4096).await?;

    let mut dir = Directory::single(0);
    tree.create_node(store, &Node::new(0, vec![1])).await?;
    tree.put_dir(store, &dir).await?;

    let mut next_id = 1u64;
    let mut tail = 0u64;
    let mut key = 1u64;
    let mut done = 0usize;
    // ⚠️ 只能查**真正插进去过的** key：循环退出那一轮的 `key` 如果是撞上分裂，
    //    它**根本没被插入**（分裂那支只搬 key、不追加）—— 拿它去查会得到 `Absent`，
    //    而 `hops()` 照样返回一个数字 —— 所以这里只记真正插入过的 key。
    let mut last_inserted = 1u64;

    while done < splits {
        key += 10;
        let (node, etag) = tree.rightmost(store, tail).await?;
        if node.is_full() {
            let (new_left, new_right) = node.plan_split(next_id);
            // 顺序沿用 S2 验过的：先落右兄弟，再 CAS 改左节点
            tree.create_node(store, &new_right).await?;
            tree.cas_node(store, &new_left, &etag).await?;

            tail = next_id;
            next_id += 1;
            done += 1;

            if fix_dir {
                // 修父指针 = 多写一次目录（单点对象）
                let bk = new_left.high_key.unwrap_or(u64::MAX);
                dir.add_split(bk, new_right.id);
                tree.put_dir(store, &dir).await?;
            }
        } else {
            let mut grown = node.clone();
            grown.keys.push(key);
            tree.cas_node(store, &grown, &etag).await?;
            last_inserted = key;
        }
    }

    // 量一次「查最大的 key 走几个节点」
    let result = tree.search(store, &dir, last_inserted).await?;

    Ok(ChainStat {
        nodes: next_id,
        hops: result.hops(),
        found: result.is_found(),
    })
}

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    println!("        （构造 3 组对照树，每组两棵，需要一点时间）");
    println!("        分裂次数 | 节点数 | 不修目录: 查找跳数 | 修目录: 查找跳数 | 修的成本");

    let mut all_found = true;
    let mut monotonic = true; // 不修的跳数是否随分裂次数增长
                              // 最后一组的结果留到最后引用（**不重建** —— 最大的那棵树重建一次要多花一倍时间）
    let mut unfixed_last = None;
    let mut fixed_last = None;

    let mut prev_hops = 0usize;
    for &n in &SPLIT_COUNTS {
        let unfixed = build_chain(store, &format!("toy/s4/unfixed/{n}"), n, false).await?;
        let fixed = build_chain(store, &format!("toy/s4/fixed/{n}"), n, true).await?;

        all_found &= unfixed.found && fixed.found;
        if unfixed.hops <= prev_hops {
            monotonic = false;
        }
        prev_hops = unfixed.hops;

        println!(
            "        {:>8} | {:>6} | {:>18} | {:>15} | 每分裂 +1 次写(单点对象)",
            n, unfixed.nodes, unfixed.hops, fixed.hops
        );

        unfixed_last = Some(unfixed);
        fixed_last = Some(fixed);
    }

    let last = *SPLIT_COUNTS.last().unwrap();
    let unfixed_last = unfixed_last.expect("循环至少跑一次");
    let fixed_last = fixed_last.expect("循环至少跑一次");

    let grew = unfixed_last.hops > 1 && monotonic;
    let pass = all_found && grew && fixed_last.hops <= 2;
    let verdict = if pass { "PASS" } else { "FAIL" };

    println!(
        "[{verdict}] s4 父指针延后修复的代价: 两棵树查找结果都正确={all_found} \
         不修目录的跳数随分裂增长={grew} 修目录的跳数={}(恒定) \
         | {last} 次分裂后: 不修={}跳 vs 修={}跳",
        fixed_last.hops, unfixed_last.hops, fixed_last.hops
    );

    if pass {
        println!(
            "        ⇒ 结论：「父指针可以延后修」成立，**但「延后」必须有个界**。\n\
             \x20         不修 ⇒ 查找跳数随分裂次数**线性增长**（{last} 次分裂就要走 {} 跳）；\n\
             \x20         修   ⇒ 恒定（{} 跳），代价是**每次分裂多写一次目录**，\n\
             \x20         而目录是**单个对象** ⇒ 所有分裂者争它（热点的来源）。\n\
             \x20         所以这是个真实权衡：**用查找的跳数换写入的争用**，不是「顺手优化一下」。\n\
             \x20         ⇒ 设计上要定的不是「修不修」，而是**每分裂多少次、或链长到多少就强制修一次**。\n\
             \x20         ⚠️ 这里的数字来自单层链，真实树里陈旧父指针会把查找带到目标附近而不是链头，\n\
             \x20            常数更小 —— 要看的是**形状**（线性 vs 恒定），不是这个具体跳数。",
            unfixed_last.hops, fixed_last.hops
        );
    }

    Ok(())
}
