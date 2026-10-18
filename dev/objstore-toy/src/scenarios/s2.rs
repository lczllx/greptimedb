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

//! **S2 —— 分裂的两步顺序是承重的。**
//!
//! 假设：分裂走「① 先 `if_not_exists` 落右兄弟 → ② 再 CAS 改左节点」时，
//! **中间态对读路径是安全的**（上半区的 key 仍然搜得到）。
//!
//! 怎么证：真造出那个中间态（**故意不往下走第二步**），再搜一个属于上半区的 key。
//! 关键在**对照**：把顺序反过来（先 CAS 改左节点、右兄弟还没写），
//! 同一个中间态下上半区的 key 必须**搜不到** —— 否则「先右后左」就只是风格问题，
//! 不是承重约束，这条结论对设计就没有约束力。
//!
//! 判定：`正确顺序可搜` **且** `错误顺序不可达` ⇒ PASS。
//! 只有一个成立都算 FAIL —— 前者不成立说明设计是错的，后者不成立说明这个 toy 什么都没验出来。
//!
//! 同时带一个**下界对照**：错误顺序下**下半区**的 key 依然搜得到。
//! 这排除了"整个树都坏了"这种解释，把危害精确定位到「上半区不可达」。

use crate::kv::{CasStore, StorageError};
use crate::tree::{BLinkTree, Directory, Node, Search};

/// 初始满叶子的 4 个 key（也是分裂前的全部内容）。
const KEYS: [u64; 4] = [10, 20, 30, 40];
/// 分裂后会挪到右兄弟的 key（上半区）。
const UPPER_KEY: u64 = 30;
/// 分裂后留在左节点的 key（下半区）—— 对照组。
const LOWER_KEY: u64 = 10;

/// 一次分裂实验的观察结果。
struct Observation {
    /// 中间态（只做了第一步）下搜上半区 key 的结果。
    mid_upper: Search,
    /// 中间态下搜下半区 key 的结果（对照）。
    mid_lower: Search,
    /// 补完第二步之后，搜上半区 key 的结果。
    after_upper: Search,
}

/// 正确顺序：① 先落右兄弟 → ② 再 CAS 改左节点。
///
/// 在 ①②之间停下来观察 —— 这就是 S2 的全部。
async fn correct_order(store: &CasStore) -> Result<Observation, StorageError> {
    let tree = BLinkTree::new("toy/s2/right-first");
    tree.wipe(store, 64).await?;
    let left = Node::new(0, KEYS.to_vec());
    let etag = tree.create_node(store, &left).await?;
    tree.put_dir(store, &Directory::single(0)).await?;

    let (new_left, new_right) = left.plan_split(1);

    // ---- 步骤 ① ----
    tree.create_node(store, &new_right).await?;
    // ★★ 中间态：到此为止。**左节点还完全没动**，右兄弟是个还没人指向的孤儿。
    let mid_upper = tree.search_from(store, 0, UPPER_KEY).await?;
    let mid_lower = tree.search_from(store, 0, LOWER_KEY).await?;

    // ---- 步骤 ② ----
    tree.cas_node(store, &new_left, &etag).await?;
    let after_upper = tree.search_from(store, 0, UPPER_KEY).await?;

    Ok(Observation {
        mid_upper,
        mid_lower,
        after_upper,
    })
}

/// 错误顺序（对照）：① 先 CAS 改左节点 → ② 再落右兄弟。
///
/// 两步内容和上面**完全一样**，只有先后不同。
async fn wrong_order(store: &CasStore) -> Result<Observation, StorageError> {
    let tree = BLinkTree::new("toy/s2/left-first");
    tree.wipe(store, 64).await?;
    let left = Node::new(0, KEYS.to_vec());
    let etag = tree.create_node(store, &left).await?;
    tree.put_dir(store, &Directory::single(0)).await?;

    let (new_left, new_right) = left.plan_split(1);

    // ---- 步骤 ①（顺序换了）----
    tree.cas_node(store, &new_left, &etag).await?;
    // ★★ 中间态：左节点已经改小、并且指向了 1 号右兄弟 —— 但 **1 号还不存在**。
    let mid_upper = tree.search_from(store, 0, UPPER_KEY).await?;
    let mid_lower = tree.search_from(store, 0, LOWER_KEY).await?;

    // ---- 步骤 ② ----
    tree.create_node(store, &new_right).await?;
    let after_upper = tree.search_from(store, 0, UPPER_KEY).await?;

    Ok(Observation {
        mid_upper,
        mid_lower,
        after_upper,
    })
}

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    let ok = correct_order(store).await?;
    let bad = wrong_order(store).await?;

    // 四条断言，缺一不可
    let ok_mid_searchable = ok.mid_upper.is_found();
    let ok_after_searchable = ok.after_upper.is_found();
    let ok_lower_sane = ok.mid_lower.is_found();

    let bad_mid_unreachable = matches!(bad.mid_upper, Search::Unreachable { .. });
    let bad_lower_still_ok = bad.mid_lower.is_found();
    let bad_recovers = bad.after_upper.is_found();

    let pass = ok_mid_searchable
        && ok_after_searchable
        && ok_lower_sane
        && bad_mid_unreachable
        && bad_lower_still_ok
        && bad_recovers;

    let verdict = if pass { "PASS" } else { "FAIL" };
    println!(
        "[{verdict}] s2 分裂中间态: \
         | 正确顺序(先右后左) 中间态搜上半区key({UPPER_KEY})={} 下半区({LOWER_KEY})={} 补完第二步后={} \
         | 错误顺序(先左后右) 中间态搜上半区key({UPPER_KEY})={} 下半区({LOWER_KEY})={} 补完第二步后={}",
        ok.mid_upper.label(),
        ok.mid_lower.label(),
        ok.after_upper.label(),
        bad.mid_upper.label(),
        bad.mid_lower.label(),
        bad.after_upper.label(),
    );

    if pass {
        println!(
            "        ⇒ 结论：「先落右兄弟、再 CAS 改左节点」是**承重顺序**，不是风格偏好。\n\
             \x20         正确顺序下中间态**完全可读**（左节点还没动，上半区 key 还在它手里）；\n\
             \x20         顺序一反过来，左节点先自断其臂（把上半区交出去）却没有任何人接手 ⇒\n\
             \x20         **上半区 key 不可达**（注意是 Unreachable，不是 Absent —— 链断了）。\n\
             \x20         危害范围精确：只有上半区受影响（下半区照常），且补完第二步后自愈 ⇒\n\
             \x20         这是**窗口期**问题，不是永久损坏 —— 但窗口期内读到的就是错的。"
        );
    }
    if !bad_mid_unreachable {
        println!(
            "        ⚠️ 错误顺序下上半区 key 竟然还能搜到（{}）—— \
             说明这个对照没验出「顺序承重」，别拿它当证据。",
            bad.mid_upper.label()
        );
    }

    Ok(())
}
