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

//! **S3 —— 分裂中途崩溃：孤儿右兄弟无害，但会泄漏。**
//!
//! 假设：在执行完步骤 ①（右兄弟已落盘）之后崩溃，那个右兄弟就成了**没人指向的孤儿**，
//! 但它**不丢数据、也不破坏查找** —— 因为左节点还没被改，上半区的 key 还在它手里。
//!
//! 怎么证：只做步骤 ①，然后**当成进程重启**（重新读目录、重新走查找路径），
//! ① 搜全部 key 看是不是都还在；② 从目录出发沿右兄弟链走一遍，看孤儿在不在可达集合里。
//!
//! 判定：`全部 key 仍可搜` **且** `孤儿存在但无人指向` ⇒ PASS。
//!
//! 这条结论的**设计含义不是"没关系"，而是"无害但泄漏"** ⇒ 必须有 GC。
//! 所以它同时是一条 GC 需求的实测依据 —— 光看代码是推论，这里是证据。
//!
//! 注意与 S2 的分工：S2 问的是「中间态读得到吗」（**读**），
//! S3 问的是「崩了之后残留物怎么处理」（**清理**）。

use crate::kv::{CasStore, StorageError};
use crate::tree::{BLinkTree, Directory, Node};

const KEYS: [u64; 4] = [10, 20, 30, 40];

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    let prefix = "toy/s3/crash-after-step1";
    let tree = BLinkTree::new(prefix);
    tree.wipe(store, 64).await?;

    // ---- 崩溃之前的现场 ----
    let left = Node::new(0, KEYS.to_vec());
    let _etag = tree.create_node(store, &left).await?;
    tree.put_dir(store, &Directory::single(0)).await?;

    let (_new_left, new_right) = left.plan_split(1);
    // 步骤 ①：右兄弟落盘
    tree.create_node(store, &new_right).await?;
    // 💥 这里"崩"了 —— 步骤 ②（CAS 改左节点）**永远不会执行**。
    //    对象存储里留下的就是这一刻的状态。

    // ================= 以下模拟「进程重启后的恢复流程」 =================
    //
    // 关键：**不沿用上面任何内存状态**，目录从对象存储里重新读。
    let dir = tree
        .get_dir(store)
        .await?
        .ok_or_else(|| StorageError::NotFound(format!("{prefix}/dir")))?;

    // ① 全部 key 还在吗？
    let mut missing = Vec::new();
    for k in KEYS {
        let found = tree.search(store, &dir, k).await?;
        if !found.is_found() {
            missing.push((k, found.label()));
        }
    }
    let all_keys_present = missing.is_empty();

    // ② 孤儿在不在"可达集合"里？
    let reachable = tree.reachable(store, dir.route(0)).await?;
    let orphan_exists = tree.read_node(store, 1).await?.is_some();
    let orphan_routed = dir.routes_to(1);
    let orphan_referenced = orphan_routed || reachable.contains(&1);
    let orphan_is_unreferenced = orphan_exists && !orphan_referenced;

    // 附带信息：孤儿的内容是不是**冗余**的（决定它是不是"丢的数据"）
    let node0 = tree.read_node(store, 0).await?;
    let node1 = tree.read_node(store, 1).await?;

    let pass = all_keys_present && orphan_is_unreferenced;
    let verdict = if pass { "PASS" } else { "FAIL" };

    println!(
        "[{verdict}] s3 分裂中途崩溃: 全部key仍可搜={all_keys_present} \
         孤儿(节点1)仍存在={orphan_exists} 无人指向={orphan_is_unreferenced} \
         目录可达链={reachable:?}",
    );
    if !all_keys_present {
        println!("        ⚠️ 搜不到的 key: {missing:?}");
    }
    if let (Some(n0), Some(n1)) = (&node0, &node1) {
        println!(
            "        现场：节点0.keys={:?} (high_key={:?}, right={:?}) | 节点1.keys={:?} (high_key={:?}, right={:?})",
            n0.keys, n0.high_key, n0.right, n1.keys, n1.high_key, n1.right
        );
    }

    if pass {
        println!(
            "        ⇒ 结论：孤儿右兄弟**无害但泄漏**。\n\
             \x20         无害：左节点没被改，上半区 key 仍在它手里；孤儿的 keys 是**冗余副本**，\n\
             \x20         不是「丢了的数据」—— 这也是为什么崩溃点选在这里是安全的。\n\
             \x20         泄漏：没有任何指针指向它，它**永远不会被访问、也永远不会自己消失** ⇒\n\
             \x20         **必须 GC**。这条从「看代码能推论」变成了「实测有证据」。\n\
             \x20         ⚠️ GC 的判据不能是「对象很旧了」—— 一个正在被读的孤儿和真孤儿长得一样，\n\
             \x20         得靠「从根做一遍可达性遍历」来判定。这是设计里要单独想的一块。"
        );
    }

    Ok(())
}
