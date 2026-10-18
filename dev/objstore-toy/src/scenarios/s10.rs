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

//! **S10 —— 一个对象两个写者：`(值, ETag)` 必须同源。**
//!
//! 假设来源：设计里 Manifest 有**两个写者各写各的字段** ——
//! 提交路径写 `committed_lsn`、applier 写 `applied_lsn`（§2.2 / §4.1）。
//! 两者都做 read-modify-write，都靠同一个 CAS 保护。问题是：
//! **CAS 保护的是哪个东西？**
//!
//! 答案是：CAS 只保证"**你交给它的那个 ETag** 还是当前版本"。
//! 它**不检查**"你写回去的那些值，是不是与那个 ETag 同一次读出来的"。
//! 而 opendal 的 API 让这件事很容易做错 ——
//! **`read` 只给值不给 ETag，`stat` 只给 ETag 不给值**，
//! 想要"值 + ETag"就必须发**两次**请求，两次之间对象可能被改。
//!
//! 于是顺序成了承重结构：
//!
//! | 取的顺序 | 中间被改时拿到 | CAS 结果 | 后果 |
//! | --- | --- | --- | --- |
//! | `stat` → `read`（先 ETag 后值） | **旧 ETag** + 新值 | **失败** | 安全：重试即可 |
//! | `read` → `stat`（先值后 ETag） | 旧值 + **新 ETag** | **成功** | ⚠️ **写回旧值 ⇒ 静默回退对方字段** |
//!
//! ⇒ 这就是 S5b"后到的写入填补"之外，第二条**不会报错的错误**：
//! 两个写者都没违反任何 CAS 契约，但一方的进度被另一方悄悄抹掉了。
//!
//! ## 三幕（前两幕是变异与对照组，缺一不可）
//!
//! - **第一幕 · 真并发**：两个写者并发各跑 N 轮，都用 `stat → read` + CAS + 冲突重试。
//!   断言：最终 `committed == N_A` 且 `applied == N_B`（**协议可行**），并记下冲突次数
//!   （这个数是"重试成本"的实测，不是日志噪音）。
//! - **第二幕 · 变异（顺序反过来）**：单写者用 `read → stat`，**在两次请求之间**让另一方改掉对象。
//!   断言：CAS **成功**、且被改过的那个字段**被回退**。
//! - **第三幕 · 对照（顺序正确）**：同样的中间改动、同样的写者，但用 `stat → read`。
//!   断言：CAS **被拒**、字段**不回退**。
//!
//! 只做第一幕会得出"协议没问题"的结论 —— 而那个结论**只在写者不出错时成立**；
//! 第二幕给出的是**出错长什么样**，第三幕给出的是**代价只有一次重试**。
//! 有第二幕而无第三幕，则无法排除"是并发本身让字段丢的"。

use std::sync::atomic::{AtomicUsize, Ordering};

use crate::kv::{CasStore, StorageError};

const KEY: &str = "toy/s10/manifest";

/// 两个字段各自的写者各跑多少轮。故意取不同值，便于从最终值上区分是谁丢了。
const ROUNDS_A: usize = 8;
const ROUNDS_B: usize = 5;

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    // ---------------------------------------------------------------- 第一幕
    let _ = store.delete(KEY).await;
    store.write_if_not_exists(KEY, encode(0, 0)).await?;

    let conflicts = AtomicUsize::new(0);
    let (a, b) = tokio::join!(
        writer(store, Field::Committed, ROUNDS_A, &conflicts),
        writer(store, Field::Applied, ROUNDS_B, &conflicts),
    );
    let (a_ok, b_ok) = (a?, b?);
    let (c1, a1) = decode(&store.read(KEY).await?);
    let conflicts = conflicts.load(Ordering::Relaxed);

    let concurrent_ok = c1 == ROUNDS_A as u64 && a1 == ROUNDS_B as u64;
    println!(
        "[INFO] s10 第一幕 真并发（stat→read + CAS + 冲突重试）: \
         A 成功 {a_ok}/{ROUNDS_A} 轮、B 成功 {b_ok}/{ROUNDS_B} 轮、CAS 冲突 {conflicts} 次 \
         ⇒ 最终 committed={c1}（期望 {ROUNDS_A}）applied={a1}（期望 {ROUNDS_B}）\
         ⇒ {}",
        if concurrent_ok {
            "两个字段都到齐"
        } else {
            "★ 有字段没到齐"
        }
    );

    // ---------------------------------------------------------------- 第二幕
    let _ = store.delete(KEY).await;
    store.write_if_not_exists(KEY, encode(0, 0)).await?;
    // A 先把 committed 推到 3。
    for _ in 0..3 {
        let (etag, (c, a)) = snapshot_stat_then_read(store).await?;
        store.write_if_match(KEY, encode(c + 1, a), &etag).await?;
    }
    // 变异：B 用 **read → stat** 取快照 —— 值的请求在前，此时拿到的是**旧值**。
    let stale_body = store.read(KEY).await?;
    let (c_stale, a_stale) = decode(&stale_body);
    // 中间：A 再改一次 committed（这就是"两次请求之间被改"）。
    {
        let (etag, (c, a)) = snapshot_stat_then_read(store).await?;
        store.write_if_match(KEY, encode(c + 1, a), &etag).await?;
    }
    // B 现在用**新** ETag（stat 是后取的）配**旧**值写回。
    let fresh_etag = store.etag(KEY).await?;
    let (c_before_a, _) = decode(&store.read(KEY).await?);
    let write_back = store
        .write_if_match(KEY, encode(c_stale, a_stale + 1), &fresh_etag)
        .await;
    let (c_after, _) = decode(&store.read(KEY).await?);
    let mutation_rolled_back = write_back.is_ok() && c_after < c_before_a;
    println!(
        "[INFO] s10 第二幕 变异（read→stat，中间被改）: CAS {} 、committed {c_before_a} → {c_after} \
         ⇒ {}",
        match &write_back {
            Ok(_) => "成功",
            Err(StorageError::Conflict(_)) => "被拒",
            Err(_) => "其它错误",
        },
        if mutation_rolled_back {
            "★ 对方的字段被静默回退"
        } else {
            "没有回退（与假设不符）"
        }
    );

    // ---------------------------------------------------------------- 第三幕
    let _ = store.delete(KEY).await;
    store.write_if_not_exists(KEY, encode(0, 0)).await?;
    for _ in 0..3 {
        let (etag, (c, a)) = snapshot_stat_then_read(store).await?;
        store.write_if_match(KEY, encode(c + 1, a), &etag).await?;
    }
    // 对照：同样"先取快照、中途被改"，但这次 ETag 是**先**取的。
    let (stale_etag, (c_snap, a_snap)) = snapshot_stat_then_read(store).await?;
    {
        let (etag, (c, a)) = snapshot_stat_then_read(store).await?;
        store.write_if_match(KEY, encode(c + 1, a), &etag).await?;
    }
    let (c_before_control, _) = decode(&store.read(KEY).await?);
    let control_write = store
        .write_if_match(KEY, encode(c_snap, a_snap + 1), &stale_etag)
        .await;
    let (c_control, _) = decode(&store.read(KEY).await?);
    let control_rejected = matches!(control_write, Err(StorageError::Conflict(_)));
    let control_safe = control_rejected && c_control == c_before_control;
    println!(
        "[INFO] s10 第三幕 对照（stat→read，中间被改）: CAS {} 、committed {c_before_control} → {c_control} \
         ⇒ {}",
        match &control_write {
            Ok(_) => "成功",
            Err(StorageError::Conflict(_)) => "被拒",
            Err(_) => "其它错误",
        },
        if control_safe {
            "被拒且不回退（代价只有一次重试）"
        } else {
            "★ 与「安全顺序」的假设不符"
        }
    );

    let verdict = if concurrent_ok && mutation_rolled_back && control_safe {
        "PASS"
    } else {
        "FAIL"
    };
    println!(
        "[{verdict}] s10 一个对象两个写者: 并发两种顺序下字段都到齐={concurrent_ok} \
         反序会静默回退={mutation_rolled_back} 正序只丢一次 CAS={control_safe}"
    );
    if verdict == "PASS" {
        println!(
            "        ⇒ 设计含义：Manifest 的\"读\"必须是 **stat → read** 这个顺序，\n\
             \x20         而且必须把 (值, ETag) 当作**一个快照**传递 ——\n\
             \x20         分开取、且值是先取的那个，CAS 会成功但把对方字段写回旧值。\n\
             \x20         ⇒ 这条约束要在 M3 的 Manifest 读写封装里定死，不能留给调用方自觉。"
        );
    }

    let _ = store.delete(KEY).await;
    Ok(())
}

/// 两个字段各自的写者。
#[derive(Clone, Copy)]
enum Field {
    Committed,
    Applied,
}

/// 一个写者：跑 `rounds` 轮，每轮"取一次快照 → 改自己的字段 → CAS"，冲突则重试。
///
/// 返回成功轮数；冲突次数累加到共享计数器。
async fn writer(
    store: &CasStore,
    field: Field,
    rounds: usize,
    conflicts: &AtomicUsize,
) -> Result<usize, StorageError> {
    let mut ok = 0;
    for _ in 0..rounds {
        loop {
            let (etag, (c, a)) = snapshot_stat_then_read(store).await?;
            let next = match field {
                Field::Committed => encode(c + 1, a),
                Field::Applied => encode(c, a + 1),
            };
            match store.write_if_match(KEY, next, &etag).await {
                Ok(_) => {
                    ok += 1;
                    break;
                }
                Err(StorageError::Conflict(_)) => {
                    conflicts.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }
    }
    Ok(ok)
}

/// **正确顺序**的快照：先 `stat`（拿 ETag）再 `read`（拿值）。
///
/// 两次之间若对象被改，拿到的是"旧 ETag + 新值" ⇒ CAS 必然失败 ⇒ 安全。
/// 这正是第一幕与第三幕用的顺序。
async fn snapshot_stat_then_read(store: &CasStore) -> Result<(String, (u64, u64)), StorageError> {
    let etag = store.etag(KEY).await?;
    let value = store.read(KEY).await?;
    Ok((etag, decode(&value)))
}

fn encode(c: u64, a: u64) -> Vec<u8> {
    format!("c={c};a={a}").into_bytes()
}

fn decode(value: &[u8]) -> (u64, u64) {
    let text = String::from_utf8_lossy(value);
    let mut c = 0;
    let mut a = 0;
    for part in text.split(';') {
        let mut kv = part.split('=');
        match (kv.next(), kv.next()) {
            (Some("c"), Some(v)) => c = v.parse().unwrap_or_default(),
            (Some("a"), Some(v)) => a = v.parse().unwrap_or_default(),
            _ => {}
        }
    }
    (c, a)
}
