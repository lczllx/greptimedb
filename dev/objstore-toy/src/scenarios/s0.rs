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

//! **S0 —— CAS 基本盘**（0.59.2 版本回归，不是新研究）。
//!
//! 假设：opendal 0.59.2 上 `if_match` / `if_not_exists` 的行为与 0.58 一致，
//! 且**并发**两个写者拿同一个 etag 做 CAS ⇒ 恰好一胜一败。
//!
//! ⚠️ **原 demo 的「并发冲突」是假的**：它是同一个进程**按顺序**再调一次造成的
//! （旧 etag 自然对不上当前摘要）。所以本场景分两部分，第二部分用 `tokio::spawn`
//! 补上**真并发** —— 否则「一胜一败」这条判据根本没被验过。

use crate::kv::{CasStore, StorageError};

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    // ---- 第一部分：顺序 6 步（搬自 demo，验证 0.59.2 行为没变）----
    let key = "toy/s0/leader";
    let _ = store.delete(key).await;

    let v1 = store.write_if_not_exists(key, b"node-A".to_vec()).await?;
    let dup_create_rejected = matches!(
        store.write_if_not_exists(key, b"node-B".to_vec()).await,
        Err(StorageError::Conflict(_))
    );
    let v2 = store
        .write_if_match(key, b"node-A-keepalive".to_vec(), &v1)
        .await?;
    let stale_write_rejected = matches!(
        store
            .write_if_match(key, b"stale-writer".to_vec(), &v1)
            .await,
        Err(StorageError::Conflict(_))
    );
    let v3 = store.delete_if_match(key, &v2).await?;
    let double_resign_rejected = matches!(
        store.delete_if_match(key, &v2).await,
        Err(StorageError::Conflict(_))
    );
    let tombstones_differ = v1 != v2 && v2 != v3 && v1 != v3;

    // ---- 第二部分：**真并发**（两个任务、同一个 etag、同时写）----
    let race_key = "toy/s0/race";
    let _ = store.delete(race_key).await;
    let base = store
        .write_if_not_exists(race_key, b"base".to_vec())
        .await?;

    let s_a = store.clone();
    let s_b = store.clone();
    let (k_a, k_b) = (race_key.to_string(), race_key.to_string());
    let (e_a, e_b) = (base.clone(), base.clone());
    let h_a =
        tokio::spawn(async move { s_a.write_if_match(&k_a, b"writer-A".to_vec(), &e_a).await });
    let h_b =
        tokio::spawn(async move { s_b.write_if_match(&k_b, b"writer-B".to_vec(), &e_b).await });
    let r_a = h_a.await.expect("task A panicked");
    let r_b = h_b.await.expect("task B panicked");

    let wins = usize::from(r_a.is_ok()) + usize::from(r_b.is_ok());
    let conflicts = usize::from(matches!(&r_a, Err(StorageError::Conflict(_))))
        + usize::from(matches!(&r_b, Err(StorageError::Conflict(_))));
    let exactly_one_won = wins == 1 && conflicts == 1;

    // 胜者的值真的落盘了（防止「两个都报告成功但内容没变」这种假绿）
    let landed = store.read(race_key).await?;
    let landed_is_one_of_them = landed == b"writer-A" || landed == b"writer-B";

    let ok = dup_create_rejected
        && stale_write_rejected
        && double_resign_rejected
        && tombstones_differ
        && exactly_one_won
        && landed_is_one_of_them;
    let verdict = if ok { "PASS" } else { "FAIL" };

    println!(
        "[{verdict}] s0 CAS 基本盘: dup_create_rejected={dup_create_rejected} \
         stale_write_rejected={stale_write_rejected} double_resign_rejected={double_resign_rejected} \
         | 真并发 wins={wins} conflicts={conflicts} landed={:?}",
        String::from_utf8_lossy(&landed)
    );

    let _ = store.delete(key).await;
    let _ = store.delete(race_key).await;
    Ok(())
}
