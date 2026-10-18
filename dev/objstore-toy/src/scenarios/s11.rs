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

//! **S11 —— 重试层与 CAS 冲突：只吸收瞬态，不吞冲突。**
//!
//! 假设：挂上 `RetryLayer::default()`（`opendal-layer-retry-0.59.2`）之后，
//! **CAS 冲突不会被重试** —— `ConditionNotMatch` 不是 `is_temporary()`，
//! 所以它每次都立刻冒上来，重试计数**一次都不动**。
//!
//! ## 为什么这条要单独测
//!
//! 设计把"网络抖了一下"和"我并发输了"当成**两条完全不同的路**：
//!
//! | 情况 | 谁负责 | 期望行为 |
//! | --- | --- | --- |
//! | 传输层瞬态（连接被半关、超时） | 基础设施层 | **自动重试**，业务层看不见 |
//! | CAS 冲突（ETag 过期） | 上层的"重读 → 收敛" | **原封不动地抛上来**，必须能分辨 |
//!
//! 如果这层把两者混在一起，`LostRace` 就没法是个**独立变体**了 ——
//! 而"输给并发"与"传输失败"要采取的动作正好相反（前者重读后重试，后者原地重试）。
//!
//! S8 的实测已经给出前半：瞬态**确实**会被这层吸收（关掉 S8 的注入延迟后，
//! 10 次里仍有 9 次记到一次重试）。本场景补后半 ——
//! **故意制造**的冲突不会被这层吞掉。两半合起来才是 M1 要的那条边界。
//!
//! ## 判定
//!
//! - **PASS** = N 次过期写**全部**返回 `Conflict`，且**重试计数增量为 0**
//! - **FAIL** = 有写被重试（计数增长）或有写没被拒（值被写进去了）

use crate::cfg;
use crate::kv::{CasStore, StorageError};

/// 重复次数。挑 20 是为了让"零重试"这件事不像偶然：如果冲突真的会触发重试，
/// 20 次里出现 0 次的概率可以忽略。
const ROUNDS: usize = 20;

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    let key = "toy/s11/retry";
    let _ = store.delete(key).await;

    let e1 = store.write_if_not_exists(key, b"v1".to_vec()).await?;
    let e2 = store.write_if_match(key, b"v2".to_vec(), &e1).await?;
    let stale = e1 != e2;

    let before = cfg::transient_retries();
    let mut conflicts = 0usize;
    let mut landed = 0usize;
    for _ in 0..ROUNDS {
        match store
            .write_if_match(key, b"stale-should-not-land".to_vec(), &e1)
            .await
        {
            Err(StorageError::Conflict(_)) => conflicts += 1,
            // 被写进去了：条件失效（比"被重试"更糟）
            Ok(_) => landed += 1,
            Err(_) => {}
        }
    }
    let after = cfg::transient_retries();
    let retries = after - before;

    // 顺带确认那 20 次过期写一个字都没落下去。
    let body = store.read(key).await?;
    let untouched = body == b"v2";

    let verdict = if conflicts == ROUNDS && retries == 0 && landed == 0 && untouched {
        "PASS"
    } else {
        "FAIL"
    };
    println!(
        "[{verdict}] s11 重试层边界: stale_etag={stale} {ROUNDS} 次过期写 → \
         Conflict {conflicts} / 落盘 {landed} / 被重试 {retries} 次 ⇒ 值未被改动={untouched}"
    );
    if verdict == "PASS" {
        println!(
            "        ⇒ \"输给并发\" 不会被当成 \"传输失败\" 重试 —— 上层能分清这两条路。\n\
             \x20         另一半由 S8 提供（瞬态确实被吸收）：两半合起来 = M1 要的那条边界。"
        );
    }

    let _ = store.delete(key).await;
    Ok(())
}
