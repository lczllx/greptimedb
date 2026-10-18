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

//! **S7 —— 条件删除：MinIO 是否静默忽略 `If-Match`。**
//!
//! 假设：MinIO 对带 `If-Match` 的 DELETE **静默忽略** ——
//! 过期 etag 照样返 2xx，而且对象**真的被删掉**。
//!
//! 这是「**advertise 了但实际忽略**」型缺陷，也是最危险的一类：
//! 调用方以为有保护，其实没有，而且不看 capability 根本发现不了。
//!
//! 三种结果都要记清楚（判定信号的区别很重要）：
//!
//! | 实测 | 含义 | 判定 |
//! | --- | --- | --- |
//! | `Ok` + 对象没了 | **静默忽略**，条件形同虚设 | **PASS**（假设成立，坏消息） |
//! | `ConditionNotMatch` | 条件被服务端尊重 | FAIL（假设不成立，是好消息） |
//! | `Unsupported` | capability 门控挡在 **API 层** | FAIL（安全，但**不是**服务端挡的） |
//!
//! 第三种最容易被误读成"安全"：真正的含义是「换个 advertise 了该能力的后端，
//! 就会掉进第一种」。所以 S7 的结论不能只看结果，**必须连 `advertised` 一起看**。

use opendal::ErrorKind;

use crate::kv::{CasStore, StorageError};

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    let key = "toy/s7/delete";
    let _ = store.delete(key).await;

    let advertised = store.capability_delete_with_if_match();

    let e1 = store.write_if_not_exists(key, b"v1".to_vec()).await?;
    let e2 = store.write_if_match(key, b"v2".to_vec(), &e1).await?;
    // e1 现在是**过期** etag（当前摘要已经是 e2）。
    let stale = e1 != e2;

    let outcome = store.delete_with_if_match(key, &e1).await;
    let still_exists = store.exists(key).await?;

    let (label, verdict) = match &outcome {
        Ok(()) if !still_exists => ("silently-ignored", "PASS"),
        Ok(()) => ("ok-but-object-remains", "FAIL"),
        Err(StorageError::Conflict(_)) => ("condition-honored", "FAIL"),
        Err(StorageError::Opendal(e)) if e.kind() == ErrorKind::Unsupported => {
            ("unsupported", "FAIL")
        }
        Err(_) => ("other-error", "FAIL"),
    };

    println!(
        "[{verdict}] s7 条件删除: advertised={advertised} stale_etag={stale} \
         outcome={label} 删除后对象仍存在={still_exists}"
    );
    if label == "silently-ignored" {
        println!(
            "        ⇒ 条件删除不可信 ⇒ 删除一律走**墓碑写**（`delete_if_match`）。\n\
             \x20         注意 advertised={advertised}：能力**声明了**却**不执行**，\n\
             \x20         这正是「advertise 了但忽略」—— 只信 capability 会踩空。"
        );
    } else if label == "unsupported" {
        println!(
            "        ⇒ 注意：这是 **API 层门控**挡住的，不是服务端拦住的。\n\
             \x20         换个 advertise 了 `delete_with_if_match` 的后端就会掉进静默忽略。"
        );
    }

    let _ = store.delete(key).await;
    Ok(())
}
