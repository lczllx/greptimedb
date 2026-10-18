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

//! **S9 —— 条件读：`If-None-Match` 是否真的被执行。**
//!
//! 假设：**服务端尊重条件读** —— 用**当前** ETag 发 `If-None-Match` 会得到
//! `ConditionNotMatch`（内容零传输），用**过期** ETag 会拿到内容。
//!
//! 这是 S7 的同族问题，但落点不同：S7 问的是**删除**路径的条件，S9 问的是**读**路径的。
//! 设计文档把条件读写进了"对象存储提供的原子原语"表，用法是
//! **"内容变了吗"的零传输探针**（省一次 GET 的传输量，也省一次 304 之外的带宽）——
//! 它成立的前提正是"服务端真的执行了这个条件"，而这一点当时没有任何实测。
//!
//! ## 为什么这组对照是**判别性**的（而不是走个过场）
//!
//! 两个请求用**同一个** key、**同一个**方法，只换 ETag 参数：
//!
//! | 用的 ETag | 服务端尊重条件 | 服务端静默忽略 |
//! | --- | --- | --- |
//! | 当前（内容没变） | `ConditionNotMatch` | `Ok` + 内容 |
//! | 过期（内容变了） | `Ok` + 内容 | `Ok` + 内容 |
//!
//! ⇒ 只看"过期 ETag 能读到内容"区分不了两种后端；**必须看当前 ETag 那一格**。
//! 所以判定信号取"内容没变时是不是 `ConditionNotMatch`"，
//! 第二格只作对照（它证明请求确实带上了条件、而不是整条路被绕过）。
//!
//! ## 判定
//!
//! - **PASS** = 当前 ETag ⇒ `ConditionNotMatch`（条件被尊重 ⇒ 零传输探针可用）
//! - **FAIL** = 当前 ETag ⇒ `Ok`（**静默忽略** ⇒ 与 S7 同类缺陷：声明了却不执行，
//!   探针退化成"每次都把整个对象读回来"，比不用探针更糟：多一次全量传输）
//! - `unsupported` 单列：那是 **API 层门控**挡住的（本 toy 里不会出现，
//!   S3 后端 advertise 了这一位），含义与"服务端拦住"完全不同。

use opendal::ErrorKind;

use crate::kv::{CasStore, StorageError};

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    let key = "toy/s9/cond-read";
    let _ = store.delete(key).await;

    let advertised = store.capability_read_with_if_none_match();

    // 造出一个"过期 etag"：e1 写完后被 e2 覆盖。
    let e1 = store.write_if_not_exists(key, b"v1".to_vec()).await?;
    let e2 = store.write_if_match(key, b"v2".to_vec(), &e1).await?;
    let stale = e1 != e2;

    // ① 当前 ETag：内容**没变** ⇒ 期望 `ConditionNotMatch`
    let unchanged = store.read_with_if_none_match(key, &e2).await;
    // ② 过期 ETag：内容**变了** ⇒ 期望拿到内容
    let changed = store.read_with_if_none_match(key, &e1).await;

    let unchanged_label = label(&unchanged);
    let changed_label = label(&changed);

    let (verdict, why) = match (&unchanged, &changed) {
        (Err(StorageError::Conflict(_)), Ok(body)) if body == b"v2" => (
            "PASS",
            "条件被服务端尊重：内容没变 ⇒ 零传输；内容变了 ⇒ 返回内容。探针可用。",
        ),
        (Ok(_), Ok(_)) => (
            "FAIL",
            "**静默忽略**：两种 ETag 都返回了内容 ⇒ 条件读形同虚设。\n\
             \x20         探针不但没用，还比不用更糟 —— 每次都把整个对象传回来。",
        ),
        (Err(StorageError::Opendal(e)), _) if e.kind() == ErrorKind::Unsupported => (
            "FAIL",
            "**API 层门控**挡住（不是服务端拦住的）：换个 advertise 了该能力的后端就会掉进静默忽略。",
        ),
        _ => ("FAIL", "语义与契约不符（对照格没拿到内容，或错误分类异常）。"),
    };

    println!(
        "[{verdict}] s9 条件读: advertised={advertised} stale_etag={stale} \
         内容没变={unchanged_label} 内容变了={changed_label}"
    );
    println!("        ⇒ {why}");

    let _ = store.delete(key).await;
    Ok(())
}

/// 把一次条件读的结果压成一个词，便于打印与判定。
fn label(result: &Result<Vec<u8>, StorageError>) -> String {
    match result {
        Ok(body) => format!("returned({})", String::from_utf8_lossy(body)),
        Err(StorageError::Conflict(_)) => "condition-not-match".to_string(),
        Err(StorageError::NotFound(_)) => "not-found".to_string(),
        Err(StorageError::Opendal(e)) if e.kind() == ErrorKind::Unsupported => {
            "unsupported".to_string()
        }
        Err(_) => "other-error".to_string(),
    }
}
