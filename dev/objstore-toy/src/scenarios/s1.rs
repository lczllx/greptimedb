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

//! **S1 —— ABA 守卫：ETag 会重现，所以「值必须单调」是承重不变量。**
//!
//! 假设：因为 ETag = 内容摘要且**不单调**（A→B→A 之后旧 ETag 回来了），
//! `if_match(e1)` 只能保证「**值**没变」，**保证不了「值没被别人动过」**。
//!
//! 怎么证：走 A→B→A，然后拿**第一次读到的 `e1`** 去 CAS（注意 —— 不是拿另一个
//! 过期 etag，那必然被拒，什么也证明不了）。
//!
//! 判定：**`if_match(e1)` 成功 ⇒ PASS**（假设成立，ABA 洞存在）。
//! 这是一个**坏消息被证实** —— 所以它同时是一条设计不变量：
//! **要让 CAS 等价于 fencing，对象的值必须严格单调**（值不重复 ⇒ ABA 不可能）。

use crate::kv::{CasStore, StorageError};

const VERSION_A: &[u8] = b"version-A";
const VERSION_B: &[u8] = b"version-B";

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    let key = "toy/s1/aba";
    let _ = store.delete(key).await;

    // ---- 第一幕：A → B → A ----
    let e1 = store.write_if_not_exists(key, VERSION_A.to_vec()).await?;
    let e3 = store.write_if_match(key, VERSION_B.to_vec(), &e1).await?;
    let e4 = store.write_if_match(key, VERSION_A.to_vec(), &e3).await?;

    // e1 == e4 是 ABA 的**前提**：内容绕一圈回来，摘要跟着回来。
    let etag_recurred = e4 == e1;

    // ---- 第二幕：拿「A→B→A 之前」的 e1 去 CAS ----
    //
    // 这是全场景的关键一步。此刻对象的摘要就是 e4（== e1），
    // 所以 e1 **对得上** ⇒ 这个写会成功，尽管对象在中间被改过两次。
    // 换句话说：一个持有「读时 etag」的写者，**无法察觉这中间的两次修改**。
    let stale_holder_write = store
        .write_if_match(key, b"written-by-stale-holder".to_vec(), &e1)
        .await;
    let aba_succeeded = stale_holder_write.is_ok();

    let verdict = if etag_recurred && aba_succeeded {
        "PASS"
    } else {
        "FAIL"
    };
    println!(
        "[{verdict}] s1 ABA: e1==e4(旧ETag重现)={etag_recurred} \
         持有 e1 的写者在 A→B→A 之后 CAS 竟然成功={aba_succeeded}"
    );
    if aba_succeeded {
        println!(
            "        ⇒ 结论：CAS 只保证「值没变」，不保证「值没被改过」。\n\
             \x20         要让它等价于 fencing，对象的值必须**严格单调**（值不重复 ⇒ ABA 不可能）。\n\
             \x20         反过来说：任何**值可能重复**的对象（可回滚的水位、配置 blob）\n\
             \x20         都不能只靠 ETag 保护 —— 要么值里加单调递增的计数，要么改用 epoch。"
        );
    }

    let _ = store.delete(key).await;
    Ok(())
}
