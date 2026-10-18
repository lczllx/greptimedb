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

//! V5 —— 能力探测：不信 `Capability` 位，**实发请求**看真实行为。
//!
//! ## 为什么必须"实发"
//!
//! S7 给过一个反例：MinIO **声明**支持条件删除
//! （`Capability::delete_with_if_match == true`），实际发出去却被忽略。
//! 只看 capability 位的自检会把这类后端判成"安全" —— 而它恰恰是最危险的一类：
//! 调用方以为有保护、实际没有，**且从错误报告上看不出来**。
//!
//! 所以这里对每个条件操作发**一对**请求：
//!
//! | | 请求 | 期望 |
//! | --- | --- | --- |
//! | 正值 | 条件**成立** | 成功 |
//! | 负值 | 条件**不成立** | 被拒 |
//!
//! 两个结果合起来才定位得了能力。只发正值是不行的：一个**完全无视**条件头的后端
//! 也会成功，看起来和真支持一模一样。
//!
//! ## 探测要把两种"静默"分开
//!
//! 因为它们换后端之后命运相反：
//!
//! - **服务端静默忽略**：条件随着请求发出去了，服务端不执行。
//!   ⇒ 换一个真支持的后端就好（AWS S3 的单条条件删除是支持的）。
//! - **客户端静默丢弃**：条件在客户端就被扔了，请求里根本没有它。
//!   ⇒ **换任何后端都一样**。批量删除路径属于这一类：单条删除走
//!   `BatchDeleter::flush_buffer` 的「缓冲只有 1 条」分支 → `delete_once`，
//!   它明确设置 `If-Match` 头（`opendal-service-s3-0.59.2/src/core.rs:746`）；
//!   而缓冲 ≥ 2 条时走 `delete_batch`，`DeleteObjects` 的请求体**只序列化
//!   `Key`/`VersionId`**（同文件 `:1385`），`if_match` 仅被用来分类**返回的错误**
//!   （同 crate `deleter.rs:100`），不会出现在请求里。
//!   `OpDelete` 一路上都带着这个条件（`opendal-core` 的 `raw/ops.rs:128`），
//!   是最后一跳丢的；`CorrectnessCheckLayer` 只核对 capability 位
//!   （`layers/correctness_check.rs:84`），对"被批量吞掉"这件事一无所知。
//!
//!   这与 `DeleteOptions` 自己的文档（"OpenDAL never silently drops a condition"）
//!   是矛盾的，值得报给上游；对本项目则是一条硬约束：**条件语义不许走批量路径**。
//!
//! ## 判定与产出
//!
//! 逐行输出"这个后端实际支持什么"，供 M1 的启动自检直接用：**可以用** / **必须绕开**。
//! 探测在独立前缀上自建自清，可重复跑。
//!
//! 跑法（需 MinIO 在 127.0.0.1:9000）：`cargo run --bin capability_probe`

use objstore_toy::cfg;
use objstore_toy::kv::{CasStore, StorageError};
use opendal::options::DeleteOptions;
use opendal::ErrorKind;

/// 探测用的独立前缀。真实现里这套逻辑要能在启动时跑，所以它**不能**碰业务 key。
const PREFIX: &str = "toy/probe";

/// 一个条件操作的四态判定。
#[derive(Clone, Copy, PartialEq, Eq)]
enum Verdict {
    /// 声明 ✓、正值成功、负值被拒 ⇒ 条件真的生效。
    Trusted,
    /// 无条件的操作（没有条件可验）⇒ 只问"能不能用"。
    Available,
    /// 声明 ✓、正值成功、**负值也成功** ⇒ 服务端收下了带条件的请求却无视它。
    /// 最危险的一类：`capability()` 与实际行为相反，而且失败是静默的。
    ServerIgnores,
    /// 条件在**发出之前**就被丢掉了（客户端行为）⇒ 与后端无关。
    ClientDrops,
    /// 请求被门控挡成 `Unsupported`（后端没声明支持）⇒ 至少是**看得见**的失败。
    Gated,
    /// 条件成立时就不成功 ⇒ 声明与行为方向相反。
    PositiveFailed,
    /// 探测的**前提**没成立（例如"陈旧 etag"其实不陈旧）⇒ 这次探测什么也没验到。
    /// 单列一态是刻意的：这种情况必须报出来，**不许**混进"可信"里
    /// （S13 变异③ 的教训 —— 恒真的检查比没有检查更坏）。
    VoidProbe,
}

impl Verdict {
    fn label(self) -> &'static str {
        match self {
            Verdict::Trusted => "可信（条件生效）",
            Verdict::Available => "可用（无条件）",
            Verdict::ServerIgnores => "危险：服务端静默忽略",
            Verdict::ClientDrops => "危险：客户端丢弃条件",
            Verdict::Gated => "门控挡住（看得见）",
            Verdict::PositiveFailed => "声明与行为不符",
            Verdict::VoidProbe => "无效探测",
        }
    }

    fn usable(self) -> bool {
        matches!(self, Verdict::Trusted | Verdict::Available)
    }
}

/// 由一对请求（正值 / 负值）定位能力。
fn judge<T>(positive: &Result<T, StorageError>, negative_rejected: bool) -> Verdict {
    match positive {
        // 门控挡住：请求根本没发出去（后端没声明这个条件）
        Err(StorageError::Opendal(e)) if e.kind() == opendal::ErrorKind::Unsupported => {
            Verdict::Gated
        }
        // 条件成立却失败：声明与实际对不上，但**方向相反**，比"忽略"容易发现
        Err(_) => Verdict::PositiveFailed,
        Ok(_) if negative_rejected => Verdict::Trusted,
        Ok(_) => Verdict::ServerIgnores,
    }
}

/// 只描述"成没成 / 怎么失败的"，成的时候返回的是新 ETag 还是内容都不重要。
fn outcome<T>(r: &Result<T, StorageError>) -> String {
    match r {
        Ok(_) => "ok".to_string(),
        Err(StorageError::Conflict(_)) => "Conflict".to_string(),
        Err(StorageError::NotFound(_)) => "NotFound".to_string(),
        Err(StorageError::Opendal(e)) => format!("{:?}", e.kind()),
    }
}

/// 读回内容并比对；对象不存在算"不相等"。
async fn body_equals(store: &CasStore, key: &str, want: &[u8]) -> bool {
    matches!(store.read(key).await, Ok(b) if b.as_slice() == want)
}

/// 把 key 推到第 2 版，返回 `(陈旧 etag, 当前 etag)`，并登记进自清清单。
///
/// 探测负值全靠那个陈旧 etag：它必须**真的**陈旧，否则条件恰好成立、
/// 负值请求会"被拒"，于是一个无视条件头的后端也会被判成可信。
/// 所以下面每一处都把它当前提**显式检查**（`stale != fresh`），不成立就报 `VoidProbe`。
async fn two_versions(
    store: &CasStore,
    scratch: &mut Vec<String>,
    key: &str,
) -> Result<(String, String), StorageError> {
    scratch.push(key.to_string());
    let _ = store.delete(key).await;
    let stale = store.write_if_not_exists(key, b"v1".to_vec()).await?;
    let fresh = store.write_if_match(key, b"v2".to_vec(), &stale).await?;
    Ok((stale, fresh))
}

struct Row {
    name: &'static str,
    declared: &'static str,
    positive: String,
    negative: String,
    verdict: Verdict,
}

#[tokio::main]
async fn main() -> Result<(), StorageError> {
    let op = cfg::build_operator()?;
    let store = CasStore::new(op.clone());
    let cap = op.info().capability();
    let mut scratch: Vec<String> = Vec::new();
    let mut rows: Vec<Row> = Vec::new();

    // ---------------------------------------------------------------- 条件写 If-Match
    let k = format!("{PREFIX}/write-if-match");
    let (stale, fresh) = two_versions(&store, &mut scratch, &k).await?;
    let stale_is_stale = stale != fresh;
    let positive = store.write_if_match(&k, b"v3".to_vec(), &fresh).await;
    let negative = store
        .write_if_match(&k, b"SHOULD-NOT-LAND".to_vec(), &stale)
        .await;
    let neg_rejected = matches!(negative, Err(StorageError::Conflict(_)))
        && !body_equals(&store, &k, b"SHOULD-NOT-LAND").await;
    rows.push(Row {
        name: "write_if_match",
        declared: yes_no(cap.write_with_if_match),
        positive: outcome(&positive),
        negative: outcome(&negative),
        verdict: if stale_is_stale {
            judge(&positive, neg_rejected)
        } else {
            Verdict::VoidProbe
        },
    });

    // ---------------------------------------------------- 条件写 If-None-Match（原子创建）
    let k = format!("{PREFIX}/write-if-not-exists");
    scratch.push(k.clone());
    let _ = store.delete(&k).await;
    let positive = store.write_if_not_exists(&k, b"v1".to_vec()).await;
    let negative = store
        .write_if_not_exists(&k, b"SHOULD-NOT-LAND".to_vec())
        .await;
    let neg_rejected =
        matches!(negative, Err(StorageError::Conflict(_))) && body_equals(&store, &k, b"v1").await;
    rows.push(Row {
        name: "write_if_not_exists",
        declared: yes_no(cap.write_with_if_not_exists),
        positive: outcome(&positive),
        negative: outcome(&negative),
        verdict: judge(&positive, neg_rejected),
    });

    // ---------------------------------------------------------------- 条件读 If-None-Match
    let k = format!("{PREFIX}/read-if-none-match");
    let (stale, fresh) = two_versions(&store, &mut scratch, &k).await?;
    let stale_is_stale = stale != fresh;
    // 正值：拿**陈旧**的 etag 去读 ⇒ 当前 etag 与它不等 ⇒ 条件成立 ⇒ 返回内容
    let positive = store.read_with_if_none_match(&k, &stale).await.map(|_| ());
    // 负值：拿**当前** etag 去读 ⇒ 条件不成立 ⇒ 必须被拒（内容一个字节都不该传）
    let negative = store.read_with_if_none_match(&k, &fresh).await.map(|_| ());
    let neg_rejected = matches!(negative, Err(StorageError::Conflict(_)));
    rows.push(Row {
        name: "read_if_none_match",
        declared: yes_no(cap.read_with_if_none_match),
        positive: outcome(&positive),
        negative: outcome(&negative),
        verdict: if stale_is_stale {
            judge(&positive, neg_rejected)
        } else {
            Verdict::VoidProbe
        },
    });

    // -------------------------------------------------- 条件删（单条，走 delete_once）
    let kp = format!("{PREFIX}/delete-if-match-positive");
    let (_sp, fp) = two_versions(&store, &mut scratch, &kp).await?;
    // 正值里，证据不止"请求成功"：对象得**真的**没了
    let positive = store.delete_with_if_match(&kp, &fp).await;
    let positive = match positive {
        Ok(()) if !store.exists(&kp).await? => Ok(()),
        Ok(()) => Err(StorageError::NotFound(
            "条件删除报成功但对象还在".to_string(),
        )),
        Err(e) => Err(e),
    };

    let kn = format!("{PREFIX}/delete-if-match-negative");
    let (sn, _fn) = two_versions(&store, &mut scratch, &kn).await?;
    let negative = store.delete_with_if_match(&kn, &sn).await;
    // 负值的证据是"对象**还活着**"——只看返回码会把"报错了但删掉了"读成可信
    let neg_rejected =
        matches!(negative, Err(StorageError::Conflict(_))) && store.exists(&kn).await?;
    rows.push(Row {
        name: "delete_if_match",
        declared: yes_no(cap.delete_with_if_match),
        positive: outcome(&positive),
        negative: outcome(&negative),
        verdict: judge(&positive, neg_rejected),
    });

    // ------------------------------------------------------------ 无条件删除（GC 依赖它）
    let k = format!("{PREFIX}/delete-plain");
    scratch.push(k.clone());
    let _ = store.delete(&k).await;
    let _ = store.write_if_not_exists(&k, b"v1".to_vec()).await?;
    // `delete` 在对象不存在时也是 `Ok`（S3 语义）⇒ 返回码本身不构成证据，
    // 唯一的证据是"删完之后它真的不在了"。
    let _ = store.delete(&k).await;
    let gone = !store.exists(&k).await?;
    rows.push(Row {
        name: "delete",
        declared: "—",
        positive: if gone { "ok" } else { "still there" }.to_string(),
        negative: "—".to_string(),
        verdict: if gone {
            Verdict::Available
        } else {
            Verdict::PositiveFailed
        },
    });

    // --------------------------------------- 批量删除带条件：条件在**客户端**就被丢掉
    let ka = format!("{PREFIX}/batch-a");
    let kb = format!("{PREFIX}/batch-b");
    let (sa, _fa) = two_versions(&store, &mut scratch, &ka).await?;
    let (sb, _fb) = two_versions(&store, &mut scratch, &kb).await?;
    let mut deleter = op.deleter().await?;
    for (k, etag) in [(&ka, &sa), (&kb, &sb)] {
        deleter
            .delete((
                k.clone(),
                DeleteOptions {
                    if_match: Some(etag.clone()),
                    ..Default::default()
                },
            ))
            .await?;
    }
    // 缓冲里现在是 2 条 ⇒ 走 `delete_batch`（DeleteObjects），`If-Match` 不会出现在请求里。
    // 两条用的都是**陈旧** etag：条件真生效的话，两条都不该被删掉。
    let batch_result = deleter.close().await;
    let both_gone = !store.exists(&ka).await? && !store.exists(&kb).await?;
    rows.push(Row {
        name: "delete_batch + if_match",
        declared: "—",
        positive: "—".to_string(),
        negative: if both_gone { "accepted" } else { "rejected" }.to_string(),
        verdict: match batch_result {
            // 条件真被送到服务端并被拒 ⇒ 批量路径也是可信的
            Err(e) if e.kind() == ErrorKind::ConditionNotMatch => Verdict::Trusted,
            Err(e) if e.kind() == ErrorKind::Unsupported => Verdict::Gated,
            Err(_) => Verdict::PositiveFailed,
            Ok(()) if both_gone => Verdict::ClientDrops,
            Ok(()) => Verdict::Trusted,
        },
    });

    // ------------------------------------------------------------------------- 输出
    println!("=== 能力探测（实发请求，不信 Capability 位）—— V5 ===");
    println!(
        "后端 scheme={}｜探测前缀 {PREFIX}/（自建自清，可重复跑）",
        op.info().scheme()
    );
    println!();
    println!(
        "{:<24} {:>4}  {:<10} {:<10} verdict",
        "op", "declared", "positive", "negative"
    );
    for r in &rows {
        println!(
            "{:<24} {:>4}  {:<10} {:<10} {}",
            r.name,
            r.declared,
            r.positive,
            r.negative,
            r.verdict.label()
        );
    }

    // 危险的两行"怎么读"单独说清楚 —— 判定词只有一个字眼，
    // 而这两行的证据差在**责任方**上，混着看会把"换后端能修"和"换后端也修不了"读成同一件事。
    if rows.iter().any(|r| r.verdict == Verdict::ServerIgnores) {
        println!(
            "\n注 delete_if_match：正值 ok（对象真的被删掉）**且**陈旧 etag 也被接受\n\
             \x20  ⇒ 条件确实发出去了（单条删除走 `delete_once`，见文件头），是**服务端**没执行它。"
        );
    }
    if rows.iter().any(|r| r.verdict == Verdict::ClientDrops) {
        println!(
            "注 delete_batch + if_match：两条都用陈旧 etag，仍被全部删掉。\n\
             \x20  ⇒ 这一次观测本身分不出「客户端丢的」还是「服务端忽略的」——\n\
             \x20    责任方是读 opendal 源码定的（`DeleteObjects` 请求体里没有条件头，见文件头）。"
        );
    }

    let usable: Vec<&str> = rows
        .iter()
        .filter(|r| r.verdict.usable())
        .map(|r| r.name)
        .collect();
    let avoid: Vec<String> = rows
        .iter()
        .filter(|r| !r.verdict.usable())
        .map(|r| format!("{}（{}）", r.name, r.verdict.label()))
        .collect();

    println!();
    let mark = if avoid.is_empty() { "[OK]" } else { "[WARN]" };
    println!("{mark} 可以用：{}", usable.join("、"));
    println!("     必须绕开：{}", avoid.join("、"));
    println!(
        "     ⇒ 这就是 M1 启动自检的输出形状：**声明**与**实发**不一致时，以后者为准。\n\
         \x20      两种「绕开」里，「服务端静默忽略」换后端会变（S3 支持单条条件删除），\n\
         \x20      「客户端丢弃条件」换后端不会变 —— 那是 opendal 自己在最后一跳丢的。"
    );

    // 自清：探测留下的对象一个都不留
    for k in &scratch {
        let _ = store.delete(k).await;
    }
    // 但"删过了"和"删干净了"不是一回事（删错 key、前缀写错、漏登记都长这样），
    // 所以清完再列一遍。这次探测自己就是"检查器"，它也不该只靠自述。
    let leftover = store.list(&format!("{PREFIX}/")).await?;
    if leftover.is_empty() {
        println!("     [OK] 自清干净：前缀 {PREFIX}/ 下 0 个对象");
    } else {
        println!(
            "     [WARN] 前缀 {PREFIX}/ 下还剩 {} 个对象：{}",
            leftover.len(),
            leftover.join("、")
        );
    }

    Ok(())
}

fn yes_no(v: bool) -> &'static str {
    if v {
        "✓"
    } else {
        "✗"
    }
}
