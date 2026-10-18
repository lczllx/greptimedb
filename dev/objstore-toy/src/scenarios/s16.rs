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

//! **S16 —— 崩溃恢复要多久？（= 设计的 `f(MAX_UNAPPLIED)`）**
//!
//! 假设：恢复时间随要重放的日志段数**线性**，且这个量**有上界**
//! （设计 §6.1 的背压上限 `MAX_UNAPPLIED` 把 `committed_lsn - applied_lsn` 掐住）。
//!
//! ## 为什么这条要紧：它是一张「承诺表」里唯一没实测过的一格
//!
//! 设计把「A 失能 → B 开始服务」拆成四项之和（§5.4.4）：
//!
//! ```text
//! failover ≈ 租约过期(T+ε) + 发现过期(≤P) + recovery + 一次竞选 CAS
//!          ≈    5 + 2.5        +  ε        + f(MAX_UNAPPLIED) + 一跳
//! ```
//!
//! 头两项、末一项都有实打实的来源，**只有 `f(MAX_UNAPPLIED)` 那一格标着
//! 「分析模型，非实测」，而且它自己加了一句「依赖 §3.3 还没定的攒批参数 ⇒ M4 定下来后复核一次」**。
//! 本场景就是去把这一格量出来 —— 顺带看看那个「还没定的参数」到底有多要紧。
//!
//! ## 量什么
//!
//! `recover_by_manifest` 的耗时 = 「要重放的段**对象**数 × 每个对象的读+解码成本」。
//! 于是：
//!
//! - **线性**：段数翻 k 倍，耗时应翻 k 倍（不是 k²）；
//! - **有界**：重放量 = `committed_lsn - applied_lsn`。同一个总量下，
//!   把 `applied_lsn` 推高 ⇒ 恢复变快。**界来自 applied 水位，不来自"系统跑了多久"**；
//! - **每对象的成本**：本机是 loopback，绝对值不能直接当 S3 用 ——
//!   平移办法见 L1 的**可加性验证**（注入 RTT 后残差 ≤ 1.4ms）。
//!
//! ## 判定
//!
//! - 线性成立 **且** 同总量下窗口小则恢复快 ⇒ PASS
//! - 认输的一部分写在「诚实边界」：本场景**不验背压本身**（toy 里没实现背压），
//!   它验的是「窗口的大小决定恢复成本」这条因果，以及代入 `MAX_UNAPPLIED` 之后的耗时段。

use std::time::{Duration, Instant};

use serde_json::json;
use tokio::task::JoinSet;

use crate::commit::{Batch, CommitLog, Manifest};
use crate::kv::{CasStore, StorageError};

const PREFIX: &str = "toy/s16";
/// 换一个 epoch 就用一套全新的 key 空间（段 key 带 epoch）。
const EPOCH: u64 = 9001;

/// 造数（写段）的并发度。造数**不是**被测对象，所以怎么快怎么来。
const SETUP_CONCURRENCY: usize = 32;
/// 并行预取的并发度 —— 恢复路径**可以**这么干（段之间互相独立），设计里也写了
/// 「段之间可并行预取」。这一档是用来量「那条路能买到多少」的。
const PREFETCH_CONCURRENCY: usize = 32;

/// 一个段对象里放几条事务（`put`）。段的大小会影响传输项，见 [`TRANSFER_NEGLIGIBLE_BYTES`]。
const PUTS_PER_SEGMENT: usize = 4;
/// 每个 value 的字节数。真 meta 的 value 从几百字节（route 的一小片）到 1.4KB 不等。
const VALUE_BYTES: usize = 256;
/// 段对象小于这个量级时，传输项相对「每跳固定开销」可以忽略 —— 依据是 L1 拟合出的带宽
/// （GET 636–705 MB/s，即 1KB 才 1.4µs）：本场景的段是 KB 级，所以每对象成本 ≈ 那条
/// 拟合曲线的截距 `a`。这也是「本机数字能平移到真 S3」的一半理由（另一半是可加性）。
const TRANSFER_NEGLIGIBLE_BYTES: usize = 64 * 1024;

/// 恢复预算：最大那一档的**顺序**恢复要能在这么多秒内跑完。
/// 100k 段在 loopback 上顺序读要几分钟，一次 `all` 不该被它拖垮 ⇒ 自适应选档。
const RECOVER_BUDGET_SECS: f64 = 30.0;
/// 造数预算（并发写）。
const SETUP_BUDGET_SECS: f64 = 90.0;

/// L1 拟合出的 loopback GET 截距 `a`（三遍 1.60–1.90ms）。
const LOOPBACK_GET_A_MS: f64 = 1.75;
/// 真 S3 的每跳延迟。设计 §5.3 用的就是 20–50ms 这个区间，这里取中点。
const S3_HOP_MS: f64 = 30.0;
/// 设计的背压上限（§6.1）：`10⁵` 条事务。
const MAX_UNAPPLIED_TXNS: u64 = 100_000;
/// §5.4.4 里 recovery **之前**那三项（T+ε + P + 一次竞选 CAS）的合计。
/// 用它把 recovery 代入 failover 分解，看哪一格会把「8~10s」顶破。
const FAILOVER_WITHOUT_RECOVERY_SECS: f64 = 7.5;

// ============================================================================

/// 一段 = 若干条 put，内容按真实形状（key 是路径、value 是 JSON）。
fn segment_batch(lsn: u64) -> Batch {
    let value = "v".repeat(VALUE_BYTES);
    (0..PUTS_PER_SEGMENT)
        .map(|i| {
            (
                format!("greptime_public_metrics/lsn{lsn}/k{i}"),
                serde_json::to_string(&json!({ "v": value })).expect("可序列化"),
            )
        })
        .collect()
}

fn encode_manifest(m: &Manifest) -> Vec<u8> {
    serde_json::to_vec(m).expect("Manifest 可序列化")
}

/// 并发跑一批互不依赖的 future，最多 `concurrency` 个在飞（滑动窗口）。
///
/// 用 `JoinSet` 而不是静态切片：切片的话最慢的那一片决定总时长，
/// 而对象存储的抖动正好会让某一片明显慢 —— 那会把「并发到底买到了多少」量歪。
/// （`Semaphore` 要 `tokio/sync`，本 crate 没开；`JoinSet` 在 `rt` 里，已经有了。）
async fn in_flight<T, F, Fut>(
    items: Vec<T>,
    concurrency: usize,
    f: F,
) -> Vec<Result<(), StorageError>>
where
    T: Send + 'static,
    F: Fn(T) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Result<(), StorageError>> + Send + 'static,
{
    let mut set: JoinSet<Result<(), StorageError>> = JoinSet::new();
    let mut it = items.into_iter();
    let mut out = Vec::new();

    loop {
        while set.len() < concurrency {
            match it.next() {
                Some(item) => {
                    set.spawn(f(item));
                }
                None => break,
            }
        }
        if set.is_empty() {
            break;
        }
        match set.join_next().await {
            Some(Ok(r)) => out.push(r),
            Some(Err(e)) => panic!("并发任务 panic 了: {e}"),
            None => break,
        }
    }
    out
}

/// 一档规模的一次观测。
struct Rung {
    segments: u64,
    setup: Duration,
    recover: Duration,
}

fn secs(d: Duration) -> f64 {
    d.as_secs_f64()
}

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    // ------------------------------------------------------------ 幕〇 校准
    // 先量一个对象的成本，再据此选规模 —— 硬写 1k/10k/100k 会在慢机器上把一次
    // 跑拖成十几分钟。校准本身也是数据（它就是「每对象成本」的第一个样本）。
    let cal_log = CommitLog::new(&format!("{PREFIX}/cal"));
    let cal_n = 64u64;
    let t0 = Instant::now();
    for lsn in 1..=cal_n {
        store
            .put(
                &cal_log.batch_key(EPOCH, lsn),
                encode_batch(&segment_batch(lsn)),
            )
            .await?;
    }
    let put_ms = secs(t0.elapsed()) * 1000.0 / cal_n as f64;
    let t0 = Instant::now();
    for lsn in 1..=cal_n {
        let _ = cal_log.read_batch(store, EPOCH, lsn).await?;
    }
    let get_ms = secs(t0.elapsed()) * 1000.0 / cal_n as f64;
    cal_log.wipe(store, EPOCH).await?;

    // 幕五要把本机数字平移到真 S3，前提是「每对象成本 ≈ 那条拟合曲线的截距 a」，
    // 也就是传输项可以忽略。这个前提**每次运行都查**，不留给读代码的人自己记。
    let seg_bytes = encode_batch(&segment_batch(1)).len();
    assert!(
        seg_bytes < TRANSFER_NEGLIGIBLE_BYTES,
        "段对象 {seg_bytes}B 已大到传输项不能忽略（阈值 {TRANSFER_NEGLIGIBLE_BYTES}B）⇒ 幕五的平移不成立"
    );

    // 最大档：顺序恢复要落在预算内。四舍五入到 16 的整数倍，好读。
    let max_by_recover = (RECOVER_BUDGET_SECS * 1000.0 / get_ms.max(0.05)) as u64;
    let max_by_setup =
        (SETUP_BUDGET_SECS * 1000.0 * SETUP_CONCURRENCY as f64 / put_ms.max(0.05)) as u64;
    let n_max = max_by_recover.min(max_by_setup).max(256) / 16 * 16;
    // 三档几何（×4），比值 k = 1 / 4 / 16 由**这里的设计**给出 —— 后面断言用的带宽
    // 就是它，不是实测出来的数（那会犯 S13 变异③ 那个「区间由被检对象自己生成」的错）。
    let ladder: Vec<u64> = vec![n_max / 16, n_max / 4, n_max];

    // ------------------------------------------------------------ 幕一 造数 + 顺序恢复
    let mut rungs: Vec<Rung> = Vec::new();
    for &n in &ladder {
        let log = CommitLog::new(&format!("{PREFIX}/run{n}"));
        // 段：并发写（造数不是被测对象）
        let setup_t0 = Instant::now();
        let items: Vec<u64> = (1..=n).collect();
        let log = std::sync::Arc::new(log);
        let log_w = std::sync::Arc::clone(&log);
        let store_w = store.clone();
        let results = in_flight(items, SETUP_CONCURRENCY, move |lsn| {
            let log = std::sync::Arc::clone(&log_w);
            let store = store_w.clone();
            Box::pin(async move {
                store
                    .put(
                        &log.batch_key(EPOCH, lsn),
                        encode_batch(&segment_batch(lsn)),
                    )
                    .await
            })
        })
        .await;
        for r in results {
            r?;
        }
        // Manifest：水位推到 n，`applied_lsn` 停在 0。
        // ⚠️ 这个初值是**构造出来的最坏情况**，不是自然状态 —— 含义见「诚实边界」：
        //    它代表「树的持久状态落在恢复窗口的另一头」，也就是「要重放全部 n 段」。
        store
            .put(
                &log.manifest_key(),
                encode_manifest(&Manifest {
                    epoch: EPOCH,
                    committed_lsn: n,
                    applied_lsn: 0,
                }),
            )
            .await?;
        let setup = setup_t0.elapsed();

        // 恢复：**顺序**（这是模型现在的实现，也是被测对象）
        let rt0 = Instant::now();
        let recovered = log.recover_by_manifest(store).await?;
        let recover = rt0.elapsed();
        assert_eq!(recovered.applied.len() as u64, n, "应重放全部 {n} 段");
        assert!(recovered.missing.is_empty(), "不该有缺失的段");

        rungs.push(Rung {
            segments: n,
            setup,
            recover,
        });
    }

    // 线性：`t_i / t_0` 应约等于 `N_i / N_0`。带宽由上面的阶梯（k = 1/4/16）给，
    // 只留 0.65–1.5 的松紧度吸收预热与抖动。（平方实现会给出 16 而不是 4 ⇒ 越界。）
    let linear = rungs.iter().all(|r| {
        let k = r.segments as f64 / rungs[0].segments as f64;
        let ratio = secs(r.recover) / secs(rungs[0].recover).max(1e-9);
        ratio >= k * 0.65 && ratio <= k * 1.5
    });

    // ------------------------------------------------------------ 幕二 窗口 ≠ 总量
    // 同一个总量，只把 applied 水位推高，恢复就该按**窗口**缩小。
    // 窗口取 128 段（真实系统里它就是"上次把 applied 落盘之后又提交了多少"）。
    let biggest = rungs.last().expect("至少一档");
    let big_n = biggest.segments;
    let big_log = CommitLog::new(&format!("{PREFIX}/run{big_n}"));
    let window = 128u64.min(big_n);
    store
        .put(
            &big_log.manifest_key(),
            encode_manifest(&Manifest {
                epoch: EPOCH,
                committed_lsn: big_n,
                applied_lsn: big_n - window,
            }),
        )
        .await?;
    let wt0 = Instant::now();
    let w_rec = big_log.recover_by_manifest(store).await?;
    let windowed = wt0.elapsed();
    // 「只重放了窗口内的段」与「因此更快」是两件事，分开断言。
    // 写成 `assert_eq!` 的话，一个"忽略 applied 水位"的实现会让场景**panic** 而不是给出
    // 一行 FAIL —— 那也算红，但读的人看不出是哪条假设不成立了。
    let window_exact = w_rec.applied.len() as u64 == window;
    // 断言：窗口是总量的 1/125，恢复至少该快 20 倍。带宽 20 由**窗口比例**给出，
    // 不是从两个实测数里挑的。
    let window_governs = secs(windowed) * 20.0 < secs(biggest.recover);

    // ------------------------------------------------------------ 幕三 并行预取
    // 段之间互不依赖 ⇒ 恢复**可以**并发读。这是把 S3 上的「每跳 30ms」平摊掉的唯一杠杆。
    // 用同一份数据、同一个总量，只换读取的并发度。
    let mid_n = ladder[1];
    let mid_log = std::sync::Arc::new(CommitLog::new(&format!("{PREFIX}/run{mid_n}")));
    let pt0 = Instant::now();
    let items: Vec<u64> = (1..=mid_n).collect();
    let log_p = std::sync::Arc::clone(&mid_log);
    let store_p = store.clone();
    let results = in_flight(items, PREFETCH_CONCURRENCY, move |lsn| {
        let log = std::sync::Arc::clone(&log_p);
        let store = store_p.clone();
        Box::pin(async move {
            let b = log.read_batch(&store, EPOCH, lsn).await?;
            assert!(b.is_some(), "段 {lsn} 应存在");
            Ok(())
        })
    })
    .await;
    for r in results {
        r?;
    }
    let prefetched = pt0.elapsed();

    // ------------------------------------------------------------ 幕四 列举对照
    // "列举 bucket 把看得见的段全重放"这条路 S5 已证是**错的**；这里量它的**代价**。
    // 拆成两段报：`list` 本身 + 逐段读。
    let lt0 = Instant::now();
    let keys = store.list(&mid_log.batch_prefix(EPOCH)).await?;
    let list_only = lt0.elapsed();
    assert_eq!(keys.len() as u64, mid_n, "列举应看见全部段");
    let ct0 = Instant::now();
    let by_listing = mid_log.recover_by_listing(store, EPOCH).await?;
    let listed = ct0.elapsed();
    assert_eq!(by_listing.applied.len() as u64, mid_n);

    // ------------------------------------------------------------ 幕五 代入设计的参数
    // 每对象成本 → 真 S3：`c_s3 ≈ c_本机 − loopback 的 a + 真 S3 每跳`（L1 的可加性）。
    let c_loop = get_ms;
    let c_s3 = (c_loop - LOOPBACK_GET_A_MS).max(0.0) + S3_HOP_MS;

    // ============================================================ 判定
    let verdict = if linear && window_exact && window_governs {
        "PASS"
    } else {
        "FAIL"
    };

    let ratios: Vec<String> = ladder
        .iter()
        .zip(&rungs)
        .map(|(n, r)| {
            format!(
                "{n}段→{:.2}s(比值{:.1})",
                secs(r.recover),
                secs(r.recover) / secs(rungs[0].recover).max(1e-9)
            )
        })
        .collect();

    println!(
        "[{verdict}] s16 恢复要多久: 校准 写={put_ms:.2}ms/段 读={get_ms:.2}ms/段 | \
         顺序恢复 {} | 对段数线性={linear} | \
         窗口(同为{big_n}段, 只重放{window}段)={:.3}s vs 全量{:.2}s ⇒ 只重放窗口内={window_exact} 由applied水位决定={window_governs}",
        ratios.join(" "),
        secs(windowed),
        secs(biggest.recover),
    );
    println!(
        "        幕一 造数（并发 {SETUP_CONCURRENCY}，不是被测对象）：{}｜段对象 {seg_bytes}B",
        ladder
            .iter()
            .zip(&rungs)
            .map(|(n, r)| format!("{n}段 {:.1}s", secs(r.setup)))
            .collect::<Vec<_>>()
            .join(" ")
    );
    println!(
        "        幕三 并行预取（{mid_n} 段，并发 {PREFETCH_CONCURRENCY}）= {:.2}s vs 顺序 {:.2}s \
         ⇒ 加速 {:.1}×（loopback 上受 MinIO 服务端吞吐限制，真 S3 上它是**延迟受限**，能拿到更多）",
        secs(prefetched),
        secs(rungs[1].recover),
        secs(rungs[1].recover) / secs(prefetched).max(1e-9),
    );
    println!(
        "        幕四 列举对照（{mid_n} 段）：list 本身 {:.3}s + 逐段读 {:.2}s = {:.2}s \
         ⇒ 捷径不比正路便宜（而且它**会把孤儿段应用进去**，S5 已证）",
        secs(list_only),
        secs(listed),
        secs(list_only + listed),
    );
    println!("        幕五 代入设计的 MAX_UNAPPLIED = {MAX_UNAPPLIED_TXNS} 条事务：");
    println!(
        "          {:>10}  {:>10}  {:>16}  {:>18}  {:>18}  {:>16}",
        "每段条数", "段对象数", "本机顺序", "真S3顺序(30ms/跳)", "真S3并发×32", "failover 总时"
    );
    for per_seg in [1u64, 10, 100, 1000] {
        let objs = MAX_UNAPPLIED_TXNS / per_seg;
        let loop_s = objs as f64 * c_loop / 1000.0;
        let s3_s = objs as f64 * c_s3 / 1000.0;
        let s3_par = s3_s / PREFETCH_CONCURRENCY as f64;
        let failover = FAILOVER_WITHOUT_RECOVERY_SECS + s3_par;
        let flag = if s3_par > 2.0 {
            "  ← 顶破 8~10s"
        } else {
            ""
        };
        println!(
            "          {per_seg:>10}  {objs:>10}  {:>14.2}s  {:>16.1}s  {:>16.2}s  {:>14.1}s{flag}",
            loop_s, s3_s, s3_par, failover
        );
    }
    println!(
        "        ⇒ 设计 §5.4.4 那句「failover ≈ 8~10s」的第三项，实测后是**有条件的**：\n\
         \x20          ① 靠**并行预取**（不并发的话把上表「真S3并发×32」那列乘 32）；\n\
         \x20          ② 靠 §3.3 的攒批参数 —— **每段条数 ≥ 100** 才能顺序也进秒级。\n\
         \x20          ⇒ §3.3 那个「还没定」的攒批参数不是一个吞吐调优项：它直接决定启动窗口，\n\
         \x20            M4 定它的时候要把这张表当约束用。"
    );

    // ------------------------------------------------------------ 自清
    // 顺带量一件事：**恢复和 GC 是同一笔账的两面** —— 要读的段对象数就是要删的段对象数。
    let ct0 = Instant::now();
    let mut deleted = 0usize;
    for &n in &ladder {
        let log = CommitLog::new(&format!("{PREFIX}/run{n}"));
        let keys = store.list(&log.batch_prefix(EPOCH)).await?;
        deleted += keys.len();
        let store_d = store.clone();
        let results = in_flight(keys, SETUP_CONCURRENCY, move |key| {
            let store = store_d.clone();
            Box::pin(async move { store.delete(&key).await })
        })
        .await;
        for r in results {
            r?;
        }
        let _ = store.delete(&log.manifest_key()).await;
    }
    println!(
        "        自清 {deleted} 个段对象耗时 {:.2}s ⇒ 与顺序恢复同量级（同一笔账）：\n\
         \x20          恢复要**读**多少段对象，GC 就要**删**多少段对象。",
        secs(ct0.elapsed())
    );

    Ok(())
}

fn encode_batch(b: &Batch) -> Vec<u8> {
    serde_json::to_vec(b).expect("批量可序列化")
}
