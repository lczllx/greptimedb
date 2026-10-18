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

//! 树节点大小（layout 参数）的实测探针。
//!
//! 回答「节点大小取多少」：候选 64KB / 256KB / 1MB，按 `depth × 单跳延迟`
//! 与 GC 粒度权衡。结论与推导见 `FINDINGS.md` 的 L1。
//!
//! ## 为什么是 bin，不是场景 S9
//!
//! 场景的契约是打印一行 `[PASS]` / `[FAIL]`。这个探针没有通过或不通过 ——
//! 它产出的是**一条曲线加一组系数**。所以照 `etag_probe` 的样子做成独立 bin。
//!
//! ## 它到底测什么
//!
//! **不建树。** 整个决策只依赖一个两常数模型：
//!
//! ```text
//! 单跳      t(N) = a + N/b          a = 请求固定开销（含 RTT），b = 有效带宽
//! 查找      T(N) = hops(N) × t(N)
//! 层数      hops  由叶扇出与内扇出递推（见 tree_shape）
//!            叶扇出 = N / 单条目字节      （叶子存 key + value）
//!            内扇出 = N / 分隔键字节      （内部节点只存 key + 子指针）
//!            叶数   = D / N               ← 与单条目字节无关，两者约掉了
//! ```
//!
//! 需要拟合的只有 `a` 和 `b` 两个数，而它们**与树的结构无关** —— 所以不用等
//! B-link 树写出来就能测。
//!
//! 探针做四件事：
//!
//! 1. 在 MinIO 上扫 N ∈ {16K, 64K, 256K, 1M, 4M}，拟合 `a`、`b`；
//! 2. 注入 RTT 再扫一遍，**验证**「a 原样平移、b 不变」这条可加性假设 ——
//!    这一步是让 loopback 上的数字能外推到真 S3 的**唯一依据**，不能省；
//! 3. 把拟合系数代进树形，打印「候选节点大小 → 查找延迟 / 写放大 / GC 粒度」；
//! 4. 对 D（全树字节数）做敏感性扫描 —— **D 是这里唯一没测准的输入**，
//!    而拐点几乎完全由它决定。
//!
//! ## ⚠️ loopback 的数字不能直接读
//!
//! 本地 `a` 是零点几到几毫秒，真 S3 的 `a` 是 20~50 毫秒。**要看的是 b 和线性度**，
//! 然后把真实的 a 代进去（§4 那张表就是这么算的）。别读 §1 的原始表下结论。
//!
//! ## 跑法
//!
//! ```text
//! cargo run --manifest-path dev/objstore-toy/Cargo.toml --bin layout_probe
//!
//! # 可调（--k=v，逗号分隔的用列表）：
//! #   --iters=100 --sizes=16384,65536,262144,1048576
//! #   --rtts=0,20,50 --dataset-bytes=5242880 --entry-bytes=750 --sep-bytes=48
//! ```

use std::time::{Duration, Instant};

use objstore_toy::cfg;
use objstore_toy::kv::{CasStore, StorageError};

type Result<T> = std::result::Result<T, StorageError>;

/// 探针自建自清的 key 前缀，不碰任何既有数据。
const PREFIX: &str = "toy/probe-layout";

/// 代入树形时用的几个「真实 S3」RTT 档位（毫秒）。
/// 同区 P50 大致 20ms，跨区/公网 50~100ms。
const S3_RTT_MS: [f64; 3] = [20.0, 50.0, 100.0];

/// §4b 敏感性扫描的 D 档位（MB）。
///
/// 之所以要扫：D 是这里**唯一没测准的输入**，而拐点几乎完全由它决定。
/// 低档 ≈ 实测估算（中等规模部署），高档留给「估算错了 50 倍」的情况。
const SENSITIVITY_D_MB: [f64; 4] = [5.0, 16.0, 64.0, 256.0];

// ============================================================================
// 一、参数
// ============================================================================

#[derive(Debug, Clone)]
struct Args {
    /// 要扫的节点大小（字节）。
    sizes: Vec<usize>,
    /// 每个 (N, rtt) 组合采几次。
    iters: usize,
    /// 先跑几次不记数的，把连接池和 MinIO 的读缓存预热掉。
    warmup: usize,
    /// 要注入的 RTT 档位（毫秒）。`0` 就是 loopback 原始延迟。
    rtts_ms: Vec<f64>,
    /// 全树字节数 D —— 代入树形的输入，**不是测出来的**。
    dataset_bytes: f64,
    /// 单条目（key + value）字节数 —— 代入树形的输入，**不是测出来的**。
    entry_bytes: f64,
    /// 内部节点的分隔键 + 子指针占的字节数 —— 代入树形的输入。
    sep_bytes: f64,
}

impl Default for Args {
    fn default() -> Self {
        Self {
            sizes: vec![16 << 10, 64 << 10, 256 << 10, 1 << 20, 4 << 20],
            iters: 60,
            warmup: 3,
            rtts_ms: vec![0.0, 20.0, 50.0],
            // 下面三个默认值来自对 meta 层 key 空间的清点（中等规模部署：
            // ~1000 表 × 10 region、10 datanode）：
            //   D ≈ 5 MB —— 字节数由 __table_info（~1.2KB/表）与
            //               __table_route（~190B/region）主导；
            //   key 数 ≈ 7e3 —— 由 __dn_table 主导，故单条目均摊 ≈ 750 B；
            //   内部节点只存分隔键 + 子指针，约 48 B。
            // 三个都是**估算**，误差方向见 §4b。
            dataset_bytes: 5.0 * 1024.0 * 1024.0,
            entry_bytes: 750.0,
            sep_bytes: 48.0,
        }
    }
}

impl Args {
    fn parse() -> Self {
        let mut a = Args::default();
        for arg in std::env::args().skip(1) {
            let Some((k, v)) = arg.split_once('=') else {
                continue;
            };
            let v = v.replace('_', "");
            match k {
                "--iters" => a.iters = v.parse().unwrap_or(a.iters),
                "--warmup" => a.warmup = v.parse().unwrap_or(a.warmup),
                "--sizes" => a.sizes = parse_list(&v).into_iter().map(|f| f as usize).collect(),
                "--rtts" => a.rtts_ms = parse_list(&v),
                "--dataset-bytes" => a.dataset_bytes = v.parse().unwrap_or(a.dataset_bytes),
                "--entry-bytes" => a.entry_bytes = v.parse().unwrap_or(a.entry_bytes),
                "--sep-bytes" => a.sep_bytes = v.parse().unwrap_or(a.sep_bytes),
                other => eprintln!("（忽略未知参数 `{other}`）"),
            }
        }
        a
    }
}

fn parse_list(s: &str) -> Vec<f64> {
    s.split(',')
        .filter_map(|x| x.trim().parse::<f64>().ok())
        .collect()
}

// ============================================================================
// 二、测量
// ============================================================================

/// 一个 (N, rtt) 组合下的样本，已排序。
struct Sample {
    put_us: Vec<f64>,
    get_us: Vec<f64>,
}

/// 扫一个节点大小：PUT / GET 各 `iters` 次，返回排序后的耗时（微秒）。
///
/// 注入 RTT 的做法是**在计时的区间里 sleep** —— 直接把它当成「这次往返多花了 RTT」。
/// 它不模拟 TCP 窗口增长之类的东西，这一点由 §3 的对照组 + 可加性验证兜底。
async fn sweep(store: &CasStore, n: usize, args: &Args, rtt: Duration) -> Result<Sample> {
    let key = format!("{PREFIX}/n{n}");
    // 可压缩性无关紧要（MinIO/S3 都不压），但内容得是**确定**的，
    // 否则两次跑的 ETag 不同、跨跑比较就没意义了。
    let payload: Vec<u8> = (0..n).map(|i| b'a' + (i % 26) as u8).collect();

    for _ in 0..args.warmup {
        store.put(&key, payload.clone()).await?;
        let _ = store.read(&key).await?;
    }

    let mut put_us = Vec::with_capacity(args.iters);
    let mut get_us = Vec::with_capacity(args.iters);
    for _ in 0..args.iters {
        let p = payload.clone();
        let t0 = Instant::now();
        if !rtt.is_zero() {
            tokio::time::sleep(rtt).await;
        }
        store.put(&key, p).await?;
        put_us.push(t0.elapsed().as_secs_f64() * 1e6);

        let t0 = Instant::now();
        if !rtt.is_zero() {
            tokio::time::sleep(rtt).await;
        }
        let body = store.read(&key).await?;
        get_us.push(t0.elapsed().as_secs_f64() * 1e6);
        // 长度不对说明测的不是同一个东西，宁可当场炸掉。
        assert_eq!(body.len(), n, "读回来 {n} 字节的对象的长度不对");
    }

    let _ = store.delete(&key).await; // 自清，保证可重复跑
    put_us.sort_by(f64::total_cmp);
    get_us.sort_by(f64::total_cmp);
    Ok(Sample { put_us, get_us })
}

/// 对照组：单独给 `sleep(rtt)` 计时。
///
/// **没有这个对照，「可加性成立」就是在用它自己证明自己** —— 定时器晚醒多少，
/// 会原样混进「实测差」里，然后被容忍度吃掉。第一次跑就撞上了：注入 20ms
/// 实测差是 22.4ms，多出来的 2.4ms 不是存储的，是 sleep 自己的开销。
async fn measure_sleep(rtt: Duration, iters: usize) -> f64 {
    let mut us = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        tokio::time::sleep(rtt).await;
        us.push(t0.elapsed().as_secs_f64() * 1e6);
    }
    us.sort_by(f64::total_cmp);
    med(&us)
}

fn pct(sorted: &[f64], p: f64) -> f64 {
    if sorted.is_empty() {
        return f64::NAN;
    }
    let idx = (((sorted.len() - 1) as f64) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn med(sorted: &[f64]) -> f64 {
    pct(sorted, 0.5)
}

/// 最小二乘 `y = intercept + slope·x`，返回 `(slope, intercept, r²)`。
fn fit(xs: &[f64], ys: &[f64]) -> (f64, f64, f64) {
    let n = xs.len() as f64;
    if n < 2.0 {
        return (0.0, ys.first().copied().unwrap_or(f64::NAN), f64::NAN);
    }
    let mx = xs.iter().sum::<f64>() / n;
    let my = ys.iter().sum::<f64>() / n;
    let mut sxy = 0.0;
    let mut sxx = 0.0;
    for (x, y) in xs.iter().zip(ys) {
        sxy += (x - mx) * (y - my);
        sxx += (x - mx) * (x - mx);
    }
    let slope = if sxx == 0.0 { 0.0 } else { sxy / sxx };
    let intercept = my - slope * mx;
    let mut ss_res = 0.0;
    let mut ss_tot = 0.0;
    for (x, y) in xs.iter().zip(ys) {
        let pred = intercept + slope * x;
        ss_res += (y - pred) * (y - pred);
        ss_tot += (y - my) * (y - my);
    }
    let r2 = if ss_tot == 0.0 {
        1.0
    } else {
        1.0 - ss_res / ss_tot
    };
    (slope, intercept, r2)
}

// ============================================================================
// 三、树形（纯算术，不碰存储）
// ============================================================================

/// 树形结果。
struct Shape {
    /// 叶扇出 = N / 单条目字节。
    leaf_fanout: f64,
    /// 内扇出 = N / 分隔键字节。
    int_fanout: f64,
    /// 叶数 = D / N —— **与单条目字节数无关**（分子分母里的 entry_bytes 约掉了）。
    ///
    /// 这有点反直觉，但它正是「节点越大层数越浅」的完整来源。
    leaves: f64,
    /// 从根到叶要读几个对象。单叶树是 1，根 + 叶是 2。
    hops: u32,
    /// 叶扇出为 0 时（N < 单条目）标记出来，别静默出错数。
    degenerate: bool,
}

fn tree_shape(dataset_bytes: f64, node_bytes: f64, entry_bytes: f64, sep_bytes: f64) -> Shape {
    let leaf_fanout = (node_bytes / entry_bytes).floor();
    let int_fanout = (node_bytes / sep_bytes).floor().max(2.0);
    let leaves = (dataset_bytes / node_bytes).ceil().max(1.0);

    let degenerate = leaf_fanout < 2.0;
    let mut level = leaves;
    let mut hops = 1u32;
    while level > 1.0 {
        level = (level / int_fanout).ceil();
        hops += 1;
        if hops > 64 {
            break; // 防御：参数荒谬时别转死
        }
    }
    Shape {
        leaf_fanout,
        int_fanout,
        leaves,
        hops,
        degenerate,
    }
}

/// **两跳**（根 + 叶，即 hops = 2）能撑到的最大 D。
///
/// 条件是「叶数 ≤ 根的内扇出」，而叶数 = D/N、内扇出 = N/分隔键字节：
///
/// ```text
/// D/N ≤ N/sep  ⟺  D ≤ N² / sep
/// ```
///
/// ⚠️ 用的是**内扇出**而不是叶扇出 —— 根节点存的是分隔键 + 子指针，不是 key+value。
/// 第一次写成了叶扇出，把 16KB 的上界从 5.6MB 误报成 336KB（差 16 倍）。
/// 这条在 §4b 的矩阵里能直接对出来：16KB 在 D=5MB 是 hops 2、D=16MB 是 hops 3，
/// 拐点就该落在 5.6MB 附近。
fn d_max_for_two_hops(node_bytes: f64, sep_bytes: f64) -> f64 {
    let int_fanout = (node_bytes / sep_bytes).floor().max(2.0);
    node_bytes * int_fanout
}

// ============================================================================
// 四、序列化 CPU（第三项，也是唯一会把 N 压下来的项之一）
// ============================================================================

#[derive(serde::Serialize, serde::Deserialize)]
struct Entry {
    k: String,
    v: String,
}

/// 造一个总大小约 `n` 字节的节点（JSON 形状）。
///
/// 用 JSON 是因为 toy 只带这个依赖。**JSON 对 CPU 是悲观上界**（真实现大概率用
/// bincode/protobuf，快得多也小得多），所以这一节只用来判断「CPU 项随 N 是否线性、
/// 在 RTT 面前能不能忽略」，不能拿它的绝对值去和 I/O 项比。
fn synth_entries(n: usize, entry_bytes: f64) -> Vec<Entry> {
    let count = ((n as f64) / entry_bytes).round().max(1.0) as usize;
    // `{"k":"","v":""}` 是 15 字节骨架。
    let per_field = (((entry_bytes - 15.0) / 2.0).floor()).max(1.0) as usize;
    (0..count)
        .map(|i| Entry {
            k: (0..per_field)
                .map(|j| (b'a' + ((i + j) % 26) as u8) as char)
                .collect(),
            v: (0..per_field)
                .map(|j| (b'0' + ((i + j) % 10) as u8) as char)
                .collect(),
        })
        .collect()
}

/// 返回 `(实际序列化字节数, ser 中位微秒, deser 中位微秒)`。
///
/// 反序列化给得比序列化重，是因为**查找路径上只有 deser** ——
/// ser 只在写回那一次需要。所以把 deser 单独拎出来和 RTT 比。
fn measure_ser(n: usize, entry_bytes: f64, iters: usize) -> (usize, f64, f64) {
    let entries = synth_entries(n, entry_bytes);

    let warm = serde_json::to_vec(&entries).expect("可序列化");
    let _: Vec<Entry> = serde_json::from_slice(&warm).expect("可反序列化");

    let mut ser_us = Vec::with_capacity(iters);
    let mut de_us = Vec::with_capacity(iters);
    let mut size = 0usize;
    for _ in 0..iters {
        let t0 = Instant::now();
        let buf = serde_json::to_vec(&entries).expect("可序列化");
        ser_us.push(t0.elapsed().as_secs_f64() * 1e6);
        size = buf.len();

        let t0 = Instant::now();
        let back: Vec<Entry> = serde_json::from_slice(&buf).expect("可反序列化");
        de_us.push(t0.elapsed().as_secs_f64() * 1e6);
        std::hint::black_box(&back);
    }
    ser_us.sort_by(f64::total_cmp);
    de_us.sort_by(f64::total_cmp);
    (size, med(&ser_us), med(&de_us))
}

// ============================================================================
// 五、主流程
// ============================================================================

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let store = CasStore::new(cfg::build_operator().map_err(StorageError::Opendal)?);

    println!("## 节点大小（layout 参数）实测探针");
    println!(
        "iters={} sizes={:?} rtts={:?}ms",
        args.iters, args.sizes, args.rtts_ms
    );

    // ---- §1 扫描：每个 rtt 档位各扫一遍 -------------------------------------
    let mut per_rtt: Vec<Vec<Sample>> = Vec::new();
    for &rtt_ms in &args.rtts_ms {
        let rtt = Duration::from_secs_f64(rtt_ms / 1000.0);
        let mut row = Vec::new();
        for &n in &args.sizes {
            row.push(sweep(&store, n, &args, rtt).await?);
        }
        per_rtt.push(row);
    }

    print_raw(&args, &per_rtt);

    // ---- §2 拟合（只用 rtt=0 那一档）---------------------------------------
    let base = &per_rtt[0];
    let xs: Vec<f64> = args.sizes.iter().map(|&n| n as f64).collect();
    let put_ys: Vec<f64> = base.iter().map(|s| med(&s.put_us)).collect();
    let get_ys: Vec<f64> = base.iter().map(|s| med(&s.get_us)).collect();
    let (put_slope, put_a, put_r2) = fit(&xs, &put_ys);
    let (get_slope, get_a, get_r2) = fit(&xs, &get_ys);

    println!();
    println!("=== 2. 拟合 t(N) = a + N/b  （只用 rtt=0 那一档）===");
    report_fit("PUT", put_a, put_slope, put_r2);
    report_fit("GET", get_a, get_slope, get_r2);
    println!("  查找只读，**决策用 GET 这一行**。");
    // 交叉检验：最小 N 的 p50 里传输项几乎为零，所以它本身就是 a 的一个锚点。
    // 拟合值若明显大于它，说明模型有轻微曲率（=a 被大 N 那端拉高了）。
    let anchor = med(&base[0].get_us);
    let anchor_transfer = get_slope * args.sizes[0] as f64;
    println!(
        "  交叉检验：最小 N（{}）的实测 p50 = {:.3} ms，其中传输项只有 {:.4} ms",
        fmt_bytes(args.sizes[0]),
        anchor / 1000.0,
        anchor_transfer / 1000.0
    );
    println!(
        "  ⇒ 它约等于纯 a = {:.3} ms，拟合值 {:.3} ms {}。斜率（b）不受这点曲率影响。",
        anchor / 1000.0,
        get_a / 1000.0,
        if get_a > anchor {
            "偏大（大 N 端把截距拉高了）"
        } else {
            "偏小"
        }
    );

    // ---- §3 可加性验证（带对照组）------------------------------------------
    println!();
    println!("=== 3. 可加性验证（注入 RTT 后 a 是否原样平移）===");
    println!("  这一节是 loopback 数字能外推到真 S3 的**唯一依据**，不是可选的。");
    println!("  残差 = 实测差 − 纯 sleep 实测中位。**只有残差小，才叫可加性成立。**");
    println!(
        "  {:>6}  {:>12}  {:>7}  {:>11}  {:>11}  {:>10}  判定",
        "注入", "纯sleep实测", "N", "实测 get 差", "应扣掉的", "残差"
    );
    let mut additive_ok = true;
    let mut worst_resid = 0.0f64;
    for (ri, &rtt_ms) in args.rtts_ms.iter().enumerate().skip(1) {
        let rtt = Duration::from_secs_f64(rtt_ms / 1000.0);
        let sleep_med = measure_sleep(rtt, args.iters).await;
        for (si, &n) in args.sizes.iter().enumerate() {
            let got = med(&per_rtt[ri][si].get_us) - med(&base[si].get_us);
            let resid = got - sleep_med;
            let tol = (rtt_ms * 1000.0 * 0.10).max(500.0);
            let ok = resid.abs() <= tol;
            additive_ok &= ok;
            worst_resid = worst_resid.max(resid.abs());
            println!(
                "  {:>4.0}ms  {:>10.3}ms  {:>7}  {:>9.3}ms  {:>9.3}ms  {:>8.3}ms  {}",
                rtt_ms,
                sleep_med / 1000.0,
                fmt_bytes(n),
                got / 1000.0,
                sleep_med / 1000.0,
                resid / 1000.0,
                if ok { "ok" } else { "← 偏离" }
            );
        }
    }
    println!(
        "  ⇒ 可加性{}（最大残差 {:.3} ms）。{}",
        if additive_ok {
            "成立"
        } else {
            "**不成立**"
        },
        worst_resid / 1000.0,
        if additive_ok {
            "可以把 §4 的表当真 S3 的预测读。"
        } else {
            "**§4 的表不可信** —— 得挂真延迟（toxiproxy / tc netem）重测，别外推。"
        }
    );

    // ---- §4 代入树形 -------------------------------------------------------
    println!();
    println!("=== 4. 代入树形（这是本探针的产出）===");
    println!(
        "  输入（**假设**，不是测出来的）：全树 D = {}，单条目 {} B，分隔键 {} B",
        fmt_bytes(args.dataset_bytes as usize),
        args.entry_bytes,
        args.sep_bytes
    );
    println!("    叶数 = D/N（与单条目大小无关）；叶扇出 = N/单条目；内扇出 = N/分隔键。");
    println!();
    print!(
        "  {:>7}  {:>7}  {:>7}  {:>7}  {:>5}  {:>10}  {:>9}  {:>9}  {:>9}",
        "N", "叶扇出", "内扇出", "叶数", "hops", "T@loopback", "T@20ms", "T@50ms", "T@100ms"
    );
    println!();
    for &n in &args.sizes {
        let sh = tree_shape(
            args.dataset_bytes,
            n as f64,
            args.entry_bytes,
            args.sep_bytes,
        );
        let hops = sh.hops as f64;
        let t_local = hops * (get_a + get_slope * n as f64) / 1000.0;
        let ts: Vec<f64> = S3_RTT_MS
            .iter()
            .map(|&r| hops * (get_a + r * 1000.0 + get_slope * n as f64) / 1000.0)
            .collect();
        println!(
            "  {:>7}  {:>7.0}  {:>7.0}  {:>7.0}  {:>5}  {:>8.3}ms  {:>7.2}ms  {:>7.2}ms  {:>7.2}ms{}",
            fmt_bytes(n),
            sh.leaf_fanout,
            sh.int_fanout,
            sh.leaves,
            sh.hops,
            t_local,
            ts[0],
            ts[1],
            ts[2],
            if sh.degenerate { "  ← N 装不下一个条目！" } else { "" }
        );
    }

    // ---- §4b D 的敏感性 ----------------------------------------------------
    println!();
    println!("=== 4b. 对 D 的敏感性（D 是唯一没测准的输入，拐点几乎全由它决定）===");
    println!(
        "  表体 = hops（读几个对象）。hops 每多 1，就在 T@20ms 上加约 {:.0}ms。",
        20.0 + get_a / 1000.0
    );
    print!("  {:>10}", "D \\ N");
    for &n in &args.sizes {
        print!("  {:>7}", fmt_bytes(n));
    }
    println!();
    for &d_mb in &SENSITIVITY_D_MB {
        let d = d_mb * 1024.0 * 1024.0;
        print!("  {:>10}", format!("{d_mb:.0}MB"));
        for &n in &args.sizes {
            let sh = tree_shape(d, n as f64, args.entry_bytes, args.sep_bytes);
            print!("  {:>7}", sh.hops);
        }
        println!();
    }
    println!();
    println!("  每个 N 的硬上界（叶数超过**内**扇出就要多一跳）：");
    for &n in &args.sizes {
        println!(
            "    {:>7}  2 跳撑到 D ≤ {}（= N² / 分隔键字节）",
            fmt_bytes(n),
            fmt_bytes(d_max_for_two_hops(n as f64, args.sep_bytes) as usize)
        );
    }

    // ---- §5 序列化 CPU -----------------------------------------------------
    println!();
    println!("=== 5. 序列化 CPU（JSON，悲观上界）===");
    println!("  查找路径上只有 deser，所以拿 deser×hops 去和 T@20ms 比 —— 占比高才值得管。");
    println!(
        "  {:>7}  {:>9}  {:>10}  {:>11}  {:>13}  {:>12}",
        "N", "实际字节", "ser 中位", "deser 中位", "查找 CPU(deser×hops)", "占 T@20ms"
    );
    for &n in &args.sizes {
        let (size, ser, de) = measure_ser(n, args.entry_bytes, args.iters.min(50));
        let sh = tree_shape(
            args.dataset_bytes,
            n as f64,
            args.entry_bytes,
            args.sep_bytes,
        );
        let hops = sh.hops as f64;
        let lookup_cpu = de * hops;
        let t20 = hops * (get_a + 20_000.0 + get_slope * n as f64);
        println!(
            "  {:>7}  {:>9}  {:>8.3}ms  {:>9.3}ms  {:>11.3}ms  {:>11.1}%",
            fmt_bytes(n),
            fmt_bytes(size),
            ser / 1000.0,
            de / 1000.0,
            lookup_cpu / 1000.0,
            lookup_cpu / t20 * 100.0
        );
    }
    println!("  真实现用 bincode/protobuf 会显著更快更小；这一节的用处是判断**它能不能忽略**，");
    println!("  不是拿 JSON 的绝对值去和 I/O 项比。占比是个位数就当作可忽略。");

    println!();
    println!("=== 怎么读 ===");
    println!("  1. 先看 §3 的**残差**列。不成立就停在这里，去挂真延迟重测，别往下读。");
    println!("  2. 再看 §4 的 T@20ms 一列 —— 那是真 S3 的量级。找它在哪一档变平。");
    println!("     变平之后再增大 N，只增加传输、不减少 hops。");
    println!("  3. **别急着按 §4.5 那句「写放大最小的 N」选**：S3 的 PUT 是按请求计费、");
    println!("     不按字节，所以写放大（=N）在这里几乎不花钱；真正随 N 变差的是");
    println!("     §5 的 CPU 和 GC 粒度。");
    println!("  4. 决定性的一步在 §4b：D 只要估错，拐点就整体平移。挑那个在**整张矩阵里");
    println!("     hops 都没变差、且硬上界留了足够余量**的 N —— 也就是拿字节换鲁棒性。");
    println!("  5. 结论写进 FINDINGS；节点大小做成配置项，默认值取第 4 步选出的 N。");

    Ok(())
}

fn report_fit(label: &str, a: f64, slope: f64, r2: f64) {
    let b_mb_s = if slope > 0.0 {
        1.0 / slope
    } else {
        f64::INFINITY
    };
    println!(
        "  {label}:  a = {:.3} ms   b = {:.1} MB/s   (斜率 {:.6} µs/B)   r² = {:.4}",
        a / 1000.0,
        b_mb_s,
        slope,
        r2
    );
}

fn print_raw(args: &Args, per_rtt: &[Vec<Sample>]) {
    println!();
    println!("=== 1. 原始测量 ===");
    println!("  ⚠️ loopback 上的绝对值不代表 S3，只看形状和带宽。");
    for (ri, &rtt_ms) in args.rtts_ms.iter().enumerate() {
        println!("  --- 注入 RTT = {rtt_ms:.0} ms ---");
        println!(
            "  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>9}  {:>9}",
            "N", "put p50", "put p99", "get p50", "get p99", "put MB/s", "get MB/s"
        );
        for (si, &n) in args.sizes.iter().enumerate() {
            let s = &per_rtt[ri][si];
            let p50 = med(&s.put_us);
            let g50 = med(&s.get_us);
            // 按**原始 p50** 折算，所以小 N 的 MB/s 会被固定开销压得很难看 —— 那是正常的。
            let put_bw = (n as f64) / p50; // B/µs == MB/s
            let get_bw = (n as f64) / g50;
            println!(
                "  {:>8}  {:>8.2}ms  {:>8.2}ms  {:>8.2}ms  {:>8.2}ms  {:>9.1}  {:>9.1}",
                fmt_bytes(n),
                p50 / 1000.0,
                pct(&s.put_us, 0.99) / 1000.0,
                g50 / 1000.0,
                pct(&s.get_us, 0.99) / 1000.0,
                put_bw,
                get_bw
            );
        }
    }
}

fn fmt_bytes(n: usize) -> String {
    let n = n as f64;
    if n >= 1024.0 * 1024.0 * 1024.0 {
        format!("{:.1}GB", n / (1024.0 * 1024.0 * 1024.0))
    } else if n >= 1024.0 * 1024.0 {
        format!("{:.1}MB", n / (1024.0 * 1024.0))
    } else if n >= 1024.0 {
        format!("{:.0}KB", n / 1024.0)
    } else {
        format!("{n:.0}B")
    }
}
