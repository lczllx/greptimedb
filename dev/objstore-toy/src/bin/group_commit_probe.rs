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

//! **V6 —— group commit 的请求分摊**（`objstore-toy-plan.md` §十二）。
//!
//! **不是场景**：没有 PASS/FAIL，产出的是**数字**（照 `layout_probe` 的先例）。
//! 唯一会拦人的是最后的**前提**行：攒批臂若组大小恒为 1，
//! 那这一趟压根没攒到东西，账面比值再好看也是空的。
//!
//! ## 三条臂只差两个参数
//!
//! | 臂 | 是什么 | 每 txn 请求数（预期） |
//! | --- | --- | --- |
//! | `逐条·每次读` | `CommitLog::commit` 原样，加锁串行化 | 4（put + cas + stat + get） |
//! | `逐条·持 etag` | 攒批器 `max_group = 1` | 2（put + cas） |
//! | `攒批` | 攒批器 `max_group = N` | 2 / 组大小 |
//!
//! 「逐条·每次读」→「逐条·持 etag」差的是**读不读 Manifest**（真 leader 持着 etag），
//! 「逐条·持 etag」→「攒批」差的是**攒不攒批**。两笔账分开算，
//! 免得把"缓存 etag"的功劳记到"攒批"头上。
//!
//! ## 每条臂都自己核对正确性
//!
//! 攒批是**合并**动作，最坏的结果是"请求数很漂亮，但丢了几个 txn"。所以每条臂跑完都要：
//! ① 每个 txn 都拿到回执；② `committed_lsn` == 组数；③ 从 Manifest 重放出全部写、且没有缺段。
//! 不成立时该臂标 `!!` 并把问题打出来，而不是"数字照看"。

use std::time::{Duration, Instant};

use objstore_toy::cfg;
use objstore_toy::commit::{Batch, CommitLog, CommitOutcome};
use objstore_toy::group_commit::{median, percentile, GroupCommit};
use objstore_toy::kv::{
    reset_store_ops, store_ops, store_ops_delta, store_ops_total, CasStore, StorageError, StoreOp,
};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

type Result<T> = std::result::Result<T, StorageError>;
/// 一个提交任务的产出：自己的延迟 + 结果（外层 `Err` = 任务本身没跑完）。
type TaskOut = (Duration, std::result::Result<Result<u64>, String>);

const PREFIX: &str = "toy/v6";
const EPOCH: u64 = 1;
const NS: [usize; 3] = [1, 8, 64];
const WINDOWS_MS: [u64; 2] = [0, 2];
/// 摊开到达的间隔（微秒），差一个数量级 —— 用来分开"窗口"和"到达速率"两笔账。
const STAGGERS_US: [u64; 2] = [200, 2000];

/// 一条臂跑完之后的全部事实。
struct Arm {
    kind: Kind,
    n: usize,
    window_ms: Option<u64>,
    groups: usize,
    median_group: f64,
    max_group: usize,
    /// 提交路径上的请求，**已经减掉启动时读 Manifest 的那 2 次**。
    ops: [(StoreOp, usize); 6],
    /// 启动固定开销（每条臂都为读 Manifest 花的，不计入每 txn 请求数）。
    fixed_ops: usize,
    /// 每个 txn 摊到的请求数。
    per_txn: f64,
    p50_ms: f64,
    p99_ms: f64,
    wall: Duration,
    /// 拿到回执的 txn 数。
    acked: usize,
    /// 核对里发现的问题（空 = 这条臂的结论成立）。
    problems: Vec<String>,
}

/// 臂的种类。比按名字比对稳：名字是给人看的，可以改。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    /// 逐条，每次提交都重读 Manifest（4 请求/txn）。
    PerTxnRead,
    /// 逐条，持 etag（2 请求/txn）。
    PerTxnHeld,
    /// 攒批。`stagger_us` 是提交者之间的到达间隔（**微秒**）—— 0 = 一起到。
    Batched { stagger_us: u64 },
}

impl Arm {
    fn label(&self) -> String {
        match self.kind {
            Kind::PerTxnRead => "逐条·每次读".to_string(),
            Kind::PerTxnHeld => "逐条·持 etag".to_string(),
            Kind::Batched { stagger_us: 0 } => {
                format!("攒批·齐到·{}ms", self.window_ms.unwrap_or(0))
            }
            Kind::Batched { stagger_us } => {
                format!("攒批·摊开{stagger_us}µs·{}ms", self.window_ms.unwrap_or(0))
            }
        }
    }

    fn ok(&self) -> bool {
        self.problems.is_empty()
    }

    fn op(&self, op: StoreOp) -> usize {
        self.ops
            .iter()
            .find(|(o, _)| *o == op)
            .map(|(_, n)| *n)
            .unwrap_or(0)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let store = CasStore::new(cfg::build_operator().map_err(StorageError::Opendal)?);

    println!("[V6] group commit 的请求分摊 —— 并发提交 {NS:?}，量每 txn 的实际请求数");
    println!();
    println!("口径");
    println!("  请求 = toy 对 store 的一次调用（≈ 一次 HTTP 往返；`list` 翻页时不止一次）。");
    println!("  提交路径 = PUT（日志段）+ CAS（Manifest）；「逐条·每次读」另加 stat+get 各一次。");
    println!("  每条臂开头清场、重建 Manifest（committed_lsn = 0），");
    println!("  并减掉启动时读 Manifest 的那 2 次 —— 否则 N = 1 时它占掉一半。");

    let mut arms = Vec::new();
    for n in NS {
        arms.push(run_per_txn_read(n).await?);
        arms.push(run_batched(n, 1, None, 0).await?);
        for w in WINDOWS_MS {
            arms.push(run_batched(n, usize::MAX, Some(w), 0).await?);
        }
    }
    // 摊开到达：提交者一个接一个来。两个间隔差一个数量级 ——
    // 齐到的臂里"攒到"是白送的（同伴本来就排在队列里），只有到达被摊开，
    // 才看得出**摊销倍数到底由什么决定**（是窗口，还是到达速率本身）。
    for stagger_us in STAGGERS_US {
        for w in WINDOWS_MS {
            arms.push(run_batched(64, usize::MAX, Some(w), stagger_us).await?);
        }
    }

    println!();
    print_table(&arms);
    println!();
    print_correctness(&arms);
    println!();
    print_verdict(&arms);

    // 自清：探针也自己收场 —— 清完**再列一遍**核对（见 P19 / P21）。
    let log = CommitLog::new(PREFIX);
    log.wipe(&store, EPOCH).await?;
    let left = store.list(&format!("{PREFIX}/")).await?;
    println!();
    println!(
        "[V6] 自清：{} 个对象{}｜瞬态重试 {} 次（重试发生在账本之下，会拉长墙钟但不计入请求数）",
        left.len(),
        if left.is_empty() {
            String::new()
        } else {
            format!("（残留 {:?}）", &left[..left.len().min(3)])
        },
        cfg::transient_retries()
    );
    Ok(())
}

// ============================================================================
// 臂一：逐条提交，每次读 Manifest
// ============================================================================

/// `CommitLog::commit` 的原样路径：落段、**重读** Manifest、CAS。
///
/// 并发调用它必须自己加锁 —— 否则 N 个写者会拿同一个 etag 去 CAS，只有一个能赢，
/// 而输的那些的段已经落盘了（S6 的「同一 (epoch, lsn) 内容必须唯一」那条约束）。
/// 这里加锁 = 「leader 内部串行化」，是**没有攒批时最客气的做法**：
/// 请求数照旧线性增长，但至少不互撞。
async fn run_per_txn_read(n: usize) -> Result<Arm> {
    let store = CasStore::new(cfg::build_operator().map_err(StorageError::Opendal)?);
    let log = CommitLog::new(PREFIX);
    log.wipe(&store, EPOCH).await?;
    log.init(&store, EPOCH).await?;

    let gate = std::sync::Arc::new(Mutex::new(0u64));
    reset_store_ops();
    let fixed = store_ops();
    let started = Instant::now();

    let mut tasks = Vec::with_capacity(n);
    for i in 0..n {
        let store = store.clone();
        let gate = gate.clone();
        tasks.push(tokio::spawn(async move {
            let t0 = Instant::now();
            // **整段提交都在这把锁里**，锁里存的是 lsn 计数。
            //
            // 只锁计数是不够的：lsn 分配对了，段和 CAS 仍会互撞 ——
            // N 个写者都读同一份 Manifest、都去 CAS，只有一个能赢，
            // 而输的那些**段已经落盘了**（同一 (epoch, lsn) 被写了两份内容，
            // 正是 S6 那条「同一 lsn 内容必须唯一」的约束）。这条臂要量的是
            // 「每次提交都重读 Manifest 的成本」，不是并发互撞 —— 后者是 S6 的题目。
            let mut held = gate.lock().await;
            *held += 1;
            let lsn = *held;
            let res: Result<u64> = async {
                let log = CommitLog::new(PREFIX);
                match log.commit(&store, EPOCH, lsn, &txn_batch(i)).await? {
                    CommitOutcome::Committed { lsn, .. } => Ok(lsn),
                    // 串行化之后不该输；真输了就是"这条臂的假设不成立"，要报出来。
                    CommitOutcome::LostRace => Err(StorageError::NotFound(
                        "输了 CAS（加锁后不该发生）".to_string(),
                    )),
                }
            }
            .await;
            drop(held); // 锁活到这里：它跨着整段提交才叫"串行化"
            (t0.elapsed(), Ok(res))
        }));
    }
    let results = join_all(tasks).await;
    let wall = started.elapsed();
    let after = store_ops();

    let mut arm = Arm {
        kind: Kind::PerTxnRead,
        n,
        window_ms: None,
        // 逐条：一个 txn 一个 lsn 一个段。
        groups: n,
        median_group: 1.0,
        max_group: 1,
        ops: store_ops_delta(&fixed, &after),
        fixed_ops: store_ops_total(&fixed),
        per_txn: 0.0,
        p50_ms: 0.0,
        p99_ms: 0.0,
        wall,
        acked: 0,
        problems: Vec::new(),
    };
    finish(&mut arm, results);
    // 重放核对要在**账本快照之后**做，否则它的 stat+get 会漏进每 txn 请求数。
    arm.problems.extend(check_replay(&store, n).await);
    Ok(arm)
}

// ============================================================================
// 臂二 / 臂三：同一条攒批器，只改 max_group
// ============================================================================

async fn run_batched(
    n: usize,
    max_group: usize,
    window_ms: Option<u64>,
    stagger_us: u64,
) -> Result<Arm> {
    let store = CasStore::new(cfg::build_operator().map_err(StorageError::Opendal)?);
    let log = CommitLog::new(PREFIX);
    log.wipe(&store, EPOCH).await?;
    log.init(&store, EPOCH).await?;

    let window = Duration::from_millis(window_ms.unwrap_or(0));
    reset_store_ops();
    let spawned = GroupCommit::spawn(store.clone(), PREFIX.to_string(), EPOCH, window, max_group);
    // 等攒批器把 Manifest 载进来；那 2 次读是启动固定开销，不算提交路径的账。
    spawned
        .ready
        .await
        .map_err(|_| StorageError::NotFound("攒批任务没载入 Manifest 就退出了".to_string()))?;
    let fixed = store_ops();

    let started = Instant::now();
    let handle = spawned.commit.clone();
    let mut tasks = Vec::with_capacity(n);
    for i in 0..n {
        let commit = handle.clone();
        // 摊开到达：第 i 个提交者晚 `i × stagger` 才来。间隔只有**本地**的量级
        // （没有 I/O），所以它是"提交速率"的一个纯参数，不掺网络抖动。
        let delay = Duration::from_micros(stagger_us * i as u64);
        tasks.push(tokio::spawn(async move {
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
            let t0 = Instant::now();
            let res = commit.submit(txn_batch(i)).await;
            // 外层 `Ok`：任务本身跑完了（没跑完由 `join_all` 填 `Err`）。
            (t0.elapsed(), Ok(res))
        }));
    }
    let results = join_all(tasks).await;
    let wall = started.elapsed();

    // 所有提交句柄都掉了 ⇒ 通道关闭 ⇒ 攒批任务收工交账。
    drop(handle);
    drop(spawned.commit);
    let group_log = spawned
        .done
        .await
        .map_err(|e| StorageError::NotFound(format!("攒批任务 panic: {e}")))??;
    let after = store_ops();

    // 种类如实反映参数：`max_group = 1` 量到的根本不是"攒批"。
    let kind = if max_group == 1 {
        Kind::PerTxnHeld
    } else {
        Kind::Batched { stagger_us }
    };
    let mut arm = Arm {
        kind,
        n,
        window_ms,
        groups: group_log.groups(),
        median_group: group_log.median_group_size(),
        max_group: group_log.max_group_size(),
        ops: store_ops_delta(&fixed, &after),
        fixed_ops: store_ops_total(&fixed),
        per_txn: 0.0,
        p50_ms: 0.0,
        p99_ms: 0.0,
        wall,
        acked: 0,
        problems: Vec::new(),
    };
    // 攒批器自己记的"每组发了几个请求"必须和账本对得上 ——
    // 对不上就说明有人在账本之外发请求（这正是"写死的结论"要防的事，见 P24）。
    let logged: usize = group_log.per_group_ops.iter().map(|(p, c)| p + c).sum();
    if logged != store_ops_total(&arm.ops) {
        arm.problems.push(format!(
            "攒批器记的请求数 {logged} != 账本记的 {}",
            store_ops_total(&arm.ops)
        ));
    }
    finish(&mut arm, results);
    // `committed_lsn` 数的是**组数**，不是 txn 数 —— 攒批把这个等式改了，要钉住。
    match log.read_manifest(&store).await? {
        Some((m, _)) if m.committed_lsn as usize == arm.groups => {}
        Some((m, _)) => arm.problems.push(format!(
            "committed_lsn {} != 组数 {}",
            m.committed_lsn, arm.groups
        )),
        None => arm.problems.push("Manifest 不见了".to_string()),
    }
    arm.problems.extend(check_replay(&store, n).await);
    Ok(arm)
}

// ============================================================================
// 共用：回执盘点 + 正确性核对
// ============================================================================

/// 回执盘点。`results` 是「这条 txn 的延迟 + 提交结果」。
fn finish(arm: &mut Arm, results: Vec<TaskOut>) {
    let mut latencies = Vec::with_capacity(results.len());
    let mut lsns = Vec::new();
    for (elapsed, res) in results {
        latencies.push(elapsed.as_secs_f64() * 1000.0);
        match res {
            Ok(Ok(lsn)) => {
                lsns.push(lsn);
                arm.acked += 1;
            }
            Ok(Err(e)) => arm.problems.push(format!("提交失败：{e}")),
            Err(e) => arm.problems.push(format!("任务没跑完：{e}")),
        }
    }
    arm.per_txn = store_ops_total(&arm.ops) as f64 / arm.n as f64;
    arm.p50_ms = median(&latencies);
    arm.p99_ms = percentile(&latencies, 0.99);
    if arm.acked != arm.n {
        arm.problems
            .push(format!("只有 {}/{} 个 txn 拿到回执", arm.acked, arm.n));
    }
    // 回执的 lsn 必须落在 (0, 组数] 里：越界意味着水位和段对不上。
    if lsns.iter().any(|&l| l == 0 || l as usize > arm.groups) {
        arm.problems.push(format!("有回执的 lsn 越界：{lsns:?}"));
    }
}

/// 从 Manifest 重放，核对"每个 txn 的写都真正生效了"。
///
/// 这是攒批最该被问的问题：**合并有没有把谁的写吃掉**。
/// 每个 txn 写一个互不相同的 key ⇒ 丢一个立刻就看得出来。
async fn check_replay(store: &CasStore, n: usize) -> Vec<String> {
    let log = CommitLog::new(PREFIX);
    let mut problems = Vec::new();
    match log.recover_by_manifest(store).await {
        Ok(rec) => {
            if !rec.missing.is_empty() {
                problems.push(format!("水位说生效但段不见了：{:?}", rec.missing));
            }
            for i in 0..n {
                let (k, v) = txn_batch(i).remove(0);
                match rec.state.get(&k) {
                    Some(got) if *got == v => {}
                    Some(got) => problems.push(format!("{k} 的值是 {got}，期望 {v}")),
                    None => problems.push(format!("{k} 没被重放出来")),
                }
            }
            if rec.state.len() != n {
                problems.push(format!("重放出 {} 个 key，期望 {n} 个", rec.state.len()));
            }
        }
        Err(e) => problems.push(format!("重放失败：{e}")),
    }
    problems
}

/// 第 `i` 个 txn 的写：一个 key、一个值。
fn txn_batch(i: usize) -> Batch {
    vec![(format!("k/{i:04}"), format!("v{i}"))]
}

/// 按序收工。任务本身是并发跑的，这里只是**收结果**（顺序 await 不会让它们串行）。
async fn join_all(tasks: Vec<JoinHandle<TaskOut>>) -> Vec<TaskOut> {
    let mut out = Vec::with_capacity(tasks.len());
    for t in tasks {
        match t.await {
            Ok(v) => out.push(v),
            Err(e) => out.push((Duration::ZERO, Err(format!("join 失败：{e}")))),
        }
    }
    out
}

// ============================================================================
// 输出
// ============================================================================

fn print_table(arms: &[Arm]) {
    println!("N    臂                  组数   组大小(中位/最大)  请求 put/cas/stat/get   每 txn 请求数  p50      p99      墙钟");
    println!("---  ------------------  -----  ----------------  ---------------------  ------------  -------  -------  -------");
    for a in arms {
        println!(
            "{:<4} {:<19} {:<6} {:<17} {:<22} {:<13} {:<8} {:<8} {:<7}",
            a.n,
            a.label(),
            a.groups,
            format!("{:.0} / {}", a.median_group, a.max_group),
            format!(
                "{}/{}/{}/{}",
                a.op(StoreOp::Put),
                a.op(StoreOp::Cas),
                a.op(StoreOp::Stat),
                a.op(StoreOp::Get)
            ),
            format!("{:.2}", a.per_txn),
            format!("{:.1}ms", a.p50_ms),
            format!("{:.1}ms", a.p99_ms),
            format!("{:.2}s", a.wall.as_secs_f64()),
        );
    }
}

fn print_correctness(arms: &[Arm]) {
    println!("正确性核对（回执齐全 / 水位 == 组数 / 重放出全部写）");
    for a in arms {
        println!(
            "  [{}] N={:<3} {:<19} 回执 {}/{}｜组数 {}｜启动固定 {} 次｜{}",
            if a.ok() { "ok" } else { "!!" },
            a.n,
            a.label(),
            a.acked,
            a.n,
            a.groups,
            a.fixed_ops,
            if a.ok() {
                "干净".to_string()
            } else {
                a.problems.join("；")
            }
        );
    }
}

fn print_verdict(arms: &[Arm]) {
    // 查找键 = (种类, 并发度, 窗口)。窗口属于臂而不属于种类，所以要一起比。
    let find = |kind: Kind, n: usize, window_ms: Option<u64>| -> Option<&Arm> {
        arms.iter()
            .find(|a| a.kind == kind && a.n == n && a.window_ms == window_ms)
    };
    let per_txn = |kind: Kind, n: usize, window_ms: Option<u64>| -> Option<f64> {
        find(kind, n, window_ms).map(|a| a.per_txn)
    };

    println!("[V6] 每 txn 请求数（提交路径，已减启动固定开销）");
    for n in NS {
        let mut line = format!(
            "        N={n:<3} 逐条·每次读 {} → 逐条·持 etag {}",
            fmt_opt(per_txn(Kind::PerTxnRead, n, None)),
            fmt_opt(per_txn(Kind::PerTxnHeld, n, None))
        );
        for w in WINDOWS_MS {
            line.push_str(&format!(
                " → 齐到·窗{w}ms {}",
                fmt_opt(per_txn(Kind::Batched { stagger_us: 0 }, n, Some(w)))
            ));
        }
        println!("{line}");
    }
    // 摊开到达那一组只有 N=64，单独几行。
    for stagger_us in STAGGERS_US {
        let mut line = format!("        N=64  摊开到达（{stagger_us}µs/txn）");
        for w in WINDOWS_MS {
            line.push_str(&format!(
                " → 窗{w}ms {}",
                fmt_opt(per_txn(Kind::Batched { stagger_us }, 64, Some(w)))
            ));
        }
        println!("{line}");
    }

    // 前提：攒批臂得**真的攒到过**组。组大小恒为 1 时，"每 txn 请求数下降"这件事
    // 根本没发生 —— 而账面照样能算出一个比值来，所以这一条必须单独判。
    // N = 1 的臂不算：只有一个 txn 在飞，本来就没有同伴可攒。
    let testable: Vec<&Arm> = arms
        .iter()
        .filter(|a| matches!(a.kind, Kind::Batched { .. }) && a.n > 1)
        .collect();
    let coalesced = testable.iter().filter(|a| a.median_group > 1.0).count();
    println!();
    println!(
        "[前提] 攒批臂真的攒到了组（N>1 且组大小中位数 > 1）：{}/{} 条成立",
        coalesced,
        testable.len()
    );
    let missed: Vec<String> = testable
        .iter()
        .filter(|a| a.median_group <= 1.0)
        .map(|a| format!("N={} {}", a.n, a.label()))
        .collect();
    if !missed.is_empty() {
        println!(
            "       没攒到的是 {} —— 那不是「攒批没用」，是**这一次没攒到**；",
            missed.join("、")
        );
        println!("       它的标志很直白：那一行的每 txn 请求数会**正好等于**「逐条·持 etag」。");
    }

    // 结论按上面的数算出来，不是写好的形容词（见 P24）。
    let base = per_txn(Kind::PerTxnHeld, 64, None);
    let mut staggers = vec![("齐到", 0u64)];
    for s in STAGGERS_US {
        staggers.push(("摊开到达", s));
    }
    for w in WINDOWS_MS {
        println!("[V6] N=64 窗{w}ms —— 组数 / 每 txn 请求数 / 相对「逐条·持 etag」的倍数");
        for (label, stagger) in &staggers {
            if let Some(a) = find(
                Kind::Batched {
                    stagger_us: *stagger,
                },
                64,
                Some(w),
            ) {
                let ratio = base.filter(|b| *b > 0.0).map(|b| b / a.per_txn);
                println!(
                    "        {:<10}{:>5}µs/txn：{:>3} 组  {}  {}",
                    label,
                    stagger,
                    a.groups,
                    fmt_opt(Some(a.per_txn)),
                    match ratio {
                        Some(r) => format!("**{r:.1}×**"),
                        None => "—".to_string(),
                    },
                );
            }
        }
    }
    // 可重复性提示：**组数**才是稳的那个量（复跑 ±1），组大小的**分布**不是 ——
    // 「摊开·窗0」三条复跑分出过 5/28 、 20/40 、 28/35 三种组大小，而组数一直是 3。
    // 报数时报组数，别报组大小中位。
    println!(
        "        注：摊开臂复跑时**组数**稳定（±1），组大小分布不稳定 —— 报组数，别报组大小中位。"
    );
    let bad: Vec<String> = arms
        .iter()
        .filter(|a| !a.ok())
        .map(|a| format!("N={} {}", a.n, a.label()))
        .collect();
    println!(
        "[V6] 正确性：{}",
        if bad.is_empty() {
            "全部臂干净（攒批没有吃掉任何一个 txn 的写）".to_string()
        } else {
            format!("**{} 条臂有问题**：{}", bad.len(), bad.join("、"))
        }
    );
}

fn fmt_opt(v: Option<f64>) -> String {
    match v {
        Some(v) => format!("{v:.2}"),
        None => "—".to_string(),
    }
}
