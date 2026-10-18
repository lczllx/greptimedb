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

//! **S17 = V7 —— 背压能收敛。**
//!
//! 假设（设计 §6.1）：applier 滞后时，提交路径的**同步等待**能收敛，不死锁。
//! 计划里写下的三问：
//! ① 未 apply 量被压在上界内 ② 到上界后**返可重试错**而不是无限等 ③ **迟滞**回落能恢复。
//!
//! ## 为什么 ① 不能只写「峰值 ≤ 上限」
//!
//! 那是控制器**自己的逻辑**推出来的（顶到上限就不放行）—— 恒真，什么都没验到（P14 / P16 那族）。
//! 真正有区分力的是**对照**：**同样负载、但不拦**（`cap = u64::MAX`），看未 apply 量会涨到哪。
//! 于是 ① 变成一句有内容的比较：*不拦时 5900，拦时 100*。
//! 对照臂还顺带当了**前提检查** —— 它若没超过上限，说明这个负载压根没造出压力，
//! 幕一测的就是一场空（P16：恒真的前提不会让检查报错，它让检查照常给结论）。
//!
//! ## 为什么 ③ 不能只写「放行点 ≤ 上限的一半」
//!
//! 同上：那也正是代码写的。**迟滞真正防的是抖动** —— 没有它，水位一降一个就又放进去一批，
//! 于是在阈值上下反复横跳。所以 ③ 的主指标是**拦截段数**（`episodes`）：
//! 有迟滞时是个位数，没有时是几十上百。变异检查（把 `release_below` 抬到 `cap`）会证明这一点。
//!
//! ## 幕二为什么是「applier 完全停住」
//!
//! 「慢 10 倍」还不足以逼出无限等 —— applier 只要真的在动，等待者终究会被叫醒。
//! ③ 那条「等待有超时」只有在 applier **一动不动**时才承重。所以幕二把 applier 关掉，
//! 连做几次准入，每一次都必须在**超时预算内**返可重试错。
//!
//! ## 本场景不碰 store
//!
//! 控制器依赖的输入只有「到达速率」与「应用速率」两个参数；提交/应用各自的**存储成本**
//! 是 V6 与 S16 量的。混进来只会让"控制器收敛不收敛"被 3.3ms 的往返淹没。
//! ⇒ 自清这一节在这里没有对象可清（场景一个字节都没写）。

use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::backpressure::{AtCap, Backpressure, BpConfig};
use crate::kv::{CasStore, StorageError};

/// 上限。设计里是 10⁵ 条 / 64MB —— 这里取 100，因为要验的是**形状**（收敛/有界/迟滞），
/// 不是那个绝对数（换成 10⁵ 只改这一个常量，墙钟会长 1000 倍而已）。
const CAP: u64 = 100;
/// 迟滞线 = 上限的一半（设计 §6.1 明写「降到上限的一半才恢复放行」）。
const RELEASE_BELOW: u64 = CAP / 2;
/// 单次等待的超时预算。
const ADMIT_TIMEOUT: Duration = Duration::from_millis(200);

/// 提交耗时（合成）。**不是** MinIO 往返 —— 那个是 V6 量的 3.3ms。
const COMMIT_COST: Duration = Duration::from_millis(1);
/// applier 每应用一格的耗时。「慢 10 倍」就是它 = 10 × `COMMIT_COST`。
const APPLY_COST: Duration = Duration::from_millis(10);
/// 并发提交者数。
const SUBMITTERS: usize = 4;
/// 幕一/对照每臂跑多久。
const ACT: Duration = Duration::from_millis(1500);
/// 幕二连做几次准入。
const STALL_PROBES: usize = 5;

/// 迟滞下的拦截段数上限。充满约 26ms、排空到一半约 500ms ⇒ 一个周期 ~526ms，
/// 1.5s 里约 3 段。取 8 已经留了 2.5 倍余量；**无迟滞时是 ~150 段** ⇒ 这个门限有 ~19 倍区分力。
const EPISODE_CEILING: u32 = 8;

pub async fn run(_store: &CasStore) -> Result<(), StorageError> {
    println!(
        "[S17] 背压收敛：上限 {CAP}｜迟滞线 {RELEASE_BELOW}｜超时 {}ms｜applier 慢 {}×",
        ADMIT_TIMEOUT.as_millis(),
        APPLY_COST.as_millis() / COMMIT_COST.as_millis(),
    );

    // ── 幕〇 + 幕一：同样负载，一遍不拦、一遍拦 ──────────────────────────────
    // 两臂的负载**完全一样**（同样的到达速率、同样的应用速率、同样跑 ACT），
    // 差别只有 cap ⇒ 两边的峰值之差就是"背压买到了什么"。
    let free_stats = {
        let bp = Arc::new(Backpressure::new(BpConfig {
            cap: u64::MAX, // 不拦
            release_below: u64::MAX,
            timeout: ADMIT_TIMEOUT,
        }));
        run_load(bp.clone(), ACT, Some(APPLY_COST)).await;
        bp.stats()
    };
    let capped_stats = {
        let bp = Arc::new(Backpressure::new(BpConfig {
            cap: CAP,
            release_below: RELEASE_BELOW,
            timeout: ADMIT_TIMEOUT,
        }));
        run_load(bp.clone(), ACT, Some(APPLY_COST)).await;
        bp.stats()
    };

    // ── 幕二：applier 完全停住 ──────────────────────────────────────────────
    // 先填到上限，然后**关掉 applier**，连做几次准入：每次都必须超时返可重试错。
    let stall = {
        let bp = Backpressure::new(BpConfig {
            cap: CAP,
            release_below: RELEASE_BELOW,
            timeout: ADMIT_TIMEOUT,
        });
        for _ in 0..CAP {
            bp.admit().await.expect("填水位阶段不该被拦");
        }
        let filled = bp.unapplied();
        let mut probes = Vec::new();
        for _ in 0..STALL_PROBES {
            let t = Instant::now();
            let r = bp.admit().await;
            probes.push((t.elapsed(), r));
        }
        (filled, probes)
    };
    let (stall_filled, stall_probes) = &stall;
    let stall_waits: Vec<Duration> = stall_probes.iter().map(|(d, _)| *d).collect();
    let stall_all_retryable = stall_probes
        .iter()
        .all(|(_, r)| matches!(r, Err(AtCap { .. })));
    let stall_min_wait = stall_waits.iter().copied().min().unwrap_or_default();
    let stall_max_wait = stall_waits.iter().copied().max().unwrap_or_default();

    // ── 幕三：applier 恢复 ⇒ 迟滞放行 ───────────────────────────────────────
    let resumed_stats = {
        let bp = Arc::new(Backpressure::new(BpConfig {
            cap: CAP,
            release_below: RELEASE_BELOW,
            timeout: ADMIT_TIMEOUT,
        }));
        for _ in 0..CAP {
            bp.admit().await.expect("填水位阶段不该被拦");
        }
        run_load(bp.clone(), ACT, Some(APPLY_COST)).await;
        bp.stats()
    };

    // ── 判定 ────────────────────────────────────────────────────────────────
    // 前提：对照臂必须**真的**把水位拉过上限，否则幕一什么也没测到。
    let premise = free_stats.peak > CAP;
    // ① 有界（与对照比，不是与控制器自己的逻辑比）。
    let bounded = capped_stats.peak <= CAP;
    // ② 有超时、是可重试错、且**真的等过**（不是秒返，那就没测到等待）。
    let timed = stall_all_retryable
        && stall_min_wait >= ADMIT_TIMEOUT / 2
        && stall_max_wait <= ADMIT_TIMEOUT * 2;
    // ③ 迟滞：段数少（不抖动）+ 放行点确实在半线以下。
    let hysteresis_quiet = capped_stats.episodes > 0 && capped_stats.episodes <= EPISODE_CEILING;
    let hysteresis_release = !capped_stats.resumes.is_empty()
        && capped_stats.resumes.iter().all(|&r| r <= RELEASE_BELOW);
    let resumed = !resumed_stats.resumes.is_empty();

    println!();
    println!("  幕〇·对照（同样负载，cap = ∞ 不拦）");
    println!(
        "        未 apply 峰值 {}｜放行 {}｜拦截 {} 段",
        free_stats.peak, free_stats.admitted, free_stats.episodes
    );
    println!("  幕一·有界（cap = {CAP}）");
    println!(
        "        未 apply 峰值 {}｜放行 {}｜拦截 {} 段｜等你 {} 次｜超时 {} 次｜迟滞放行点 {:?}",
        capped_stats.peak,
        capped_stats.admitted,
        capped_stats.episodes,
        capped_stats.waits,
        capped_stats.timeouts,
        &capped_stats.resumes[..capped_stats.resumes.len().min(4)]
    );
    println!("  幕二·applier 停住（水位填到 {stall_filled}，之后一次都不推进）");
    for (i, (d, r)) in stall_probes.iter().enumerate() {
        let how = match r {
            Err(AtCap { unapplied, cap, .. }) => {
                format!("可重试错 AtCap {{ unapplied: {unapplied}, cap: {cap} }}")
            }
            Ok(lsn) => format!("**放行了（lsn {lsn}）—— 不该发生**"),
        };
        println!(
            "        [{i}] 等了 {:.0}ms → {how}",
            d.as_secs_f64() * 1000.0
        );
    }
    println!("  幕三·applier 恢复");
    println!(
        "        拦截 {} 段｜迟滞放行点 {:?}（都该 ≤ {RELEASE_BELOW}）｜峰值 {}",
        resumed_stats.episodes,
        &resumed_stats.resumes[..resumed_stats.resumes.len().min(4)],
        resumed_stats.peak
    );

    let verdict =
        if premise && bounded && timed && hysteresis_quiet && hysteresis_release && resumed {
            "PASS"
        } else {
            "FAIL"
        };
    println!();
    println!(
        "[{verdict}] s17 背压收敛: 上限 {CAP}｜峰值 {}（对照不拦时 {}，{:.0}×）｜拦截 {} 段（门限 {EPISODE_CEILING}）\
         ｜停住时 {STALL_PROBES} 次全部在 {:.0}ms 内返可重试错（最久 {:.0}ms）｜恢复后放行点 {:?}",
        capped_stats.peak,
        free_stats.peak,
        free_stats.peak as f64 / capped_stats.peak.max(1) as f64,
        capped_stats.episodes,
        stall_max_wait.as_secs_f64() * 1000.0,
        stall_max_wait.as_secs_f64() * 1000.0,
        resumed_stats.resumes.first()
    );
    if verdict == "PASS" {
        println!(
            "        ⇒ 四要素缺一不可：上限（有界）/ 同步等（不推给上层）/ 有超时（applier 真停住也不死锁）\n\
             \x20         / 迟滞（不抖动）。第 4 条的证据是**段数**：{EPISODE_CEILING} 的门限对无迟滞的 ~150 段是 ~19 倍区分力。"
        );
    } else {
        if !premise {
            println!("        ⚠️ 前提塌了：对照臂峰值 {} 没超过上限 {CAP} ⇒ 这个负载没造出压力，幕一无效。", free_stats.peak);
        }
        if !bounded {
            println!(
                "        ⚠️ 有界不成立：峰值 {} > 上限 {CAP}。",
                capped_stats.peak
            );
        }
        if !timed {
            println!(
                "        ⚠️ 超时那一条不成立（全返可重试错={stall_all_retryable}，最短 {:.0}ms，最久 {:.0}ms）\
                 —— 最短太短说明压根没等，最久超出预算说明「有超时」是假的。",
                stall_min_wait.as_secs_f64() * 1000.0,
                stall_max_wait.as_secs_f64() * 1000.0
            );
        }
        if !hysteresis_quiet || !hysteresis_release {
            println!(
                "        ⚠️ 迟滞不成立：段数 {}，放行点 {:?}。段数过多 = 在阈值上抖动。",
                capped_stats.episodes, capped_stats.resumes
            );
        }
    }
    Ok(())
}

/// 跑一段"持续提交 + 慢 applier"的负载。`apply_cost = None` ⇒ applier 停住。
async fn run_load(bp: Arc<Backpressure>, dur: Duration, apply_cost: Option<Duration>) {
    let stop = Instant::now() + dur;
    let mut tasks = Vec::new();

    for _ in 0..SUBMITTERS {
        let bp = bp.clone();
        tasks.push(tokio::spawn(async move {
            while Instant::now() < stop {
                // 放行与被拦（超时）都是这一轮的产出 —— 后者正是背压在工作。
                let _ = bp.admit().await;
                tokio::time::sleep(COMMIT_COST).await;
            }
        }));
    }

    if let Some(cost) = apply_cost {
        let bp = bp.clone();
        tasks.push(tokio::spawn(async move {
            let mut applied = 0u64;
            while Instant::now() < stop {
                tokio::time::sleep(cost).await;
                let committed = bp.committed();
                if applied < committed {
                    applied += 1;
                    bp.on_applied(applied);
                }
            }
        }));
    }

    for t in tasks {
        let _ = t.await;
    }
}
