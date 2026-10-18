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

//! 背压控制器 —— **V7 的被测对象**。
//!
//! 设计 §6.1 不是"一个阈值"，是**四件东西**，少一件都不收敛：
//!
//! | # | 要素 | 少了它会怎样 |
//! | --- | --- | --- |
//! | 1 | 上限 `MAX_UNAPPLIED` | 未 apply 量无界 ⇒ §5.3 恢复代价无界（S16 的结论） |
//! | 2 | 触及即**同步等** applier | 只报错不等 ⇒ 调用方只能自己轮询，把背压推给上层 |
//! | 3 | 等待**有超时**（返可重试错误） | applier 真停住时**无限等** ⇒ 死锁 |
//! | 4 | **迟滞**回落到一半才放行 | 在阈值上下抖动，每次抖动都要重排一次 |
//!
//! ## 为什么这个场景不碰 store
//!
//! V7 问的是**控制器**能不能收敛，不是"水位怎么写回 Manifest"（那是 S10 的题目）。
//! 所以 `applied` 水位在本模块里是**进程内**的：控制器真正依赖的输入只有两个 ——
//! 到达速率与应用速率，两个都是参数 ⇒ 合成它们不损失结论。
//! 提交/应用各自的**存储成本**是 V6 与 S16 量的，这里刻意不混进来
//! （混进来只会让"控制器收敛不收敛"这件事被 3.3ms 的往返淹没）。
//!
//! ## 等待用 `watch` 而不是 `Notify`
//!
//! `watch` 带**版本号**，每个等待者记着"我上次看到哪一版" ⇒
//! 「检查完状态」与「开始等」之间发生的那次推进**不会丢**。
//! `Notify` 要额外小心 permit 语义（`notify_one` 至多存一个 permit，
//! 多个等待者时谁被叫醒不确定），这里没必要去踩那个坑。

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::watch;

/// 控制器参数。`release_below` 就是迟滞线（设计里是上限的**一半**）。
///
/// # ⚠️ `release_below` 必须**严格小于** `cap`
///
/// 不是"最好小一点"，是**上限这条不变量本身依赖它**。拦截条件是 `unapplied + 1 > cap`
/// 而释放条件是 `unapplied <= release_below`：两者取成相等时，**刚进拦截态的那一瞬间
/// 释放条件就已经成立了** ⇒ 一个等待者立刻被放行、`unapplied` 变成 `cap + 1`。
/// 变异检查实测到了这一步（峰值 101 > 上限 100，且 applier 停住时也漏进去一个）。
/// 反过来，只留一格迟滞（`cap - 1`）能守住上限，但会在阈值上反复横跳 ——
/// **迟滞的两个作用（守上限 / 防抖动）是分开的，两个都需要**。
#[derive(Debug, Clone, Copy)]
pub struct BpConfig {
    pub cap: u64,
    pub release_below: u64,
    pub timeout: Duration,
}

/// 被拦下的原因。**可重试** —— 与设计 §6.1「返可重试错误」一致：
/// 这不是"你错了"，是"现在收不下，等会儿再来"。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AtCap {
    pub unapplied: u64,
    pub cap: u64,
    pub waited: Duration,
}

/// 控制器跑完之后的事实。**判定只读这里**，不读控制器内部。
#[derive(Debug, Clone, Default)]
pub struct BpStats {
    /// 进入拦截态的次数 —— **迟滞是否承重就看这个数**（无迟滞时它会是几十上百）。
    pub episodes: u32,
    /// 每次迟滞放行时的 `unapplied`。都在 `release_below` 之下才说明迟滞真的生效了。
    pub resumes: Vec<u64>,
    /// 真正等过 applier 的次数（被拦截态下每次进入等待都算一次）。
    pub waits: u32,
    /// 等超时、返可重试错的次数。
    pub timeouts: u32,
    /// 观察到的 `unapplied` 峰值。**这是"上限有没有被守住"的那个数。**
    pub peak: u64,
    /// 单次等待的最长时间 —— 断言它 ≤ 超时预算（否则"有超时"是假的）。
    pub max_wait: Duration,
    /// 放行的提交数。
    pub admitted: u64,
}

pub struct Backpressure {
    cfg: BpConfig,
    committed: AtomicU64,
    applied: AtomicU64,
    applied_tx: watch::Sender<u64>,
    applied_rx: watch::Receiver<u64>,
    st: Mutex<Inner>,
}

#[derive(Default)]
struct Inner {
    throttling: bool,
    episodes: u32,
    resumes: Vec<u64>,
    waits: u32,
    timeouts: u32,
    peak: u64,
    max_wait: Duration,
    admitted: u64,
}

impl Backpressure {
    pub fn new(cfg: BpConfig) -> Self {
        let (applied_tx, applied_rx) = watch::channel(0u64);
        Self {
            cfg,
            committed: AtomicU64::new(0),
            applied: AtomicU64::new(0),
            applied_tx,
            applied_rx,
            st: Mutex::new(Inner::default()),
        }
    }

    /// 未 apply 量 = `committed_lsn - applied_lsn`（接 S16：恢复代价就是这个量）。
    pub fn unapplied(&self) -> u64 {
        let c = self.committed.load(Ordering::Acquire);
        let a = self.applied.load(Ordering::Acquire);
        c.saturating_sub(a)
    }

    pub fn committed(&self) -> u64 {
        self.committed.load(Ordering::Acquire)
    }

    /// applier 推进水位。**每次推进都要叫一次**，否则等待者只能在超时里醒来。
    pub fn on_applied(&self, applied: u64) {
        self.applied.store(applied, Ordering::Release);
        // 接收端至少还剩本结构自己持有的那个 ⇒ 这个 `send` 不会因为"没人听"而失败。
        let _ = self.applied_tx.send(applied);
    }

    /// 提交前的准入。放行则返回 lsn，被拦则**等到迟滞线以下**或**超时返可重试错**。
    pub async fn admit(&self) -> Result<u64, AtCap> {
        // 版本号在**检查状态之前**取：这样"检查完 → 开始等"之间发生的那次推进不会丢。
        let mut rx = self.applied_rx.clone();
        loop {
            let unapplied = self.unapplied();
            {
                let mut st = self.st.lock().expect("控制器内部锁没被毒化");
                st.peak = st.peak.max(unapplied);

                if !st.throttling {
                    if unapplied + 1 <= self.cfg.cap {
                        st.admitted += 1;
                        drop(st);
                        return Ok(self.committed.fetch_add(1, Ordering::AcqRel) + 1);
                    }
                    // 顶到上限 ⇒ 进入拦截态。**迟滞从这里开始计时。**
                    st.throttling = true;
                    st.episodes += 1;
                } else if unapplied <= self.cfg.release_below {
                    // 迟滞放行：水位已经降到一半以下，解除拦截。
                    st.throttling = false;
                    st.resumes.push(unapplied);
                    st.admitted += 1;
                    drop(st);
                    return Ok(self.committed.fetch_add(1, Ordering::AcqRel) + 1);
                }
                st.waits += 1;
            }

            let started = Instant::now();
            match tokio::time::timeout(self.cfg.timeout, rx.changed()).await {
                // applier 推进了（或发送端没了）：重新判一次，别信"醒了就一定放行"。
                Ok(_) => continue,
                Err(_) => {
                    let waited = started.elapsed();
                    let mut st = self.st.lock().expect("控制器内部锁没被毒化");
                    st.timeouts += 1;
                    st.max_wait = st.max_wait.max(waited);
                    // ⚠️ 超时**不解除** throttling。解除它就等于"一次超时换一次放行"，
                    // 未 apply 量会一个接一个地漏进去，上限就成了软的。
                    return Err(AtCap {
                        unapplied,
                        cap: self.cfg.cap,
                        waited,
                    });
                }
            }
        }
    }

    pub fn stats(&self) -> BpStats {
        let st = self.st.lock().expect("控制器内部锁没被毒化");
        BpStats {
            episodes: st.episodes,
            resumes: st.resumes.clone(),
            waits: st.waits,
            timeouts: st.timeouts,
            peak: st.peak,
            max_wait: st.max_wait,
            admitted: st.admitted,
        }
    }
}
