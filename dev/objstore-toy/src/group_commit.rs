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

//! 攒批提交器 —— **V6 的被测对象**。
//!
//! ## 攒批的收益只有一处来源
//!
//! 提交路径上**每次提交**要发两个请求：① PUT 日志段 ② CAS 推 Manifest 水位
//! （外加读 Manifest 的 `stat` + `get` —— 真 leader 持着 etag 不读，这个 toy 的
//! 逐条路径每次都读，正好用来把这两笔账分开）。攒批做的事就是**把这笔开销摊到一组 txn 上**：
//!
//! ```text
//! 逐条：N 个 txn ⇒ N × (PUT + CAS)          每 txn 2 次
//! 攒批：N 个 txn ⇒ ⌈N / 组大小⌉ × (PUT + CAS)   每组 2 次
//! ```
//!
//! 代价看起来是**延迟**：一个 txn 要待在队列里等窗外，看还有没有同伴。
//! 但 V6 量下来这笔账是**分情况**的，而且方向和直觉相反（见 FINDINGS 的 V6 节）：
//! 低并发下窗口确实是纯加延迟（N=1：7.7ms → 12.2ms），可高并发下攒批**同时**降请求数和降延迟
//! （N=64：p50 214.8ms → 10.0ms），因为逐条路径真正的延迟来自**串行化**，
//! 而攒批恰恰把串行化去掉了。所以不能笼统说"用延迟换请求数"。
//!
//! ## 两条与正确性有关的约定
//!
//! 1. **本模块是唯一写者**，所以它把 `(Manifest, etag)` 缓存在任务里、不每次读。
//!    这不是偷懒：真 leader 就是这个姿势，而 S6 第二幕说的是"**多个**写者共享一个陈旧
//!    etag"会出事 —— 单写者不在那条结论的射程里。
//! 2. **一个组 = 一个日志段 = 一个 lsn**。组内的写**拍平**成一个 [`Batch`] 落段
//!    （txn 边界不进日志 —— 与 S5/S6 已有的那层简化一致，也是 V2b 要碰的那件事）。
//!    ⇒ `committed_lsn` 数的是**组数**，不是 txn 数。

use std::time::{Duration, Instant};

use tokio::sync::{mpsc, oneshot};

use crate::commit::{Batch, CommitLog};
use crate::kv::{CasStore, StorageError};

/// 一条提交请求：一批写 + 一个回执口。
pub struct Submit {
    /// 这个 txn 想写的东西。
    pub batch: Batch,
    /// 回执。回的是**它落进了哪个 lsn**（同组的所有 txn 拿到同一个 lsn）。
    pub reply: oneshot::Sender<Result<u64, StorageError>>,
}

/// 攒批器的句柄。`clone` 出去给各个提交者用。
#[derive(Clone)]
pub struct GroupCommit {
    tx: mpsc::Sender<Submit>,
}

impl GroupCommit {
    /// 攒批器的**全部策略**就是这两个参数 —— V6 的三条臂只改这两个数。
    ///
    /// 三条臂其实是一份代码：`max_group = 1` 就是「逐条」，`window = 0` 就是「不等」。
    pub fn config() -> (Duration, usize) {
        let window_ms = std::env::var("TOY_BATCH_WINDOW_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        let max_group = std::env::var("TOY_BATCH_MAX_GROUP")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(usize::MAX);
        (Duration::from_millis(window_ms), max_group)
    }

    /// 起一个攒批任务。返回 `(句柄, 任务)` —— 任务结束时会给出这次的**分组账**。
    ///
    /// `window` 是**防抖**语义（每收到一条就重新等 `window`，等不到下一个就出组），
    /// 不是"固定攒多久"：窗口只在**队列真的空下来**的时候才真的花掉，
    /// 所以 `window = 0` 也不是"不等"而是「**把已经排到的全收走**」
    /// （`timeout(ZERO, recv)` 先 poll 内层 future，队列里有货就照样拿到）。
    pub fn spawn(
        store: CasStore,
        prefix: String,
        epoch: u64,
        window: Duration,
        max_group: usize,
    ) -> Spawned {
        // 通道开得比并发度高：提交者**不该**在这里排队 ——
        // 那会把"攒批带来的串行"和"通道容量不够带来的串行"混成一笔账。
        let (tx, rx) = mpsc::channel(4096);
        let (ready_tx, ready) = oneshot::channel();
        let done = tokio::spawn(run(store, prefix, epoch, window, max_group, rx, ready_tx));
        Spawned {
            commit: Self { tx },
            ready,
            done,
        }
    }

    /// 提交一个 txn，等它的 lsn。
    pub async fn submit(&self, batch: Batch) -> Result<u64, StorageError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Submit { batch, reply })
            .await
            .map_err(|_| StorageError::NotFound("攒批任务已经退出".to_string()))?;
        rx.await
            .map_err(|_| StorageError::NotFound("攒批任务没回执就退出了".to_string()))?
    }
}

/// 攒批任务的三件东西：提交句柄、载入完成的信号、收尾账。
///
/// `ready` 存在的理由只有测量：攒批任务开工时要读一次 Manifest（`stat` + `get`），
/// 那是**启动固定开销**，不是提交路径的一部分。测量方等它一下，就能把这 2 次请求
/// 从"每 txn 请求数"里摘出去 —— 否则 N = 1 的臂里它会占掉一半。
pub struct Spawned {
    pub commit: GroupCommit,
    pub ready: oneshot::Receiver<()>,
    pub done: tokio::task::JoinHandle<Result<GroupLog, StorageError>>,
}

/// 攒批任务的收尾账 —— 分组的形状，以及**每组实际发了几个请求**。

#[derive(Debug, Clone, Default)]
pub struct GroupLog {
    /// 每组的大小，按出组顺序。
    pub group_sizes: Vec<usize>,
    /// 每组提交路径上的（put, cas）次数 —— 摊平的证据就在这两个数里。
    pub per_group_ops: Vec<(usize, usize)>,
    /// 每组提交花掉的墙钟（从出组到 CAS 返回）。
    pub per_group_elapsed: Vec<Duration>,
    /// 组内 txn 拿到的共同 lsn。
    pub lsns: Vec<u64>,
}

impl GroupLog {
    pub fn groups(&self) -> usize {
        self.group_sizes.len()
    }

    pub fn txns(&self) -> usize {
        self.group_sizes.iter().sum()
    }

    pub fn total_ops(&self) -> usize {
        self.per_group_ops.iter().map(|(p, c)| p + c).sum()
    }

    /// 中位数组大小。**这是"攒批到底有没有攒到"的那把尺子** ——
    /// 组大小恒为 1 时，"每 txn 请求数下降"这件事根本没发生过，
    /// 而账面上它照样能算出一个比值来。
    pub fn median_group_size(&self) -> f64 {
        median(
            &self
                .group_sizes
                .iter()
                .map(|&n| n as f64)
                .collect::<Vec<_>>(),
        )
    }

    pub fn max_group_size(&self) -> usize {
        self.group_sizes.iter().copied().max().unwrap_or(0)
    }
}

async fn run(
    store: CasStore,
    prefix: String,
    epoch: u64,
    window: Duration,
    max_group: usize,
    mut rx: mpsc::Receiver<Submit>,
    ready: oneshot::Sender<()>,
) -> Result<GroupLog, StorageError> {
    let log = CommitLog::new(&prefix);
    // 单写者持 etag：载入一次，之后每次 CAS 成功就用返回的新 etag 续上。
    let (mut manifest, mut etag) = log
        .read_manifest(&store)
        .await?
        .ok_or_else(|| StorageError::NotFound(log.manifest_key()))?;
    // 载入完成 —— 测量方从这一刻起才开始记账，否则这 2 次读会混进"每 txn 请求数"。
    let _ = ready.send(());

    let mut out = GroupLog::default();
    while let Some(first) = rx.recv().await {
        let mut pending = vec![first];
        // 攒批：一边收一边等，直到攒满 / 窗内没新货。
        while pending.len() < max_group {
            match tokio::time::timeout(window, rx.recv()).await {
                Ok(Some(job)) => pending.push(job),
                // 发送端全掉了：把手上这组交出去就收工
                Ok(None) => break,
                // 窗外没有新货 ⇒ 出组
                Err(_) => break,
            }
        }

        let lsn = manifest.next_lsn();
        let merged = merge(&pending);
        let started = Instant::now();

        let before = crate::kv::store_ops_total(&crate::kv::store_ops());
        log.write_batch_idempotent(&store, epoch, lsn, &merged)
            .await?;
        let after_put = crate::kv::store_ops_total(&crate::kv::store_ops());
        manifest.committed_lsn = lsn;
        etag = log.cas_manifest(&store, &manifest, &etag).await?;
        let after_cas = crate::kv::store_ops_total(&crate::kv::store_ops());

        out.per_group_ops
            .push((after_put - before, after_cas - after_put));
        out.per_group_elapsed.push(started.elapsed());
        out.group_sizes.push(pending.len());
        out.lsns.push(lsn);

        // 一个组一个 lsn ⇒ 同组各 txn 的回执是同一个数。
        for job in pending {
            let _ = job.reply.send(Ok(lsn));
        }
    }
    Ok(out)
}

/// 把一组 txn 的写拍平成一段。**顺序 = 出组顺序**（`apply` 是后者覆盖前者）。
fn merge(pending: &[Submit]) -> Batch {
    let mut merged = Batch::new();
    for job in pending {
        merged.extend(job.batch.iter().cloned());
    }
    merged
}

/// 中位数（空集给 0）。
pub fn median(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).expect("没有 NaN"));
    let n = v.len();
    if n % 2 == 1 {
        v[n / 2]
    } else {
        (v[n / 2 - 1] + v[n / 2]) / 2.0
    }
}

/// 分位数（`q ∈ [0,1]`，最近秩法 —— 不插值，报出来的一定是**实测到的**某个值）。
pub fn percentile(xs: &[f64], q: f64) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    let mut v = xs.to_vec();
    v.sort_by(|a, b| a.partial_cmp(b).expect("没有 NaN"));
    let rank = (q * v.len() as f64).ceil() as usize;
    v[rank.saturating_sub(1).min(v.len() - 1)]
}
