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

//! 最小提交模型 —— **只实现 S5/S6 需要的那点东西**。
//!
//! 刻意不做：真正的崩溃恢复流程、epoch 切换、快照。
//!
//! ## 核心命题：提交点是 Manifest，不是「日志段写成功」
//!
//! 对象存储上没有跨对象的原子性。所以提交拆成两步，**靠一个对象当"真相"**：
//!
//! ```text
//! ① PUT  commit/{epoch}/{lsn}.batch      ← 数据先落到日志段（可变多个对象）
//! ② CAS  manifest: committed_lsn → lsn   ← 唯一的提交点（**一个**对象）
//! ```
//!
//! 于是「**bucket 里有哪些对象**」和「**哪些事务生效了**」是两回事 ——
//! 前者是「写成功」，后者由 Manifest 的水位决定。这正是 S5 要验的。
//!
//! ## 为什么 Manifest 能被 CAS 保护（接 S1）
//!
//! `committed_lsn` **只增不减** ⇒ 值永不重复 ⇒ ABA 不可能（S1 的结论）。
//! 如果这里的水位是可回滚的，ETag CAS 就挡不住 ABA 了。

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::kv::{CasStore, StorageError};

/// 提交清单 —— **整棵树的真相**。
///
/// 两个水位都是**单调递增**的，这一点是它敢用 ETag CAS 保护的前提（见 S1）。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// 纪元。换主时 +1，用来把旧主的残留物隔离开（本阶段不实现切换逻辑）。
    pub epoch: u64,
    /// **提交点**：`lsn <= committed_lsn` 的事务才算生效。
    pub committed_lsn: u64,
    /// 已应用到状态机的水位。恢复就是把它往 `committed_lsn` 推。
    pub applied_lsn: u64,
}

impl Manifest {
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            committed_lsn: 0,
            applied_lsn: 0,
        }
    }

    /// 下一个可以提交的 lsn。
    pub fn next_lsn(&self) -> u64 {
        self.committed_lsn + 1
    }
}

/// 一个批量 = 一组 KV 写入。做成"可应用的 put"，是为了让重放有**看得见的效果** ——
/// 否则"这个事务生效了吗"就只能靠断言对象存在，不如直接看状态。
pub type Batch = Vec<(String, String)>;

/// 提交的结果。
///
/// ⚠️ `LostRace` **不是成功**。它是个**独立的变体**而不是 `Err`，
/// 是因为"输了竞争"是**预期内的正常结果**（重试即可），不是错误；
/// 但它**绝不能**被当成 ack —— 调用方必须显式区分。
/// 设计上没有 `is_success()` 这种会把两者混起来的方法，就是为了不给出这个出口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    Committed {
        lsn: u64,
        manifest_etag: String,
    },
    /// 输给了并发写者：**该事务没有生效，不能 ack**。
    LostRace,
}

/// 恢复的产物。
#[derive(Debug, Clone, Default)]
pub struct Recovered {
    /// 重放出来的状态机。
    pub state: BTreeMap<String, String>,
    /// 实际应用了哪些 lsn（体现"哪些生效了"）。
    pub applied: Vec<u64>,
    /// `committed_lsn` 说有、但段**不见了**的 lsn（数据丢失的信号）。
    pub missing: Vec<u64>,
}

pub struct CommitLog {
    prefix: String,
}

impl CommitLog {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }

    pub fn manifest_key(&self) -> String {
        format!("{}/manifest", self.prefix)
    }

    pub fn batch_key(&self, epoch: u64, lsn: u64) -> String {
        format!("{}/commit/{epoch}/{lsn:020}.batch", self.prefix)
    }

    /// 列举用的前缀。lsn 补齐到 20 位（见 [`Self::batch_key`]）就是为了让
    /// **字典序 = 数字序**，否则 `10.batch` 会排在 `9.batch` 前面。
    pub fn batch_prefix(&self, epoch: u64) -> String {
        format!("{}/commit/{epoch}/", self.prefix)
    }

    // ------------------------------------------------------------------
    // Manifest
    // ------------------------------------------------------------------

    /// 建初值。已存在则不覆盖（返回现状）。
    pub async fn init(&self, store: &CasStore, epoch: u64) -> Result<Manifest, StorageError> {
        if let Some((m, _)) = self.read_manifest(store).await? {
            return Ok(m);
        }
        let m = Manifest::new(epoch);
        let _ = store
            .write_if_not_exists(&self.manifest_key(), encode(&m))
            .await;
        Ok(m)
    }

    /// 读 Manifest —— 返回的 `(值, ETag)` 是**同一个快照**。
    ///
    /// ⚠️ **顺序是承重的：先 `stat` 拿 ETag，再 `read` 拿值**（结论见 S10 第三幕）。
    /// opendal 的 `read` 不给 ETag、`stat` 不给值，想要这一对必须发**两次**请求，
    /// 两次之间对象可能被改。反序（先值后 ETag）会拿到"旧值 + 新 ETag"，
    /// 于是 CAS 会**成功**，并把另一个写者（applier 写的 `applied_lsn`）静默回退 ——
    /// 两个写者都没违反 CAS 契约，但一方的进度被悄悄抹掉了。
    pub async fn read_manifest(
        &self,
        store: &CasStore,
    ) -> Result<Option<(Manifest, String)>, StorageError> {
        let key = self.manifest_key();
        // ① 先要 ETag
        let etag = match store.etag(&key).await {
            Ok(etag) => etag,
            Err(StorageError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        // ② 再要值（中途被删 ⇒ 同样是「不存在」）
        let bytes = match store.read(&key).await {
            Ok(bytes) => bytes,
            Err(StorageError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(e),
        };
        let m: Manifest = serde_json::from_slice(&bytes)
            .map_err(|e| StorageError::NotFound(format!("{key} 不是合法 Manifest: {e}")))?;
        Ok(Some((m, etag)))
    }

    /// 推提交点。**唯一的提交点**。
    pub async fn cas_manifest(
        &self,
        store: &CasStore,
        m: &Manifest,
        etag: &str,
    ) -> Result<String, StorageError> {
        store
            .write_if_match(&self.manifest_key(), encode(m), etag)
            .await
    }

    // ------------------------------------------------------------------
    // 日志段（两种写法，S5 要对照）
    // ------------------------------------------------------------------

    /// 写日志段 —— **正确姿势：幂等覆盖**。
    ///
    /// 为什么不用 `if_not_exists`：段**不是**提交点，Manifest 才是。
    /// 用 `if_not_exists` 会把段变成一个"锁"，于是**崩在 ① 和 ② 之间的写者
    /// 会把同一个 lsn 永久卡死**（重试恒 Conflict）—— S5 里实测了这条。
    ///
    /// 覆盖写安全的前提：**同一个 `(epoch, lsn)` 对应的内容必须是唯一的**
    /// （lsn 是全局有意义的事务号，不是"我想写什么就写什么"的槽位）。
    /// 这条前提不成立的话，并发写者会互相覆盖内容 —— 见 S6 的边界说明。
    pub async fn write_batch_idempotent(
        &self,
        store: &CasStore,
        epoch: u64,
        lsn: u64,
        batch: &Batch,
    ) -> Result<(), StorageError> {
        store.put(&self.batch_key(epoch, lsn), encode(batch)).await
    }

    /// 写日志段 —— **诱人但错的姿势：`if_not_exists`**。
    ///
    /// "同一个 lsn 只能写一次"听起来是个好性质，但它把崩溃变成了**永久卡住**。
    /// 只用于 S5 的对照实验。
    pub async fn write_batch_if_absent(
        &self,
        store: &CasStore,
        epoch: u64,
        lsn: u64,
        batch: &Batch,
    ) -> Result<String, StorageError> {
        store
            .write_if_not_exists(&self.batch_key(epoch, lsn), encode(batch))
            .await
    }

    pub async fn read_batch(
        &self,
        store: &CasStore,
        epoch: u64,
        lsn: u64,
    ) -> Result<Option<Batch>, StorageError> {
        let key = self.batch_key(epoch, lsn);
        match store.read(&key).await {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(|e| {
                StorageError::NotFound(format!("{key} 不是合法批量: {e}"))
            })?)),
            Err(StorageError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    // ------------------------------------------------------------------
    // 提交协议
    // ------------------------------------------------------------------

    /// 完整提交：① 幂等落段 → ② CAS 推水位。
    ///
    /// 崩在 ① 和 ② 之间 ⇒ 留下**孤儿日志段**（不生效，S5 验）。
    pub async fn commit(
        &self,
        store: &CasStore,
        epoch: u64,
        lsn: u64,
        batch: &Batch,
    ) -> Result<CommitOutcome, StorageError> {
        // ① 数据先落盘（幂等）
        self.write_batch_idempotent(store, epoch, lsn, batch)
            .await?;

        // ② 推提交点
        let (current, etag) = self
            .read_manifest(store)
            .await?
            .ok_or_else(|| StorageError::NotFound(self.manifest_key()))?;
        self.cas_commit(store, &current, &etag, epoch, lsn).await
    }

    /// 只做第 ② 步（S6 要拿同一个 etag 让两个写者并发这一段）。
    pub async fn cas_commit(
        &self,
        store: &CasStore,
        current: &Manifest,
        etag: &str,
        epoch: u64,
        lsn: u64,
    ) -> Result<CommitOutcome, StorageError> {
        let next = Manifest {
            epoch,
            committed_lsn: lsn,
            // applied 不动 —— 应用是恢复阶段的事
            applied_lsn: current.applied_lsn,
        };
        match self.cas_manifest(store, &next, etag).await {
            Ok(new_etag) => Ok(CommitOutcome::Committed {
                lsn,
                manifest_etag: new_etag,
            }),
            // 输了竞争 ⇒ 明确地不 ack
            Err(StorageError::Conflict(_)) => Ok(CommitOutcome::LostRace),
            Err(e) => Err(e),
        }
    }

    // ------------------------------------------------------------------
    // 恢复（两种姿势，S5 的对照核心）
    // ------------------------------------------------------------------

    /// 恢复 —— **正确姿势：只看 Manifest**。
    ///
    /// 重放 `(applied_lsn, committed_lsn]` 里的段。**水位之上的段一律不看**，
    /// 哪怕它就躺在 bucket 里。这就是"孤儿段不生效"的全部机制。
    pub async fn recover_by_manifest(&self, store: &CasStore) -> Result<Recovered, StorageError> {
        let (manifest, _) = self
            .read_manifest(store)
            .await?
            .ok_or_else(|| StorageError::NotFound(self.manifest_key()))?;

        let mut out = Recovered::default();
        for lsn in (manifest.applied_lsn + 1)..=manifest.committed_lsn {
            match self.read_batch(store, manifest.epoch, lsn).await? {
                Some(batch) => {
                    apply(&mut out.state, &batch);
                    out.applied.push(lsn);
                }
                // 水位说它生效了，段却不在 ⇒ 这是**真丢数据**，必须报出来
                None => out.missing.push(lsn),
            }
        }
        // 水位之下可能还有更早的（applied 之前）—— 本阶段不重放，只看这一段的差值
        Ok(out)
    }

    /// 恢复 —— **诱人但错的姿势：列举 bucket，把看得见的段全重放**。
    ///
    /// 只用于 S5 的对照。它会把**没提交**的孤儿段也应用进去。
    pub async fn recover_by_listing(
        &self,
        store: &CasStore,
        epoch: u64,
    ) -> Result<Recovered, StorageError> {
        let mut out = Recovered::default();
        let keys = store.list(&self.batch_prefix(epoch)).await?;
        for key in keys {
            let Some(lsn) = self.parse_lsn(&key, epoch) else {
                continue;
            };
            if let Some(batch) = self.read_batch(store, epoch, lsn).await? {
                apply(&mut out.state, &batch);
                out.applied.push(lsn);
            }
        }
        Ok(out)
    }

    /// 从 `{prefix}/commit/{epoch}/{lsn}.batch` 里抠出 lsn。
    fn parse_lsn(&self, key: &str, epoch: u64) -> Option<u64> {
        let prefix = self.batch_prefix(epoch);
        let rest = key.strip_prefix(&prefix)?;
        rest.strip_suffix(".batch")?.parse::<u64>().ok()
    }

    /// 清场（只在场景开头调）。
    pub async fn wipe(&self, store: &CasStore, epoch: u64) -> Result<(), StorageError> {
        let _ = store.delete(&self.manifest_key()).await;
        for key in store.list(&self.batch_prefix(epoch)).await? {
            let _ = store.delete(&key).await;
        }
        Ok(())
    }
}

/// 便捷构造：`batch(&[("k", "v")])`。
pub fn batch(pairs: &[(&str, &str)]) -> Batch {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

fn apply(state: &mut BTreeMap<String, String>, batch: &Batch) {
    for (k, v) in batch {
        state.insert(k.clone(), v.clone());
    }
}

fn encode<T: Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).expect("toy 的类型都能序列化")
}
