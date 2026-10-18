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

//! V3 的被测对象：**日志 + 树 + 视图** 三层堆起来的 leader KV。
//!
//! 对拍的另一边是真 etcd（见 `scenarios/s15.rs`）。这一层的职责是：把同一串操作
//! **按设计的形状**执行一遍，好让 etcd 与它逐字节对得上。所以它照抄的是 **etcd 的语义**
//! （compare 全真才走 then、**分支内的操作按序生效**、range 升序），
//! 而不是"我们觉得顺手的形状" —— 语义一旦自己发明，对拍就没有意义了。
//!
//! ## 三层各管什么
//!
//! ```text
//! 视图 View            leader 内存里的权威读视图：写穿 + **未命中回源并回填**
//! 树   DurableStore    对象存储上的有序 KV —— range 的唯一来源
//! 日志 CommitLog       提交点：Manifest 的一次 CAS（复用 src/commit.rs）
//! ```
//!
//! ## 为什么 compare 在视图上求值（本场景要验的那条）
//!
//! 设计 §3.2：compare 决定走哪个分支，所以它**必须**在提交路径上求值 ——
//! 去对象存储求值等于每个 txn 又变成 `depth + 1` 次 GET，白改。唯一可行的是在 leader
//! 内存视图里求值。
//!
//! 但 v5 修正（§3.2.1）把"关键路径零请求"改成了**逐 key 成立**：
//! 视图里没有这个 key，只等于"**还没人问过**"，不等于"它不存在" —— 未命中要**回源一次**。
//!
//! ⇒ 这就是本场景的分界，也是唯一的判定信号：
//! **把未命中当成"值不存在"，冷 key 上的 compare 就会判错**，
//! 而 `LeaderCachedKvBackend` 恰好就是那么做的（它遇 CAS 直接失效删键）。

use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};

use crate::commit::{Batch, CommitLog, CommitOutcome};
use crate::kv::{is_tombstone, CasStore, StorageError, TOMBSTONE};

// ============================================================================
// 一、操作与比较 —— 形状对齐 etcd，不多不少
// ============================================================================

/// 一个写操作。
///
/// 删除在日志段里落成**墓碑**（复用对象层那个字面量），在叶子对象里表现为**移除**。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    Put(String, String),
    Delete(String),
}

/// 一个比较。
///
/// **只有值比较，没有 revision** —— 仓库的 OCC 就是值字节比较（设计 §0.2），
/// 所以这里刻意不提供 `Version` / `CreateRevision` 之类的比较算子：
/// 提供了就等于在验一套我们并不打算实现、etcd 却有的语义。
///
/// `expect = None` 表达「这个 key 此刻**不存在**」，对应 `compare_and_put` 的
/// `expect: None` 那支（也就是 `write_if_not_exists` 的形状）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cmp {
    pub key: String,
    pub expect: Option<String>,
}

/// 一次事务：`compares` **全真**才执行 `then`，否则执行 `else_`。
#[derive(Debug, Clone, Default)]
pub struct Txn {
    pub compares: Vec<Cmp>,
    pub then: Vec<Op>,
    pub else_: Vec<Op>,
}

/// 日志段里的删除标记（与对象层的墓碑同一个字面量）。
fn tombstone() -> String {
    String::from_utf8(TOMBSTONE.to_vec()).expect("墓碑是 ASCII")
}

fn is_tombstone_str(v: &str) -> bool {
    is_tombstone(v.as_bytes())
}

fn bad_json(what: &str, key: &str, e: serde_json::Error) -> StorageError {
    // 沿用 `commit.rs::read_batch` 的处置：这类"对象内容坏了"在 toy 里不单开变体。
    StorageError::NotFound(format!("{what} {key} 不是合法内容: {e}"))
}

/// key 落在哪个分片：**首字节**。见 `DurableStore` 的分片说明。
fn shard_byte(key: &str) -> u8 {
    key.as_bytes().first().copied().unwrap_or(0)
}

// ============================================================================
// 二、树（DurableStore）—— 「树」的替身
// ============================================================================

/// 目录里的一个分片：`start` 是该分片覆盖的首字节（含），`object` 是叶子对象。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Shard {
    start: u8,
    object: String,
}

/// 分片目录，按 `start` 升序。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Directory {
    shards: Vec<Shard>,
}

/// 「树」的替身：对象存储上的**有序分片** KV。
///
/// 刻意**不是**真 B-link 树 —— 树的布局、分裂顺序、父指针修复是 S2–S4 的题目，已经验过。
/// 这里只需要一个"能按 key 升序 range、且 range **会跨对象**"的权威存储，
/// 好让「range 结果 == etcd」这条比对真的有内容可比（只读一个对象的 range 是测不出组装错误的）。
///
/// 分片规则：**按 key 的首字节**，一个首字节一个叶子对象。于是
/// 「叶子内部有序 + 叶子按首字节有序」⇒ 整体有序，range 就是按目录顺序读一遍再过滤。
///
/// ⚠️ 简化之处（诚实边界）：叶子不分裂、一个叶子装多少都行。真实实现要分裂 ——
/// 那一层由 S2–S4 覆盖，本场景不重复验。
pub struct DurableStore {
    prefix: String,
}

impl DurableStore {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }

    fn dir_key(&self) -> String {
        format!("{}/dir", self.prefix)
    }

    fn leaf_key(&self, start: u8) -> String {
        format!("{}/leaf/{start:02x}", self.prefix)
    }

    /// 读目录。**可以不存在**（空库）。
    async fn load_dir(
        &self,
        store: &CasStore,
    ) -> Result<Option<(Directory, String)>, StorageError> {
        match store.read(&self.dir_key()).await {
            Ok(bytes) => {
                let dir: Directory = serde_json::from_slice(&bytes)
                    .map_err(|e| bad_json("目录", &self.dir_key(), e))?;
                // 目录有序是 range 正确性的前提，坏了就该立刻炸，而不是给出一份乱序结果。
                debug_assert!(dir.shards.windows(2).all(|w| w[0].start < w[1].start));
                let etag = store.etag(&self.dir_key()).await?;
                Ok(Some((dir, etag)))
            }
            Err(StorageError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    async fn load_leaf(
        &self,
        store: &CasStore,
        object: &str,
    ) -> Result<Vec<(String, String)>, StorageError> {
        match store.read(object).await {
            Ok(bytes) => serde_json::from_slice(&bytes).map_err(|e| bad_json("叶子", object, e)),
            Err(StorageError::NotFound(_)) => Ok(Vec::new()),
            Err(e) => Err(e),
        }
    }

    /// 点查。**这是"回源"走的那一次读。**
    pub async fn get(&self, store: &CasStore, key: &str) -> Result<Option<String>, StorageError> {
        let Some((dir, _)) = self.load_dir(store).await? else {
            return Ok(None);
        };
        let start = shard_byte(key);
        let Some(shard) = dir.shards.iter().find(|s| s.start == start) else {
            return Ok(None);
        };
        let leaf = self.load_leaf(store, &shard.object).await?;
        Ok(leaf
            .binary_search_by(|(k, _)| k.as_str().cmp(key))
            .ok()
            .map(|i| leaf[i].1.clone()))
    }

    /// 逐个分片读全部 key/value，`keep` 决定留哪些。
    ///
    /// **目录顺序（按首字节升序）× 叶子内部升序 ⇒ 输出天然全局升序**，
    /// 不用再排一次 —— 这正是"按 key 首字节分片"这个设计换来的东西。
    async fn scan(
        &self,
        store: &CasStore,
        keep: impl Fn(&str) -> bool,
    ) -> Result<Vec<(String, String)>, StorageError> {
        let Some((dir, _)) = self.load_dir(store).await? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for shard in &dir.shards {
            for (k, v) in self.load_leaf(store, &shard.object).await? {
                if keep(&k) {
                    out.push((k, v));
                }
            }
        }
        Ok(out)
    }

    /// 按前缀 range，**结果按 key 升序**。
    pub async fn range(
        &self,
        store: &CasStore,
        prefix: &str,
    ) -> Result<Vec<(String, String)>, StorageError> {
        self.scan(store, |k| k.starts_with(prefix)).await
    }

    /// **半开区间** `[start, end)` 的 range —— 这个形状才是 etcd 的 `range`。
    ///
    /// 前缀版在 etcd 那边要额外算一次"前缀的后继"，而且要小心空前缀那个边界；
    /// 对拍用同一组字面量边界（如 `["a/", "e")`）最省事，也最不容易对错。
    pub async fn range_between(
        &self,
        store: &CasStore,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, String)>, StorageError> {
        self.scan(store, |k| start <= k && k < end).await
    }

    /// 目录里现在有几个分片（= range 至少要碰几个对象）。
    pub async fn shard_count(&self, store: &CasStore) -> Result<usize, StorageError> {
        Ok(self
            .load_dir(store)
            .await?
            .map(|(d, _)| d.shards.len())
            .unwrap_or(0))
    }

    /// 无条件 range（整个 keyspace）。
    pub async fn range_all(&self, store: &CasStore) -> Result<Vec<(String, String)>, StorageError> {
        self.range(store, "").await
    }

    /// 把一个批量**按序**应用到树上。返回被触碰的分片数（打进输出，当"跨了几个对象"的证据）。
    ///
    /// 顺序是有语义的：同一个 key 先 `Put` 后 `Delete` 与反过来必须结果不同（etcd 也是这样），
    /// 所以这里先把批量**按分片分组但保持批内相对顺序**，再逐个应用。
    pub async fn apply(&self, store: &CasStore, batch: &Batch) -> Result<usize, StorageError> {
        if batch.is_empty() {
            return Ok(0);
        }
        let mut by_shard: BTreeMap<u8, Vec<&(String, String)>> = BTreeMap::new();
        for entry in batch {
            by_shard
                .entry(shard_byte(&entry.0))
                .or_default()
                .push(entry);
        }

        let (mut dir, dir_etag) = match self.load_dir(store).await? {
            Some((d, e)) => (d, Some(e)),
            None => (Directory::default(), None),
        };

        for (start, entries) in &by_shard {
            let object = self.leaf_key(*start);
            let mut leaf = self.load_leaf(store, &object).await?;
            for (k, v) in entries {
                match leaf.binary_search_by(|(ek, _)| ek.as_str().cmp(k.as_str())) {
                    Ok(i) => {
                        if is_tombstone_str(v) {
                            leaf.remove(i);
                        } else {
                            leaf[i].1 = v.clone();
                        }
                    }
                    Err(i) => {
                        if !is_tombstone_str(v) {
                            leaf.insert(i, (k.clone(), v.clone()));
                        }
                    }
                }
            }
            // S2 的顺序：**先落新对象、再改目录**。反了会造出"目录指向不存在的叶子"。
            let bytes = serde_json::to_vec(&leaf).map_err(|e| bad_json("叶子", &object, e))?;
            store.put(&object, bytes).await?;
            if !dir.shards.iter().any(|s| s.start == *start) {
                dir.shards.push(Shard {
                    start: *start,
                    object,
                });
                dir.shards.sort_by_key(|s| s.start);
            }
        }

        let bytes = serde_json::to_vec(&dir).map_err(|e| bad_json("目录", &self.dir_key(), e))?;
        match dir_etag {
            // 目录本来是纯优化（正确性由"叶子按首字节有序"独立保证），但这里它同时是
            // range 的入口，所以还是给它 CAS：单写者下不会冲突，多写者下能当场发现。
            Some(etag) => {
                store.write_if_match(&self.dir_key(), bytes, &etag).await?;
            }
            None => {
                store.write_if_not_exists(&self.dir_key(), bytes).await?;
            }
        }
        Ok(by_shard.len())
    }
}

// ============================================================================
// 三、视图（View）—— leader 侧权威读视图
// ============================================================================

/// leader 内存里的读视图。`None` = 墓碑（逻辑删除后不可见）。
///
/// **唯一的不变量**：`get` 未命中时要**回源一次再回填**。
/// 视图里没有这个 key，只说明"还没人问过"，不说明"它不存在" ——
/// 这两件事混起来，冷 key 上的 compare 就会判错（见文件头）。
pub struct View {
    map: HashMap<String, Option<String>>,
    /// compare 落在视图里，没回源。
    pub hits: usize,
    /// compare 没落在视图里 ⇒ 回源了一次。**§3.2.1 说的那个成本就在这里。**
    pub misses: usize,
    /// 回源**读到了值**的那些 miss。
    ///
    /// 单看 `misses` 不够：冷 key 上回源到一个"确实不存在"的 key，
    /// "把未命中当成不存在"照样答对，这一格对判定**没有区分力**。
    /// 有区分力的只有"回源读到了值"的那些。
    pub misses_present: usize,
}

impl Default for View {
    fn default() -> Self {
        Self::new()
    }
}

impl View {
    pub fn new() -> Self {
        Self {
            map: HashMap::new(),
            hits: 0,
            misses: 0,
            misses_present: 0,
        }
    }

    /// 命中直接返回；**未命中回源一次**（这次 GET 就是 §3.2.1 说的那个成本）并回填。
    pub async fn get(
        &mut self,
        durable: &DurableStore,
        store: &CasStore,
        key: &str,
    ) -> Result<Option<String>, StorageError> {
        if let Some(v) = self.map.get(key) {
            self.hits += 1;
            return Ok(v.clone());
        }
        self.misses += 1;
        let v = durable.get(store, key).await?;
        if v.is_some() {
            self.misses_present += 1;
        }
        self.map.insert(key.to_string(), v.clone());
        Ok(v)
    }

    /// 写穿：提交路径上写过的 key，视图里的值就是权威值（leader 是唯一写者）。
    fn write_through(&mut self, key: &str, value: Option<String>) {
        self.map.insert(key.to_string(), value);
    }
}

// ============================================================================
// 四、三层合起来（LeaderKv）
// ============================================================================

pub struct LeaderKv {
    log: CommitLog,
    durable: DurableStore,
    view: View,
    epoch: u64,
    next_lsn: u64,
}

impl LeaderKv {
    /// 打开（或初始化）一个 leader KV。
    ///
    /// ⚠️ 视图**从空开始** —— 这不是偷懒，正是要复现"新 leader 上任、视图还没预热"那一刻。
    /// 生产里 `LeaderCachedKvBackend::load()` 会预热几个前缀，但**不可能预热整个 key 空间**，
    /// 所以"树里有、视图里没有"的 key 在任何时刻都存在。
    pub async fn open(store: &CasStore, prefix: &str, epoch: u64) -> Result<Self, StorageError> {
        let log = CommitLog::new(prefix);
        let manifest = log.init(store, epoch).await?;
        Ok(Self {
            log,
            durable: DurableStore::new(&format!("{prefix}/tree")),
            view: View::new(),
            epoch,
            next_lsn: manifest.next_lsn(),
        })
    }

    /// 直接往树上写一批（**不经过日志、不经过视图**）。
    ///
    /// 只给"上一任 leader 留下的存量"用 —— 也就是对拍开始前两边都有的那批初始数据。
    /// 这批 key 在视图里**没有**，于是天然构成了本场景要的**冷 key**。
    pub async fn seed(&self, store: &CasStore, batch: &Batch) -> Result<(), StorageError> {
        self.durable.apply(store, batch).await?;
        Ok(())
    }

    /// 一次事务。返回**这个 txn 是否走了 `then` 分支**（= etcd 的 `succeeded`）。
    pub async fn txn(&mut self, store: &CasStore, txn: &Txn) -> Result<bool, StorageError> {
        // ① 所有 compare 都在**视图**上求值（未命中的会回源一次）。
        //    **刻意不短路**：短路会让"踩到几次冷 key"这个前提计数失真，
        //    而那个数正是本场景有没有验到东西的证据。
        let mut all_true = true;
        for c in &txn.compares {
            let current = self.view.get(&self.durable, store, &c.key).await?;
            if current != c.expect {
                all_true = false;
            }
        }

        // ② 选分支。空分支 = 什么都不做（但 `succeeded` 照样是上面的结果）。
        let branch = if all_true { &txn.then } else { &txn.else_ };
        if !branch.is_empty() {
            let batch: Batch = branch
                .iter()
                .map(|op| match op {
                    Op::Put(k, v) => (k.clone(), v.clone()),
                    Op::Delete(k) => (k.clone(), tombstone()),
                })
                .collect();

            // ③ 提交：幂等落段 → Manifest CAS。**提交点是这里，不是 ④ 的树更新。**
            let lsn = self.next_lsn;
            match self.log.commit(store, self.epoch, lsn, &batch).await? {
                CommitOutcome::Committed { .. } => self.next_lsn += 1,
                // 单写者场景下不该发生；真发生了要报出来，不能悄悄当成功。
                CommitOutcome::LostRace => {
                    return Err(StorageError::NotFound(format!(
                        "lsn {lsn} 提交时输给了并发写者（本场景是单写者，不该出现）"
                    )))
                }
            }

            // ④ 应用到树，再写穿视图。顺序不能反：视图是缓存，树是权威。
            self.durable.apply(store, &batch).await?;
            for (k, v) in &batch {
                let value = if is_tombstone_str(v) {
                    None
                } else {
                    Some(v.clone())
                };
                self.view.write_through(k, value);
            }
        }

        Ok(all_true)
    }

    /// range —— **从树上读**，不经过视图。
    ///
    /// 这不是偷懒：视图只覆盖"被问过的 key"，它**不可能**回答"某个前缀下有哪些 key"。
    /// `LeaderCachedKvBackend` 遇 range 也是直接穿透（`cached_kv.rs`）。
    pub async fn range(
        &self,
        store: &CasStore,
        prefix: &str,
    ) -> Result<Vec<(String, String)>, StorageError> {
        self.durable.range(store, prefix).await
    }

    pub async fn range_all(&self, store: &CasStore) -> Result<Vec<(String, String)>, StorageError> {
        self.durable.range_all(store).await
    }

    pub async fn range_between(
        &self,
        store: &CasStore,
        start: &str,
        end: &str,
    ) -> Result<Vec<(String, String)>, StorageError> {
        self.durable.range_between(store, start, end).await
    }

    pub async fn shard_count(&self, store: &CasStore) -> Result<usize, StorageError> {
        self.durable.shard_count(store).await
    }

    /// 提交水位。用来核对"提交了几次"和"日志说了几次"是不是一回事。
    pub async fn committed_lsn(&self, store: &CasStore) -> Result<u64, StorageError> {
        let (m, _) = self
            .log
            .read_manifest(store)
            .await?
            .ok_or_else(|| StorageError::NotFound(self.log.manifest_key()))?;
        Ok(m.committed_lsn)
    }

    /// `(命中, 未命中, 未命中且有值)`。
    pub fn view_stats(&self) -> (usize, usize, usize) {
        (self.view.hits, self.view.misses, self.view.misses_present)
    }
}
