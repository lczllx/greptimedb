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

//! 最小 B-link 树模型 —— **只实现 S2/S3/S4 需要的那点东西**。
//!
//! 刻意不做：内部节点、删除、节点合并、
//! 并发分裂、真正的父指针修复逻辑。这里只回答三个问题：
//!
//! - **S2**：分裂的「先落右兄弟、再 CAS 改左节点」这个顺序，是不是**承重的**？
//! - **S3**：分裂到一半崩了，那个孤儿右兄弟有没有害？
//! - **S4**：父指针（这里是目录）延后修，代价能不能量化？
//!
//! ## 为什么是 B-link 而不是普通 B+ 树
//!
//! 普通 B+ 树分裂时要同时改**三个**对象（左节点、新右节点、父节点），
//! 跨越三个对象的原子性在对象存储上不存在。B-link 的解法是给每个节点加一条
//! **右兄弟指针**：分裂变成「左节点 + 新右节点」两个对象的事，
//! 父节点只是**加速查找的索引**，修晚了顶多慢一点，不会丢数据。
//! 这正是这个课题需要的那条性质 —— 本文件就是来验证它到底成不成立。

use serde::{Deserialize, Serialize};

use crate::kv::{CasStore, StorageError};

/// 一个节点最多装几个 key。**故意设得很小（4）**，几次插入就能触发分裂，
/// 不用为了造出中间态而插入上千个 key。真实现里这个值会大得多。
pub const MAX_KEYS: usize = 4;

// ============================================================================
// 一、节点
// ============================================================================

/// B-link 树节点。
///
/// **`high_key` + `right` 就是 B-link 的全部精髓**：每个节点自己知道
/// 「我负责到哪个 key 为止」，以及「超出的去问我右兄弟」。
/// 这条链让树在**父指针还没修好**的中间态下依然可搜 —— S2 验的就是这一点。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Node {
    /// 节点 id（同时决定它在对象存储里的 key）。
    pub id: u64,
    /// 本节点负责的 key 上界（**含**）。`None` = +∞（最右节点）。
    pub high_key: Option<u64>,
    /// 右兄弟 id。`None` = 没有右兄弟。
    pub right: Option<u64>,
    /// 升序排列的 key。
    pub keys: Vec<u64>,
}

impl Node {
    pub fn new(id: u64, keys: Vec<u64>) -> Self {
        Self {
            id,
            high_key: None,
            right: None,
            keys,
        }
    }

    pub fn is_full(&self) -> bool {
        self.keys.len() >= MAX_KEYS
    }

    /// 本节点**在范围上**是否负责 `key`（不看内容）。
    ///
    /// 返回 `false` 就意味着「你该去问我右兄弟」—— 这一步是右兄弟链的入口。
    pub fn covers(&self, key: u64) -> bool {
        match self.high_key {
            Some(hk) => key <= hk,
            None => true,
        }
    }

    /// 把自己分裂成 `(改小后的自己, 新的右兄弟)`。
    ///
    /// **只计算，不落盘** —— 落盘顺序由调用方决定，而那正是 S2 要验的东西。
    /// 把"算"和"写"分开，是为了让调用点能一眼看出两步写的先后（也算一种防御：
    /// 如果这里顺手写了，顺序就被藏进函数体里，反而没法对照了）。
    pub fn plan_split(&self, right_id: u64) -> (Node, Node) {
        debug_assert!(self.keys.len() >= 2, "只分裂 >= 2 个 key 的节点");
        let mid = self.keys.len() / 2;
        let (lower, upper) = self.keys.split_at(mid);

        let new_left = Node {
            id: self.id,
            // 上半区的最小值 - 1，在这里就是 lower 的最大值
            high_key: lower.last().copied(),
            right: Some(right_id),
            keys: lower.to_vec(),
        };
        let new_right = Node {
            id: right_id,
            // ← 继承左节点原来的上界与右兄弟：这样新节点**立刻接上了原有的链**，
            //   右兄弟链不会因为插入了一个节点而断开。
            high_key: self.high_key,
            right: self.right,
            keys: upper.to_vec(),
        };
        (new_left, new_right)
    }
}

// ============================================================================
// 二、目录（「父指针」的替身）
// ============================================================================

/// 查找入口：一张「key 区间 → 从哪个节点开始找」的表。
///
/// 这就是 B-link 里**父节点**扮演的角色 —— 在单层模型里退化成一个目录对象。
/// 它是**纯优化**：目录再旧、再错，查找也能靠右兄弟链走到底找到正确结果，
/// 差别只在**走几个节点**。S4 量的就是这个"几个"。
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Directory {
    /// 升序断点：`(upper, node_id)` 表示 `key ∈ (上一项的 upper, upper]` 从 `node_id` 开始找。
    /// 最后一项的 `upper` 必须是 `u64::MAX`（兜底），否则大 key 无处可去。
    pub stops: Vec<(u64, u64)>,
}

impl Directory {
    /// 最简目录：所有 key 都从 `node_id` 开始找。
    pub fn single(node_id: u64) -> Self {
        Self {
            stops: vec![(u64::MAX, node_id)],
        }
    }

    /// 路由：`key` 应该从哪个节点开始找。
    pub fn route(&self, key: u64) -> u64 {
        for (upper, id) in &self.stops {
            if key <= *upper {
                return *id;
            }
        }
        // 兜底：理论上不可达（最后一项是 u64::MAX），真到了就取最后一个
        self.stops.last().map(|(_, id)| *id).unwrap_or(0)
    }

    /// 有没有任何一条路由指向 `id`（S3 判断孤儿是否被引用要用）。
    pub fn routes_to(&self, id: u64) -> bool {
        self.stops.iter().any(|(_, n)| *n == id)
    }

    /// 把「最后一跳」的区间劈成两半：`(.., old_id]` 与 `(bk, ..] -> new_id`。
    ///
    /// 这就是**修父指针**：分裂产生了新节点 `new_id`，它接管 `key > bk` 的那段。
    /// 不调它 = 父指针陈旧 = 大 key 的查找会先落到旧节点上、再沿右兄弟链走过去。
    pub fn add_split(&mut self, bk: u64, new_id: u64) {
        let last = match self.stops.pop() {
            Some(l) => l,
            None => (u64::MAX, new_id),
        };
        self.stops.push((bk, last.1));
        self.stops.push((u64::MAX, new_id));
    }
}

// ============================================================================
// 三、查找结果
// ============================================================================

/// 一次查找的结果。
///
/// **特意把「不可达」和「确实不存在」分开** —— 这不是洁癖：
/// S2 里两者是**完全相反**的证据。分裂顺序搞错时，上半区的 key 会变成
/// 「不可达」（链断了），而"没找到"会被误读成"这个 key 本来就不在"。
/// 如果只返回 `bool`，S2 的错误顺序对照就退化成一个**看起来正常**的 `false`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Search {
    /// 找到。
    Found { hops: usize },
    /// 沿右兄弟链走到底都没找到 ⇒ 这个 key 确实不在树里。
    Absent { hops: usize },
    /// **链断了**：右兄弟对象不存在 ⇒ key 不可达（分裂顺序错了的症状）。
    Unreachable { missing: u64, hops: usize },
}

impl Search {
    pub fn hops(&self) -> usize {
        match self {
            Search::Found { hops } | Search::Absent { hops } | Search::Unreachable { hops, .. } => {
                *hops
            }
        }
    }

    pub fn is_found(&self) -> bool {
        matches!(self, Search::Found { .. })
    }

    /// 紧凑标签，给 scenario 打印用。
    pub fn label(&self) -> String {
        match self {
            Search::Found { hops } => format!("found({hops}跳)"),
            Search::Absent { hops } => format!("absent({hops}跳)"),
            Search::Unreachable { missing, hops } => {
                format!("不可达({hops}跳, 缺节点{missing})")
            }
        }
    }
}

// ============================================================================
// 四、树
// ============================================================================

/// 一棵树 = 一个 key 前缀 + （目录对象、节点对象若干）。
pub struct BLinkTree {
    prefix: String,
}

impl BLinkTree {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }

    fn node_key(&self, id: u64) -> String {
        format!("{}/node/{id}", self.prefix)
    }

    pub fn dir_key(&self) -> String {
        format!("{}/dir", self.prefix)
    }

    // ------------------------------------------------------------------
    // 原语：节点的读写。**顺序留给调用方**，本模块不替它做决定。
    // ------------------------------------------------------------------

    /// 建节点：`if_not_exists`。已存在 ⇒ `Conflict`。
    ///
    /// 分裂的步骤① 用它 —— 「新节点必须凭空出现」这件事本身就带有互斥语义。
    pub async fn create_node(&self, store: &CasStore, node: &Node) -> Result<String, StorageError> {
        store
            .write_if_not_exists(&self.node_key(node.id), encode(node))
            .await
    }

    /// 改节点：CAS。
    ///
    /// 分裂的步骤② 用它 —— 左节点被改小，必须确认它没被别人动过。
    pub async fn cas_node(
        &self,
        store: &CasStore,
        node: &Node,
        etag: &str,
    ) -> Result<String, StorageError> {
        store
            .write_if_match(&self.node_key(node.id), encode(node), etag)
            .await
    }

    /// 读节点**内容**。返回 `None` 表示对象不存在（**不是错误** —— S2/S3 要拿这个当证据）。
    ///
    /// 只有一次往返：opendal 的 `read` 不返回 etag，要 etag 得再 `stat`。
    /// **查找路径只需要内容**，所以单独留了这条轻量读 —— S4 要跑几百次查找，
    /// 每次省一次往返是实打实的时间。
    pub async fn read_node(&self, store: &CasStore, id: u64) -> Result<Option<Node>, StorageError> {
        let key = self.node_key(id);
        match store.read(&key).await {
            Ok(bytes) => {
                let node: Node = serde_json::from_slice(&bytes)
                    .map_err(|e| StorageError::NotFound(format!("{key} 不是合法节点: {e}")))?;
                Ok(Some(node))
            }
            Err(StorageError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// 读节点**内容 + etag**（两次往返）。要接着 CAS 时必须用这条。
    pub async fn get_node(
        &self,
        store: &CasStore,
        id: u64,
    ) -> Result<Option<(Node, String)>, StorageError> {
        match self.read_node(store, id).await? {
            Some(node) => {
                let etag = store.etag(&self.node_key(id)).await?;
                Ok(Some((node, etag)))
            }
            None => Ok(None),
        }
    }

    // ------------------------------------------------------------------
    // 目录读写
    // ------------------------------------------------------------------

    /// **无条件**写目录。目录不需要 CAS —— 它是纯优化（理由见 [`Directory`]）。
    /// 给它套 CAS 只会让每次分裂都去争同一个对象。
    pub async fn put_dir(&self, store: &CasStore, dir: &Directory) -> Result<(), StorageError> {
        store.put(&self.dir_key(), encode(dir)).await
    }

    pub async fn get_dir(&self, store: &CasStore) -> Result<Option<Directory>, StorageError> {
        match store.read(&self.dir_key()).await {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes).map_err(|e| {
                StorageError::NotFound(format!("{} 不是合法目录: {e}", self.dir_key()))
            })?)),
            Err(StorageError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    // ------------------------------------------------------------------
    // 查找
    // ------------------------------------------------------------------

    /// 从目录路由到的节点开始找 `key`。
    pub async fn search(
        &self,
        store: &CasStore,
        dir: &Directory,
        key: u64,
    ) -> Result<Search, StorageError> {
        self.search_from(store, dir.route(key), key).await
    }

    /// **从指定节点开始**找 `key`，必要时沿右兄弟链往右走。
    ///
    /// `start` 是"谁让我来这儿找的" —— S2 直接把它指到分裂中的左节点，
    /// 模拟"父指针指向的旧节点"这个中间态。
    pub async fn search_from(
        &self,
        store: &CasStore,
        start: u64,
        key: u64,
    ) -> Result<Search, StorageError> {
        let mut id = start;
        let mut hops = 0usize;
        loop {
            hops += 1;
            let Some(node) = self.read_node(store, id).await? else {
                // 链断了：有人让我来找 id，但 id 不存在
                return Ok(Search::Unreachable { missing: id, hops });
            };
            if node.covers(key) {
                return Ok(if node.keys.contains(&key) {
                    Search::Found { hops }
                } else {
                    Search::Absent { hops }
                });
            }
            match node.right {
                Some(r) => id = r,
                // 走到最右还是不管这个 key ⇒ 它比树里所有 key 都大
                None => return Ok(Search::Absent { hops }),
            }
        }
    }

    /// 从 `start` 沿右兄弟链走，收集所有**可达**的节点 id（S3 验孤儿用）。
    pub async fn reachable(&self, store: &CasStore, start: u64) -> Result<Vec<u64>, StorageError> {
        let mut ids = vec![start];
        let mut id = start;
        loop {
            let Some(node) = self.read_node(store, id).await? else {
                break;
            };
            match node.right {
                Some(r) => {
                    ids.push(r);
                    id = r;
                }
                None => break,
            }
        }
        Ok(ids)
    }

    /// 走到最右节点。返回 `(节点, etag)`，方便紧接着 CAS。
    pub async fn rightmost(
        &self,
        store: &CasStore,
        start: u64,
    ) -> Result<(Node, String), StorageError> {
        let mut id = start;
        loop {
            let Some((node, etag)) = self.get_node(store, id).await? else {
                return Err(StorageError::NotFound(self.node_key(id)));
            };
            match node.right {
                Some(r) => id = r,
                None => return Ok((node, etag)),
            }
        }
    }

    /// 清场：删掉一棵树用到的所有对象（只在场景开头调，不参与正确性论证）。
    ///
    /// 节点 id 是顺序递增的，扫到第一个不存在的就停 —— 简单且足够。
    pub async fn wipe(&self, store: &CasStore, max_nodes: u64) -> Result<(), StorageError> {
        let _ = store.delete(&self.dir_key()).await;
        for id in 0..max_nodes {
            if self.read_node(store, id).await?.is_none() {
                break;
            }
            let _ = store.delete(&self.node_key(id)).await;
        }
        Ok(())
    }
}

fn encode<T: Serialize>(v: &T) -> Vec<u8> {
    serde_json::to_vec(v).expect("toy 的类型都能序列化")
}
