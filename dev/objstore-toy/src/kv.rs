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

//! 对象存储上的条件写（CAS）封装。
//!
//! 搬自 `opendal-cas-demo/src/main.rs`，相对原版的五处改动：
//!
//! 1. **`TOMBSTONE` 的理由换了。** 原注释写「OpenDAL 0.58 的 delete 路径不支持 If-Match」，
//!    而 0.59.2 起条件删除的 API 已可达 —— 但 MinIO 会**静默忽略** `If-Match`
//!    （过期 etag 照样返 204 且真删）。所以墓碑写仍是默认路径，
//!    这条以场景 **S7** 的实测结论为准，别照抄旧注释。
//! 2. 新增无条件 `delete()` —— **只给场景自清 key 前缀用**，不参与任何正确性论证。
//! 3. 新增 `is_tombstone()` / `exists()` —— 场景要区分「逻辑删除」与「真不存在」。
//! 4. 无条件 `put()` —— 给「本来就不需要 CAS 保护」的对象用
//!    （树的入口指针：纯优化，正确性由右兄弟链保证）。理由见方法文档。
//! 5. 请求账本（`StoreOp` / `store_ops`，见文件末尾「三、请求账本」）—— V6 加的：
//!    「发了几次请求」得由**被计数的代码自己**记，不能靠调用方自觉去数。

use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

use opendal::{ErrorKind, Operator};

/// 墓碑值：逻辑删除的标记。对象存储上的「删除」退化为「覆盖写墓碑」。
///
/// `pub` 是 V3（`leader_kv`）加的：日志段里的删除也用它当标记，
/// 两处各写一份字面量就会悄悄分叉。
pub const TOMBSTONE: &[u8] = b"__TOMBSTONE__";

/// 判断一个值是不是墓碑（即逻辑删除）。
pub fn is_tombstone(value: &[u8]) -> bool {
    value == TOMBSTONE
}

// ============================================================================
// 一、错误类型：显式处理 OpenDAL 的异步错误派生
// ============================================================================

/// 并发冲突错误：条件写/删时，对象的 ETag 与期望不一致。
///
/// 这是自定义的领域错误，对应「CAS 失败」这一业务语义，
/// 而不是直接向调用方暴露 `opendal::Error`。
#[derive(Debug, Clone)]
pub struct StorageConflictError {
    /// 发生冲突的对象路径。
    pub path: String,
    /// 期望的 ETag（`None` 表示期望「对象不存在」，即 If-None-Match 冲突）。
    pub expected_etag: Option<String>,
}

impl StorageConflictError {
    fn new(path: &str, expected: Option<&str>) -> Self {
        Self {
            path: path.to_string(),
            expected_etag: expected.map(str::to_string),
        }
    }
}

impl fmt::Display for StorageConflictError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.expected_etag {
            Some(e) => write!(
                f,
                "storage conflict on `{}`: object already changed, expected etag {e}",
                self.path
            ),
            None => write!(
                f,
                "storage conflict on `{}`: object already exists (if-not-exists)",
                self.path
            ),
        }
    }
}

impl std::error::Error for StorageConflictError {}

/// 统一错误类型。
///
/// 关键在于 [`StorageError::source`]：当错误是 `Opendal` 时，`source()` 显式地
/// 指向底层 `opendal::Error`，从而保留完整的错误派生链
/// `StorageError → opendal::Error → reqwest::Error → ...`。
#[derive(Debug)]
pub enum StorageError {
    /// 并发冲突（CAS 失败）。
    Conflict(StorageConflictError),
    /// 对象不存在。
    NotFound(String),
    /// 底层 OpenDAL 错误，原样透传并保留 source 链。
    Opendal(opendal::Error),
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StorageError::Conflict(c) => write!(f, "{c}"),
            StorageError::NotFound(p) => write!(f, "object not found: {p}"),
            StorageError::Opendal(e) => write!(f, "opendal error: {e}"),
        }
    }
}

impl std::error::Error for StorageError {
    /// 显式声明错误派生链：谁是我更底层的原因。
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StorageError::Conflict(c) => Some(c),
            StorageError::NotFound(_) => None,
            // ↓ 显式派生：把 opendal::Error 挂到 source 链上，不吞掉底层原因
            StorageError::Opendal(e) => Some(e),
        }
    }
}

impl From<StorageConflictError> for StorageError {
    fn from(v: StorageConflictError) -> Self {
        StorageError::Conflict(v)
    }
}

impl From<opendal::Error> for StorageError {
    fn from(v: opendal::Error) -> Self {
        StorageError::Opendal(v)
    }
}

/// 显式地把 `opendal::Error` 按 [`ErrorKind`] 分类，映射到领域错误。
///
/// 这是「显式处理 OpenDAL 异步错误派生」的核心：不依赖 `?` 的隐式 `From` 转换，
/// 而是根据错误种类（kind）决定它该变成 `Conflict`、`NotFound` 还是原样透传。
///
/// 注意 `ErrorKind::ConditionNotMatch` 是本 toy 里**所有 CAS 判定的支点** ——
/// 场景的 PASS/FAIL 全靠它区分「真冲突」和「其它错误」。
fn classify(path: &str, expected_etag: Option<&str>, err: opendal::Error) -> StorageError {
    match err.kind() {
        // If-Match / If-None-Match 不满足 → 并发冲突
        ErrorKind::ConditionNotMatch => StorageConflictError::new(path, expected_etag).into(),
        // 对象不存在
        ErrorKind::NotFound => StorageError::NotFound(path.to_string()),
        // 其它错误（网络、鉴权、限流……）原样透传，source 链保留
        _ => StorageError::Opendal(err),
    }
}

// ============================================================================
// 二、CasStore：对 Operator 的条件写封装
// ============================================================================

/// 基于 OpenDAL `Operator` 的条件写封装。
///
/// `Clone` 是给场景 S0 的**真并发**用的 —— `tokio::spawn` 要求 future 是 `'static`，
/// 所以两个任务各持一份。
#[derive(Clone)]
pub struct CasStore {
    op: Operator,
}

impl CasStore {
    pub fn new(op: Operator) -> Self {
        Self { op }
    }

    /// 读取对象的当前 ETag（对象版本标识）。
    pub async fn etag(&self, key: &str) -> Result<String, StorageError> {
        bump(StoreOp::Stat);
        let meta = self
            .op
            .stat(key)
            .await
            .map_err(|e| classify(key, None, e))?;
        meta.etag()
            .map(str::to_string)
            .ok_or_else(|| StorageError::NotFound(format!("{key} (backend returns no etag)")))
    }

    /// 读取对象内容。
    pub async fn read(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        bump(StoreOp::Get);
        let bs = self
            .op
            .read(key)
            .await
            .map_err(|e| classify(key, None, e))?;
        Ok(bs.to_bytes().to_vec())
    }

    /// 对象是否存在。
    ///
    /// **墓碑也算「存在」** —— 要区分「逻辑删除」和「真不存在」请自己读值判断
    /// [`is_tombstone`]。
    pub async fn exists(&self, key: &str) -> Result<bool, StorageError> {
        bump(StoreOp::Stat);
        match self.op.stat(key).await {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == ErrorKind::NotFound => Ok(false),
            Err(e) => Err(classify(key, None, e)),
        }
    }

    /// 原子覆盖写：仅当对象当前 ETag == `etag` 时才写入。成功返回新 ETag。
    ///
    /// 底层：`If-Match: <etag>`。对象被并发修改过 → `ConditionNotMatch`。
    ///
    /// ⚠️ **它保证「值没变」，不保证「值没被别人动过」** —— ETag 是内容摘要、会重现
    /// （A→B→A 后旧 ETag 回来），所以「改两次又改回来」这种 ABA 它能放行。
    /// 详见场景 S1。
    pub async fn write_if_match(
        &self,
        key: &str,
        data: Vec<u8>,
        etag: &str,
    ) -> Result<String, StorageError> {
        bump(StoreOp::Cas);
        let meta = self
            .op
            .write_with(key, data)
            .if_match(etag)
            .await
            .map_err(|e| classify(key, Some(etag), e))?;
        Ok(meta.etag().map(str::to_string).unwrap_or_default())
    }

    /// 原子创建：仅当对象不存在时才写入。成功返回新 ETag。
    ///
    /// 底层：`If-None-Match: *`。对象已存在 → `ConditionNotMatch`。
    pub async fn write_if_not_exists(
        &self,
        key: &str,
        data: Vec<u8>,
    ) -> Result<String, StorageError> {
        bump(StoreOp::Cas);
        let meta = self
            .op
            .write_with(key, data)
            .if_not_exists(true)
            .await
            .map_err(|e| classify(key, None, e))?;
        Ok(meta.etag().map(str::to_string).unwrap_or_default())
    }

    /// **无条件覆盖写**（不做任何条件检查）。失败只可能是网络/鉴权等真实错误。
    ///
    /// 理由：树里有一类对象**本来就不需要 CAS 保护** ——
    /// 比如「树的入口/目录指针」：它是**纯优化**（只是让查找少走几跳），
    /// 正确性由右兄弟链独立保证。给它套 `if_match` 会凭空引入一个
    /// 所有写者都要争的 CAS 热点，反而把简单问题复杂化。
    ///
    /// ⚠️ 用它的前提是**能说清"为什么这个对象不需要 CAS"**。说不清就别用。
    pub async fn put(&self, key: &str, data: Vec<u8>) -> Result<(), StorageError> {
        bump(StoreOp::Put);
        self.op
            .write(key, data)
            .await
            .map_err(|e| classify(key, None, e))?;
        Ok(())
    }

    /// 列举某个前缀下的**一层**对象 key（**升序**）。
    ///
    /// **专门用来演示「诱人但错」的恢复姿势**：
    /// 「列举 bucket，把看得见的日志段全部重放」听起来天经地义，
    /// 但 S5 会证明它会把**没有提交**的事务带进状态。
    /// ⇒ 这个方法的用处是**当反面教材**，不是当实现基础。
    ///
    /// 注意：列出的是**对象**，不是**生效的事务** —— 这两者的差就是 S5 的全部。
    /// 另外它只往下走**一层**，而对象存储没有目录 ⇒ 结果里混着 `xxx/` 这种公共前缀条目，
    /// 底下更深的 key 一个都不在里面。所以要"把一个前缀列干净"得自己递归，
    /// `s15.rs` 的 `list_all` 就是这么写的（对一个前缀条目 `delete` 是无声的空操作，
    /// 少了这一步的清场会**看起来成功**）。
    pub async fn list(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        bump(StoreOp::List);
        let entries = self
            .op
            .list(prefix)
            .await
            .map_err(|e| classify(prefix, None, e))?;
        let mut keys: Vec<String> = entries.into_iter().map(|e| e.path().to_string()).collect();
        keys.sort();
        Ok(keys)
    }

    /// 原子删除（墓碑）：仅当对象当前 ETag == `etag` 时，写入墓碑标记。
    ///
    /// ⚠️ 为什么不用条件删除：`If-Match` 对 DELETE **被 MinIO 静默忽略**
    /// （过期 etag 照样 204 且真删）⇒ 条件删除不可信，只能用写路径的 CAS 表达。
    /// 这也是对象存储 KV 后端删除节点的常见做法（逻辑删除 + 后台 GC）。
    ///
    /// 成功返回墓碑的新 ETag。
    pub async fn delete_if_match(&self, key: &str, etag: &str) -> Result<String, StorageError> {
        self.write_if_match(key, TOMBSTONE.to_vec(), etag).await
    }

    /// 无条件删除。**只给场景自清 key 前缀用**，不参与任何正确性论证。
    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        bump(StoreOp::Delete);
        self.op
            .delete(key)
            .await
            .map_err(|e| classify(key, None, e))
    }

    // ------------------------------------------------------------------------
    // 以下方法**只给探针场景用**（S7 的条件删除、S9 的条件读），
    // 不属于 `kv.rs` 的正式接口 —— 正式路径的删除一律走 `delete_if_match`（墓碑）。
    // ------------------------------------------------------------------------

    /// 后端是否**声明**支持条件删除（`Capability::delete_with_if_match`）。
    ///
    /// 这是 S7 的关键对照：**「advertise 了但实际忽略」是最危险的缺陷类型** ——
    /// 调用方以为有保护，其实没有，而且不看 capability 根本发现不了。
    pub fn capability_delete_with_if_match(&self) -> bool {
        self.op.info().capability().delete_with_if_match
    }

    /// 直接用 `If-Match` 条件删除（**不做墓碑**）。
    ///
    /// 只用于 S7 复验「MinIO 是否静默忽略 `If-Match`」：
    /// 若后端忽略条件，**过期 etag 也会返 `Ok(())` 且对象真的被删掉**。
    ///
    /// 注意 opendal 的契约（`opendal-core` 的 `DeleteOptions::if_match` 文档）：
    /// 服务不 advertise [`Self::capability_delete_with_if_match`] 时返回
    /// `ErrorKind::Unsupported` —— 那时是**门控挡住**，不是服务端拦住的。
    pub async fn delete_with_if_match(&self, key: &str, etag: &str) -> Result<(), StorageError> {
        bump(StoreOp::Delete);
        self.op
            .delete_with(key)
            .if_match(etag)
            .await
            .map_err(|e| classify(key, Some(etag), e))
    }

    // ------------------------------------------------------------------------
    // S9 的两个方法：条件**读**的探针。
    // ------------------------------------------------------------------------

    /// 后端是否**声明**支持条件读（`Capability::read_with_if_none_match`）。
    ///
    /// 与 [`Self::capability_delete_with_if_match`] 同样重要：`advertise 了但忽略`
    /// 才是危险的那一类，只看 capability 决定不了任何事。
    ///
    /// 实测：S3 后端全部置 `true`（`opendal-service-s3-0.59.2/src/backend.rs:1060`），
    /// 所以这层门控**不会**挡住请求 —— 剩下的问题只有"服务端认不认"。
    pub fn capability_read_with_if_none_match(&self) -> bool {
        self.op.info().capability().read_with_if_none_match
    }

    /// 直接用 `If-None-Match` 条件读（**不做墓碑、不做缓存**）。
    ///
    /// 只用于 S9 复验"条件读是否真的被执行"——契约（`ReadOptions::if_none_match` 文档）：
    ///
    /// | 当前对象状态 | 期望结果 |
    /// | --- | --- |
    /// | 当前 ETag **等于**给定值 | `ConditionNotMatch`（**内容一个字节都不传** ⇒ 零传输探针） |
    /// | 当前 ETag **不等于** | `Ok`，返回内容 |
    /// | 对象不存在 | `NotFound` |
    /// | 服务未 advertise | `Unsupported` |
    ///
    /// 若后端**静默忽略**这个头，前两行会退化成同一件事（都返回内容），
    /// 探针就失去意义 —— 那正是 S9 要判定的。
    pub async fn read_with_if_none_match(
        &self,
        key: &str,
        etag: &str,
    ) -> Result<Vec<u8>, StorageError> {
        bump(StoreOp::Get);
        let bs = self
            .op
            .read_with(key)
            .if_none_match(etag)
            .await
            .map_err(|e| classify(key, Some(etag), e))?;
        Ok(bs.to_bytes().to_vec())
    }
}

// ============================================================================
// 三、请求账本（V6 加的）
// ============================================================================

/// 一次 store 调用的种类。
///
/// 分类按**请求的形状**，不按方法名：`write_if_not_exists` 走的是 `Cas`
/// （它带条件），`delete_if_match` 走的是 `Cas`（它是墓碑**写**，不是删）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoreOp {
    /// 无条件写（`put`）。
    Put,
    /// 条件写（`write_if_match` / `write_if_not_exists` / `delete_if_match`）。
    Cas,
    /// `stat`（`etag` / `exists`）。
    Stat,
    /// `read`。
    Get,
    /// `list`。**一次调用至少一次往返**，翻页时不止。
    List,
    /// 真删除（`delete` / `delete_with_if_match`）。
    Delete,
}

impl StoreOp {
    /// 稳定顺序 —— 打印时按它排，跨臂可比。
    pub const ALL: [StoreOp; 6] = [
        StoreOp::Put,
        StoreOp::Cas,
        StoreOp::Stat,
        StoreOp::Get,
        StoreOp::List,
        StoreOp::Delete,
    ];

    pub fn name(self) -> &'static str {
        match self {
            StoreOp::Put => "put",
            StoreOp::Cas => "cas",
            StoreOp::Stat => "stat",
            StoreOp::Get => "get",
            StoreOp::List => "list",
            StoreOp::Delete => "del",
        }
    }

    fn index(self) -> usize {
        match self {
            StoreOp::Put => 0,
            StoreOp::Cas => 1,
            StoreOp::Stat => 2,
            StoreOp::Get => 3,
            StoreOp::List => 4,
            StoreOp::Delete => 5,
        }
    }
}

/// 请求账本。**进程级**，照 `cfg::transient_retries` 的先例 ——
/// `CasStore` 的克隆必须共用一本账（并发臂里每个任务各持一份 clone），
/// 账挂在结构体里就得再套一层 `Arc`，挂在这里免费。
///
/// 计的是 **toy 对 store 的调用次数**，一次调用 ≈ 一次 HTTP 往返。
/// 它不等于"后端的请求数"：`list` 翻页、opendal 的重试层各会放大它
/// （重试次数另有 `cfg::transient_retries` 单独记）。
static STORE_OPS: [AtomicUsize; 6] = [
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
    AtomicUsize::new(0),
];

fn bump(op: StoreOp) {
    STORE_OPS[op.index()].fetch_add(1, Ordering::Relaxed);
}

/// 读账本，按 [`StoreOp::ALL`] 的顺序。
///
/// 只在**所有写者都 join 之后**读，所以 `Relaxed` 够用（join 本身给了同步）。
pub fn store_ops() -> [(StoreOp, usize); 6] {
    std::array::from_fn(|i| {
        let op = StoreOp::ALL[i];
        (op, STORE_OPS[op.index()].load(Ordering::Relaxed))
    })
}

/// 账本清零。**每条测量臂开头都要调**，否则上一个臂的数会漏进这一次。
///
/// ⚠️ 这是"减掉起点"而不是"从零开始"：清零那一瞬间如果还有别的任务在发请求，
/// 它会被算进这一臂 —— 所以清零只能在**没有在途写者**的时候做。
pub fn reset_store_ops() {
    for c in &STORE_OPS {
        c.store(0, Ordering::Relaxed);
    }
}

/// [`store_ops`] 的差值：`later - earlier`（逐项饱和，不会回绕成天文数字）。
pub fn store_ops_delta(
    earlier: &[(StoreOp, usize); 6],
    later: &[(StoreOp, usize); 6],
) -> [(StoreOp, usize); 6] {
    std::array::from_fn(|i| (later[i].0, later[i].1.saturating_sub(earlier[i].1)))
}

/// 一个账本里所有种类的总和。
pub fn store_ops_total(ops: &[(StoreOp, usize); 6]) -> usize {
    ops.iter().map(|(_, n)| n).sum()
}
