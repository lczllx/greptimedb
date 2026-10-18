//! OpenDAL 条件写（CAS）最小可行性 Demo。
//!
//! 用 OpenDAL 封装 S3/MinIO 的 If-Match / If-None-Match，实现：
//!   1. 原子的覆盖写（If-Match + ETag）
//!   2. 原子的创建    （If-None-Match: *）
//!   3. 原子的删除    （If-Match + ETag）
//!   4. 并发冲突时返回自定义的 `StorageConflictError`
//!   5. 显式处理 OpenDAL 的异步错误派生（按 `ErrorKind` 分类 + 保留 source 链）
//!
//! 这个 Demo 演示的原语，正是「对象存储 KV 后端」里 leader 选举 CAS 抢锁、
//! 以及多 key 事务「单对象 CAS 提交点」的底层基础。

use std::fmt;

use opendal::services::S3;
use opendal::{ErrorKind, Operator};

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

/// 墓碑值：逻辑删除的标记。对象存储上「删除」退化为「覆盖写墓碑」，
/// 因为 OpenDAL 0.58 的 delete 路径不支持 If-Match 条件。
const TOMBSTONE: &[u8] = b"__TOMBSTONE__";

/// 基于 OpenDAL `Operator` 的条件写封装。
pub struct CasStore {
    op: Operator,
}

impl CasStore {
    pub fn new(op: Operator) -> Self {
        Self { op }
    }

    /// 读取对象的当前 ETag（对象版本标识）。
    pub async fn etag(&self, key: &str) -> Result<String, StorageError> {
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
        let bs = self
            .op
            .read(key)
            .await
            .map_err(|e| classify(key, None, e))?;
        Ok(bs.to_bytes().to_vec())
    }

    /// 原子覆盖写：仅当对象当前 ETag == `etag` 时才写入。成功返回新 ETag。
    ///
    /// 底层：`If-Match: <etag>`。对象被并发修改过 → `ConditionNotMatch`。
    pub async fn write_if_match(
        &self,
        key: &str,
        data: Vec<u8>,
        etag: &str,
    ) -> Result<String, StorageError> {
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
        let meta = self
            .op
            .write_with(key, data)
            .if_not_exists(true)
            .await
            .map_err(|e| classify(key, None, e))?;
        Ok(meta.etag().map(str::to_string).unwrap_or_default())
    }

    /// 无条件删除。**不是** CAS 语义，仅用于清理上一次运行的残留对象。
    ///
    /// 需要它的原因：本 demo 的「删除」是墓碑写（见下），不会真正移除对象，
    /// 所以每次运行之间 key 会残留。删除不存在的对象时 S3 返回成功，因此可安全重复调用。
    pub async fn delete(&self, key: &str) -> Result<(), StorageError> {
        self.op.delete(key).await.map_err(StorageError::Opendal)?;
        Ok(())
    }

    /// 原子删除（墓碑）：仅当对象当前 ETag == `etag` 时，写入墓碑标记。
    ///
    /// ⚠️ OpenDAL 0.58 的 `delete_with` 只有 `.version()`（对象版本号），**没有
    /// `.if_match()` 条件删除**。所以对象存储上的「原子删除」无法直接用 delete 表达，
    /// 这里退化为「墓碑写」：用 If-Match 覆盖成墓碑值，原子性由写路径的 CAS 保证。
    /// 这也是对象存储 KV 后端删除节点的常见做法（逻辑删除 + 后台 GC）。
    ///
    /// 成功返回墓碑的新 ETag。
    pub async fn delete_if_match(&self, key: &str, etag: &str) -> Result<String, StorageError> {
        self.write_if_match(key, TOMBSTONE.to_vec(), etag).await
    }
}

// ============================================================================
// 三、构建 Operator（连接 MinIO）
// ============================================================================

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// 连接 MinIO（S3 兼容）。默认值对齐仓库 `tests-integration` 的 docker-compose。
fn build_operator() -> opendal::Result<Operator> {
    let endpoint = env_or("MINIO_ENDPOINT", "http://127.0.0.1:9000");
    let region = env_or("MINIO_REGION", "us-west-2");
    let ak = env_or("MINIO_ACCESS_KEY_ID", "superpower_ci_user");
    let sk = env_or("MINIO_SECRET_ACCESS_KEY", "superpower_password");
    let bucket = env_or("MINIO_BUCKET", "greptime"); // docker-compose 已默认创建

    Operator::new(
        S3::default()
            .endpoint(&endpoint)
            .region(&region)
            .access_key_id(&ak)
            .secret_access_key(&sk)
            .bucket(&bucket)
            .root("/"),
    )
}

// ============================================================================
// 四、演示：模拟 leader 选举的 CAS 抢锁 + 并发冲突
// ============================================================================

#[tokio::main]
async fn main() -> Result<(), StorageError> {
    let op = build_operator().map_err(StorageError::Opendal)?;
    let store = CasStore::new(op);
    let key = "cas-demo/leader";

    println!("=== OpenDAL 条件写(CAS) Demo ===\n");

    // [0] 清理上一次运行的残留。本 demo 的「删除」（步骤 [5]）是**墓碑写**、
    //     不会真正移除对象，所以 key 会留在 bucket 里；不先清掉的话，
    //     下面的 write_if_not_exists 必然拿到 Conflict，整个 demo 只能跑一次。
    println!("[0] delete({key})  ← 清理上一次运行的残留（本 demo 的删除是墓碑写，不真删）");
    store.delete(key).await?;
    println!("    ok: 已清空\n");

    // [1] 原子创建：进程 A 抢 leader
    println!("[1] write_if_not_exists({key})  ← 进程 A 抢 leader（If-None-Match: *）");
    let etag_v1 = store.write_if_not_exists(key, b"node-A".to_vec()).await?;
    println!("    ok: 当选，etag = {etag_v1}\n");

    // [2] 再次原子创建 → 冲突：进程 B 并发抢同一把锁
    println!("[2] write_if_not_exists({key})  ← 进程 B 并发抢同一把锁");
    match store.write_if_not_exists(key, b"node-B".to_vec()).await {
        Ok(_) => println!("    UNEXPECTED: 重复创建竟然成功了"),
        Err(StorageError::Conflict(c)) => println!("    conflict caught: {c}"),
        Err(e) => return Err(e),
    }
    println!();

    // [3] 用正确 ETag 覆盖写：进程 A 续租（更新 lease 时间戳）
    println!("[3] write_if_match({key}, etag=v1)  ← 用当前版本续租（If-Match）");
    let etag_v2 = store
        .write_if_match(key, b"node-A-keepalive-1".to_vec(), &etag_v1)
        .await?;
    println!("    ok: 续租成功，新 etag = {etag_v2}\n");

    // [4] 用过期 ETag 覆盖写 → 冲突：并发写，对象已被别人改过
    println!("[4] write_if_match({key}, etag=v1[过期])  ← 并发写：对象已被改过");
    match store
        .write_if_match(key, b"stale-writer".to_vec(), &etag_v1)
        .await
    {
        Ok(_) => println!("    UNEXPECTED: 过期 etag 竟然写成功了"),
        Err(StorageError::Conflict(c)) => println!("    conflict caught: {c}"),
        Err(e) => return Err(e),
    }
    println!();

    // [5] 用正确 ETag 墓碑删除：进程 A 主动 resign（逻辑删除）
    println!("[5] delete_if_match({key}, etag=v2)  ← 正确版本墓碑删除（resign）");
    let etag_tomb = store.delete_if_match(key, &etag_v2).await?;
    println!("    ok: 墓碑写入成功，etag = {etag_tomb}\n");

    // [6] 用过期 ETag 对「已墓碑化」的对象再次删除 → 冲突，防止重复 resign
    println!("[6] delete_if_match({key}, etag=v2[过期])  ← 重复 resign 被拒（fencing）");
    match store.delete_if_match(key, &etag_v2).await {
        Ok(_) => println!("    UNEXPECTED: 过期 etag 竟然重复删除成功"),
        Err(StorageError::Conflict(c)) => println!("    conflict caught: {c}"),
        Err(e) => return Err(e),
    }

    println!("\n=== 全部通过 ===");
    Ok(())
}
