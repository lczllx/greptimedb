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

//! MinIO / S3 连接（搬自 `opendal-cas-demo`）。

use std::sync::atomic::{AtomicUsize, Ordering};

use opendal::layers::{RetryEvent, RetryInterceptor, RetryLayer};
use opendal::services::S3;
use opendal::Operator;

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

// ============================================================================
// 瞬态重试（S8 加进来的，见下）
// ============================================================================

/// 进程级累计：`RetryLayer` 为吸收**瞬态**错误而重试了几次。
static TRANSIENT_RETRIES: AtomicUsize = AtomicUsize::new(0);

/// 读计数器。场景把它打进输出 —— 这个数本身是一条证据，不是日志噪音。
pub fn transient_retries() -> usize {
    TRANSIENT_RETRIES.load(Ordering::Relaxed)
}

struct CountRetries;

impl RetryInterceptor for CountRetries {
    fn intercept(&self, _event: RetryEvent<'_>) {
        TRANSIENT_RETRIES.fetch_add(1, Ordering::Relaxed);
    }
}

/// 连接 MinIO（S3 兼容协议）。
///
/// 默认值对齐仓库 `tests-integration/fixtures/docker-compose.yml` 里的 minio 服务，
/// 所以只要按 README 起 compose，就**不用设任何环境变量**。
///
/// # 为什么加了 `RetryLayer`（S8 补的）
///
/// S8 第一次跑就撞上了：并发读写跑完、紧接着的下一个 `read` 报
/// `Unexpected (temporary) ... connection closed before message completed` ——
/// 10 次里有 7 次。这是**传输层**的瞬态（连接池里被服务端半关掉的 keep-alive 连接被重用），
/// 跟树、跟 CAS 语义都无关；但 toy 若把它当 FAIL 报出来，就是**用环境噪音污染结论**。
///
/// 挂上这一层之后**没有再失败过**；瞬态本身没有消失，只是被吸收了 ——
/// 把 S8 的分裂延迟关掉再跑（那是触发它的时序），10 次里仍有 9 次记到一次重试。
///
/// ⇒ 挂上 opendal 自己的 `RetryLayer`：它**只**重试 `Error::is_temporary()` 为真的错误。
/// 这条边界正是设计想要的：
///
/// - 瞬态传输错误（`is_temporary() == true`）⇒ 自动重试，业务层看不见；
/// - **CAS 冲突 `ConditionNotMatch` 不是 temporary ⇒ 不会被这一层吞掉**，
///   `Conflict` 该怎么冒出来还怎么冒出来。S1/S5b/S7/S8 那些**故意制造**的竞争不受影响。
///
/// 重试次数被记下来并在场景输出里打印 —— 这个数字是证据（说明这层不是可选项），不是日志。
pub fn build_operator() -> opendal::Result<Operator> {
    let endpoint = env_or("MINIO_ENDPOINT", "http://127.0.0.1:9000");
    let region = env_or("MINIO_REGION", "us-west-2");
    let ak = env_or("MINIO_ACCESS_KEY_ID", "superpower_ci_user");
    let sk = env_or("MINIO_SECRET_ACCESS_KEY", "superpower_password");
    let bucket = env_or("MINIO_BUCKET", "greptime"); // compose 已默认创建

    Operator::new(
        S3::default()
            .endpoint(&endpoint)
            .region(&region)
            .access_key_id(&ak)
            .secret_access_key(&sk)
            .bucket(&bucket)
            .root("/"),
    )
    .map(|op| op.layer(RetryLayer::default().with_notify(CountRetries)))
}
