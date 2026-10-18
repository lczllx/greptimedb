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

//! 子命令分发：一个场景一个子命令，每个场景自建自清 key 前缀、打印一行判定。
//!
//! 场景清单与判定信号见 `README.md`。

use objstore_toy::cfg;
use objstore_toy::kv::{CasStore, StorageError};
use objstore_toy::scenarios;

type Result<T> = std::result::Result<T, StorageError>;

#[tokio::main]
async fn main() -> Result<()> {
    let scenario = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "ping".to_string());
    let store = CasStore::new(cfg::build_operator().map_err(StorageError::Opendal)?);

    match scenario.as_str() {
        "ping" => scenario_ping(&store).await?,
        "s0" => scenarios::s0::run(&store).await?,
        "s1" => scenarios::s1::run(&store).await?,
        "s2" => scenarios::s2::run(&store).await?,
        "s3" => scenarios::s3::run(&store).await?,
        "s4" => scenarios::s4::run(&store).await?,
        "s5" => scenarios::s5::run(&store).await?,
        "s6" => scenarios::s6::run(&store).await?,
        "s7" => scenarios::s7::run(&store).await?,
        "s8" => scenarios::s8::run(&store).await?,
        "s9" => scenarios::s9::run(&store).await?,
        "s10" => scenarios::s10::run(&store).await?,
        "s11" => scenarios::s11::run(&store).await?,
        "s12" => scenarios::s12::run(&store).await?,
        "s13" => scenarios::s13::run(&store).await?,
        "s15" => scenarios::s15::run(&store).await?,
        "s16" => scenarios::s16::run(&store).await?,
        "s17" => scenarios::s17::run(&store).await?,
        // 全部场景（S0–S16；S14 留给 V2b）。
        // ⚠️ s15 要对拍本机 etcd（127.0.0.1:2379），所以 `all` 也带上了这个依赖；
        //    etcd 不可达时它会报 FAIL 并让整条命令非零退出 —— 不静默跳过。
        "all" => {
            scenario_ping(&store).await?;
            scenarios::s0::run(&store).await?;
            scenarios::s1::run(&store).await?;
            scenarios::s2::run(&store).await?;
            scenarios::s3::run(&store).await?;
            scenarios::s4::run(&store).await?;
            scenarios::s5::run(&store).await?;
            scenarios::s6::run(&store).await?;
            scenarios::s7::run(&store).await?;
            scenarios::s8::run(&store).await?;
            scenarios::s9::run(&store).await?;
            scenarios::s10::run(&store).await?;
            scenarios::s11::run(&store).await?;
            scenarios::s12::run(&store).await?;
            scenarios::s13::run(&store).await?;
            scenarios::s15::run(&store).await?;
            scenarios::s16::run(&store).await?;
            scenarios::s17::run(&store).await?;
        }
        other => {
            eprintln!(
                "unknown scenario `{other}`；当前可用：ping | s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7 | s8 | s9 | s10 | s11 | s12 | s13 | s15 | s16 | s17 | all"
            );
            eprintln!("场景清单与判定信号见 README.md");
            std::process::exit(2);
        }
    }
    Ok(())
}

/// 连通性自检：能读、能 CAS、冲突能分类。
///
/// 覆盖三条路：原子创建、读、CAS 覆盖写，外加一次**必须被拒**的过期写
/// （顺带验证 `ConditionNotMatch → Conflict` 的错误分类是通的）。
async fn scenario_ping(store: &CasStore) -> Result<()> {
    let key = "toy/ping";
    // 清掉上一轮的残留（可能是墓碑），保证可重复跑。
    let _ = store.delete(key).await;

    let e1 = store.write_if_not_exists(key, b"ping-v1".to_vec()).await?;
    let body = store.read(key).await?;
    let e2 = store.write_if_match(key, b"ping-v2".to_vec(), &e1).await?;

    // 拿过期的 e1 再 CAS 一次：必须被拒。
    let stale_rejected = matches!(
        store
            .write_if_match(key, b"should-not-land".to_vec(), &e1)
            .await,
        Err(StorageError::Conflict(_))
    );

    let verdict = if e1 != e2 && body == b"ping-v1" && stale_rejected {
        "PASS"
    } else {
        "FAIL"
    };
    println!(
        "[{verdict}] ping: create_if_absent etag={e1} read={:?} cas_put etag={e2} stale_write_rejected={stale_rejected}",
        String::from_utf8_lossy(&body)
    );

    store.delete(key).await?; // 自清
    Ok(())
}
