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

//! **S15 —— 视图求值 == etcd：随机事务序列两边对拍（含冷 key）。**
//!
//! 这是清单里的**承重墙**。设计 §3.2 的立论是"compare 在 leader 内存视图上求值，
//! 于是提交路径不用为每个 compare 发一次 GET"；§3.2.1 把它收紧成**逐 key 成立**：
//! 视图没覆盖的 key 只是"还没人问过"，**未命中要回源一次**。
//!
//! ⇒ 本场景判定的是**那一句收紧到底够不够**：同一串事务分别喂给真 etcd 和
//! toy 的「日志 + 树 + 视图」，两边必须处处一致。**视图替代不了 etcd 的话，§3.2 要重设计。**
//!
//! ## 为什么故意让 compare 打冷 key
//!
//! 对拍本身不难，难在**造出会判错的那一格**。把未命中当成"值不存在"的实现，
//! 四种 compare 里有两格答得和正确实现一模一样：
//!
//! | 冷 key 上的 compare | "当不存在" | 回源 | 有区分力？ |
//! | --- | --- | --- | --- |
//! | `expect = None`，key 确实不在 | 真 | 真 | 否 |
//! | `expect = Some(x)`，x 是编的 | 假 | 假（真实值 ≠ x） | 否 |
//! | `expect = None`，key **在** | 真 | 假 | **是** |
//! | `expect = Some(真实值)`，key **在** | 假（`None ≠ Some`） | 真 | **是** |
//!
//! 所以生成器七成的 compare 都写成"**应当为真**"（拿树里的真实值当期望值）——
//! 这既是真实 OCC 的形状（读过值、再拿它做 CAS），也覆盖了上面两格有区分力的。
//! 剩下三成故意写成陈旧的期望，专门去撞另外两格（撞不中也没关系，本场景要的不是覆盖率）。
//!
//! `View::misses_present` 数的正是"冷 key 上回源**读到了值**"的次数，
//! 它是本场景的**前提断言**：这个数若是 0，本次对拍什么都没验到，
//! 而它会照样打印 `[PASS]`（V5 变异① 的教训）。
//!
//! ## 对拍的另一边为什么是真 etcd
//!
//! 自己对拍的参照物不能是自己写的模型 —— 那种"两边都错得一样"的对拍毫无价值。
//! etcd 是这套语义的**上游定义**（仓库的 OCC 就是照它做的：值比较、无 revision），
//! 所以拿它当参照物，误差才有意义。
//!
//! ## 边界（诚实说明）
//!
//! - 需要本机 etcd（`127.0.0.1:2379`）。**不可达就报 FAIL 而不是跳过** ——
//!   跳过会让"没跑"和"跑绿了"看起来一样。
//! - etcd 那边的 key 直接用裸的 `a/…`（本 toy 专用容器，不与其他用途共用）；
//!   场景开头结尾都把 `[a/, e)` 这个区间清干净，清完**再列一遍核对**。
//! - `Cmp::expect = None` 在 etcd 侧用 `Compare::version(key, Equal, 0)` 表达
//!   （"这个 key 一个版本都没有"）。这**不代表**我们的存储有 revision ——
//!   只是为了向 etcd 问出同一个谓词。
//! - `DurableStore` 是"树"的替身（按 key 首字节分片），不是真 B-link 树；
//!   树的布局与分裂由 S2–S4 覆盖，本场景只管"range 会跨对象"。
//! - 同一个 txn 里不重复碰同一个 key：etcd-client 的文档明确写着
//!   "It is not allowed to modify the same key several times within one txn"。
//!   这是 etcd 的约束，toy 侧照做，免得对拍撞在一个与本题无关的限制上。

use std::collections::BTreeMap;

use etcd_client::{Client, Compare, CompareOp, DeleteOptions, GetOptions, Txn, TxnOp};

use crate::kv::{CasStore, StorageError};
use crate::leader_kv::{Cmp, LeaderKv, Op, Txn as ToyTxn};

/// etcd 端点。只对拍本机明文 etcd（起法见 README.md）。
const ETCD_ENDPOINT: &str = "http://127.0.0.1:2379";

/// toy 在对象存储上的 key 前缀（本场景独占）。
const PREFIX: &str = "toy/s15";
const EPOCH: u64 = 1;

/// 对拍的 key 空间：4 组 × 32 个。分组字母不同 ⇒
/// `DurableStore` 按首字节分片时会切成 **4 个叶子对象**，range 必须跨对象组装。
const GROUPS: [&str; 4] = ["a", "b", "c", "d"];
const PER_GROUP: usize = 32;

/// range 的边界：`["a/", "e")` 正好覆盖四组（`"e"` 比任何 `"d/…"` 都大）。
const RANGE_START: &str = "a/";
const RANGE_END: &str = "e";

const STEPS: usize = 80;

pub async fn run(store: &CasStore) -> Result<(), StorageError> {
    let mut client = match Client::connect([ETCD_ENDPOINT], None).await {
        Ok(c) => c,
        Err(e) => {
            println!("[FAIL] s15 需要本机 etcd（{ETCD_ENDPOINT}）：{e}");
            println!("       起法见 README.md。本场景不对拍就没有意义，所以不跳过。");
            return Err(StorageError::NotFound(format!("etcd 不可达: {e}")));
        }
    };

    let all = keyspace();

    // ① 两边清空，并且**核对清空这件事本身**（V5 的教训：动作 ≠ 验证）。
    wipe(store).await?;
    etcd_wipe(&mut client).await?;
    let toy_residue = kv_range(store).await?;
    let etcd_residue = etcd_range(&mut client).await?;
    if !toy_residue.is_empty() || !etcd_residue.is_empty() {
        println!(
            "[FAIL] s15 清场不干净：toy 残留 {} 个、etcd 残留 {} 个",
            toy_residue.len(),
            etcd_residue.len()
        );
        return Ok(());
    }

    // ② 铺初始数据 —— **只进树，不进日志、不进视图**。
    //    于是这批 key 在视图里一个都没有：本场景要的冷 key 天然就有了。
    let seed: Vec<(String, String)> = all
        .iter()
        .enumerate()
        .map(|(i, k)| (k.clone(), format!("seed-{i:03}")))
        .collect();
    let mut kv = LeaderKv::open(store, PREFIX, EPOCH).await?;
    kv.seed(store, &seed).await?;
    for (k, v) in &seed {
        client
            .put(k.as_str(), v.as_str(), None)
            .await
            .map_err(|e| etcd_err("seed put", e))?;
    }

    // ③ 起点必须一模一样，否则后面比出来的差异是清场/铺数据的锅，不是视图的锅。
    let toy_start = kv.range_between(store, RANGE_START, RANGE_END).await?;
    let etcd_start = etcd_range(&mut client).await?;
    let shards = kv.shard_count(store).await?;
    if toy_start != etcd_start {
        println!(
            "[FAIL] s15 起点就不一致：toy {} 个、etcd {} 个",
            toy_start.len(),
            etcd_start.len()
        );
        return Ok(());
    }

    // ④ 随机事务序列，两边各跑一遍。
    //
    //    影子模型 `shadow` 是**独立的第三方**：它只按 etcd 的语义推"该走哪个分支"。
    //    它既用来生成"应当为真"的 compare，也用来对最终状态做第三路核对。
    let mut shadow: BTreeMap<String, String> = seed.iter().cloned().collect();
    let mut rng = Rng::new(0x5DEE_CE66_D1CE_B00D);

    let mut compares_total = 0usize;
    let mut then_taken = 0usize;
    let mut commits = 0u64;
    let mut branch_mismatch = 0usize;

    for step in 0..STEPS {
        let txn = gen_txn(&mut rng, &all, &shadow, step);
        compares_total += txn.compares.len();

        // 影子说这次该走哪支。
        let expect_true = txn
            .compares
            .iter()
            .all(|c| shadow.get(&c.key).cloned() == c.expect);

        let toy_true = kv.txn(store, &txn).await?;
        let etcd_true = etcd_txn(&mut client, &txn).await?;
        if toy_true != expect_true || etcd_true != expect_true || toy_true != etcd_true {
            branch_mismatch += 1;
            if branch_mismatch <= 5 {
                println!(
                    "        [MISMATCH] step={step} 该走={expect_true} toy={toy_true} etcd={etcd_true} \
                     compares={:?}",
                    txn.compares
                );
            }
        }

        // 按**影子的结论**推进影子（不是按任何一边的结论）—— 否则影子会被坏实现带跑。
        let branch = if expect_true { &txn.then } else { &txn.else_ };
        for op in branch {
            match op {
                Op::Put(k, v) => {
                    shadow.insert(k.clone(), v.clone());
                }
                Op::Delete(k) => {
                    shadow.remove(k);
                }
            }
        }
        if expect_true {
            then_taken += 1;
        }
        if !branch.is_empty() {
            commits += 1;
        }
    }

    // ⑤ 三路核对最终状态。
    let toy_final = kv.range_between(store, RANGE_START, RANGE_END).await?;
    let etcd_final = etcd_range(&mut client).await?;
    let shadow_final: Vec<(String, String)> =
        shadow.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

    // ⑥ 视图的记账 —— 顺便当**前提检查**。
    let (hits, misses, misses_present) = kv.view_stats();
    let lsn = kv.committed_lsn(store).await?;

    let mut problems: Vec<String> = Vec::new();
    if branch_mismatch > 0 {
        problems.push(format!("{branch_mismatch} 个 txn 的分支判定不一致"));
    }
    if toy_final != etcd_final {
        problems.push(format!(
            "最终状态 toy {} 个 vs etcd {} 个",
            toy_final.len(),
            etcd_final.len()
        ));
    }
    if toy_final != shadow_final {
        problems.push("最终状态与影子模型不符（生成器或事务语义理解有偏差）".to_string());
    }
    if misses_present == 0 {
        problems.push(
            "一次都没在冷 key 上回源读到值 ⇒ 对拍没有区分力（未命中的两种处理答得一样）"
                .to_string(),
        );
    }
    if lsn != commits {
        problems.push(format!("提交水位 {lsn} ≠ 实际提交次数 {commits}"));
    }

    let verdict = if problems.is_empty() { "PASS" } else { "FAIL" };
    // 标题和结论句都**不能写死**：写死的话，FAIL 的那一版会打出
    // "[FAIL] … 两边逐条一致" 这种自己跟自己打架的话（MUTATION-3 就是这么露的马脚）。
    let headline = if problems.is_empty() {
        "两边逐条一致"
    } else {
        "两边有分歧"
    };

    println!(
        "[{verdict}] s15 视图求值 == etcd：{STEPS} 个 txn / {compares_total} 次 compare {headline}"
    );
    println!(
        "        key 空间 {}（{shards} 个叶子对象 × {}）｜range [{RANGE_START}, {RANGE_END})",
        all.len(),
        PER_GROUP
    );
    println!(
        "        分支判定：then {then_taken} / else {}｜不一致 {branch_mismatch}",
        STEPS - then_taken
    );
    println!(
        "        视图记账：命中 {hits}｜未命中 {misses}｜**其中回源读到值 {misses_present}**（有区分力的那一格）"
    );
    println!("        提交水位：实际提交 {commits} / manifest.committed_lsn {lsn}");
    let agreed = toy_final == etcd_final && toy_final == shadow_final;
    println!(
        "        最终状态：toy {} / etcd {} / 影子 {} 个 key｜三者{}",
        toy_final.len(),
        etcd_final.len(),
        shadow_final.len(),
        if agreed { "一致" } else { "**不一致**" }
    );
    for p in &problems {
        println!("        ⇒ {p}");
    }

    // ⑦ 收尾清场（同样要核对）。
    wipe(store).await?;
    etcd_wipe(&mut client).await?;
    let toy_residue = kv_range(store).await?;
    let etcd_residue = etcd_range(&mut client).await?;
    if toy_residue.is_empty() && etcd_residue.is_empty() {
        println!("        收尾：两边都清干净（各 0 个 key）");
    } else {
        println!(
            "        [WARN] 收尾没清干净：toy {} 个、etcd {} 个",
            toy_residue.len(),
            etcd_residue.len()
        );
    }

    Ok(())
}

// ============================================================================
// 事务生成
// ============================================================================

/// 生成一个 txn：七成的 compare 是"应当为真"的（见文件头那格判定表）。
fn gen_txn(
    rng: &mut Rng,
    all: &[String],
    shadow: &BTreeMap<String, String>,
    step: usize,
) -> ToyTxn {
    let mut compares = Vec::new();

    let target = all[rng.below(all.len())].clone();
    let expect = if rng.below(10) < 7 {
        // 应当为真：拿当前值当期望值（真实 OCC 的形状）
        shadow.get(&target).cloned()
    } else {
        // 故意陈旧：把"在不在"翻一下
        match shadow.get(&target) {
            Some(_) => None,
            None => Some(format!("stale-{step}")),
        }
    };
    compares.push(Cmp {
        key: target,
        expect,
    });

    // 两成半再加一个恒真的 compare（多 compare 的 txn 也是真实形状）。
    if rng.below(4) == 0 {
        let k2 = all[rng.below(all.len())].clone();
        let e2 = shadow.get(&k2).cloned();
        compares.push(Cmp {
            key: k2,
            expect: e2,
        });
    }

    let then_ops = gen_ops(rng, all, step, 2);
    let else_ops = gen_ops(rng, all, step, 1);

    ToyTxn {
        compares,
        then: then_ops,
        else_: else_ops,
    }
}

/// 生成至多 `count` 个操作，**同一个 txn 内不重复碰同一个 key**（etcd 的限制）。
fn gen_ops(rng: &mut Rng, all: &[String], step: usize, count: usize) -> Vec<Op> {
    let mut picked: Vec<String> = Vec::new();
    let mut ops = Vec::new();
    for _ in 0..count {
        let k = all[rng.below(all.len())].clone();
        if picked.contains(&k) {
            continue;
        }
        picked.push(k.clone());
        if rng.below(10) < 3 {
            ops.push(Op::Delete(k));
        } else {
            ops.push(Op::Put(k, format!("v{step}-{}", ops.len())));
        }
    }
    ops
}

/// 固定 seed 的 xorshift64* —— 对拍要**可复现**，且不给 toy 添依赖。
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // 0 是 xorshift 的不动点，换个非零初值。
        Self(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }
}

// ============================================================================
// 两边各自的薄封装
// ============================================================================

fn keyspace() -> Vec<String> {
    let mut ks = Vec::new();
    for g in GROUPS {
        for i in 0..PER_GROUP {
            ks.push(format!("{g}/{i:03}"));
        }
    }
    ks
}

fn etcd_err(what: &str, e: etcd_client::Error) -> StorageError {
    // toy 的错误面刻意很小，没有"别的"这个桶 —— 沿用既有惯例塞进 NotFound。
    StorageError::NotFound(format!("etcd {what}: {e}"))
}

/// **toy 侧**的 range（走树，不经过视图）。
async fn kv_range(store: &CasStore) -> Result<Vec<(String, String)>, StorageError> {
    let durable = crate::leader_kv::DurableStore::new(&format!("{PREFIX}/tree"));
    durable.range_between(store, RANGE_START, RANGE_END).await
}

/// **etcd 侧**的同一个 range。
async fn etcd_range(client: &mut Client) -> Result<Vec<(String, String)>, StorageError> {
    let resp = client
        .get(RANGE_START, Some(GetOptions::new().with_range(RANGE_END)))
        .await
        .map_err(|e| etcd_err("range", e))?;
    Ok(resp
        .kvs()
        .iter()
        .map(|kv| {
            (
                String::from_utf8_lossy(kv.key()).into_owned(),
                String::from_utf8_lossy(kv.value()).into_owned(),
            )
        })
        .collect())
}

/// 把 toy 的 txn 翻译成 etcd 的 txn，只取 `succeeded`。
///
/// 翻译表就两行，但每一行都要说清是在"问同一个谓词"，而不是在假装我们也有这个概念：
///
/// | toy | etcd |
/// | --- | --- |
/// | `expect = Some(v)` | `Compare::value(key, Equal, v)` |
/// | `expect = None` | `Compare::version(key, Equal, 0)`（一个版本都没有 ⇒ 不存在） |
async fn etcd_txn(client: &mut Client, txn: &ToyTxn) -> Result<bool, StorageError> {
    let compares: Vec<Compare> = txn
        .compares
        .iter()
        .map(|c| match &c.expect {
            Some(v) => Compare::value(c.key.as_str(), CompareOp::Equal, v.as_str()),
            None => Compare::version(c.key.as_str(), CompareOp::Equal, 0),
        })
        .collect();

    let to_ops = |ops: &[Op]| -> Vec<TxnOp> {
        ops.iter()
            .map(|op| match op {
                Op::Put(k, v) => TxnOp::put(k.as_str(), v.as_str(), None),
                Op::Delete(k) => TxnOp::delete(k.as_str(), None),
            })
            .collect()
    };

    let resp = client
        .txn(
            Txn::new()
                .when(compares)
                .and_then(to_ops(&txn.then))
                .or_else(to_ops(&txn.else_)),
        )
        .await
        .map_err(|e| etcd_err("txn", e))?;
    Ok(resp.succeeded())
}

async fn etcd_wipe(client: &mut Client) -> Result<(), StorageError> {
    client
        .delete(
            RANGE_START,
            Some(DeleteOptions::new().with_range(RANGE_END)),
        )
        .await
        .map_err(|e| etcd_err("wipe", e))?;
    Ok(())
}

/// 递归列出 `root` 下的**全部对象**。
///
/// ⚠️ `CasStore::list` **只管一层**，而且返回的东西里混着 `.../tree/` 这种
/// **"目录"条目**（对象存储没有目录，但列表 API 会把公共前缀当成一项返回）。
/// 直接 `for key in list(prefix) { delete(key) }` 会**看起来清干净了**：
/// 顶层对象真删了，嵌套的一层没碰，连删除"目录条目"都是无声的空操作。
///
/// 这条是 S15 第一次跑就撞上的 —— 判定依据是收尾自检报了 "toy 99 个、etcd 0 个"。
async fn list_all(store: &CasStore, root: &str) -> Result<Vec<String>, StorageError> {
    let mut pending = vec![root.to_string()];
    let mut objects = Vec::new();
    while let Some(prefix) = pending.pop() {
        for entry in store.list(&prefix).await? {
            if entry.ends_with('/') {
                pending.push(entry); // 目录条目：往下走
            } else {
                objects.push(entry);
            }
        }
    }
    Ok(objects)
}

/// 清掉 toy 这个场景用到的全部对象（manifest、日志段、树的目录与叶子）。
///
/// 清完**再列一遍**核对：删错 key、前缀写错、漏登记都长成"清干净了"的样子
/// （V5 的教训：动作 ≠ 验证）。还剩东西就把 key 打出来 —— 否则只知道"没清干净"，
/// 不知道剩的是哪一类对象。
async fn wipe(store: &CasStore) -> Result<(), StorageError> {
    let root = format!("{PREFIX}/");
    for key in list_all(store, &root).await? {
        if let Err(e) = store.delete(&key).await {
            println!("        [WARN] delete {key} 失败：{e}");
        }
    }
    let after = list_all(store, &root).await?;
    if !after.is_empty() {
        println!(
            "        [WARN] 清场后 {root} 下还剩 {} 个对象，前几个：{:?}",
            after.len(),
            &after[..after.len().min(5)]
        );
    }
    Ok(())
}
