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

//! **S13 —— 一次 txn 到底多大？**
//!
//! 假设：真实 DDL 的一次 txn **远小于**候选段大小 ⇒ 日志段永远装得下一个 txn。
//!
//! 为什么这条要紧：设计说 `max_txn_ops` 从存储层的**硬**约束（etcd 的 128 op）
//! **降级为段大小的软参数**，但当时没写清"软参数超了怎么办"，一度看起来是
//! 「实现要多复杂」的分水岭（要不要保留「一个 txn 跨多段」那套）。
//!
//! **这条分水岭后来被设计口径消掉了（2026-10-15）：段大小是「攒批凑到这个量就提交」的
//! 目标值，不是上限。** 超过它的单个事务自己占一个段、单独提交，原子性仍然来自
//! Manifest 那一次 CAS —— 段多大不参与任何正确性判断。所以本场景量出来的
//! "一次 txn 有多大"仍然有用（它决定一个段最大会长多大、一次提交的写会多重），
//! 但它不再是一个需要被遵守、超了要有对策的界。
//!
//! ## 怎么量（不连对象存储，纯字节数）
//!
//! 真实编码是 **JSON**（`MetadataValue::try_as_raw_value` → `serde_json::to_vec`），
//! 所以字节数**对字段名敏感** ⇒ 本文件把真实结构**逐字段照抄**（连 serde 属性一起），
//! 只把值的**规模**参数化：
//!
//! - `R` = 一张表的 region 数，`D` = datanode 数，`C` = 列数。
//!
//! ## 两个放大项（都是真实的，不是本场景构造出来的）
//!
//! 1. `PhysicalTableRouteValue.region_routes` 存**一张表的全部路由** ⇒ `O(R)`。
//! 2. `DatanodeTableValue.region_info.region_wal_options` 存的是**整张表**的
//!    per-region wal options（`datanode_table.rs` 里的 FIXME 自己承认了这件事），
//!    而它**每个 datanode 一份** ⇒ 一次建表的合并 txn 里这一项是 `O(R·D)`。
//!    更妙的是它的值是**再嵌一层的 JSON 字符串**（`region_wal_options_serde`），
//!    于是每个 region 还要为转义多付几个字节。
//!
//! ## 判定
//!
//! **默认规模小到可以忽略** 且 **对 R 线性（不是平方）** 且
//! **256KB 的段能装下「一百个 region、十个 datanode」的表** ⇒ PASS。
//! 无论 PASS/FAIL 都打印**crossover 表**：多少个 region 的建表事务会自己就顶满每个候选段大小
//! （= 到这个量级，攒批对它已经不起作用了）。

use std::collections::{BTreeMap, HashMap};

use serde::Serialize;

use crate::kv::{CasStore, StorageError};

// ============================================================================
// 真实结构的镜像
//
// ⚠️ 字段名、顺序、serde 属性都照抄仓库里的定义 —— 字节数对它们敏感。
// 每个类型上面标了真实定义的位置。
// ============================================================================

/// `src/common/rpc` 里的 `Peer`（proto 生成）：只有 id 和 addr。
#[derive(Serialize)]
struct Peer {
    id: u64,
    addr: String,
}

/// `common/meta/src/rpc/router.rs` 的 `Region`。
#[derive(Serialize)]
struct Region {
    id: u64,
    name: String,
    attrs: BTreeMap<String, String>,
    partition_expr: String,
}

/// `common/meta/src/rpc/router.rs` 的 `RegionRoute`。
///
/// 注意 `leader_down_since` **没有** `skip_serializing_if` ⇒ `None` 也占位（`null`）；
/// 而 `leader_state` / `write_route_policy` 有 ⇒ 不占位。这种不对称正是"照抄"的意义。
#[derive(Serialize)]
struct RegionRoute {
    region: Region,
    leader_peer: Option<Peer>,
    follower_peers: Vec<Peer>,
    #[serde(skip_serializing_if = "Option::is_none")]
    leader_state: Option<LeaderState>,
    leader_down_since: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    write_route_policy: Option<()>,
}

/// 占位：真实类型是 `LeaderState`。本场景**从不设它**（那正是 `None` 分支）。
#[derive(Serialize)]
struct LeaderState {
    #[allow(dead_code)]
    downgraded: bool,
}

/// `key/table_route.rs` 的 `TableRouteValue` —— internally tagged。
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum TableRouteValue {
    Physical(PhysicalTableRouteValue),
}

/// `key/table_route.rs` 的 `PhysicalTableRouteValue`。
#[derive(Serialize)]
struct PhysicalTableRouteValue {
    region_routes: Vec<RegionRoute>,
    max_region_number: u32,
    version: u64,
}

/// `key/datanode_table.rs` 的 `RegionInfo`。
///
/// `region_wal_options` 的**值类型是 `String`** 而不是 `WalOptions` ——
/// 真实实现先把 `WalOptions` 序列化成 JSON 字符串再放进去
/// （`wal_provider.rs` 的 `serialize_region_wal_options`）⇒ 外层 JSON 里要**再转义一次**。
#[derive(Serialize)]
struct RegionInfo {
    engine: String,
    region_storage_path: String,
    region_options: HashMap<String, String>,
    region_wal_options: HashMap<u32, String>,
}

/// `key/datanode_table.rs` 的 `DatanodeTableValue` —— `region_info` 是 `#[serde(flatten)]`。
#[derive(Serialize)]
struct DatanodeTableValue {
    table_id: u32,
    regions: Vec<u32>,
    follower_regions: Vec<u32>,
    #[serde(flatten)]
    region_info: RegionInfo,
    version: u64,
}

/// `common/wal/src/options.rs` 的 `WalOptions` —— internally tagged。
///
/// `RaftEngine` 是 `#[default]`，所以"没配 WAL"的表走这一支，最小。
#[derive(Serialize)]
#[serde(tag = "wal.provider", rename_all = "snake_case")]
enum WalOptions {
    RaftEngine,
    Kafka(KafkaWalOptions),
    /// 真实枚举里有这一支，但本场景不构造它（没有哪个表的 WAL 是 noop 还写元数据的）。
    /// 留着是为了让这个镜像**完整** —— 字段/变体少一个就可能让字节数失真。
    #[allow(dead_code)]
    Noop,
    ObjectStore(ObjectStoreWalOptions),
}

/// 表的 WAL 形态 —— **它决定那个放大项的系数**（每种形态的 JSON 长度差好几倍）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WalKind {
    Raft,
    Kafka,
    ObjectStore,
}

impl WalKind {
    fn label(self) -> &'static str {
        match self {
            WalKind::Raft => "raft_engine",
            WalKind::Kafka => "kafka",
            WalKind::ObjectStore => "object_store",
        }
    }
}

/// 真实类型带 `with_prefix!("wal.kafka.")` ⇒ 字段名前面**真的**多个前缀。
#[derive(Serialize)]
struct KafkaWalOptions {
    #[serde(rename = "wal.kafka.topic")]
    topic: String,
    #[serde(
        rename = "wal.kafka.initial_pruned_entry_id",
        skip_serializing_if = "Option::is_none"
    )]
    initial_pruned_entry_id: Option<u64>,
}

#[derive(Serialize)]
struct ObjectStoreWalOptions {
    #[serde(rename = "wal.object_store.prefix")]
    prefix: String,
}

/// `key/topic_region.rs` 的 `TopicRegionValue`。
#[derive(Serialize)]
struct TopicRegionValue {
    #[serde(skip_serializing_if = "Option::is_none")]
    checkpoint: Option<()>,
}

// ============================================================================
// 合成参数
// ============================================================================

/// 一张表在某个规模下的全部产物。`C` 只影响 `TableInfoValue`，单独算。
#[derive(Debug, Clone, Copy)]
struct Scale {
    /// 表的 region 数。
    regions: u32,
    /// 参与这张表的 datanode 数（每个 datanode 一份 `DatanodeTableValue`）。
    datanodes: u32,
}

/// 表名长度按真实形状取：`{catalog}_{schema}_{table}_{region_number}`。
fn table_name() -> &'static str {
    "greptime_public_metrics_snapshot"
}

/// datanode 地址：真实部署里是 service DNS 名，不是 `127.0.0.1`。
fn peer_addr(i: u32) -> String {
    format!("greptimedb-datanode-{i}.greptimedb.svc.cluster.local:4001")
}

fn peer(i: u32) -> Peer {
    Peer {
        id: i as u64,
        addr: peer_addr(i),
    }
}

/// 一个 region 的 wal options。
///
/// Kafka 的 topic 名按真实形状取（`greptimedb_wal_topic_<随机后缀>`），
/// object store 的 prefix 同理 —— 它们的长短**直接决定放大项的系数**。
fn wal_options(kind: WalKind) -> WalOptions {
    match kind {
        WalKind::Raft => WalOptions::RaftEngine,
        WalKind::Kafka => WalOptions::Kafka(KafkaWalOptions {
            topic: "greptimedb_wal_topic_0123456789abcdef".to_string(),
            initial_pruned_entry_id: None,
        }),
        WalKind::ObjectStore => WalOptions::ObjectStore(ObjectStoreWalOptions {
            prefix: "greptimedb_wal/0123456789abcdef".to_string(),
        }),
    }
}

/// 构造一条 `RegionRoute`。`region_number` 是表内序号。
fn region_route(region_number: u32, datanodes: u32) -> RegionRoute {
    let leader = region_number % datanodes.max(1);
    RegionRoute {
        region: Region {
            id: 1024u64 << 32 | region_number as u64,
            name: format!("{}_{}", table_name(), region_number),
            attrs: BTreeMap::new(),
            partition_expr: format!("host=host{}", region_number % 8),
        },
        leader_peer: Some(peer(leader)),
        follower_peers: (0..datanodes.min(2))
            .filter(|i| *i != leader)
            .map(peer)
            .collect(),
        leader_state: None,
        leader_down_since: None,
        write_route_policy: None,
    }
}

/// 一个 datanode 的 `DatanodeTableValue`。
///
/// ⚠️ `region_wal_options` 收的是**整张表**的 region（不只是本 datanode 的）——
/// 这就是那个 `O(R·D)` 放大项。
fn datanode_table_value(scale: Scale, d: u32, kind: WalKind) -> DatanodeTableValue {
    let all_regions: Vec<u32> = (0..scale.regions).collect();
    let wal: HashMap<u32, String> = (0..scale.regions)
        .map(|r| {
            (
                r,
                serde_json::to_string(&wal_options(kind)).expect("可序列化"),
            )
        })
        .collect();
    DatanodeTableValue {
        table_id: 1024,
        // 本 datanode 上的 leader region（取 1/D）
        regions: all_regions
            .iter()
            .copied()
            .filter(|r| r % scale.datanodes == d)
            .collect(),
        follower_regions: vec![],
        region_info: RegionInfo {
            engine: "mito".to_string(),
            region_storage_path: format!("/greptime/{}/1024/{}", table_name(), d),
            region_options: HashMap::new(),
            region_wal_options: wal,
        },
        version: 3,
    }
}

/// 列名形状照抄 `table_info.rs:290` 那条真实样例：5 列 ≈ 1.4KB ⇒ 约 280B/列。
///
/// 本函数**不**逐字段镜像 `TableInfo`（那个结构太大且与放大项无关），
/// 而是直接按"每列多少字节"合成 —— 这是本场景唯一的近似，已写进诚实边界。
fn table_info_value_bytes(columns: u32) -> usize {
    // 列定义模板取真实样例里最长的那种（带 time index 和 metadata）。
    let per_column = r#"{"name":"greptime_timestamp","data_type":{"Timestamp":{"Millisecond":null}},"is_nullable":false,"is_time_index":true,"default_constraint":null,"metadata":{"greptime:time_index":"true"}},"#
        .len();
    let head = r#"{"version":1,"table_info":{"ident":{"table_id":1024,"version":0},"name":"#.len()
        + table_name().len()
        + r#"","desc":"Created on insertion","catalog_name":"greptime","schema_name":"public","meta":{"schema":{"column_schemas":["#
            .len();
    let tail = r#"],"timestamp_index":0,"version":0},"primary_key_indices":[],"value_indices":[],"engine":"mito","next_column_id":16,"region_numbers":[],"engine_options":{},"options":{"write_buffer_size":null,"ttl":null,"extra_options":{}},"created_on":"1970-01-01T00:00:00Z"},"table_type":"Base"}}"#
        .len();
    head + columns as usize * per_column + tail
}

// ============================================================================
// 量
// ============================================================================

fn bytes_of<T: Serialize>(v: &T) -> usize {
    serde_json::to_vec(v).expect("可序列化").len()
}

/// key 也占字节 —— `TxnOp::Put(key, value)` 两个都要写进去。
fn key_bytes(len: usize) -> usize {
    len
}

/// 一次 `create_table_metadata` 的**载荷字节数** = Σ(key + value)。
///
/// 真实调用点见 `key.rs` 的 `create_table_metadata`（合并 table_name + table_info +
/// table_route + topic_region + 每个 datanode 一份 datanode_table）。
///
/// 这里只算**载荷**：段格式还没定，任何正经格式都会把这些字节基本原样存一遍。
/// 字段名/标签带来的额外开销取决于最终选的编码，不在这里下结论。
fn create_table_txn_payload(scale: Scale, columns: u32, kind: WalKind) -> Breakdown {
    let route = TableRouteValue::Physical(PhysicalTableRouteValue {
        region_routes: (0..scale.regions)
            .map(|r| region_route(r, scale.datanodes))
            .collect(),
        max_region_number: scale.regions,
        version: 7,
    });

    let dn: usize = (0..scale.datanodes)
        .map(|d| {
            key_bytes("__dn_table/".len() + 1 + 1 + format!("{}", scale.datanodes).len())
                + bytes_of(&datanode_table_value(scale, d, kind))
        })
        .sum();

    Breakdown {
        table_info: key_bytes(64) + table_info_value_bytes(columns),
        route: key_bytes(64) + bytes_of(&route),
        datanode_tables: dn,
        // 每个 region 一个空的 topic_region Put
        topic_region: scale.regions as usize
            * (key_bytes(40) + bytes_of(&TopicRegionValue { checkpoint: None })),
        table_name: key_bytes(80) + 64,
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct Breakdown {
    table_info: usize,
    route: usize,
    datanode_tables: usize,
    topic_region: usize,
    table_name: usize,
}

impl Breakdown {
    fn total(&self) -> usize {
        self.table_info + self.route + self.datanode_tables + self.topic_region + self.table_name
    }
}

/// 参照用的四个段大小 —— 都是现成的数，用来回答"一个事务在哪个量级上会自己顶满它"。
/// 第一个是我们原来的候选（照抄 FINDINGS 的 L1：节点大小取 256KB），
/// 其余三个是"段不必等于节点"时的替代值。
///
/// 它们**不是上限**（见本文件开头：段大小是攒批的目标值），所以这里不叫"上界"。
const SEGMENT_CANDIDATES: [(usize, &str); 4] = [
    (256 * 1024, "256KB（= 候选节点大小）"),
    (1024 * 1024, "1MB"),
    (5 * 1024 * 1024, "5MB（S3 multipart 阈值）"),
    (32 * 1024 * 1024, "32MB（etcd 解码上限）"),
];

/// 在固定 `D` 下，第一个让 txn 超过 `limit` 的 region 数。
///
/// **不能线性扫描**：每试一个 R 都要构造 R 条 route ⇒ 扫到二十万就是 O(R²)。
/// 而载荷对 R 是**严格线性**的（各分量都是 `常数 + R × 每 region`，见 `Breakdown`），
/// 所以取两点求斜率、外推、再**在推出来的那个点复测一次**即可 —— O(1)。
///
/// 复测是必需的：`max_region_number` 的十进制位数会随 R 变，外推值可能差一两个 region。
fn crossover_regions(datanodes: u32, columns: u32, kind: WalKind, limit: usize) -> Option<u32> {
    let at = |r: u32| {
        create_table_txn_payload(
            Scale {
                regions: r,
                datanodes,
            },
            columns,
            kind,
        )
        .total()
    };

    let (r0, r1) = (0u32, 512u32);
    let (b0, b1) = (at(r0), at(r1));
    let slope = (b1 as f64 - b0 as f64) / (r1 - r0) as f64;
    if slope <= 0.0 {
        return None; // 载荷不随 R 增长 ⇒ 永远超不过
    }
    if b0 >= limit {
        return Some(1);
    }
    let est = (r0 as f64 + (limit as f64 - b0 as f64) / slope)
        .ceil()
        .max(1.0) as u64;
    // 上限保护：复测要真的构造 est 条 route，估计值离谱时会挂住
    // （变异③ 用平方项把 est 推到天文数字，实测就是这么卡死的）。
    if est > MAX_PROBE_REGIONS {
        return None;
    }
    // 只在估计点附近小范围复测 —— 大范围扫描又会退化成 O(R²)
    let lo = est.saturating_sub(4).max(1);
    (lo..=est.saturating_add(4))
        .map(|r| r as u32)
        .find(|r| at(*r) > limit)
}

/// `crossover_regions` 复测的 region 数上限 —— 超过就不实测了（见那里的注释）。
const MAX_PROBE_REGIONS: u64 = 2_000_000;

fn kb(n: usize) -> String {
    format!("{:.1}KB", n as f64 / 1024.0)
}

// ============================================================================

pub async fn run(_store: &CasStore) -> Result<(), StorageError> {
    // ------------------------------------------------------------ 基线形状
    let one = Scale {
        regions: 1,
        datanodes: 1,
    };
    let default_txn = create_table_txn_payload(one, 10, WalKind::Raft);
    let route_1 = bytes_of(&TableRouteValue::Physical(PhysicalTableRouteValue {
        region_routes: vec![region_route(0, 1)],
        max_region_number: 1,
        version: 7,
    }));
    let dn_1_raft = bytes_of(&datanode_table_value(one, 0, WalKind::Raft));
    let dn_1_kafka = bytes_of(&datanode_table_value(one, 0, WalKind::Kafka));
    let route_100 = bytes_of(&TableRouteValue::Physical(PhysicalTableRouteValue {
        region_routes: (0..100).map(|r| region_route(r, 3)).collect(),
        max_region_number: 100,
        version: 7,
    }));

    // ------------------------------------------------------------ 线性检验
    // 固定 D=3，看 R 翻倍时增量是否恒定（"线性"而不是"平方"）。
    let size = |r: u32| {
        create_table_txn_payload(
            Scale {
                regions: r,
                datanodes: 3,
            },
            10,
            WalKind::Raft,
        )
        .total()
    };
    // ⚠️ 比的是**每 region 的增量**，不是原始增量 —— 三个区间是 100/200/400 宽，
    // 直接比原始增量的话，线性增长会让它们**逐次翻倍**，看起来像平方。
    // （第一版就是这么写的，跑出 FAIL；分项全对 ⇒ 先怀疑断言。）
    let slope = |a: u32, b: u32| (size(b) - size(a)) as f64 / (b - a) as f64;
    let s1 = slope(100, 200);
    let s2 = slope(200, 400);
    let s3 = slope(400, 800);
    let d1 = size(200) - size(100);
    let d2 = size(400) - size(200);
    let d3 = size(800) - size(400);
    // ⚠️ 必须拿**第一个斜率当基准**逐个比 —— 不能拿这三个数自己的 min/max 当区间：
    // 那样区间由这三个数生成，它们必然全落进去，检查**恒真**。
    // （变异③ 实测：把 wal options 改成平方项后斜率逐次翻倍，第一版照样报 true。
    //   恒真的断言比没有断言更坏，因为它看起来在被检查。）
    let linear = {
        let within_1pct = |s: f64| (s / s1 - 1.0).abs() < 0.01;
        within_1pct(s2) && within_1pct(s3)
    };

    // ------------------------------------------------------------ WAL 形态对比
    // 放大项的系数由 WAL 形态决定 —— 这一格是"别把 Kafka 的 topic 名当免费"的依据。
    let wal_forms: Vec<(WalKind, usize)> = [WalKind::Raft, WalKind::ObjectStore, WalKind::Kafka]
        .iter()
        .map(|k| {
            (
                *k,
                datanode_table_value(
                    Scale {
                        regions: 1000,
                        datanodes: 1,
                    },
                    0,
                    *k,
                )
                .region_info
                .region_wal_options
                .values()
                .map(|s| s.len())
                .sum::<usize>(),
            )
        })
        .collect();

    // ------------------------------------------------------------ crossover
    // 取放大项最大的那种形态，**按最坏情况报**。
    //
    // 这里刻意**不写死**是哪种：上一版写死了 Kafka，而上面 `wal_forms` 打出来的
    // object_store 更大（91B/region 对 82B），于是报出去的并不是最坏情况。
    let worst_kind = wal_forms
        .iter()
        .max_by_key(|(_, bytes)| *bytes)
        .map(|(k, _)| *k)
        .expect("wal_forms 非空");
    let crossings: Vec<(usize, &str, Option<u32>)> = SEGMENT_CANDIDATES
        .iter()
        .map(|(limit, label)| {
            (
                *limit,
                *label,
                crossover_regions(10, 10, worst_kind, *limit),
            )
        })
        .collect();

    // ------------------------------------------------------------ 判定
    // ① 默认规模（不分区、单 datanode、10 列）小到可以忽略
    let default_small = default_txn.total() < 8 * 1024;
    // ② 对 R 线性
    // ③ 256KB 装得下「100 region × 10 datanode」的表 —— 判定同样按最坏形态取
    let fits_256k = create_table_txn_payload(
        Scale {
            regions: 100,
            datanodes: 10,
        },
        10,
        worst_kind,
    )
    .total()
        < 256 * 1024;

    let pass = default_small && linear && fits_256k;

    let verdict = if pass { "PASS" } else { "FAIL" };
    println!(
        "[{verdict}] s13 一次 txn 多大: \
         默认(1区/1节点/10列)={} | 单条route={}B 100条route={} | 单datanode值(1区) raft={}B kafka={}B \
         | 对R线性={linear} 每region增量 {:.0}/{:.0}/{:.0}B (Δ100→200={}B Δ200→400={}B Δ400→800={}B) \
         | 默认小到可忽略={default_small} 100区×10节点装进256KB={fits_256k}",
        kb(default_txn.total()),
        route_1,
        kb(route_100),
        dn_1_raft,
        dn_1_kafka,
        s1,
        s2,
        s3,
        d1,
        d2,
        d3,
    );

    // ------------------------------------------------------------ 明细表
    println!("        R×D 扫描（10 列、RaftEngine WAL、载荷字节）：");
    println!(
        "        {:>7} {:>7} {:>10} {:>12} {:>12} {:>10} {:>10}",
        "R", "D", "table_info", "route", "dn_tables", "topic", "合计"
    );
    for (r, d) in [
        (1u32, 1u32),
        (10, 3),
        (100, 3),
        (100, 10),
        (1000, 10),
        (1000, 100),
    ] {
        let b = create_table_txn_payload(
            Scale {
                regions: r,
                datanodes: d,
            },
            10,
            WalKind::Raft,
        );
        println!(
            "        {r:>7} {d:>7} {:>10} {:>12} {:>12} {:>10} {:>10}",
            kb(b.table_info),
            kb(b.route),
            kb(b.datanode_tables),
            kb(b.topic_region),
            kb(b.total())
        );
    }

    print!("        WAL 形态对放大项的影响（1000 个 region 的 wal options 原始字节）：");
    for (k, n) in &wal_forms {
        print!(" {}={}B", k.label(), n);
    }
    println!();

    println!(
        "        crossover（D=10、10 列、{} WAL = 最坏形态）：多大的建表事务会自己顶满这一段",
        worst_kind.label()
    );
    for (_, label, r) in &crossings {
        match r {
            Some(r) => println!("          {label:<24} ⇒ 第 {r} 个 region 起，事务自己就顶满"),
            None => println!("          {label:<24} ⇒ 远超 200 万 region（外推值过大，不再实测）"),
        }
    }

    // 「分区表 / repartition 之后能到的量级」那一格也按最坏形态量一次 ——
    // R×D 扫描那张表是 RaftEngine 形态的，直接拿它的 4.6MB 会低估。
    let big_1000x100 = create_table_txn_payload(
        Scale {
            regions: 1000,
            datanodes: 100,
        },
        10,
        worst_kind,
    )
    .total();
    println!(
        "        1000 region × 100 datanode（{} WAL）：{}",
        worst_kind.label(),
        kb(big_1000x100)
    );

    // 结论文案里引用的倍数直接从上面那张表取，不再硬编码 ——
    // 硬编码正是上一版换个 WAL 形态就整体失效的原因。
    let cx = |limit: usize| -> u32 {
        crossings
            .iter()
            .find(|(l, _, _)| *l == limit)
            .and_then(|(_, _, r)| *r)
            .unwrap_or_else(|| panic!("crossover 表里没有 {limit} 字节那一档"))
    };
    let wal_bytes = wal_forms
        .iter()
        .map(|(k, n)| format!("{} {}B", k.label(), n / 1000))
        .collect::<Vec<_>>()
        .join("、");

    if pass {
        println!(
            "        ⇒ 结论一：默认规模下这个担心不成立。不分区、单 datanode 的一次建表 txn 约 {}，\n\
             \x20         载荷对 region 数是**严格线性**的（每 region 递增 {:.0}B，不是平方），\n\
             \x20         所以「一个 txn 撑爆一个段」在小表上不是问题。",
            kb(default_txn.total()),
            s1
        );
        println!(
            "        ⇒ 结论二：单个事务**可以很大**，而且大多少算得出来。\n\
             \x20         256KB 在「{} 个 region × 10 个 datanode」处就被一个建表事务自己顶满 ——\n\
             \x20         那是分区表 / repartition 之后真能到的量级（1000 region × 100 datanode 是 {}）。\n\
             \x20         ⇒ 但这**不是限制**：段大小是「攒批凑到这个量就提交」的目标值，不是上限。\n\
             \x20            超过它的单个事务自己占一个段、单独提交，原子性仍然来自 Manifest 那一次 CAS，\n\
             \x20            段多大不参与任何正确性判断 ⇒ 不需要跨段方案，也不需要写入路径上的检查。\n\
             \x20         ⇒ 这个数真正说明的是：攒批对建表事务几乎不起作用 —— 上百 region 的建表，\n\
             \x20            一个事务自己就顶满了（对照：默认规模 1 region 才 {}）。",
            cx(256 * 1024),
            kb(big_1000x100),
            kb(default_txn.total()),
        );
        println!(
            "        ⇒ 放大项的归属：不是 route，是 `region_wal_options` 被**每个 datanode 复制一份**\n\
             \x20         （`O(R·D)`，`datanode_table.rs` 里的 FIXME 自己承认了这个冗余）。\n\
             \x20         每 region 的原始字节数由表的 WAL 形态决定：{wal_bytes} ⇒ 换 WAL 形态就是给\n\
             \x20         这个大小乘一个系数；上表按其中最大的 {worst} 报。",
            worst = worst_kind.label(),
        );
    }

    Ok(())
}
