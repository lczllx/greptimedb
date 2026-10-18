# objstore-toy

对象存储元数据后端核心假设的验证用例集。

## 背景

GreptimeDB 的 Metasrv 元数据目前依赖 etcd 或关系数据库，即使数据层已实现存算分离，
高可用部署仍需额外维护一套有状态元数据组件。主流对象存储已普遍提供单对象条件写
（S3 的 `If-None-Match` / `If-Match`、GCS 的 generation、Azure Blob 的 ETag），
但语义与 etcd 差异明显：只有单对象级原子性，没有跨对象事务、服务端 TTL 与 Watch，
也不适合把每个 KV 简单映射成一个对象。

本目录是在这条路径上跑的一批验证用例，用来确认若干**纸面推演说服力不足、且结论会改变设计**
的假设。自成 workspace，不并入主 workspace，因此不与 GreptimeDB 的构建互相影响。

## 它回答什么

判据两条：**纸面推演说服力不足**、**结论会改变设计**，两条都不满足的一律不进。

产出是**知识**（哪条假设是假的、假了该怎么改），逐条汇总在 `FINDINGS.md`，
格式为「假设 → 实测 → 结论 → 对设计的影响」。

## 跑法

MinIO 走仓库既有 fixture：

```bash
cd tests-integration/fixtures && docker compose up -d minio
```

> 别另起一台 MinIO —— 9000 已经被 fixture 占着，会报 `port is already allocated`；
> 而且新开的卷和 fixture 的 `fixtures_minio_data` 不是同一份数据。

S15 还要一台本机 etcd（**对拍用**，见下）：

```bash
docker run -d --name toy-etcd --network host \
  --entrypoint /opt/bitnami/etcd/bin/etcd greptime/etcd:3.6.1-debian-12-r3 \
  --name=toy-etcd --data-dir=/tmp/etcd-data \
  --listen-client-urls=http://127.0.0.1:2379 --advertise-client-urls=http://127.0.0.1:2379 \
  --listen-peer-urls=http://127.0.0.1:2380 --initial-advertise-peer-urls=http://127.0.0.1:2380 \
  --initial-cluster=toy-etcd=http://127.0.0.1:2380 --initial-cluster-state=new \
  --initial-cluster-token=toy
```

> 绕开镜像自带的 bitnami entrypoint 直接跑二进制：它会做一堆权限检查，
> 而这个容器只用 `127.0.0.1:2379` 的明文端口。数据落在容器内 `/tmp/etcd-data`，
> 是一次性的 —— 场景每次开头都会自己清干净它用的那一段 key 空间。

然后：

```bash
cd dev/objstore-toy

cargo run -- ping              # 连通性自检：能读、能 CAS、冲突能分类
cargo run -- all               # 全部场景（ping + s0…s17；含 s15，所以要 etcd 在跑）
cargo run -- s8                # 单个场景
cargo run -- s15               # 与真 etcd 对拍（V3）—— 需要上面那台 etcd
cargo run -- s17               # 背压收敛（V7）—— 纯内存，不碰 store
cargo run --bin etag_probe        # ETag 语义探针（场景 S1 的基础）
cargo run --bin layout_probe      # 节点大小（layout 参数）实测 —— 结论见 FINDINGS 的 L1
cargo run --bin capability_probe  # 能力探测：实发条件写/读/删，看后端真实行为 —— 见 FINDINGS 的 V5
cargo run --bin group_commit_probe # 攒批：每 txn 实际发了几次请求，以及摊销倍数由什么决定 —— 见 FINDINGS 的 V6
```

凭据默认对齐 fixture：`superpower_ci_user` / `superpower_password` / bucket `greptime` /
`127.0.0.1:9000`，可用 `MINIO_*` 环境变量覆盖。

> 在仓库根目录下运行时 `--manifest-path dev/objstore-toy/Cargo.toml` 不可省略：
> `cargo` 按当前目录选择 workspace，省略时会开始构建整个 GreptimeDB。

## 约定

- 每个场景一个子命令，**自建自清** key 前缀（都以 `toy/` 开头），可重复跑
  （`s17` 例外：控制器是进程内的，一个字节都不写）。
- 每个场景打印**一行判定**：`[PASS]` / `[FAIL]` + 关键数值。
- 结论逐条汇总进 `FINDINGS.md`：**假设 → 实测 → 结论 → 对设计的影响**。

## 目录

| 文件 | 作用 |
| --- | --- |
| `src/cfg.rs` | MinIO / S3 连接；挂载瞬态重试层（理由见 FINDINGS 的 S8） |
| `src/kv.rs` | 条件写 CAS 封装 + 错误分类 |
| `src/tree.rs` | 最小 B-link 树模型（单层，够 S2 / S3 / S4 用） |
| `src/commit.rs` | 提交点、日志段与恢复的模型 |
| `src/group_commit.rs` | 攒批提交器（单写者持 etag，一个组 = 一个 lsn）—— V6 的被测对象 |
| `src/backpressure.rs` | 背压控制器（上限 / 同步等 / 超时 / 迟滞）—— V7 的被测对象，纯内存不碰 store |
| `src/leader_kv.rs` | 「日志 + 树 + 视图」三层堆起来的 leader KV —— S15 的被测对象（V3） |
| `src/main.rs` | 子命令分发 |
| `src/scenarios/` | 各场景（s0…s17） |
| `src/bin/etag_probe.rs` | ETag 语义探针（S1 的基础） |
| `src/bin/layout_probe.rs` | 节点大小（layout 参数）探针 —— **不是场景**：没有 PASS/FAIL，产出的是曲线加系数（L1） |
| `src/bin/capability_probe.rs` | 能力探测 —— **不是场景**：不信 `Capability` 位，实发"条件成立/不成立"一对请求，产出"可以用 / 必须绕开"（V5） |
| `src/bin/group_commit_probe.rs` | 攒批请求分摊探针 —— **不是场景**：16 条臂对比每 txn 请求数与延迟，产出"摊销倍数由什么决定"（V6） |
| `FINDINGS.md` | 全部结论汇总 |

## 环境

- opendal **0.59.2**，且 crate 家族全部为 0.59.2 —— 理由与验收命令见 `FINDINGS.md` 开头
- MinIO `greptime/minio:2024`，由 `tests-integration/fixtures/docker-compose.yml` 启动
- etcd `greptime/etcd:3.6.1-debian-12-r3`（仅 S15 对拍用，起法见上）；`etcd-client = 0.17`
  与仓库 `src/common/meta` 保持一致 ⇒ toy 里对拍的 etcd 语义就是真实现面对的那套
