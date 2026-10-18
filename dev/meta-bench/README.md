# meta-bench

产出要求 7（与 etcd 后端的性能与成本对比报告）的最小测量装置。

口径、环境冻结字段、已定死的参数、以及目前跑出来的结果见
`p_b/application/申请书要求核对.md` §六 / §6.1 / §6.2 / §6.3 / §6.4 / §6.5。
本目录只放**装置**。

## 两个臂是两套装置

| 臂 | 起集群 | 理由 |
| --- | --- | --- |
| **DDL 臂** | `cluster.sh` | metasrv 用 memory-store 之外的默认后端，datanode 本地存储 |
| **Failover 臂** | `cluster_failover.sh` | metasrv 必须 `[wal] provider = "kafka"`（failover 的硬前提），datanode 必须共享 S3 |

不能合成一套：failover 的两条前置条件会把 DDL 臂的配置也改掉，两臂的数字就不再同口径。

## 跑法

DDL 臂：

```bash
./cluster.sh up 2 runs/$(date +%s)
python3 ddl_probe.py --out runs/<ts>/raw/ddl-serial.tsv --n 200
python3 resmon.py   --out runs/<ts>/raw/res.tsv &     # 与上面同时跑
./cluster.sh down runs/<ts>
```

Failover 臂：

```bash
# 一把跑 N 轮，每轮全新集群、轮换被杀的 datanode
./failover_runs.sh 3 runs/fo-$(date +%s)

# 或者手工一轮
./cluster_failover.sh up 3 runs/fo-1
python3 failover_probe.py --out runs/fo-1/raw/fo.tsv \
  --n 60 --kill-pid-file runs/fo-1/datanode-3.pid
./cluster_failover.sh down runs/fo-1
```

`failover_runs.sh` 认这几个环境变量：`N_TABLES`、`SWEEP_PAUSE`、`MAX_SECS`、`DRAIN_SECS`。

## 装置的三条约束

1. **两臂同网** —— etcd 与 minio 一律用 `tests-integration/fixtures` 的容器（同一张 bridge、
   都只把端口映射到宿主机），**不要**用 host network 的 `toy-etcd`：它会让两臂的传输路径不对称，
   而这次对比的全部意义就在每跳延迟。
2. **输出逐条样本，不是聚合值** —— warmup 丢几条、报 p50 还是 p99 是分析期参数，
   只要每条样本各占一行，事后都能重算。
3. **每个 run 一个目录**，内含 `env.txt`（宿主 + commit + 容器 digest）、`conf/`、`raw/`、`logs/`。
   `env.txt` 每次运行都抓，不只在开工时抓。

## 文件

| 文件 | 作用 |
| --- | --- |
| `cluster.sh` | DDL 臂：起 / 停 metasrv + N datanode + frontend；抓环境快照 |
| `cluster_failover.sh` | Failover 臂：kafka WAL + 共享 MinIO + region failover；**起服务前清 keyspace** |
| `ddl_probe.py` | 串行或并发发 `CREATE TABLE`，逐条记往返时间 |
| `failover_probe.py` | 造表 → 预热 → `SIGKILL` 一个 datanode → 扫表到全绿；逐条记样本 |
| `failover_runs.sh` | Failover 臂的重复轮（轮换受害节点） |
| `resmon.py` | 采 `/proc` —— 判「4 vCPU 上能放几个 datanode」靠的是**换页**不是内存余量 |

## 已知的坑

- `cluster.sh down` / `cluster_failover.sh down` **按 pidfile 杀**，不用 `pkill -f`（会杀掉自己的 shell）。
- metasrv 的 `region_failure_detector_initialization_delay` 默认 **10m** —— 跑到 Failover 场景时
  必须覆盖它，否则失败检测器在 10 分钟内根本不会建立。注意它只决定**检测器何时被建起来**，
  「多久算死」是 `[failure_detector]` 的 `threshold` / `acceptable_heartbeat_pause`（默认 8 / 10s，
  取自 Akka），那两个不要动，动了就是在测自己的配置。
- `enable_region_failover = true` **要求 metasrv 的 `[wal] provider = "kafka"`**
  （`src/meta-srv/src/metasrv/builder.rs:284,341`，`is_remote_wal` 只看 metasrv 自己的 wal 配置）。
  datanode 侧才有 `experimental_object_store`，且它**不能**转成 metasrv wal
  （`src/common/wal/src/config.rs:90-93`）。
- **共享存储模式下所有 datanode 必须共用同一个 `[storage] root`。** 各用一个前缀会让迁过去的
  region 在目标节点上 `RegionNotFound: Empty region directory`，procedure 报 `NonRetryable`
  却无限重试，集群永远恢复不了；metasrv 侧只看得到 `Region ... is not opened by datanode Peer{..}`。
- **起服务前必须清 keyspace。** 上一轮被杀的 datanode 不会注销，`__node_address/0/<id>` 留着，
  新的 metasrv 会把它们当存活但失联的节点，开始给并不存在的节点跑 failover。
- Failover 期间客户端请求是**挂住**而不是快速报错，所以客户端的超时预算会直接决定
  测出来的「失败」时长。`failover_probe.py` 的 `--client-timeout` 默认 500ms（成功请求 max 165ms）。
- 全停写的 `--drain-secs` 用来区分「卡住是探针负载自持的」还是「真的卡住」—— 目前 180 张表
  那组是**真的卡住**（静默 30s 仍失败）。
