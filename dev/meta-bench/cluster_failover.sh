#!/usr/bin/env bash
# Failover 臂的集群：metasrv(kafka WAL + region failover) + N datanode(kafka WAL + MinIO 共享存储)。
#
# 与 cluster.sh 的区别（都在这里，因为 Failover 迫使它们分开）：
#   - metasrv 的 `[wal] provider = "kafka"` —— enable_region_failover 的硬前提
#     （src/meta-srv/src/metasrv/builder.rs:284,341，is_remote_wal 只看 metasrv 自己的 wal 配置）
#   - datanode 用 MinIO 作共享存储；它们的本地 WAL 不够，因为 region 要在别的节点上重开
#     **所有 datanode 必须共用同一个 `[storage] root`** —— 各用一个前缀（dn1/dn2/dn3）会让
#     迁过去的 region 在目标节点上 `RegionNotFound: Empty region directory`（实测：
#     每个 failover procedure 都报 NonRetryable 又无限重试，集群永远恢复不了，
#     而症状在 metasrv 侧只表现为 "Region ... is not opened by datanode Peer{..}"）
#   - region_failure_detector_initialization_delay 默认 10m，必须覆盖，否则检测器根本不启动
#
# 用法：./cluster_failover.sh up [N] [run_dir] / down [run_dir]

set -euo pipefail
cd "$(dirname "$0")"

BIN=${BIN:-/home/lcz/dev_version/greptimedb/target/debug/greptime}
FIXTURES=/home/lcz/dev_version/greptimedb/tests-integration/fixtures

META_GRPC=3002
META_HTTP=3010
FRONT_HTTP=4000
FRONT_GRPC=4001
FRONT_MYSQL=4002
FRONT_PG=4003
dn_grpc() { echo $((3100 + $1)); }
dn_http() { echo $((3200 + $1)); }

# MinIO（fixture 容器，见 docker-compose.yml:73-84）
MINIO_ENDPOINT="http://127.0.0.1:9000"
MINIO_BUCKET="greptime"
MINIO_AK="superpower_ci_user"
MINIO_SK="superpower_password"
MINIO_REGION="us-west-2"

KAFKA_ENDPOINTS='["127.0.0.1:9092"]'
# 检测器延迟：默认 10m 一次都跑不完，压到 5s（记录里要写明这是**被覆盖过**的一项）
DETECTOR_DELAY="5s"
NUM_TOPICS=8

up() {
  local n=${1:-3}
  local run_dir=${2:?run_dir required}
  mkdir -p "$run_dir"/{conf,logs,raw}

  (cd "$FIXTURES" && docker compose up -d etcd zookeeper kafka minio >/dev/null)
  wait_tcp 127.0.0.1 2379 etcd
  wait_tcp 127.0.0.1 9092 kafka

  # 清 keyspace。与 matrix.sh 里那条同源，但在 Failover 臂上后果更重，所以放在起服务之前：
  # 上一轮被杀掉的 datanode 不会注销，etcd 里留着 __node_address/0/<id> 的过期注册，
  # 新的 metasrv 一启动就把它们当**存活但失联**的节点，在 datanode 还没起来时就开跑 failover
  # （实测：3 节点集群上 supervisor 对着并不存在的 datanode 4 报 region failure，
  # 而本地 datanode 因为 meta 里那 862 个残留 region 指向空目录而启动失败）。
  local before after
  before=$(etcd_keys)
  docker exec fixtures-etcd-1 etcdctl --endpoints=127.0.0.1:2379 \
    del "" --prefix >/dev/null 2>&1 || true
  after=$(etcd_keys)
  echo "keyspace wiped: ${before} -> ${after} keys"

  # 每个 run 的存储根目录都要是干净的：S3 上的残留 region 目录会被当成"region 已存在"。
  docker exec fixtures-minio-1 sh -c \
    "command -v mc >/dev/null && mc rm --recursive --force local/${MINIO_BUCKET}/mb/${run_dir##*/} >/dev/null 2>&1" || true

  # --- metasrv ---
  cat >"${run_dir}/conf/metasrv.toml" <<EOF
data_home = "${run_dir}/metasrv"
store_addrs = ["127.0.0.1:2379"]
backend = "etcd_store"
enable_region_failover = true
region_failure_detector_initialization_delay = "${DETECTOR_DELAY}"
[wal]
provider = "kafka"
broker_endpoints = ${KAFKA_ENDPOINTS}
topic_name_prefix = "mb_meta"
num_topics = ${NUM_TOPICS}
[grpc]
bind_addr = "127.0.0.1:${META_GRPC}"
server_addr = "127.0.0.1:${META_GRPC}"
[http]
addr = "127.0.0.1:${META_HTTP}"
EOF

  "$BIN" metasrv start -c "${run_dir}/conf/metasrv.toml" \
    --log-dir "${run_dir}/logs" >"${run_dir}/raw/metasrv.stdout" 2>&1 &
  echo $! >"${run_dir}/metasrv.pid"
  wait_http "http://127.0.0.1:${META_HTTP}/health" metasrv

  # --- datanodes ---
  for i in $(seq 1 "$n"); do
    local g h
    g=$(dn_grpc "$i"); h=$(dn_http "$i")
    cat >"${run_dir}/conf/datanode-${i}.toml" <<EOF
node_id = ${i}
[wal]
provider = "kafka"
broker_endpoints = ${KAFKA_ENDPOINTS}
topic_name_prefix = "mb_data"
num_topics = ${NUM_TOPICS}
[storage]
type = "S3"
bucket = "${MINIO_BUCKET}"
root = "mb/${run_dir##*/}/shared"
access_key_id = "${MINIO_AK}"
secret_access_key = "${MINIO_SK}"
endpoint = "${MINIO_ENDPOINT}"
region = "${MINIO_REGION}"
[grpc]
bind_addr = "127.0.0.1:${g}"
server_addr = "127.0.0.1:${g}"
[http]
addr = "127.0.0.1:${h}"
EOF

    "$BIN" datanode start -c "${run_dir}/conf/datanode-${i}.toml" \
      --data-home "${run_dir}/datanode-${i}" \
      --metasrv-addrs "127.0.0.1:${META_GRPC}" \
      --log-dir "${run_dir}/logs" >"${run_dir}/raw/datanode-${i}.stdout" 2>&1 &
    echo $! >"${run_dir}/datanode-${i}.pid"
    wait_http "http://127.0.0.1:${h}/health" "datanode-${i}"
  done

  # --- frontend ---
  "$BIN" frontend start \
    --metasrv-addrs "127.0.0.1:${META_GRPC}" \
    --http-addr "127.0.0.1:${FRONT_HTTP}" \
    --grpc-bind-addr "127.0.0.1:${FRONT_GRPC}" \
    --grpc-server-addr "127.0.0.1:${FRONT_GRPC}" \
    --mysql-addr "127.0.0.1:${FRONT_MYSQL}" \
    --postgres-addr "127.0.0.1:${FRONT_PG}" \
    --log-dir "${run_dir}/logs" >"${run_dir}/raw/frontend.stdout" 2>&1 &
  echo $! >"${run_dir}/frontend.pid"
  wait_http "http://127.0.0.1:${FRONT_HTTP}/health" frontend

  ./cluster.sh env "$run_dir" 2>/dev/null || true
  echo "failover cluster up: ${n} datanode(s), detector delay=${DETECTOR_DELAY} (run_dir=${run_dir})"
}

down() {
  local run_dir=${1:?run_dir required}
  for f in "$run_dir"/frontend.pid "$run_dir"/datanode-*.pid "$run_dir"/metasrv.pid; do
    [ -f "$f" ] || continue
    local pid; pid=$(cat "$f")
    kill -0 "$pid" 2>/dev/null && kill "$pid" 2>/dev/null || true
    rm -f "$f"
  done
  for _ in $(seq 1 40); do
    [ "$(pgrep -x greptime 2>/dev/null | wc -l)" = "0" ] && return 0
    sleep 0.5
  done
  echo "warning: some greptime processes still alive" >&2
}

etcd_keys() {
  docker exec fixtures-etcd-1 etcdctl --endpoints=127.0.0.1:2379 \
    get "" --prefix --keys-only 2>/dev/null | grep -c . || true
}

wait_tcp() {
  local host=$1 port=$2 what=$3
  for _ in $(seq 1 90); do
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then exec 3>&-; return 0; fi
    sleep 1
  done
  echo "timeout waiting for ${what}" >&2; return 1
}

wait_http() {
  local url=$1 what=$2
  for _ in $(seq 1 120); do
    curl -fsS --max-time 2 "$url" >/dev/null 2>&1 && return 0
    sleep 1
  done
  echo "timeout waiting for ${what} (${url})" >&2; return 1
}

case "${1:-}" in
  up)   up "${2:-3}" "${3:-}" ;;
  down) down "${2:?run_dir required}" ;;
  *)    sed -n '2,15p' "$0"; exit 1 ;;
esac
