#!/usr/bin/env bash
# 最小测量装置：起 metasrv + N datanode + frontend，供 DDL / Failover 计时用。
#
# 用法：
#   ./cluster.sh up   [N] [run_dir]   # N = datanode 个数，默认 2
#   ./cluster.sh down [run_dir]
#   ./cluster.sh env  [run_dir]       # 抓一次环境快照（§6.2 三类记录里的第一、三类）
#
# 设计约束（见 p_b/application/申请书要求核对.md §6.3）：
#   - 两臂都必须走 fixture 的 bridge 网络 ⇒ etcd 与 minio 一律用 fixture 容器，
#     不要用 host network 的 toy-etcd。
#   - 每次运行独立的 run_dir；原始输出（日志 + 逐条样本 + 环境快照）全落在里面。
#   - 不用 pkill -f（会杀掉自己的 shell），一律按 pidfile 杀。

set -euo pipefail

BIN=${BIN:-/home/lcz/dev_version/greptimedb/target/debug/greptime}
FIXTURES=/home/lcz/dev_version/greptimedb/tests-integration/fixtures
REPO=/home/lcz/dev_version/greptimedb

# 端口：各进程互不重叠，避开 standalone 默认的 4000-4003。
META_GRPC=3002
META_HTTP=3010
FRONT_HTTP=4000
FRONT_GRPC=4001
FRONT_MYSQL=4002
FRONT_PG=4003
dn_grpc() { echo $((3100 + $1)); }
dn_http() { echo $((3200 + $1)); }

up() {
  local n=${1:-2}
  local run_dir=${2:?run_dir required}
  mkdir -p "$run_dir"/{conf,logs,raw}

  # --- 环境快照（每次运行都抓，不只在开工时） ---
  env_snapshot "$run_dir"

  # --- 依赖容器：fixture 的 etcd（两臂对称的那一个）---
  (cd "$FIXTURES" && docker compose up -d etcd >/dev/null)
  wait_tcp 127.0.0.1 2379 metasrv-etcd

  # --- metasrv ---
  "$BIN" metasrv start \
    --grpc-bind-addr "127.0.0.1:${META_GRPC}" \
    --grpc-server-addr "127.0.0.1:${META_GRPC}" \
    --http-addr "127.0.0.1:${META_HTTP}" \
    --backend etcd-store \
    --store-addrs "127.0.0.1:2379" \
    --data-home "${run_dir}/metasrv" \
    --log-dir "${run_dir}/logs" \
    >"${run_dir}/raw/metasrv.stdout" 2>&1 &
  echo $! >"${run_dir}/metasrv.pid"
  wait_http "http://127.0.0.1:${META_HTTP}/health" metasrv

  # --- datanodes ---
  for i in $(seq 1 "$n"); do
    local g h
    g=$(dn_grpc "$i"); h=$(dn_http "$i")
    cat >"${run_dir}/conf/datanode-${i}.toml" <<EOF
[wal]
provider = "raft_engine"
dir = "${run_dir}/datanode-${i}/wal"
[storage]
data_home = "${run_dir}/datanode-${i}/data"
EOF
    "$BIN" datanode start \
      --config-file "${run_dir}/conf/datanode-${i}.toml" \
      --node-id "$i" \
      --grpc-bind-addr "127.0.0.1:${g}" \
      --grpc-server-addr "127.0.0.1:${g}" \
      --http-addr "127.0.0.1:${h}" \
      --metasrv-addrs "127.0.0.1:${META_GRPC}" \
      --data-home "${run_dir}/datanode-${i}" \
      --log-dir "${run_dir}/logs" \
      >"${run_dir}/raw/datanode-${i}.stdout" 2>&1 &
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
    --log-dir "${run_dir}/logs" \
    >"${run_dir}/raw/frontend.stdout" 2>&1 &
  echo $! >"${run_dir}/frontend.pid"
  wait_http "http://127.0.0.1:${FRONT_HTTP}/health" frontend

  echo "cluster up: ${n} datanode(s), frontend http://127.0.0.1:${FRONT_HTTP} (run_dir=${run_dir})"
}

down() {
  local run_dir=${1:?run_dir required}
  # 先 frontend、再 datanode、最后 metasrv —— 反过来会在日志里留下大量重连噪声。
  for f in "$run_dir"/frontend.pid "$run_dir"/datanode-*.pid "$run_dir"/metasrv.pid; do
    [ -f "$f" ] || continue
    local pid; pid=$(cat "$f")
    if kill -0 "$pid" 2>/dev/null; then kill "$pid" 2>/dev/null || true; fi
    rm -f "$f"
  done
  # 等它们真的退出（最多 20s），再确认端口放开
  for _ in $(seq 1 40); do
    still=$(pgrep -x greptime 2>/dev/null | wc -l || true)
    [ "$still" = "0" ] && return 0
    sleep 0.5
  done
  echo "warning: some greptime processes are still alive" >&2
}

env_snapshot() {
  local run_dir=$1
  {
    echo "=== captured_at ==="; date -u +%Y-%m-%dT%H:%M:%SZ
    echo "=== repo ==="
    echo "commit: $(git -C "$REPO" rev-parse HEAD 2>/dev/null || echo unknown)"
    echo "branch: $(git -C "$REPO" rev-parse --abbrev-ref HEAD 2>/dev/null || echo unknown)"
    echo "--- git status --short ---"
    git -C "$REPO" status --short 2>/dev/null || true
    echo "=== bin ==="
    ls -l "$BIN" 2>/dev/null || echo "binary missing"
    echo "=== host ==="
    echo "cpu: $(nproc) vcpu"; grep -m1 'model name' /proc/cpuinfo || true
    free -m | sed 's/^/mem: /'
    echo "=== containers ==="
    docker ps --format '{{.Names}}\t{{.Image}}\t{{.Ports}}' 2>/dev/null || true
    echo "--- digests ---"
    docker image inspect --format '{{.RepoTags}} {{.Id}}' \
      greptime/etcd:3.6.1-debian-12-r3 greptime/minio:2024 2>/dev/null || true
    echo "=== etcd params ==="
    echo "ETCD_MAX_REQUEST_BYTES: $(docker exec fixtures-etcd-1 printenv ETCD_MAX_REQUEST_BYTES 2>/dev/null || echo unavailable)"
  } >"$run_dir/env.txt" 2>&1
}

wait_tcp() {
  local host=$1 port=$2 what=$3
  for _ in $(seq 1 60); do
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then exec 3>&- ; return 0; fi
    sleep 1
  done
  echo "timeout waiting for ${what} (${host}:${port})" >&2
  return 1
}

wait_http() {
  local url=$1 what=$2
  for _ in $(seq 1 120); do
    if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  echo "timeout waiting for ${what} (${url})" >&2
  return 1
}

case "${1:-}" in
  up)   up "${2:-2}" "${3:-}" ;;
  down) down "${2:?run_dir required}" ;;
  env)  env_snapshot "${2:?run_dir required}" ;;
  *)    sed -n '2,20p' "$0"; exit 1 ;;
esac
