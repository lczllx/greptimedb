#!/usr/bin/env bash
# 规模对照：[2, 3] 个 datanode 各跑一轮 DDL，每轮都从**干净的 keyspace** 起。
#
# 为什么每轮要清 keyspace：metasrv 启动时会把全部表的元数据读进内存目录，
# 上一轮留下的 210 张表会同时抬高本轮的内存基数与启动耗时 —— 不清就分不清
# 「datanode 多了」和「表多了」哪个是原因。
#
# 用法：./matrix.sh [输出根目录]

set -euo pipefail
cd "$(dirname "$0")"

ROOT=${1:-runs/$(date -u +%m%dT%H%M%SZ)-matrix}
N_TABLES=${N_TABLES:-200}
WARMUP=${WARMUP:-10}
mkdir -p "$ROOT"

wipe_keyspace() {
  docker exec fixtures-etcd-1 etcdctl --endpoints=127.0.0.1:2379 \
    del "" --prefix >/dev/null 2>&1 || true
  local n
  n=$(docker exec fixtures-etcd-1 etcdctl --endpoints=127.0.0.1:2379 \
    get "" --prefix --keys-only 2>/dev/null | grep -c . || true)
  echo "  keyspace wiped: ${n} keys remain"
}

for N in 2 3; do
  RUN="$ROOT/cap${N}"
  echo "=== datanodes=${N} -> ${RUN} ==="
  mkdir -p "$RUN/raw"

  echo "  [1/5] wipe keyspace"
  wipe_keyspace

  echo "  [2/5] start cluster"
  ./cluster.sh up "$N" "$RUN" >"$RUN/raw/cluster-up.log" 2>&1
  tail -1 "$RUN/raw/cluster-up.log" | sed 's/^/  /'

  echo "  [3/5] start resmon"
  python3 resmon.py --out "$RUN/raw/res.tsv" --interval 0.5 \
    >"$RUN/raw/resmon.log" 2>&1 &
  RESMON=$!
  sleep 2   # 让它先把基线采下来

  echo "  [4/5] DDL probe (n=${N_TABLES}, warmup=${WARMUP})"
  python3 ddl_probe.py --out "$RUN/raw/ddl-serial.tsv" \
    --n "$N_TABLES" --warmup "$WARMUP" --tag "c${N}" \
    >"$RUN/raw/ddl-probe.log" 2>&1

  echo "  [5/5] stop resmon + cluster"
  kill "$RESMON" 2>/dev/null || true
  wait "$RESMON" 2>/dev/null || true
  ./cluster.sh down "$RUN" >"$RUN/raw/cluster-down.log" 2>&1

  tail -1 "$RUN/raw/ddl-probe.log" | sed 's/^/  /'
done

echo "matrix done -> ${ROOT}"
