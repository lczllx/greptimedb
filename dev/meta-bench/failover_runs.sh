#!/usr/bin/env bash
# Failover 臂的重复测量：每轮一个全新集群（keyspace 清空、S3 root 独立），
# 轮换被杀的 datanode —— 顺手排除"某个节点恰好承载更多 region"这种不对称。
#
# 用法：./failover_runs.sh [轮数] [输出根目录]

set -euo pipefail
cd "$(dirname "$0")"

R=${1:-3}
ROOT=${2:-runs/$(date -u +%m%dT%H%M%SZ)-failover}
N_TABLES=${N_TABLES:-60}
SWEEP_PAUSE=${SWEEP_PAUSE:-0}
MAX_SECS=${MAX_SECS:-180}
DRAIN_SECS=${DRAIN_SECS:-0}
mkdir -p "$ROOT"

for i in $(seq 1 "$R"); do
  # 轮换目标：第 i 轮杀 datanode $(((i-1) % 3 + 1))
  VICTIM=$(( (i - 1) % 3 + 1 ))
  RUN="$ROOT/r${i}-kill${VICTIM}"
  echo "=== run ${i}/${R}: kill datanode-${VICTIM} -> ${RUN} ==="
  mkdir -p "$RUN/raw"

  ./cluster_failover.sh up 3 "$RUN" >"$RUN/raw/cluster-up.log" 2>&1
  tail -1 "$RUN/raw/cluster-up.log" | sed 's/^/  /'

  python3 failover_probe.py \
    --out "$RUN/raw/fo.tsv" --proc "$RUN/raw/proc.tsv" \
    --n "$N_TABLES" --concurrency 8 \
    --kill-pid-file "$RUN/datanode-${VICTIM}.pid" \
    --client-timeout 0.5 --quiet-secs 20 --max-secs "$MAX_SECS" \
    --sweep-pause "$SWEEP_PAUSE" --drain-secs "$DRAIN_SECS" --tag fo \
    >"$RUN/raw/probe.log" 2>&1 || true

  grep -E 'ALL GREEN|NOT recovered|done:' "$RUN/raw/probe.log" | sed 's/^/  /' || true

  ./cluster_failover.sh down "$RUN" >"$RUN/raw/cluster-down.log" 2>&1 || true
done

echo "runs done -> ${ROOT}"
