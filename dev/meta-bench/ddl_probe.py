#!/usr/bin/env python3
"""DDL 延迟探针：串行发 N 条 CREATE TABLE，逐条落样本。

输出是**逐条样本**，不是聚合值 —— 统计口径（warmup 丢几条、报 p50 还是 p99）留到分析期再定，
见 p_b/application/申请书要求核对.md §6.3 决定 3。

用法：
  ./ddl_probe.py --out runs/<ts>/raw/ddl-serial.tsv --n 200 --warmup 10
  ./ddl_probe.py --out ... --concurrency 4 --n 200

计时口径：客户端发出 SQL 到收到响应，不减去任何一段（§6.3 决定 4）。
"""

import argparse
import http.client
import json
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

HOST = "127.0.0.1"
PORT = 4000
DB = "public"

COLS = [
    "seq", "phase", "worker",
    "t_start_ns", "t_end_ns", "elapsed_us",
    "status", "server_ms", "err",
]


def post_sql(conn, sql):
    """一条 SQL，返回 (status, server_ms, err)。计时在调用方做。"""
    body = urllib.parse.urlencode({"sql": sql})
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    conn.request("POST", f"/v1/sql?db={DB}", body=body, headers=headers)
    resp = conn.getresponse()
    raw = resp.read()
    if resp.status != 200:
        return "http_%d" % resp.status, "", raw[:200].decode("utf-8", "replace")
    try:
        payload = json.loads(raw)
    except Exception as e:  # noqa: BLE001
        return "badjson", "", str(e)
    if payload.get("code", 0) != 0:
        return "err", "", str(payload.get("error", payload))[:300]
    return "ok", str(payload.get("execution_time_ms", "")), ""


def one(conn, sql, seq, phase, worker):
    t0 = time.perf_counter_ns()
    try:
        status, server_ms, err = post_sql(conn, sql)
    except Exception as e:  # noqa: BLE001
        status, server_ms, err = "exc", "", f"{type(e).__name__}: {e}"
        conn.close()
    t1 = time.perf_counter_ns()
    return (seq, phase, worker, t0, t1, (t1 - t0) // 1000, status, server_ms, err)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--n", type=int, default=200)
    p.add_argument("--warmup", type=int, default=10)
    p.add_argument("--concurrency", type=int, default=1)
    p.add_argument("--tag", default=str(int(time.time())))
    a = p.parse_args()

    rows = []
    total = a.warmup + a.n

    def sql_for(i):
        return (f"CREATE TABLE bench_{a.tag}_{i} "
                f"(ts TIMESTAMP TIME INDEX, v INT)")

    if a.concurrency == 1:
        conn = http.client.HTTPConnection(HOST, PORT, timeout=120)
        for i in range(total):
            phase = "warmup" if i < a.warmup else "measure"
            r = one(conn, sql_for(i), i, phase, 0)
            rows.append(r)
            print("\t".join(map(str, r)), flush=True)
        conn.close()
    else:
        # 并发臂：每个 worker 一条长连接，各跑一部分。
        def worker(w):
            conn = http.client.HTTPConnection(HOST, PORT, timeout=120)
            out = []
            for i in range(w, total, a.concurrency):
                phase = "warmup" if i < a.warmup else "measure"
                out.append(one(conn, sql_for(i), i, phase, w))
            conn.close()
            return out

        with ThreadPoolExecutor(max_workers=a.concurrency) as ex:
            for part in ex.map(worker, range(a.concurrency)):
                rows.extend(part)
        rows.sort(key=lambda r: r[0])
        for r in rows:
            print("\t".join(map(str, r)), flush=True)

    with open(a.out, "w") as f:
        f.write("\t".join(COLS) + "\n")
        for r in rows:
            f.write("\t".join(map(str, r)) + "\n")

    bad = [r for r in rows if r[6] != "ok"]
    print(f"\n{len(rows)} samples -> {a.out}; non-ok: {len(bad)}", file=sys.stderr)
    if bad:
        print(f"first failure: {bad[0]}", file=sys.stderr)


if __name__ == "__main__":
    main()
