#!/usr/bin/env python3
"""Failover 臂的驱动：造表 → 预热 → SIGKILL 一个 datanode → 扫到全绿为止。

口径（三条，都要写进报告）：

1. **端到端 = 客户端视角。** 起点是 `os.kill(SIGKILL)` 返回的那一刻，终点是**一轮完整扫表
   全部成功**。不看 metasrv 内部状态 —— 之所以这么定，是因为 metasrv 的 admin 面
   （`service/admin.rs:49-97`）没有 region 分布端点，内部状态客户看不见，
   而"用户什么时候能重新查到数据"才是这个指标的意义。

2. **用 SIGKILL 不是 SIGTERM。** 优雅停机时 datanode 会注销自己，metasrv 走的是另一条路径；
   "节点挂了"的故障模型是进程猝死。这个选择直接改变测出来的数字，必须记。

3. **改过一项 metasrv 配置：`region_failure_detector_initialization_delay` 10m → 5s。**
   它是运维尺度的旋钮（决定检测器**何时被建起来**），不改则 10 分钟内一次都不检测。
   **不动** `[failure_detector]` 的 threshold/acceptable_heartbeat_pause ——
   那两个才是"多久算死"的产品行为（默认 Akka 值：threshold=8、pause=10s），
   改了就是在测自己的配置。

输出（全部逐条样本，不预聚合）：
  --out  TSV: seq, phase, sweep, tbl, t_start_ns, t_end_ns, elapsed_us, ok, http_code, err
  --proc TSV: t_ns, pause, pending, running, <每条 procedure 的 json>
"""

import argparse
import http.client
import json
import os
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

HOST, PORT = "127.0.0.1", 4000
DB = "public"


TIMEOUT = 0.5  # 客户端预算，见 --client-timeout；成功请求实测 max 165ms，留 3 倍余量


def connect():
    return http.client.HTTPConnection(HOST, PORT, timeout=TIMEOUT)


def sql(conn, query):
    """返回 (ok, http_code, err, t_start_ns, t_end_ns)。"""
    body = f"sql={query}"
    t0 = time.perf_counter_ns()
    try:
        conn.request(
            "POST", f"/v1/sql?db={DB}", body=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        resp = conn.getresponse()
        raw = resp.read()
        code = resp.status
        t1 = time.perf_counter_ns()
        if code == 200:
            return True, code, "", t0, t1
        try:
            msg = json.loads(raw).get("error", raw[:200].decode("utf-8", "replace"))
        except Exception:
            msg = raw[:200].decode("utf-8", "replace")
        return False, code, msg.replace("\t", " ").replace("\n", " ")[:300], t0, t1
    except Exception as e:  # noqa: BLE001 — 连接层失败也是样本
        t1 = time.perf_counter_ns()
        # 连接坏了就换一条，否则后续样本全废
        return False, 0, f"{type(e).__name__}: {e}"[:300], t0, t1


class Worker:
    """每个线程一条长连接 —— 每次请求新建连接会把 TCP 握手算进延迟里。"""

    _local = threading.local()

    @property
    def conn(self):
        c = getattr(self._local, "conn", None)
        if c is None:
            c = connect()
            self._local.conn = c
        return c

    def run(self, query):
        ok, code, err, t0, t1 = sql(self.conn, query)
        # 只对**连接层**失败重试一次（换条连接）——超时不重试：
        # 超时是"服务端没回"，重试只会把测到的延迟翻倍，让客户端预算主导结果。
        if code == 0 and not err.startswith("TimeoutError"):
            try:
                self._local.conn.close()
            except Exception:
                pass
            self._local.conn = connect()
            ok, code, err, t0, t1 = sql(self._local.conn, query)
        return ok, code, err, t0, t1


def proc_status():
    """读 metasrv 的 procedure-manager 状态；读不到就返回 None（不当作失败）。"""
    try:
        c = http.client.HTTPConnection("127.0.0.1", 3010, timeout=3)
        c.request("GET", "/admin/procedure-manager/status")
        r = c.getresponse()
        body = r.read()
        c.close()
        if r.status != 200:
            return None
        return json.loads(body)
    except Exception:  # noqa: BLE001
        return None


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--proc", default=None, help="procedure 状态轮询的落盘路径")
    p.add_argument("--n", type=int, default=60, help="表数量")
    p.add_argument("--concurrency", type=int, default=8)
    p.add_argument("--kill-pid-file", required=True, help="被 SIGKILL 的 datanode pidfile")
    p.add_argument("--pre-sweeps", type=int, default=2, help="kill 之前的基线扫次数")
    p.add_argument("--max-secs", type=float, default=300, help="护栏：超时就停")
    p.add_argument("--client-timeout", type=float, default=2.0,
                   help="单条 SQL 的客户端预算（秒）。这是口径的一部分："
                        "它决定「服务端不返回」多久被记成一次失败")
    p.add_argument("--quiet-secs", type=float, default=60, help="全绿后继续观察多久")
    p.add_argument("--sweep-pause", type=float, default=0.0,
                   help="每轮之间的停顿（秒）。用来排除『探针自己的写负载把迁移卡住』这个自扰")
    p.add_argument("--drain-secs", type=float, default=0.0,
                   help="放弃前先**完全停止写入**这么多秒，再做最后一次扫描。"
                        "用来区分『卡住是探针的负载自持的』还是『真的永久卡住』")
    p.add_argument("--tag", default="fo")
    args = p.parse_args()

    global TIMEOUT
    TIMEOUT = args.client_timeout

    tables = [f"{args.tag}_t{i:04d}" for i in range(args.n)]
    out = open(args.out, "w", buffering=1)
    out.write("seq\tphase\tsweep\ttbl\tt_start_ns\tt_end_ns\telapsed_us\tok\thttp_code\terr\n")
    seq = 0

    def emit(phase, sweep, tbl, r):
        nonlocal seq
        ok, code, err, t0, t1 = r
        out.write(f"{seq}\t{phase}\t{sweep}\t{tbl}\t{t0}\t{t1}\t"
                  f"{(t1 - t0) // 1000}\t{int(ok)}\t{code}\t{err}\n")
        seq += 1

    w = Worker()

    # ---- 建表 ----
    print(f"[1/5] create {args.n} tables (concurrency={args.concurrency})", flush=True)
    with ThreadPoolExecutor(args.concurrency) as ex:
        futs = {
            ex.submit(w.run, f"CREATE TABLE IF NOT EXISTS {t} (ts TIMESTAMP TIME INDEX, v INT)")
            for t in tables
        }
        bad = [f for f in futs if not f.result()[0]]
    if bad:
        print(f"  !! {len(bad)} CREATE failures; first: {bad[0].result()[2]}", flush=True)
    print(f"  created, {len(bad)} failures", flush=True)

    def sweep(phase, idx):
        """一轮全表 —— 返回 (本轮是否全绿, 失败数)。"""
        with ThreadPoolExecutor(args.concurrency) as ex:
            futs = [
                (t, ex.submit(w.run, f"INSERT INTO {t} VALUES (NOW(), {i})"))
                for i, t in enumerate(tables)
            ]
            nfail = 0
            for t, f in futs:
                r = f.result()
                emit(phase, idx, t, r)
                if not r[0]:
                    nfail += 1
        return nfail == 0, nfail

    # ---- 预热：确认写路径通 ----
    print(f"[2/5] warmup sweeps", flush=True)
    for i in range(args.pre_sweeps):
        good, nfail = sweep("warmup", i)
        print(f"  warmup sweep {i}: {'OK' if good else f'{nfail} failures'}", flush=True)
        if not good:
            print("  !! warmup failed, aborting — 后面测的就不是 failover 了", flush=True)
            out.close()
            sys.exit(1)

    # ---- 杀 ----
    pid = int(Path(args.kill_pid_file).read_text().strip())
    t_kill = time.perf_counter_ns()
    t_kill_wall = time.time_ns()  # 与 metasrv 日志（UTC 墙钟）对轴用
    os.kill(pid, 9)
    print(f"[3/5] SIGKILL pid={pid} at t_kill={t_kill} wall={t_kill_wall}", flush=True)
    out.write(f"-1\tKILL\t-1\tpid{pid}\t{t_kill}\t{t_kill}\t0\t1\t0\t"
              f"SIGKILL wall_ns={t_kill_wall}\n")

    # ---- 扫到全绿 ----
    print(f"[4/5] probing until all-green", flush=True)
    proc_f = open(args.proc, "w", buffering=1) if args.proc else None
    if proc_f:
        proc_f.write("t_ns\tpause\tpending\trunning\tprocedures\n")

    deadline = time.time() + args.max_secs
    recovered_at = None
    qi = 0
    sweep_i = 0
    while time.time() < deadline:
        good, nfail = sweep("after", sweep_i)
        if proc_f:
            st = proc_status()
            if st is not None:
                proc_f.write(json.dumps({
                    "t_ns": time.perf_counter_ns(),
                    "pause": st.get("pause"),
                    "pending": st.get("pending"),
                    "running": st.get("running"),
                    "procedures": [
                        {"id": pr.get("id"), "type": pr.get("type"),
                         "status": pr.get("status"), "name": pr.get("name")}
                        for pr in (st.get("procedures") or [])
                    ],
                }) + "\n")
        if good and recovered_at is None:
            recovered_at = time.perf_counter_ns()
            print(f"  sweep {sweep_i}: ALL GREEN  -> recovery in "
                  f"{(recovered_at - t_kill) / 1e9:.3f}s", flush=True)
            break
        if sweep_i % 5 == 0:
            print(f"  sweep {sweep_i}: {nfail} failures "
                  f"(+{(time.perf_counter_ns() - t_kill) / 1e9:.1f}s)", flush=True)
        sweep_i += 1
        qi += 1
        if args.sweep_pause > 0:
            time.sleep(args.sweep_pause)

    # ---- 恢复后继续观察（抖动 / 反复） ----
    if recovered_at is not None and args.quiet_secs > 0:
        print(f"[5/5] post-recovery watch {args.quiet_secs}s", flush=True)
        t_end = time.time() + args.quiet_secs
        while time.time() < t_end:
            sweep("quiet", sweep_i)
            sweep_i += 1
            time.sleep(1)

    # ---- 静默期：完全停写，看卡住的状态能不能自己解开 ----
    drained_ok = None
    if recovered_at is None and args.drain_secs > 0:
        print(f"[drain] 停止写入 {args.drain_secs}s，然后做最后一次扫描", flush=True)
        time.sleep(args.drain_secs)
        good, nfail = sweep("drain", sweep_i)
        drained_ok = good
        print(f"[drain] 静默后扫一轮: {'ALL GREEN' if good else f'{nfail} still failing'}",
              flush=True)

    if proc_f:
        proc_f.close()
    out.close()
    if recovered_at is None:
        print("!! NOT recovered within budget"
              + ("" if drained_ok is None else
                 ("（静默后已恢复 -> 卡住是写入负载自持的）" if drained_ok
                  else "（静默后仍卡住 -> 不是负载问题）")), flush=True)
        sys.exit(2)
    print(f"done: kill->all-green = {(recovered_at - t_kill) / 1e9:.3f}s "
          f"({sweep_i} sweeps)", flush=True)


if __name__ == "__main__":
    main()
