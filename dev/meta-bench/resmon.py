#!/usr/bin/env python3
"""资源采样器：判定「4 vCPU 上能放几个 datanode」。

要回答的不是「内存够不够」（那看 MemAvailable 就行），而是**换页抖动**——
一旦开始 swap，进程还在跑但每一步都被推迟，两臂的延迟差会被噪声埋掉。

所以采两类量：
  1. 系统级 /proc/vmstat 的 pswpin/pswpout 增量  -> 抖动的直接证据
  2. 每个 greptime 进程的 utime+stime 增量 / 墙上时间 -> CPU 占用率（>1.0 即超订）
     RSS 与 majflt 增量 -> 谁在被换出去

用法：./resmon.py --out runs/<ts>/raw/res.tsv --interval 0.5
      （Ctrl-C 或 SIGTERM 停止）
"""

import argparse
import os
import signal
import sys
import time

CLK_TCK = os.sysconf("SC_CLK_TCK")


def greptime_pids():
    out = []
    for pid in os.listdir("/proc"):
        if not pid.isdigit():
            continue
        try:
            with open(f"/proc/{pid}/comm") as f:
                if f.read().strip() == "greptime":
                    out.append(int(pid))
        except OSError:
            pass
    return sorted(out)


def proc_stat(pid):
    """返回 (name, utime+stime ticks, rss_pages, majflt) 或 None。"""
    try:
        with open(f"/proc/{pid}/stat") as f:
            raw = f.read()
        # comm 可能含空格/括号，从最后一个 ')' 之后切
        rest = raw[raw.rindex(")") + 2:].split()
        # rest[0] = state; 字段号 -3 之后对齐
        utime, stime = int(rest[11]), int(rest[12])
        majflt = int(rest[9])
        with open(f"/proc/{pid}/statm") as f:
            rss_pages = int(f.read().split()[1])
        return utime + stime, rss_pages, majflt
    except (OSError, ValueError, IndexError):
        return None


def vmstat():
    d = {}
    try:
        with open("/proc/vmstat") as f:
            for line in f:
                k, _, v = line.partition(" ")
                if k in ("pswpin", "pswpout", "pgmajfault"):
                    d[k] = int(v)
    except OSError:
        pass
    return d


def meminfo():
    d = {}
    try:
        with open("/proc/meminfo") as f:
            for line in f:
                k, _, v = line.partition(":")
                d[k] = int(v.split()[0])
    except OSError:
        pass
    return d


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--out", required=True)
    p.add_argument("--interval", type=float, default=0.5)
    a = p.parse_args()

    stop = {"v": False}
    signal.signal(signal.SIGTERM, lambda *_: stop.__setitem__("v", True))
    signal.signal(signal.SIGINT, lambda *_: stop.__setitem__("v", True))

    prev = {}
    prev_vm = vmstat()
    prev_t = time.monotonic()

    with open(a.out, "w") as f:
        f.write("t_mono\twall\tmem_avail_kb\tswap_used_kb\t"
                "pswpin_d\tpswpout_d\tpgmajfault_d\t"
                "pid\tcpu\tcpu_pct\trss_mb\tmajflt_d\n")
        while not stop["v"]:
            time.sleep(a.interval)
            now = time.monotonic()
            dt = now - prev_t
            if dt <= 0:
                continue
            wall = time.strftime("%H:%M:%S")
            mi = meminfo()
            vm = vmstat()
            avail = mi.get("MemAvailable", 0)
            swap_used = mi.get("SwapTotal", 0) - mi.get("SwapFree", 0)
            d_in = vm.get("pswpin", 0) - prev_vm.get("pswpin", 0)
            d_out = vm.get("pswpout", 0) - prev_vm.get("pswpout", 0)
            d_maj = vm.get("pgmajfault", 0) - prev_vm.get("pgmajfault", 0)

            pids = greptime_pids()
            if not pids:
                f.write(f"{now:.3f}\t{wall}\t{avail}\t{swap_used}\t"
                        f"{d_in}\t{d_out}\t{d_maj}\t-\t-\t-\t-\t-\n")
            for pid in pids:
                cur = proc_stat(pid)
                if cur is None:
                    continue
                ticks, rss_pages, majflt = cur
                old = prev.get(pid)
                if old is None:
                    cpu_pct, rss_mb, d_majflt = -1.0, -1.0, -1
                else:
                    o_ticks, _, o_majflt = old
                    cpu_pct = (ticks - o_ticks) / CLK_TCK / dt * 100.0
                    rss_mb = rss_pages * 4096 / 1024 / 1024
                    d_majflt = majflt - o_majflt
                prev[pid] = cur
                f.write(f"{now:.3f}\t{wall}\t{avail}\t{swap_used}\t"
                        f"{d_in}\t{d_out}\t{d_maj}\t{pid}\t"
                        f"{cpu_pct / 100.0:.3f}\t{cpu_pct:.1f}\t{rss_mb:.1f}\t{d_majflt}\n")
            f.flush()
            prev_vm = vm
            prev_t = now

    print(f"resmon stopped -> {a.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
