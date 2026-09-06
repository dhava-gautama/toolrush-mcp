#!/usr/bin/env python3
"""Transport-floor shootout: Python server vs pure-C server.
Measures ping round trip (zero server work => pure transport+JSON cost)
and doctor round trip (real work) against both. n=200 for tight medians."""
import json
import os
import statistics
import subprocess
import sys
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)


def bench(cmd, method, params=None, n=200):
    srv = subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                           stderr=subprocess.DEVNULL, text=True)

    def rpc(i, m, p=None):
        srv.stdin.write(json.dumps({"jsonrpc": "2.0", "id": i, "method": m,
                                    "params": p or {}}) + "\n")
        srv.stdin.flush()
        srv.stdout.readline()

    rpc(0, "initialize")
    rpc(1, method, params)  # warm
    samples = []
    for i in range(2, n + 2):
        t0 = time.perf_counter()
        rpc(i, method, params)
        samples.append((time.perf_counter() - t0) * 1000)
    srv.stdin.close()
    srv.wait(timeout=5)
    return statistics.median(samples), min(samples), max(samples)


def main():
    py = [sys.executable, os.path.join(ROOT, "python", "toolrush_mcp.py")]
    c = [os.path.join(HERE, "ping_ref")]  # build: gcc -O2 -o ping_ref ping_ref.c

    med, lo, hi = bench(py, "ping")
    print(f"python ping   {med:7.3f} ms  (min {lo:.3f} max {hi:.3f})")
    med, lo, hi = bench(c, "ping")
    print(f"C      ping   {med:7.3f} ms  (min {lo:.3f} max {hi:.3f})")
    med, lo, hi = bench(py, "tools/call", {"name": "doctor", "arguments": {}})
    print(f"python doctor {med:7.3f} ms  (min {lo:.3f} max {hi:.3f})")

    # startup cost: spawn -> first reply
    for name, cmd in (("python", py), ("C     ", c)):
        ts = []
        for _ in range(20):
            t0 = time.perf_counter()
            srv = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.DEVNULL, text=True)
            srv.stdin.write(json.dumps({"jsonrpc": "2.0", "id": 0,
                                        "method": "initialize"}) + "\n")
            srv.stdin.flush()
            srv.stdout.readline()
            ts.append((time.perf_counter() - t0) * 1000)
            srv.stdin.close()
            srv.wait(timeout=5)
        print(f"{name} startup+init  {statistics.median(ts):7.2f} ms")


if __name__ == "__main__":
    main()
