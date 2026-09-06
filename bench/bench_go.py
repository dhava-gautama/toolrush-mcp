#!/usr/bin/env python3
"""Language shootout: Python server vs Go server vs pure-C ping reference.
Measures startup, ping floor, real work (doctor), a heavy cold batch_read,
and the head-of-line blocking test: fast_read issued WHILE a 2s warm_exec
is in flight. n=200 for microbenches, n=5 for the batch."""
import json
import os
import statistics
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
PY = [sys.executable, os.path.join(ROOT, "python", "toolrush_mcp.py")]
GO = [os.path.join(ROOT, "toolrush-go")]
C = [os.path.join(HERE, "ping_ref")]  # build: gcc -O2 -o ping_ref ping_ref.c


def spawn(cmd):
    return subprocess.Popen(cmd, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.DEVNULL, text=True)


def rpc(srv, i, method, params=None):
    srv.stdin.write(json.dumps({"jsonrpc": "2.0", "id": i, "method": method,
                                "params": params or {}}) + "\n")
    srv.stdin.flush()
    return srv.stdout.readline()


def bench_rtt(cmd, method, params=None, n=200):
    srv = spawn(cmd)
    rpc(srv, 0, "initialize")
    rpc(srv, 1, method, params)
    ts = []
    for i in range(2, n + 2):
        t0 = time.perf_counter()
        rpc(srv, i, method, params)
        ts.append((time.perf_counter() - t0) * 1000)
    srv.stdin.close(); srv.wait(timeout=5)
    return statistics.median(ts)


def bench_startup(cmd, n=20):
    ts = []
    for _ in range(n):
        t0 = time.perf_counter()
        srv = spawn(cmd)
        rpc(srv, 0, "initialize")
        ts.append((time.perf_counter() - t0) * 1000)
        srv.stdin.close(); srv.wait(timeout=5)
    return statistics.median(ts)


def bench_cold_batch(cmd, n=5):
    lines = [f"line {i} with realistic padding to make fifty chars total" for i in range(1000)]
    ts = []
    for _ in range(n):
        with tempfile.TemporaryDirectory() as td:
            ops = []
            for i in range(16):
                p = os.path.join(td, f"f{i}.txt")
                open(p, "w").write("\n".join(lines))
                ops.append({"path": p})
            srv = spawn(cmd)
            rpc(srv, 0, "initialize")
            t0 = time.perf_counter()
            rpc(srv, 1, "tools/call", {"name": "batch_read", "arguments": {"ops": ops}})
            ts.append((time.perf_counter() - t0) * 1000)
            srv.stdin.close(); srv.wait(timeout=5)
    return statistics.median(ts)


def bench_head_of_line(cmd):
    """fast_read DURING a 2s warm_exec. Serial server: ~2000ms.
    Concurrent server: ~0ms."""
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False) as f:
        f.write("hello\n")
        path = f.name
    srv = spawn(cmd)
    rpc(srv, 0, "initialize")
    rpc(srv, 1, "tools/call", {"name": "warm_exec",
                               "arguments": {"command": "true"}})
    t0 = time.perf_counter()
    srv.stdin.write(json.dumps({"jsonrpc": "2.0", "id": 2, "method": "tools/call",
                                "params": {"name": "warm_exec",
                                           "arguments": {"command": "sleep 2"}}}) + "\n")
    srv.stdin.write(json.dumps({"jsonrpc": "2.0", "id": 3, "method": "tools/call",
                                "params": {"name": "fast_read",
                                           "arguments": {"path": path}}}) + "\n")
    srv.stdin.flush()
    fast_read_wait = None
    while True:
        msg = json.loads(srv.stdout.readline())
        if msg.get("id") == 3:
            fast_read_wait = (time.perf_counter() - t0) * 1000
            break
    srv.stdin.close(); srv.wait(timeout=10)
    os.unlink(path)
    return fast_read_wait


_id = [0]


def rpc_id():
    _id[0] += 1
    return _id[0]


def bench_batch_search(cmd, n=10):
    """6 content searches over a real tree: ONE batch_search call (the Go
    server fans the ops out across goroutines) vs 6 sequential fast_search
    calls. Median of n. Returns (parallel_ms, sequential_ms)."""
    root = "/usr/share/doc" if os.path.isdir("/usr/share/doc") else ROOT
    pats = ["copyright", "license", "version", "software", "warranty",
            "redistribution"]
    ops = [{"pattern": p, "path": root, "limit": 20} for p in pats]
    srv = spawn(cmd)
    rpc(srv, rpc_id(), "initialize")
    # warmup: rg path discovery, page caches
    rpc(srv, rpc_id(), "tools/call",
        {"name": "batch_search", "arguments": {"ops": ops}})
    for op in ops:
        rpc(srv, rpc_id(), "tools/call", {"name": "fast_search", "arguments": op})
    par, seq = [], []
    for _ in range(n):
        t0 = time.perf_counter()
        rpc(srv, rpc_id(), "tools/call",
            {"name": "batch_search", "arguments": {"ops": ops}})
        par.append((time.perf_counter() - t0) * 1000)
        t0 = time.perf_counter()
        for op in ops:
            rpc(srv, rpc_id(), "tools/call",
                {"name": "fast_search", "arguments": op})
        seq.append((time.perf_counter() - t0) * 1000)
    srv.stdin.close(); srv.wait(timeout=5)
    return statistics.median(par), statistics.median(seq)


def main():
    print(f"{'':24}{'python':>10}{'go':>10}{'C':>10}")
    print(f"{'startup+init (ms)':24}{bench_startup(PY):>10.2f}{bench_startup(GO):>10.2f}{bench_startup(C):>10.2f}")
    print(f"{'ping RTT (ms)':24}{bench_rtt(PY, 'ping'):>10.3f}{bench_rtt(GO, 'ping'):>10.3f}{bench_rtt(C, 'ping'):>10.3f}")
    print(f"{'doctor RTT (ms)':24}{bench_rtt(PY, 'tools/call', {'name': 'doctor', 'arguments': {}}):>10.3f}{bench_rtt(GO, 'tools/call', {'name': 'doctor', 'arguments': {}}):>10.3f}{'-':>10}")
    print(f"{'cold batch 16x1000 (ms)':24}{bench_cold_batch(PY):>10.2f}{bench_cold_batch(GO):>10.2f}{'-':>10}")
    print(f"{'read during 2s exec (ms)':24}{bench_head_of_line(PY):>10.2f}{bench_head_of_line(GO):>10.2f}{'-':>10}")

    root = "/usr/share/doc" if os.path.isdir("/usr/share/doc") else ROOT
    print(f"\nbatch_search: 6 searches over {root} (median of 10, ms)")
    pp, ps = bench_batch_search(PY)
    gp, gs = bench_batch_search(GO)
    print(f"  parallel   (1 batch_search call)  python {pp:>7.2f}   go {gp:>7.2f}")
    print(f"  sequential (6 fast_search calls)  python {ps:>7.2f}   go {gs:>7.2f}")
    print(f"  parallel speedup: go {gs / gp:.2f}x, python {ps / pp:.2f}x "
          f"(python reference runs its ops sequentially — its win is 1 RPC, "
          f"the Go win is real process parallelism)")


if __name__ == "__main__":
    main()
