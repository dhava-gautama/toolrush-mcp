#!/usr/bin/env python3
"""ToolRush-MCP evidence bench — measures the two lanes that matter on Linux:
warm shell vs spawn-per-command, and batched reads vs sequential.
No mocks: real bash, real files, wall time. Prints a table."""
import os
import statistics
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import toolrush_mcp as tr


def timeit(fn, n=20):
    samples = []
    for _ in range(n):
        t0 = time.perf_counter()
        fn()
        samples.append((time.perf_counter() - t0) * 1000)
    return statistics.median(samples), min(samples), max(samples)


def main():
    print(f"python {sys.version.split()[0]}  platform {sys.platform}  n=20\n")

    med, lo, hi = timeit(lambda: subprocess.run(
        ["bash", "--noprofile", "--norc", "-c", "echo bench"],
        capture_output=True, text=True))
    print(f"cold spawn  (bash -c echo)   {med:7.2f} ms  (min {lo:.2f} max {hi:.2f})")

    shell = tr.WarmShell()
    shell.run("true")  # warm up: spawn cost paid once
    med, lo, hi = timeit(lambda: shell.run("echo bench"))
    print(f"warm_exec   (persist shell)  {med:7.2f} ms  (min {lo:.2f} max {hi:.2f})")

    with tempfile.TemporaryDirectory() as td:
        files = []
        for i in range(8):
            p = os.path.join(td, f"f{i}.txt")
            with open(p, "w") as f:
                f.write("x\n" * 200)
            files.append(p)
        tr.fast_read(files[0])  # warm the cache path
        med, lo, hi = timeit(lambda: [tr.fast_read(p) for p in files])
        print(f"\n8 reads sequential           {med:7.2f} ms")
        med, lo, hi = timeit(lambda: tr.batch_read([{"path": p} for p in files]))
        print(f"8 reads batch_read (4 wkrs)  {med:7.2f} ms")

    import json
    d = json.loads(tr.doctor())
    print(f"\ndoctor: rg={d['rg']}  shell_alive={d['warm_shell_alive']}")

    # The honest floor: full MCP JSON-RPC round trip through a real server
    # subprocess (initialize once, then doctor calls) — this is the per-call
    # transport tax no server-side optimization can remove.
    srv = subprocess.Popen([sys.executable,
                            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                         "toolrush_mcp.py")],
                           stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                           stderr=subprocess.DEVNULL, text=True)
    import json as _json
    def rpc(i, method, params=None):
        srv.stdin.write(_json.dumps({"jsonrpc": "2.0", "id": i,
                                     "method": method,
                                     "params": params or {}}) + "\n")
        srv.stdin.flush()
        srv.stdout.readline()
    rpc(0, "initialize")
    rpc(1, "tools/call", {"name": "doctor", "arguments": {}})  # warm
    med, lo, hi = timeit(lambda: rpc(9, "tools/call",
                                     {"name": "doctor", "arguments": {}}))
    print(f"MCP round trip (tools/call)  {med:7.2f} ms  (min {lo:.2f} max {hi:.2f})")
    srv.stdin.close()
    srv.wait(timeout=5)


if __name__ == "__main__":
    main()
