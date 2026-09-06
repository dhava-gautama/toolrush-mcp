#!/usr/bin/env python3
"""ToolRush-MCP smoke test — speaks real MCP (JSON-RPC over stdio) to the
server and exercises every lane, including negative controls (kill-switches)
and the warm-shell state-persistence proof (exports survive across calls,
which per-call spawns cannot do).

Usage: python3 smoke.py
"""
import json
import os
import shlex
import subprocess
import sys
import tempfile
import time

HERE = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(HERE, "toolrush_mcp.py")

FAILS = []


def check(name, cond, detail=""):
    tag = "ok " if cond else "FAIL"
    print(f"[{tag}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        FAILS.append(name)


class Client:
    def __init__(self, env_extra=None):
        env = dict(os.environ)
        env.update(env_extra or {})
        # TOOLRUSH_SERVER overrides the launch target, e.g.
        # TOOLRUSH_SERVER=/path/to/toolrush-go python3 smoke.py
        cmd = os.environ.get("TOOLRUSH_SERVER")
        cmd = shlex.split(cmd) if cmd else [sys.executable, SERVER]
        self.proc = subprocess.Popen(
            cmd, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, env=env)
        self.next_id = 0

    def call(self, method, params=None):
        self.next_id += 1
        self.proc.stdin.write(json.dumps(
            {"jsonrpc": "2.0", "id": self.next_id, "method": method,
             "params": params or {}}) + "\n")
        self.proc.stdin.flush()
        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError("server EOF")
            msg = json.loads(line)
            if msg.get("id") == self.next_id:
                return msg

    def tool(self, name, args=None):
        r = self.call("tools/call", {"name": name, "arguments": args or {}})
        if "error" in r:
            raise RuntimeError(f"RPC error: {r['error']}")
        res = r["result"]
        return json.loads(res["content"][0]["text"]), res.get("isError", False)

    def close(self):
        self.proc.stdin.close()
        self.proc.wait(timeout=5)


def main():
    c = Client()

    init = c.call("initialize", {"protocolVersion": "2024-11-05",
                                 "clientInfo": {"name": "smoke", "version": "0"}})
    check("initialize", init["result"]["serverInfo"]["name"] == "toolrush")
    c.call("notifications/initialized")  # notification: no reply expected

    tools = [t["name"] for t in c.call("tools/list")["result"]["tools"]]
    check("tools/list", sorted(tools) == sorted(
        ["fast_read", "batch_read", "fast_search", "warm_exec", "batch_exec",
         "doctor"]), str(tools))

    with tempfile.TemporaryDirectory() as td:
        f1 = os.path.join(td, "a.txt")
        f2 = os.path.join(td, "b.txt")
        with open(f1, "w") as f:
            f.write("".join(f"line {i} needle_alpha\n" for i in range(1, 51)))
        with open(f2, "w") as f:
            f.write("second file\nneedle_beta here\n")

        r, err = c.tool("fast_read", {"path": f1, "offset": 1, "limit": 10})
        check("fast_read page", not err and r["success"] and
              r["total_lines"] == 50 and "1\tline 1 needle_alpha" in r["content"]
              and r["truncated"])
        r2, _ = c.tool("fast_read", {"path": f1, "offset": -3})
        check("fast_read tail", r2["success"] and "50\tline 50" in r2["content"])
        r3, _ = c.tool("fast_read", {"path": f1, "offset": 999})
        check("fast_read beyond-EOF hint", "beyond the end" in r3.get("hint", ""))
        r4, _ = c.tool("fast_read", {"path": f1 + ".missing"})
        check("fast_read missing file", not r4["success"])

        # offset=0 must clamp to page 1 (regression: clamp happened after the
        # beyond-EOF check, so offset 0 errored/panicked)
        r0, _ = c.tool("fast_read", {"path": f1, "offset": 0, "limit": 5})
        check("fast_read offset=0 clamps to page 1",
              r0["success"] and r0["content"].startswith("1\t"))

        # /proc files stat as 0 bytes but hold content (regression: 0-byte
        # stat was reported as "File is empty")
        if os.path.exists("/proc/version"):
            rp_, _ = c.tool("fast_read", {"path": "/proc/version"})
            check("fast_read /proc/version (0-byte stat, non-empty)",
                  rp_["success"] and rp_["content"] != "" and
                  rp_["file_size"] == 0, str(rp_))

        # MCP isError propagation: tool-level failure must set the result's
        # isError flag; partial batch success must NOT
        _, miss_err = c.tool("fast_read", {"path": f1 + ".missing"})
        check("isError: fast_read missing path", miss_err is True)

        rb, err = c.tool("batch_read", {"ops": [{"path": f1, "limit": 3},
                                                {"path": f2},
                                                {"path": f1 + ".missing"}]})
        got = rb["results"]  # raw-embedded JSON: results arrive as objects
        check("batch_read order+partial error", not err and
              got[0]["total_lines"] == 50 and "second file" in got[1]["content"]
              and not got[2]["success"])
        rb2, _ = c.tool("batch_read", {"ops": [{"path": f1}] * 17})
        check("batch_read rejects >16", not rb2["success"] and "split" in rb2["error"])
        rb3, _ = c.tool("batch_read", {"ops": [{"path": f1}, {"nope": 1}]})
        check("batch_read whole-batch validation", not rb3["success"])
        _, br_err = c.tool("batch_read", {"ops": [{"path": f1},
                                                  {"path": f1 + ".missing"}]})
        check("isError: batch_read partial failure stays false",
              br_err is False)

        rs, err = c.tool("fast_search", {"pattern": "needle_beta", "path": td})
        check("fast_search rg", not err and rs["success"] and
              rs["engine"] == "rg" and rs["total_hits"] == 1 and
              rs["hits"][0]["line"] == 2)
        rs2, _ = c.tool("fast_search", {"pattern": "NEEDLE_ALPHA", "path": td,
                                        "case_sensitive": False, "limit": 5})
        check("fast_search -i + limit", rs2["shown"] == 5 and rs2["truncated"])

        # colons in paths broke ripgrep invocation parsing (regression)
        cdir = os.path.join(td, "co:lon")
        os.makedirs(cdir)
        with open(os.path.join(cdir, "f.txt"), "w") as f:
            f.write("needle_colon first line\nother line\n")
        rcol, err = c.tool("fast_search", {"pattern": "needle_colon", "path": td})
        check("fast_search colon in path",
              not err and rcol["success"] and rcol["total_hits"] == 1 and
              rcol["hits"][0]["path"].endswith(os.path.join("co:lon", "f.txt")) and
              rcol["hits"][0]["line"] == 1, str(rcol.get("hits")))

        # warm_exec: the money lane — state must survive across calls
        rw, err = c.tool("warm_exec", {"command": "export TR_PROOF=alive$$ && cd /tmp"})
        check("warm_exec basic", not err and rw["success"] and rw["exit_code"] == 0)
        rw2, _ = c.tool("warm_exec", {"command": "printf '%s@%s' \"$TR_PROOF\" \"$PWD\""})
        check("warm_exec state survives calls",
              rw2["success"] and rw2["stdout"].startswith("alive") and
              rw2["stdout"].endswith("@/tmp"), str(rw2))
        rw3, _ = c.tool("warm_exec", {"command": "(exit 7)"})
        check("warm_exec exit code", rw3["exit_code"] == 7, str(rw3))
        rw3b, _ = c.tool("warm_exec", {"command": "exit 3"})
        check("warm_exec bare exit kills shell (rc -1)", rw3b["exit_code"] == -1)
        rw3c, _ = c.tool("warm_exec", {"command": "echo resurrected"})
        check("warm_exec respawns after bare exit",
              rw3c["success"] and "resurrected" in rw3c["stdout"])
        rw4, _ = c.tool("warm_exec", {"command": "echo out; echo err >&2"})
        check("warm_exec stderr merged", "out" in rw4["stdout"] and "err" in rw4["stdout"])
        rw5, _ = c.tool("warm_exec", {"command": "sleep 30", "timeout": 2})
        check("warm_exec timeout kills tree", rw5["exit_code"] == 124)
        rw6, _ = c.tool("warm_exec", {"command": "echo after-timeout"})
        check("warm_exec respawns after kill",
              rw6["success"] and "after-timeout" in rw6["stdout"])
        rw7, _ = c.tool("warm_exec", {"command": "echo $TR_PROOF", "reset": True})
        check("warm_exec reset clears state", rw7["success"] and rw7["stdout"] == "")

        # >8MB output must truncate with notice, keep exit_code, set flag;
        # small commands always carry the truncated field (regressions:
        # unbounded buffer / missing field)
        rt_big, err = c.tool("warm_exec", {"command": "seq 1 2000000"})
        check("warm_exec truncates >8MB output",
              not err and rt_big["exit_code"] == 0 and rt_big["truncated"] and
              "[output truncated at 8MB]" in rt_big["stdout"] and
              len(rt_big["stdout"]) <= 8 * 1024 * 1024,
              f"exit={rt_big.get('exit_code')} len={len(rt_big.get('stdout', ''))}")
        rt_sm, _ = c.tool("warm_exec", {"command": "echo hi"})
        check("warm_exec truncated flag present (false) on small output",
              rt_sm["truncated"] is False)

        # batch_exec: failing command must raise MCP isError
        _, be_err = c.tool("batch_exec", {"commands": ["true", "false"]})
        check("isError: batch_exec propagates command failure", be_err is True)

        # batch_exec: one call, sequential, state flows between commands
        be, err = c.tool("batch_exec", {"commands": [
            "export BX=flow && cd /tmp",
            "printf '%s@%s' \"$BX\" \"$PWD\"",
            "echo third"]})
        ber = be["results"]
        check("batch_exec sequencing+state", not err and be["success"] and
              ber[1]["stdout"] == "flow@/tmp" and ber[2]["stdout"] == "third",
              str(ber))
        be2, _ = c.tool("batch_exec", {"commands": ["sleep 5", "echo back"],
                                       "timeout": 1})
        be2r = be2["results"]
        check("batch_exec timeout mid-batch", be2r[0]["exit_code"] == 124 and
              be2r[1]["success"] and "fresh shell" in be2r[1].get("note", ""),
              str(be2r))
        be3, _ = c.tool("batch_exec", {"commands": ["true"] * 17})
        check("batch_exec rejects >16", not be3["success"])

        rd, _ = c.tool("doctor")
        check("doctor", rd["success"] and rd["rg"] and
              rd["stats"]["warm_exec"] >= 6)

    # streamRead window: >64MB files serve the requested offset window
    # without scanning to EOF (regression: scanned whole file per call)
    with tempfile.TemporaryDirectory() as td:
        big = os.path.join(td, "big.txt")
        with open(big, "w") as f:
            for start in range(1, 1100001, 50000):
                f.writelines(f"row {i} " + "pad" * 24 + "\n"
                             for i in range(start, start + 50000))
        t0 = time.monotonic()
        rsw, err = c.tool("fast_read", {"path": big, "offset": 1000000,
                                        "limit": 10})
        dt = time.monotonic() - t0
        check("fast_read stream window (>64MB, offset deep)",
              not err and rsw["success"] and
              rsw["content"].startswith("1000000\t") and
              rsw["total_lines"] is None and
              "streamed" in rsw.get("hint", ""), str(rsw)[:200])
        check("fast_read stream window wall time <10s", dt < 10,
              f"{dt:.2f}s")

    # cache byte budget: big files are served but never enter the lines
    # cache (>4MB). Snapshot doctor immediately before the two big reads so
    # no other check's cache activity pollutes the delta.
    with tempfile.TemporaryDirectory() as td:
        bigc = os.path.join(td, "bigcache.txt")
        with open(bigc, "w") as f:
            for _ in range(5 * 1024 * 1024 // 16):
                f.write("0123456789abcde\n")
        d0, _ = c.tool("doctor")
        s0 = d0["stats"]
        c.tool("fast_read", {"path": bigc})
        c.tool("fast_read", {"path": bigc})
        d1, _ = c.tool("doctor")
        s1 = d1["stats"]
        check("cache byte budget: >4MB file served but not cached",
              s1["cache_hits"] == s0["cache_hits"] and
              s1["fast_read"] == s0["fast_read"] + 2,
              f"cache_hits {s0['cache_hits']}→{s1['cache_hits']}, "
              f"fast_read {s0['fast_read']}→{s1['fast_read']}")

    c.close()

    # Negative control: kill-switches refuse acceleration, fail closed
    nc = Client(env_extra={"TOOLRUSH_PERSIST": "0", "TOOLRUSH_FASTLANE": "0",
                           "TOOLRUSH_SEARCH": "0"})
    nc.call("initialize")
    r, _ = nc.tool("fast_read", {"path": "/etc/hostname"})
    check("NEG TOOLRUSH_FASTLANE=0 refuses", not r["success"] and "FASTLANE" in r["error"])
    r, _ = nc.tool("warm_exec", {"command": "echo spawn-mode"})
    check("NEG TOOLRUSH_PERSIST=0 spawn fallback",
          r["success"] and r["mode"] == "spawn" and "spawn-mode" in r["stdout"])
    with tempfile.TemporaryDirectory() as td:
        with open(os.path.join(td, "x.txt"), "w") as f:
            f.write("needle_gamma\n")
        r, _ = nc.tool("fast_search", {"pattern": "needle_gamma", "path": td})
        check("NEG TOOLRUSH_SEARCH=0 walk fallback", r["success"] and
              r["engine"] == "walk" and r["total_hits"] == 1)
    nc.close()

    # Walk fallback bounds: files >16MB are skipped, not walked
    wc = Client(env_extra={"TOOLRUSH_SEARCH": "0"})
    wc.call("initialize")
    with tempfile.TemporaryDirectory() as td:
        small = os.path.join(td, "small.txt")
        with open(small, "w") as f:
            f.write("needle_walk in small file\n")
        bigw = os.path.join(td, "big_walk.txt")
        with open(bigw, "w") as f:
            f.write("needle_walk in big file\n")
            block = "x" * 65536 + "\n"
            for _ in range(17 * 1024 * 1024 // 65537 + 1):
                f.write(block)
        r, _ = wc.tool("fast_search", {"pattern": "needle_walk", "path": td})
        paths = [h["path"] for h in r.get("hits", [])]
        check("walk fallback skips >16MB files",
              r["success"] and r["engine"] == "walk" and
              r["total_hits"] == 1 and len(paths) == 1 and
              paths[0].endswith("small.txt"), str(paths))
    wc.close()

    print(f"\n{'SMOKE OK — all checks passed' if not FAILS else f'FAILURES: {FAILS}'}")
    return 0 if not FAILS else 1


if __name__ == "__main__":
    sys.exit(main())
