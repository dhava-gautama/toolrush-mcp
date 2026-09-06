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

        rs, err = c.tool("fast_search", {"pattern": "needle_beta", "path": td})
        check("fast_search rg", not err and rs["success"] and
              rs["engine"] == "rg" and rs["total_hits"] == 1 and
              rs["hits"][0]["line"] == 2)
        rs2, _ = c.tool("fast_search", {"pattern": "NEEDLE_ALPHA", "path": td,
                                        "case_sensitive": False, "limit": 5})
        check("fast_search -i + limit", rs2["shown"] == 5 and rs2["truncated"])

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

    print(f"\n{'SMOKE OK — all checks passed' if not FAILS else f'FAILURES: {FAILS}'}")
    return 0 if not FAILS else 1


if __name__ == "__main__":
    sys.exit(main())
