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
        ["fast_read", "batch_read", "fast_search", "batch_search", "fast_tree",
         "warm_exec", "batch_exec", "doctor"]), str(tools))

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

        # wrong-type arg must fail as a clean validation error (pinned
        # texts), never a protocol crash. isError is true here — both impls
        # derive it from success==false — so assert the envelope text only.
        rbad_t, err = c.tool("fast_read", {"path": 123})
        emsg = rbad_t.get("error", "")
        check("fast_read non-string path: pinned arg error, no crash",
              not rbad_t["success"] and
              (emsg.startswith("invalid arguments") or
               emsg == "path must be a non-empty string"), str(rbad_t)[:200])

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
        # offset=0 must clamp to page 1 inside batch members too (regression:
        # the clamp was missing here and the op surfaced an RPC-level error)
        rb4, err = c.tool("batch_read", {"ops": [{"path": f1, "offset": 0,
                                                  "limit": 2}]})
        check("batch_read offset=0 clamps to page 1",
              not err and rb4["success"] and rb4["results"][0]["success"] and
              rb4["results"][0]["content"].startswith("1\t"), str(rb4)[:200])
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

        # context param: N lines of gutter around each hit. Both engines
        # (rg -C and the walk fallback) must emit the same gutter format:
        # "L-line" before the match, "L+line" after. Default 0 must keep
        # the envelope byte-identical (no "context" key at all).
        cfile = os.path.join(td, "ctx.txt")
        with open(cfile, "w") as f:
            f.write("before_line\nneedle_ctx target\nafter_line\n")
        rctx, err = c.tool("fast_search", {"pattern": "needle_ctx",
                                           "path": cfile, "context": 1})
        h = (rctx.get("hits") or [{}])[0]
        check("fast_search context=1 gutter", not err and rctx["success"] and
              h.get("line") == 2 and h.get("content") == "needle_ctx target" and
              h.get("context") == "1-before_line\n3+after_line", str(h))
        rctx2, _ = c.tool("fast_search", {"pattern": "needle_ctx",
                                          "path": cfile, "context": 99})
        check("fast_search context clamps to 5",
              "context" in rctx2["hits"][0])
        rctx0, _ = c.tool("fast_search", {"pattern": "needle_ctx", "path": cfile})
        check("fast_search context absent by default",
              "context" not in rctx0["hits"][0])

        # rg --json regression: context rows around a match carried
        # timestamps, and re-deriving line numbers from row content turned
        # them into phantom hits. Exactly ONE hit, real path, real line.
        tfix = os.path.join(td, "ts.txt")
        with open(tfix, "w") as f:
            f.write("12:00:01 INFO boot\nNEEDLE_TS\n12:00:02 INFO done\n")
        rts, err = c.tool("fast_search", {"pattern": "NEEDLE_TS",
                                          "path": tfix, "context": 1})
        hts = (rts.get("hits") or [{}])[0]
        check("fast_search rg --json: context rows are not phantom hits",
              not err and rts["success"] and rts["total_hits"] == 1 and
              os.path.realpath(hts.get("path", "")) == os.path.realpath(tfix) and
              hts.get("line") == 2 and
              hts.get("context") == "1-12:00:01 INFO boot\n3+12:00:02 INFO done",
              str(rts)[:200])

        # colon+digit dir name ("a:1:b") broke rg invocation parsing
        adir = os.path.join(td, "a:1:b")
        os.makedirs(adir)
        with open(os.path.join(adir, "f.txt"), "w") as f:
            f.write("first\nNEEDLE_CD\n")
        rcd, err = c.tool("fast_search", {"pattern": "NEEDLE_CD", "path": td})
        hcd = (rcd.get("hits") or [{}])[0]
        check("fast_search colon+digit path",
              not err and rcd["success"] and rcd["total_hits"] == 1 and
              hcd.get("path", "").endswith(os.path.join("a:1:b", "f.txt")) and
              hcd.get("line") == 2, str(rcd.get("hits")))

        # gutter line numbers must come from rg's line_number field, never
        # re-parsed out of the content ("2-two" would fake a line number)
        hyf = os.path.join(td, "hy.txt")
        with open(hyf, "w") as f:
            f.write("one\n2-two\nNEEDLE-here\n4-four\n")
        rhy, err = c.tool("fast_search", {"pattern": "NEEDLE-here",
                                          "path": hyf, "context": 1})
        hhy = (rhy.get("hits") or [{}])[0]
        check("fast_search gutter: line numbers not re-derived from content",
              not err and rhy["success"] and rhy["total_hits"] == 1 and
              hhy.get("line") == 3 and
              hhy.get("context") == "2-2-two\n4+4-four", str(hhy))

        # envelope always carries the capped flag (regression: missing field)
        rcp, err = c.tool("fast_search", {"pattern": "NEEDLE-here", "path": hyf})
        check("fast_search capped flag present (false) on small search",
              not err and rcp["success"] and rcp.get("capped") is False)

        # capped=true: rg's --json output blows past the 8MB cap — success
        # stays true, shown sticks to the requested limit
        bigf = os.path.join(td, "many.txt")
        with open(bigf, "w") as f:
            f.writelines(f"NEEDLE row {i:06d} xxxxxxxxxxxxxxx\n"
                         for i in range(250000))
        rbig, err = c.tool("fast_search", {"pattern": "NEEDLE", "path": bigf,
                                           "limit": 10})
        check("fast_search capped flag true past 8MB rg output",
              not err and rbig["success"] and rbig["capped"] is True and
              rbig["shown"] == 10, str({k: rbig.get(k)
                                        for k in ("success", "capped", "shown")}))

        # batch_search: the parallel member of the batching triad. Order
        # preserved, whole batch validated up front, per-op failures isolated.
        bs, err = c.tool("batch_search", {"ops": [
            {"pattern": "needle_alpha", "path": td, "limit": 2},
            {"pattern": "needle_beta", "path": td}]})
        bsr = bs["results"]
        check("batch_search order+results", not err and bs["success"] and
              len(bsr) == 2 and bsr[0]["total_hits"] == 50 and
              bsr[0]["shown"] == 2 and bsr[1]["hits"][0]["line"] == 2,
              str(bsr)[:200])
        bs2, _ = c.tool("batch_search", {"ops": [
            {"pattern": "needle_alpha", "path": td},
            {"pattern": "[", "path": td}]})  # invalid regex: op-level failure
        check("batch_search per-op error isolated",
              bs2["results"][0]["success"] and
              not bs2["results"][1]["success"])
        bs3, _ = c.tool("batch_search", {"ops": [{"pattern": "p", "path": td}] * 17})
        check("batch_search rejects >16",
              not bs3["success"] and "split" in bs3["error"])
        bs4, _ = c.tool("batch_search", {"ops": [
            {"pattern": "x", "path": td}, {"pattern": "", "path": td}]})
        check("batch_search whole-batch validation",
              not bs4["success"] and bs4["error"].startswith("op 1 invalid"),
              str(bs4))
        bs5, _ = c.tool("batch_search", {"ops": [
            {"pattern": "needle_ctx", "path": td, "context": 1}]})
        check("batch_search op context param",
              bs5["results"][0]["hits"][0].get("context") ==
              "1-before_line\n3+after_line")

        # fast_tree: budgeted listing. Deterministic dirs-first order,
        # depth budget, shared skip list, truncation flag.
        ttree = os.path.join(td, "tree")
        os.makedirs(os.path.join(ttree, "sub", "deep"))
        os.makedirs(os.path.join(ttree, ".git"))
        os.makedirs(os.path.join(ttree, "node_modules"))
        for p in ("root.txt", os.path.join("sub", "mid.txt"),
                  os.path.join("sub", "deep", "leaf.txt"),
                  os.path.join(".git", "hidden.txt")):
            with open(os.path.join(ttree, p), "w") as f:
                f.write("x")
        rt, err = c.tool("fast_tree", {"path": ttree, "max_depth": 2})
        names = [os.path.basename(e["path"]) for e in rt.get("entries", [])]
        check("fast_tree depth budget + skips + order",
              not err and rt["success"] and not rt["truncated"] and
              names == ["sub", "root.txt", "deep", "mid.txt"] and
              rt["entries"][0]["is_dir"] is True and
              isinstance(rt["entries"][0]["mtime"], int),
              str(names))
        rt2, _ = c.tool("fast_tree", {"path": ttree, "max_entries": 2})
        check("fast_tree truncated on tiny max_entries",
              rt2["truncated"] and rt2["total"] == 2)
        rt3, _ = c.tool("fast_tree", {"path": ttree, "pattern": "*.txt"})
        names3 = sorted(os.path.basename(e["path"]) for e in rt3["entries"])
        check("fast_tree pattern filter",
              names3 == ["leaf.txt", "mid.txt", "root.txt"], str(names3))
        rt4, _ = c.tool("fast_tree", {"path": ttree + ".missing"})
        check("fast_tree missing dir", not rt4["success"])

        # invalid glob must fail closed, not match literally
        rbad, err = c.tool("fast_tree", {"path": ttree, "pattern": "["})
        check("fast_tree invalid pattern",
              not rbad["success"] and "invalid pattern" in rbad["error"],
              str(rbad))

        # symlinks: dir-symlink flagged (not followed as a dir), dangling
        # link still listed with its flag
        sl = os.path.join(td, "syms")
        os.makedirs(os.path.join(sl, "real"))
        with open(os.path.join(sl, "real", "inner.txt"), "w") as f:
            f.write("x")
        os.symlink(os.path.join(sl, "real"), os.path.join(sl, "dirlink"))
        os.symlink(os.path.join(sl, "gone"), os.path.join(sl, "dangling"))
        rsl, err = c.tool("fast_tree", {"path": sl})
        entries = {os.path.basename(e["path"]): e
                   for e in rsl.get("entries", [])}
        check("fast_tree symlinks flagged, dangling listed",
              not err and rsl["success"] and
              entries.get("dirlink", {}).get("is_symlink") is True and
              entries.get("dirlink", {}).get("is_dir") is False and
              entries.get("dangling", {}).get("is_symlink") is True,
              str(entries)[:300])

        # exact fit: N entries with max_entries=N is NOT truncated
        fit = os.path.join(td, "fit")
        os.makedirs(fit)
        for i in range(3):
            with open(os.path.join(fit, f"f{i}.txt"), "w") as f:
                f.write("x")
        rfit, err = c.tool("fast_tree", {"path": fit, "max_entries": 3})
        check("fast_tree exact fit not truncated",
              not err and rfit["success"] and rfit["truncated"] is False and
              len(rfit["entries"]) == 3, str(rfit)[:200])

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

        # newline in cwd must be rejected outright, not smuggled into the
        # shell command line (regression: cwd was interpolated raw)
        rn1, err = c.tool("warm_exec", {"command": "echo x", "cwd": "/tmp\n"})
        check("warm_exec cwd newline rejected",
              not rn1["success"] and
              "cwd must not contain newline" in rn1["error"], str(rn1))
        rn2, _ = c.tool("warm_exec", {"command": "echo y", "cwd": "a\nb"})
        check("warm_exec cwd embedded newline rejected",
              not rn2["success"] and
              "cwd must not contain newline" in rn2["error"], str(rn2))

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

        # write-wedge regression: STOP the warm shell mid-flight, then send
        # a >64KB frame — the server must bound the wedged pipe write (rc
        # 124), not hang forever, and the shell must respawn afterwards.
        t_wedge = time.monotonic()
        rp_, err = c.tool("warm_exec", {
            "command": "(sleep 1; kill -STOP $$) & echo planted"})
        check("warm_exec write-wedge: setup planted",
              not err and rp_["success"] and "planted" in rp_["stdout"])
        time.sleep(2.5)  # let the backgrounded STOPper freeze the shell
        rwed, _ = c.tool("warm_exec", {"command": "# " + "A" * 200000,
                                       "timeout": 3})
        check("warm_exec write-wedge: >64KB frame bounded (rc 124, no hang)",
              rwed["exit_code"] == 124 and time.monotonic() - t_wedge < 12,
              f"rc={rwed.get('exit_code')} t={time.monotonic()-t_wedge:.1f}s")
        rrec, err = c.tool("warm_exec", {"command": "echo recovered"})
        check("warm_exec write-wedge: shell respawned after bounded write",
              not err and rrec["success"] and "recovered" in rrec["stdout"] and
              time.monotonic() - t_wedge < 15,
              f"t={time.monotonic()-t_wedge:.1f}s")

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
        # cwd param applies to the FIRST command only; a cd inside the
        # batch still flows into later commands (regression: cwd was
        # re-applied around every member)
        be4, err = c.tool("batch_exec", {"commands": ["cd /etc", "pwd"],
                                         "cwd": "/tmp"})
        be4r = be4["results"]
        check("batch_exec cwd applies to first command only",
              not err and be4["success"] and be4r[0]["exit_code"] == 0 and
              be4r[1]["stdout"] == "/etc", str(be4r))
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
                           "TOOLRUSH_SEARCH": "0", "TOOLRUSH_PARALLEL": "0"})
    nc.call("initialize")
    r, _ = nc.tool("fast_read", {"path": "/etc/hostname"})
    check("NEG TOOLRUSH_FASTLANE=0 refuses", not r["success"] and "FASTLANE" in r["error"])
    r, _ = nc.tool("warm_exec", {"command": "echo spawn-mode"})
    check("NEG TOOLRUSH_PERSIST=0 spawn fallback",
          r["success"] and r["mode"] == "spawn" and "spawn-mode" in r["stdout"])
    r, _ = nc.tool("batch_search", {"ops": [{"pattern": "x", "path": "/tmp"}]})
    check("NEG TOOLRUSH_PARALLEL=0 refuses batch_search",
          not r["success"] and "PARALLEL" in r["error"])
    r, _ = nc.tool("fast_tree", {"path": "/tmp"})
    check("NEG TOOLRUSH_FASTLANE=0 refuses fast_tree",
          not r["success"] and "FASTLANE" in r["error"])
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
