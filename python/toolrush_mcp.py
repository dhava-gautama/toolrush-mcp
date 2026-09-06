#!/usr/bin/env python3
"""ToolRush MCP — universal port of ToolRush v2 (Hermes Agent plugin) to any
MCP-capable agent harness (Kimi Code CLI, Codex CLI, Claude Code, ...).

The original plugin (github.com/OnlyTerp/toolrush) kills the "tool-call tax"
by monkeypatching the Hermes harness in memory. That mechanism is not
portable; the lanes are. This server exposes them as MCP tools over stdio:

  fast_read     in-process read: one open()/stat, line-number gutter, BOM
                strip, binary sniff, beyond-EOF hint, mtime-keyed cache.
                (port of toolrush.py P1+P3)
  batch_read    1..16 reads on a 4-worker pool through ONE call, input order
                preserved, whole batch validated before dispatch.
                (port of toolrush.py P2 / v2 parallel RPC lane)
  fast_search   content search. Primary: direct rg transport (real ignore
                files, real regex grammar — v2's "one engine, accelerated
                transport"). Fallback / negative control: pure-Python walk
                (port of toolrush_search.py) when rg is missing or
                TOOLRUSH_SEARCH=0.
  warm_exec     ONE persistent bash (--noprofile --norc), per-call unique
                frame markers, rc capture, cwd/exports survive across calls,
                process-group kill on timeout, NEVER retries a submitted
                command. TOOLRUSH_PERSIST=0 -> spawn-per-call fallback.
                (port of toolrush_exec.py / v2 warm terminal lane)
  doctor        lane status, versions, kill-switch state, counters.

Kill-switches (env, fail-closed: refused acceleration errors clearly so the
agent falls back to its native tools):
  TOOLRUSH_FASTLANE=0  TOOLRUSH_SEARCH=0  TOOLRUSH_PERSIST=0  TOOLRUSH_PARALLEL=0

Safety: read-only file lanes (no write tool — writes belong behind the
harness's own permission UI). warm_exec runs with the same privileges as the
harness's terminal tool; MCP approval rules gate it. No network, no
persistence, stdlib only, stdout carries protocol frames ONLY.
"""
import itertools
import json
import os
import queue
import re
import shutil
import signal
import subprocess
import sys
import threading
import time
import uuid
from pathlib import Path

VERSION = "1.0.1"

USE_FASTLANE = os.environ.get("TOOLRUSH_FASTLANE", "1") == "1"
USE_SEARCH = os.environ.get("TOOLRUSH_SEARCH", "1") == "1"
USE_PERSIST = os.environ.get("TOOLRUSH_PERSIST", "1") == "1"
USE_PARALLEL = os.environ.get("TOOLRUSH_PARALLEL", "1") == "1"

MAX_LINE = 2000            # per-line clamp, same as upstream _max_line_length
MAX_READ_BYTES = 100_000   # response byte cap per fast_read page
MAX_OUT = 8 * 1024 * 1024  # bounded parser memory for warm_exec / fast_search
OUT_TRUNC_NOTICE = "\n[output truncated at 8MB]"
READ_DEFAULT_LIMIT = 1000
SEARCH_TIMEOUT = 60
EXEC_DEFAULT_TIMEOUT = 120
BATCH_MAX = 16
BATCH_WORKERS = 4

CACHE_FILE_MAX = 4 * 1024 * 1024    # skip the lines-cache STORE beyond this
WALK_MAX_FILE = 16 * 1024 * 1024    # walk fallback refuses to read past this

IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp", ".ico")
SKIP_SUFFIXES = IMAGE_SUFFIXES + (".pyc", ".pyo")

_STATS = {"fast_read": 0, "batch_read": 0, "fast_search": 0, "warm_exec": 0,
          "batch_exec": 0, "cache_hits": 0, "shell_respawns": 0}


def _log(msg):
    sys.stderr.write(f"[toolrush] {msg}\n")
    sys.stderr.flush()


def _j(obj):
    return json.dumps(obj, ensure_ascii=False)


def _err(msg):
    return _j({"success": False, "error": msg})


# ---------------------------------------------------------------- fast_read
#
# Cache holds DECODED LINES keyed by (path, mtime, size) — not rendered pages.
# A page turn (new offset/limit on an unchanged file) costs a dict lookup +
# render, not a re-read + re-decode.

_LINES_LOCK = threading.Lock()
_LINES_CACHE = {}  # {resolved_path_str: (mtime_ns, size, [lines])}
LINES_CACHE_MAX = 64
LARGE_FILE = 64 * 1024 * 1024  # beyond this: stream the window, don't cache


def _resolve(path):
    p = Path(path)
    if not p.is_absolute():
        p = Path(os.getcwd()) / p
    return p.resolve()


def _decode_store(rp, st, raw):
    """Decode+split+cache-store. GIL-held CPU: called from the MAIN thread
    only (pooling it measured slower than serial). Returns (lines, err)."""
    if b"\x00" in raw[:8000]:
        return None, "binary file (NUL byte in first 8000 bytes)"
    try:
        text = raw.decode("utf-8-sig")  # strips BOM like upstream _strip_bom
    except UnicodeDecodeError:
        return None, "binary file (not UTF-8 decodable)"
    lines = text.splitlines()
    # Byte budget: serving the read is fine, but caching a huge file
    # multiplies RSS (lines + raw copies). Skip the store.
    if st.st_size <= CACHE_FILE_MAX:
        with _LINES_LOCK:
            if len(_LINES_CACHE) >= LINES_CACHE_MAX:
                _LINES_CACHE.pop(next(iter(_LINES_CACHE)))  # oldest-first eviction
            _LINES_CACHE[str(rp)] = (st.st_mtime_ns, st.st_size, lines)
    return lines, None


def _get_lines(rp, st):
    key = str(rp)
    with _LINES_LOCK:
        hit = _LINES_CACHE.get(key)
    if hit is not None and hit[0] == st.st_mtime_ns and hit[1] == st.st_size:
        _STATS["cache_hits"] += 1
        return hit[2], None
    return _decode_store(rp, st, rp.read_bytes())


def _render(lines, size, offset, limit):
    """Gutter render shared by fast_read and batch_read's cache probe."""
    total = len(lines)
    # /proc-style files stat 0 but have content — only claim emptiness
    # when the read itself produced nothing.
    if size == 0 and (total == 0 or (total == 1 and lines[0] == "")):
        return _j({"success": True, "content": "", "total_lines": 0,
                   "file_size": 0, "hint": "File is empty (0 bytes)."})
    if offset < 0:  # tail mode: offset=-N -> last N lines
        offset = max(total + offset + 1, 1)
    if offset > total:
        return _j({"success": True, "content": "", "total_lines": total,
                   "file_size": size,
                   "hint": f"Note: offset {offset} is beyond the end of the file "
                           f"({total} lines total). Retry with offset <= {total}."})
    end = offset + limit - 1
    page = lines[offset - 1:end]
    gutter = []
    nbytes = 0
    clipped = False
    for i, line in enumerate(page, start=offset):
        if len(line) > MAX_LINE:
            line = line[:MAX_LINE] + "... [truncated]"
        row = f"{i}\t{line}"
        nbytes += len(row) + 1
        if nbytes > MAX_READ_BYTES:
            clipped = True
            end = i - 1
            break
        gutter.append(row)
    truncated = total > end
    d = {"success": True, "content": "\n".join(gutter),
         "total_lines": total, "file_size": size,
         "truncated": truncated, "is_binary": False, "is_image": False}
    if truncated or clipped:
        d["hint"] = (f"Use offset={end + 1} to continue reading "
                     f"(showing {offset}-{end} of {total} lines)")
    return _j(d)


def _stream_read(rp, st, offset, limit):
    """Large files: stream only the requested window; total_lines unknown."""
    if offset < 1:
        return _err("large file (>64MB): tail mode unsupported, use a positive offset")
    with open(rp, encoding="utf-8-sig", errors="strict") as f:
        window = list(itertools.islice(f, offset - 1, offset - 1 + limit))
    gutter = []
    for i, line in enumerate(window, start=offset):
        line = line.rstrip("\n")
        if len(line) > MAX_LINE:
            line = line[:MAX_LINE] + "... [truncated]"
        gutter.append(f"{i}\t{line}")
    return _j({"success": True, "content": "\n".join(gutter),
               "total_lines": None, "file_size": st.st_size,
               "truncated": len(window) == limit, "is_binary": False,
               "is_image": False,
               "hint": f"large file streamed without line count; "
                       f"offset={offset + len(window)} continues"})


def _probe(path, offset, limit):
    """Rendered envelope on a lines-cache hit; ("miss", rp, st) when the op
    is probe-able but cold (caller completes WITHOUT re-stat); None to defer
    to fast_read's full path (missing/image/large/error)."""
    try:
        rp = _resolve(path)
        st = rp.stat()
        if (not rp.is_file() or rp.suffix.lower() in IMAGE_SUFFIXES
                or st.st_size > LARGE_FILE):
            return None
        with _LINES_LOCK:
            hit = _LINES_CACHE.get(str(rp))
        if hit is None or hit[0] != st.st_mtime_ns or hit[1] != st.st_size:
            return ("miss", rp, st)
        _STATS["cache_hits"] += 1
        return _render(hit[2], st.st_size, offset, limit)
    except OSError:
        return None


def fast_read(path, offset=1, limit=READ_DEFAULT_LIMIT):
    """In-process read, ToolRush v1 P1/P3 semantics, harness-neutral gutter."""
    if not USE_FASTLANE:
        return _err("TOOLRUSH_FASTLANE=0 — lane disabled; use the harness's native read")
    try:
        offset = int(offset)
        limit = int(limit)
    except (TypeError, ValueError):
        return _err("offset and limit must be integers")
    if limit < 1:
        limit = 1
    if limit > READ_DEFAULT_LIMIT:
        limit = READ_DEFAULT_LIMIT
    if offset == 0:  # page 1; negatives stay tail mode
        offset = 1

    rp = _resolve(path)
    if not rp.is_file():
        return _err(f"File not found: {path}")
    if rp.suffix.lower() in IMAGE_SUFFIXES:
        return _err("image file — use the harness's vision-capable reader")

    st = rp.stat()
    if st.st_size > LARGE_FILE:
        _STATS["fast_read"] += 1
        return _stream_read(rp, st, offset, limit)
    lines, err = _get_lines(rp, st)
    if err is not None:
        return _err(err)
    _STATS["fast_read"] += 1
    return _render(lines, st.st_size, offset, limit)


# --------------------------------------------------------------- batch_read


def batch_read(ops):
    """1..16 reads through one call. Order-preserving. Whole batch validated
    before dispatch — one bad op rejects the batch, nothing runs."""
    if not USE_PARALLEL:
        return _err("TOOLRUSH_PARALLEL=0 — lane disabled; issue individual reads")
    if not isinstance(ops, list) or not ops:
        return _err("ops must be a non-empty list of {path, offset?, limit?}")
    if len(ops) > BATCH_MAX:
        return _err(f"batch too large: {len(ops)} > {BATCH_MAX} — split it")
    for i, op in enumerate(ops):
        if not isinstance(op, dict) or not isinstance(op.get("path"), str):
            return _err(f"op {i} invalid: each op needs a string 'path'")
    _STATS["batch_read"] += 1

    # Cache hits are served synchronously; misses run SERIALLY too. A thread
    # pool was measured SLOWER on every filesystem here (NTFS 31.8 vs 20.7ms,
    # tmpfs 6.1 vs 2.3ms for 16 files): reads are page-cache fast, while
    # decode/split/render is GIL-held CPU that threads cannot overlap —
    # dispatch overhead is pure loss. batch_read's win is ONE MCP call
    # instead of N (transport), not in-process parallelism. Wave-4 law.
    results = [None] * len(ops)
    probes = [None] * len(ops)
    misses = []
    for i, op in enumerate(ops):
        pr = _probe(op["path"], op.get("offset", 1),
                    op.get("limit", READ_DEFAULT_LIMIT))
        probes[i] = pr
        if isinstance(pr, str):
            results[i] = pr
        else:
            misses.append(i)

    for i in misses:
        op = ops[i]
        pr = probes[i]
        if isinstance(pr, tuple):  # cold-but-probed: complete without re-stat
            _, rp, st = pr
            lines, err = _get_lines(rp, st)
            _STATS["fast_read"] += 1
            results[i] = (_err(err) if err else
                          _render(lines, st.st_size, op.get("offset", 1),
                                  op.get("limit", READ_DEFAULT_LIMIT)))
        else:
            results[i] = fast_read(op["path"], op.get("offset", 1),
                                   op.get("limit", READ_DEFAULT_LIMIT))
    # Inner results are already valid JSON texts — embed raw. json.dumps on
    # the outer dict would re-escape ~1.6MB of content per batch (+6ms).
    return '{"success":true,"results":[' + ",".join(results) + "]}"


# -------------------------------------------------------------- fast_search

_RG = None
_RGVER = None

# Parses rg's --line-number --no-heading "path:line:content" rows. The
# non-greedy path + backtracking resolves colons inside the path
# (co:lon/f.txt:1:hit), which a plain partition(":") misparsed.
_HIT_LINE_RX = re.compile(r"^(.+?):(\d+):(.*)$")


def _rg():
    global _RG
    if _RG is None:
        _RG = shutil.which("rg")
        if _RG is None:
            alt = Path.home() / ".kimi-code/bin/rg"
            if alt.is_file():
                _RG = str(alt)
    return _RG


def _search_rg(pattern, path, file_glob, case_sensitive, limit, offset):
    cmd = [_rg(), "--line-number", "--no-heading", "--color", "never",
           "--encoding", "utf-8"]
    if not case_sensitive:
        cmd.append("-i")
    if file_glob:
        cmd += ["-g", file_glob]
    cmd += ["-e", pattern, "--", str(_resolve(path))]
    try:
        r = subprocess.run(cmd, capture_output=True, timeout=SEARCH_TIMEOUT)
    except subprocess.TimeoutExpired:
        return _err(f"rg timed out after {SEARCH_TIMEOUT}s")
    if r.returncode == 2:
        return _err("rg error: " + r.stderr.decode("utf-8", "replace")[:500])
    hits = []
    total = 0
    for raw in r.stdout.decode("utf-8", "replace").splitlines():
        if not raw:
            continue
        total += 1
        if total <= offset or len(hits) >= limit:
            continue
        m = _HIT_LINE_RX.match(raw)
        if m is None:
            continue
        no = int(m.group(2))
        line = m.group(3)
        if len(line) > MAX_LINE:
            line = line[:MAX_LINE] + "... [truncated]"
        hits.append({"path": m.group(1), "line": no, "content": line})
    return _j({"success": True, "engine": "rg", "hits": hits,
               "total_hits": total, "shown": len(hits),
               "truncated": total > offset + len(hits)})


def _search_walk(pattern, path, file_glob, case_sensitive, limit, offset):
    """Pure-Python fallback (port of toolrush_search.py): deterministic sorted
    walk, null-byte binary sniff, utf-8-sig decode."""
    import fnmatch
    flags = 0 if case_sensitive else re.IGNORECASE
    try:
        rx = re.compile(pattern, flags)
    except re.error as e:
        return _err(f"invalid regex: {e}")
    root = _resolve(path)
    hits = []
    total = 0
    timed_out = False
    deadline = time.monotonic() + SEARCH_TIMEOUT
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames.sort()
        if ".git" in dirnames:
            dirnames.remove(".git")
        for fn in sorted(filenames):
            if time.monotonic() > deadline:
                timed_out = True
                break
            if file_glob and not fnmatch.fnmatch(fn, file_glob):
                continue
            fp = Path(dirpath) / fn
            if fp.suffix.lower() in SKIP_SUFFIXES:
                continue
            try:
                if fp.stat().st_size > WALK_MAX_FILE:
                    continue  # too large to walk
                raw = fp.read_bytes()
            except OSError:
                continue
            if b"\x00" in raw[:8000]:
                continue
            try:
                text = raw.decode("utf-8-sig")
            except UnicodeDecodeError:
                continue
            for no, line in enumerate(text.splitlines(), start=1):
                if rx.search(line):
                    total += 1
                    if total > offset and len(hits) < limit:
                        if len(line) > MAX_LINE:
                            line = line[:MAX_LINE] + "... [truncated]"
                        hits.append({"path": str(fp), "line": no, "content": line})
        if timed_out:
            break
    return _j({"success": True, "engine": "walk", "hits": hits,
               "total_hits": total, "shown": len(hits),
               "truncated": timed_out or total > offset + len(hits)})


def fast_search(pattern, path, file_glob=None, case_sensitive=True,
                limit=100, offset=0):
    if not isinstance(pattern, str) or not pattern:
        return _err("pattern must be a non-empty string (ripgrep regex syntax)")
    limit = max(1, min(int(limit), 2000))
    offset = max(0, int(offset))
    _STATS["fast_search"] += 1
    if USE_SEARCH and _rg():
        return _search_rg(pattern, path, file_glob, case_sensitive, limit, offset)
    # SEARCH=0 or rg missing -> pure-Python lane (negative control / fallback)
    return _search_walk(pattern, path, file_glob, case_sensitive, limit, offset)


# --------------------------------------------------------------- warm_exec

class WarmShell:
    """One persistent bash. Commands framed with per-call unique markers;
    reader thread feeds a queue; consumer waits with real deadlines."""

    def __init__(self):
        self._lock = threading.Lock()
        self._proc = None
        self._q = None
        self._cwd = os.getcwd()

    def _spawn(self, cwd=None):
        self._cwd = cwd or self._cwd
        self._proc = subprocess.Popen(
            ["bash", "--noprofile", "--norc"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            cwd=self._cwd, start_new_session=True, bufsize=0)
        self._q = queue.Queue()

        def reader(proc, q):
            buf = b""
            while True:
                try:
                    chunk = os.read(proc.stdout.fileno(), 65536)
                except OSError:
                    break
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    q.put(line.decode("utf-8", "replace"))
            # Sole reaper: after EOF the shell is gone — Wait() to reap the
            # zombie (and free its pid/fd), then report the death.
            try:
                proc.wait(timeout=5)
            except Exception:
                pass
            q.put(None)  # EOF sentinel: shell died

        threading.Thread(target=reader, args=(self._proc, self._q),
                         daemon=True).start()

    def _alive(self):
        return self._proc is not None and self._proc.poll() is None

    def reset(self):
        with self._lock:
            if self._alive():
                try:
                    os.killpg(self._proc.pid, signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
            self._proc = None
            self._spawn(os.getcwd())

    def run(self, command, cwd=None, timeout=EXEC_DEFAULT_TIMEOUT):
        """Returns (stdout_str, rc, truncated). rc: 124 timeout, -1 shell died.
        Never retries a submitted command."""
        with self._lock:
            if not self._alive():
                self._spawn(cwd)
                _STATS["shell_respawns"] += 1
            proc, q = self._proc, self._q
            # Drain strays (output of earlier backgrounded jobs)
            while True:
                try:
                    q.get_nowait()
                except queue.Empty:
                    break
            if cwd and os.path.abspath(cwd) != self._cwd:
                command = f"cd {_shq(cwd)} && {{ {command}\n}}"
            mid = uuid.uuid4().hex[:12]
            begin, endm = f"TRB{mid}", f"TRE{mid}:"
            # Markers lead with \n so output without a trailing newline can
            # never glue the marker onto the last output line. The END marker
            # carries rc AND $PWD — real cwd is read back from the shell, so
            # `cd` inside a user command can never desync the tracker.
            frame = (f"printf '%s\\n' '{begin}'; {{ {command}\n}}; _rc=$?; "
                     f"printf '\\n%s:%d:%s\\n' '{endm[:-1]}' $_rc \"$PWD\"\n")
            try:
                proc.stdin.write(frame.encode())
                proc.stdin.flush()
            except (BrokenPipeError, OSError):
                self._proc = None
                return "", -1, False
            out, nbytes, rc = [], 0, -1
            truncated = False
            deadline = time.monotonic() + timeout
            while True:
                remain = deadline - time.monotonic()
                if remain <= 0:
                    self._kill_tree()
                    out.append(f"\n[timed out after {timeout}s — "
                               f"command tree killed, shell will respawn]")
                    return "".join(out), 124, truncated
                try:
                    line = q.get(timeout=remain)
                except queue.Empty:
                    self._kill_tree()
                    out.append(f"\n[timed out after {timeout}s — "
                               f"command tree killed, shell will respawn]")
                    return "".join(out), 124, truncated
                if line is None:  # shell died mid-command; do NOT retry
                    self._proc = None
                    return "".join(out), -1, truncated
                if line == begin:
                    continue
                if line.startswith(endm):
                    parts = line.split(":", 2)
                    if len(parts) == 3:
                        try:
                            rc = int(parts[1])
                        except ValueError:
                            rc = -1
                        if parts[2]:
                            self._cwd = parts[2]
                    return "".join(out), rc, truncated
                # Cap accounting reserves room for the notice so the final
                # stdout stays within MAX_OUT even after appending it.
                if truncated or nbytes + len(line) + 1 > MAX_OUT - len(OUT_TRUNC_NOTICE):
                    if not truncated:
                        out.append(OUT_TRUNC_NOTICE)
                        nbytes += len(OUT_TRUNC_NOTICE)
                        truncated = True
                    continue  # bounded parser memory: drop, keep consuming
                nbytes += len(line) + 1
                out.append(line + "\n")
            # unreachable

    def _kill_tree(self):
        proc = self._proc
        if proc is not None:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                proc.wait(timeout=5)  # reap now; reader also waits (idempotent)
            except Exception:
                pass
        self._proc = None


def _shq(p):
    return "'" + str(p).replace("'", "'\\''") + "'"


_SHELL = None


def warm_exec(command, cwd=None, timeout=EXEC_DEFAULT_TIMEOUT, reset=False):
    global _SHELL
    if not isinstance(command, str) or not command.strip():
        return _err("command must be a non-empty string")
    timeout = max(1, min(int(timeout), 3600))
    _STATS["warm_exec"] += 1
    if not USE_PERSIST:
        # Negative control: spawn-per-call, same signature (v1 exec_spawn).
        # Combined stdout+stderr capped at MAX_OUT with the same notice as
        # the warm path: half the budget each so the concatenation (plus
        # notice) stays within MAX_OUT.
        per_stream = (MAX_OUT - len(OUT_TRUNC_NOTICE)) // 2
        try:
            proc = subprocess.Popen(
                ["bash", "--noprofile", "--norc", "-c", command],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=cwd or os.getcwd())
        except OSError as e:
            return _err(f"{type(e).__name__}: {e}")

        def _drain(f, cap, sink):
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                if len(sink) < cap:
                    sink += chunk[:cap - len(sink)]

        so, se = bytearray(), bytearray()
        t_out = threading.Thread(target=_drain, args=(proc.stdout, per_stream, so))
        t_err = threading.Thread(target=_drain, args=(proc.stderr, per_stream, se))
        t_out.start()
        t_err.start()
        try:
            rc = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
            t_out.join()
            t_err.join()
            return _j({"success": False, "mode": "spawn", "stdout": "",
                       "exit_code": 124, "truncated": False,
                       "error": f"timed out after {timeout}s"})
        t_out.join()
        t_err.join()
        truncated = len(so) >= per_stream or len(se) >= per_stream
        stdout = so.decode("utf-8", "replace") + se.decode("utf-8", "replace")
        if truncated:
            stdout += OUT_TRUNC_NOTICE
        return _j({"success": rc == 0, "mode": "spawn", "stdout": stdout,
                   "exit_code": rc, "truncated": truncated})
    if _SHELL is None:
        _SHELL = WarmShell()
    if reset:
        _SHELL.reset()
    out, rc, truncated = _SHELL.run(command, cwd=cwd, timeout=timeout)
    d = {"success": rc == 0, "mode": "persist",
         "stdout": out.rstrip("\n"), "exit_code": rc, "truncated": truncated}
    if rc == 124:
        d["error"] = f"timed out after {timeout}s"
    elif rc == -1:
        d["error"] = "shell died mid-command (not retried); next call respawns"
    return _j(d)


def batch_exec(commands, cwd=None, timeout=EXEC_DEFAULT_TIMEOUT):
    """N commands through ONE call on the warm shell, sequentially — state
    flows between them (cd/export in command i visible in command i+1).
    Kills N-1 harness round trips. Per-command rc; a timeout/kill restarts
    the shell and later commands are flagged as running on fresh state."""
    if not isinstance(commands, list) or not commands:
        return _err("commands must be a non-empty list of strings")
    if len(commands) > BATCH_MAX:
        return _err(f"batch too large: {len(commands)} > {BATCH_MAX} — split it")
    for i, cmd in enumerate(commands):
        if not isinstance(cmd, str) or not cmd.strip():
            return _err(f"command {i} invalid: must be a non-empty string")
    _STATS["batch_exec"] += 1
    results = []
    fresh = False
    for cmd in commands:
        r = json.loads(warm_exec(cmd, cwd=cwd, timeout=timeout))
        if fresh:
            r["note"] = "ran on a fresh shell — prior command killed the warm one"
        results.append(_j(r))
        if r.get("exit_code") in (124, -1):
            fresh = True
    ok = all(json.loads(x).get("exit_code") == 0 for x in results)
    # Inner results are already valid JSON texts — embed raw, no re-escape.
    return ('{"success":' + ("true" if ok else "false") +
            ',"results":[' + ",".join(results) + "]}")


# ------------------------------------------------------------------- doctor

def doctor():
    global _RGVER
    rg = _rg()
    if rg and _RGVER is None:
        try:
            _RGVER = subprocess.run([rg, "--version"], capture_output=True,
                                    text=True, timeout=5).stdout.splitlines()[0]
        except Exception:
            _RGVER = "unreadable"
    return _j({
        "success": True, "version": VERSION,
        "python": sys.version.split()[0], "platform": sys.platform,
        "kill_switches": {"TOOLRUSH_FASTLANE": USE_FASTLANE,
                          "TOOLRUSH_SEARCH": USE_SEARCH,
                          "TOOLRUSH_PERSIST": USE_PERSIST,
                          "TOOLRUSH_PARALLEL": USE_PARALLEL},
        "rg": rg, "rg_version": _RGVER,
        "warm_shell_alive": bool(_SHELL and _SHELL._alive()),
        "stats": dict(_STATS),
    })


# ------------------------------------------------------------ MCP transport

TOOLS = [
    {"name": "fast_read",
     "description": "Fast in-process file read with line-number gutter "
                    "(<lineno>\\t<content>), offset/limit paging, negative "
                    "offset = tail, binary sniff, BOM strip. Read-only.",
     "inputSchema": {"type": "object",
                     "properties": {
                         "path": {"type": "string"},
                         "offset": {"type": "integer", "default": 1},
                         "limit": {"type": "integer", "default": 1000}},
                     "required": ["path"]}},
    {"name": "batch_read",
     "description": "Read 1-16 files in ONE call on a 4-worker pool. Input "
                    "order preserved; the whole batch is validated before "
                    "anything runs. Each op: {path, offset?, limit?}.",
     "inputSchema": {"type": "object",
                     "properties": {
                         "ops": {"type": "array",
                                 "items": {"type": "object",
                                           "properties": {
                                               "path": {"type": "string"},
                                               "offset": {"type": "integer"},
                                               "limit": {"type": "integer"}},
                                           "required": ["path"]}}},
                     "required": ["ops"]}},
    {"name": "fast_search",
     "description": "Content search via direct ripgrep transport (respects "
                    ".gitignore, real regex grammar). Falls back to a "
                    "pure-Python walk when rg is unavailable.",
     "inputSchema": {"type": "object",
                     "properties": {
                         "pattern": {"type": "string"},
                         "path": {"type": "string"},
                         "file_glob": {"type": "string"},
                         "case_sensitive": {"type": "boolean", "default": True},
                         "limit": {"type": "integer", "default": 100},
                         "offset": {"type": "integer", "default": 0}},
                     "required": ["pattern", "path"]}},
    {"name": "warm_exec",
     "description": "Run a shell command on ONE persistent bash: cwd, "
                    "exports and shell state survive across calls (unlike "
                    "per-call spawns). Same privileges as the harness "
                    "terminal. Timeout kills the whole command tree; "
                    "commands are never retried. reset=true restarts the "
                    "shell fresh.",
     "inputSchema": {"type": "object",
                     "properties": {
                         "command": {"type": "string"},
                         "cwd": {"type": "string"},
                         "timeout": {"type": "integer", "default": 120},
                         "reset": {"type": "boolean", "default": False}},
                     "required": ["command"]}},
    {"name": "batch_exec",
     "description": "Run 1-16 shell commands through ONE call on the warm "
                    "shell, sequentially — cd/export state flows between "
                    "commands. Per-command stdout+exit_code; a timed-out "
                    "command's tree is killed and later commands run on a "
                    "fresh shell (flagged). Prefer this over N separate "
                    "exec calls.",
     "inputSchema": {"type": "object",
                     "properties": {
                         "commands": {"type": "array",
                                      "items": {"type": "string"}},
                         "cwd": {"type": "string"},
                         "timeout": {"type": "integer", "default": 120}},
                     "required": ["commands"]}},
    {"name": "doctor",
     "description": "Report ToolRush lane status: versions, kill-switch "
                    "state, rg availability, warm-shell liveness, counters.",
     "inputSchema": {"type": "object", "properties": {}}},
]

_DISPATCH = {
    "fast_read": lambda a: fast_read(**a),
    "batch_read": lambda a: batch_read(**a),
    "fast_search": lambda a: fast_search(**a),
    "warm_exec": lambda a: warm_exec(**a),
    "batch_exec": lambda a: batch_exec(**a),
    "doctor": lambda a: doctor(),
}


def _reply(mid, result=None, error=None):
    msg = {"jsonrpc": "2.0", "id": mid}
    if error is not None:
        msg["error"] = error
    else:
        msg["result"] = result
    sys.stdout.write(json.dumps(msg, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def _env_is_error(text):
    """Derive MCP isError from the parsed tool envelope rather than a fixed
    flag: true when success==false or a non-empty "error" key exists.
    Envelopes without a success field and no error (none today) stay false."""
    try:
        env = json.loads(text)
    except (json.JSONDecodeError, TypeError, ValueError):
        return True
    if not isinstance(env, dict):
        return True
    return env.get("success") is False or bool(env.get("error"))


def serve():
    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            req = json.loads(raw)
        except json.JSONDecodeError:
            _reply(None, error={"code": -32700, "message": "parse error"})
            continue
        method = req.get("method", "")
        mid = req.get("id")
        if mid is None:  # notification (initialized, cancelled, ...) — no reply
            continue
        try:
            if method == "initialize":
                pv = (req.get("params") or {}).get("protocolVersion")
                _reply(mid, {
                    "protocolVersion": pv if isinstance(pv, str) else "2024-11-05",
                    "capabilities": {"tools": {"listChanged": False}},
                    "serverInfo": {"name": "toolrush", "version": VERSION}})
            elif method == "ping":
                _reply(mid, {})
            elif method == "tools/list":
                _reply(mid, {"tools": TOOLS})
            elif method == "tools/call":
                params = req.get("params") or {}
                name = params.get("name", "")
                args = params.get("arguments") or {}
                fn = _DISPATCH.get(name)
                if fn is None:
                    _reply(mid, error={"code": -32602,
                                       "message": f"unknown tool: {name}"})
                    continue
                try:
                    text = fn(args)
                    _reply(mid, {"content": [{"type": "text", "text": text}],
                                 "isError": _env_is_error(text)})
                except Exception as e:
                    _reply(mid, {"content": [{"type": "text",
                                              "text": _err(f"{type(e).__name__}: {e}")}],
                                 "isError": True})
            else:
                _reply(mid, error={"code": -32601,
                                   "message": f"method not found: {method}"})
        except Exception as e:
            _reply(mid, error={"code": -32603, "message": str(e)})


if __name__ == "__main__":
    _log(f"toolrush-mcp {VERSION} ready (pid {os.getpid()})")
    serve()
