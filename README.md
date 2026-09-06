# ToolRush-MCP

[![ci](https://github.com/dhava-gautama/toolrush-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/dhava-gautama/toolrush-mcp/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/dhava-gautama/toolrush-mcp)](https://github.com/dhava-gautama/toolrush-mcp/releases/latest)
[![Go Report Card](https://goreportcard.com/badge/github.com/dhava-gautama/toolrush-mcp)](https://goreportcard.com/report/github.com/dhava-gautama/toolrush-mcp)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

**Kill the tool-call tax — in any agent harness.** A fast local tool server
for [Kimi Code CLI](https://www.kimi.com/code), [Codex CLI](https://github.com/openai/codex),
[Claude Code](https://docs.anthropic.com/en/docs/claude-code), and anything
else that speaks the Model Context Protocol.

A universal port of [ToolRush](https://github.com/OnlyTerp/toolrush) (a
Hermes Agent plugin, Windows/MSYS-only) — same lanes, same fail-closed
philosophy, shipped as one static Go binary instead of harness-specific
monkeypatching.

## What you get

| Tool | What it does |
|---|---|
| `fast_read` | In-process file read: line-number gutter, offset/limit paging, negative offset = tail, binary sniff, BOM strip, >64MB files stream the window. Line-level mtime cache — page turns are cache hits |
| `batch_read` | 1–16 reads through **one** tool call, input order, whole-batch validated before anything runs (serial by design — measured faster than pooling on page-cache-fast storage) |
| `fast_search` | Direct `rg` transport — real `.gitignore`, real regex grammar. Pure-Go walk fallback when rg is absent. `context=N` (0–5) adds N lines of gutter around each hit: `L-line` before, `L+line` after |
| `batch_search` | 1–16 searches through **one** call, fanned out across goroutines — each op spawns its own rg process, so the batch wall time approaches the slowest op, not the sum. Input order, whole-batch validated, per-op failures isolated |
| `fast_tree` | Budgeted directory listing: deterministic dirs-first alphabetical order, depth (1–10) and entry (1–5000) budgets, basename pattern filter. Skips `.git`, `node_modules`, `__pycache__`, `.venv`, `target`, `dist`, `build`, `*_cache`. No `.gitignore` parsing — use `pattern=` to narrow |
| `warm_exec` | **One persistent bash**: `cd`, `export`, and shell state survive across calls (harness terminals spawn fresh shells per call). Per-call framing, rc + cwd read back from the shell, process-group kill on timeout, never retries a submitted command |
| `batch_exec` | 1–16 shell commands through **one** call on the warm shell, sequentially, state flowing between them |
| `doctor` | Lane status, versions, kill-switch state, counters |

Why it matters: every MCP tool call pays a fixed transport + dispatch cost.
The structural wins are **persistent shell state** (impossible with per-call
spawns) and **batching** (one round trip instead of sixteen) — plus the
server itself is fast enough to never be the bottleneck.

## Measured performance

From `bench/bench_go.py` (Linux, i7-8750H, medians; your numbers will vary):

| Metric | Python impl | Go impl | C ping ref |
|---|---:|---:|---:|
| startup → first reply | 60.3 ms | **2.5 ms** | 0.9 ms |
| ping round trip | 0.034 ms | 0.035 ms | 0.019 ms |
| cold `batch_read`, 16×1000-line files | 31.3 ms | 22.9 ms | — |
| `fast_read` **during** a 2s `warm_exec` | 2002.4 ms | **0.37 ms** | — |
| 6 searches: `batch_search` (1 call) vs 6× `fast_search` | 2899.9 ms | **2574.6 ms** vs 2714.3 ms | — |

The last row is 6 full-tree `rg` scans over `/usr/share/doc` (424 MB, median
of 10, this i7-8750H + HDD box). The Go server fans the ops out across
goroutines but wins only ~1.05x here: the scans are disk-bound, and six
concurrent rg processes saturate the same spindle (raw concurrent rg outside
the server measures the same 1.05x). On page-cache-hot trees or
CPU-bound regexes the parallelism shows up for real; on cold HDD scans,
honestly, it mostly saves the five extra RPC round trips. The Python
reference runs its ops sequentially (GIL + subprocess dispatch overhead)
and, as expected, ties its own 6-call loop.

The `fast_read`-during-`warm_exec` row is the architectural one: per-request goroutines mean a long
shell command never head-of-line blocks the other tools. Per-call latency is
kernel/transport-bound and deliberately ties — there is nothing left to win
there in any language.

## Install

**Go (recommended):**

```
go install github.com/dhava-gautama/toolrush-mcp@latest
# binary lands in $(go env GOPATH)/bin/toolrush-mcp
```

**From source:**

```
git clone https://github.com/dhava-gautama/toolrush-mcp.git
cd toolrush-mcp
CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o toolrush-go .
```

Requirements: `bash`; `rg` on PATH is optional (the search lane falls back
to a built-in walker without it). POSIX only (Linux/macOS).

## Register with your harness

Use the binary path from the install step above (`$HOME/go/bin/toolrush-mcp`
for `go install`, `./toolrush-go` for source builds).

**Kimi Code CLI** — `~/.kimi-code/mcp.json` (new sessions pick it up; `/mcp` to verify):

```json
{
  "mcpServers": {
    "toolrush": {
      "command": "/home/YOU/go/bin/toolrush-mcp",
      "args": [],
      "toolTimeoutMs": 120000
    }
  }
}
```

**Codex CLI:**

```
codex mcp add toolrush -- /home/YOU/go/bin/toolrush-mcp
```

**Claude Code:**

```
claude mcp add toolrush -s user -- /home/YOU/go/bin/toolrush-mcp
```

**Any other MCP harness:** stdio server, command = the binary, no args.
Newline-delimited JSON-RPC: `initialize` / `tools/list` / `tools/call` /
`ping`.

## Kill-switches (fail-closed)

Set any of these to `0` in the server's environment to disable a lane — a
disabled lane errors clearly so the agent falls back to its native tools
(`TOOLRUSH_PERSIST=0` runs spawn-per-call instead, as a negative control):

```
TOOLRUSH_FASTLANE=0  TOOLRUSH_SEARCH=0  TOOLRUSH_PERSIST=0  TOOLRUSH_PARALLEL=0
```

## Safety model

Read/search lanes are read-only. There is deliberately **no write tool** —
writes belong behind your harness's own permission UI. `warm_exec` carries
the same privileges as the harness's terminal tool; gate it with your
harness's MCP approval rules. No network access, no persistence, stdout
carries protocol frames only (diagnostics on stderr).

## Verify / develop

```
python3 python/smoke.py                                   # 53 checks vs Python impl
TOOLRUSH_SERVER=./toolrush-go python3 python/smoke.py     # same 53 vs Go binary
python3 bench/bench_go.py                                 # Python vs Go vs C shootout
gcc -O2 -o bench/ping_ref bench/ping_ref.c                # C floor reference
python3 bench/bench_lang.py                               # transport floor
```

## Layout

| Path | What |
|---|---|
| `main.go` | the Go server (the shipped implementation) |
| `python/` | Python reference implementation (`toolrush_mcp.py`), the smoke suite, lane bench |
| `bench/` | cross-language benchmarks + pure-C transport-floor reference |

## Changelog

See [CHANGELOG.md](CHANGELOG.md).

## Origin and license

Port of [ToolRush](https://github.com/OnlyTerp/toolrush) by OnlyTerp —
the Hermes-side design, kill-switch discipline, and "measured evidence or it
didn't happen" methodology are theirs; the MCP universalization and Go
implementation are here. Dropped in the port: Hermes byte-parity quirks and
the update-survival machinery (no upstream harness to survive here).
MIT — see [LICENSE](LICENSE).
