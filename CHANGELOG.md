# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.2] - 2026-09-07

### Fixed

- The rg search lane now parses `--json` events instead of scraping text
  lines — no more phantom hits from context rows containing `:N:`, gutters
  fabricated from `-N-` inside file content, or hits relocated by a `:N:` in
  the path; CRLF content is stripped
- The `--json` message parser no longer reuses its decode struct across
  lines (found by fuzzing): a sparse/truncated message could inherit the
  previous message's fields and inflate `total_hits`
- `render` defensively clamps offset ≥ 1 (found by fuzzing; callers already
  clamped, but the function itself no longer panics on offset 0)
- `fast_search`/`batch_search` envelopes gain `capped` (true when rg output
  hit the 8MB cap, in which case `total_hits` is a lower bound) — the cap
  was previously silent
- `batch_read`: an op with `offset=0` no longer panics (clamped to 1), and
  each op's `limit` is clamped to 1..1000 like `fast_read`
- `warm_exec`: the frame write to the shell is timeout-bound — a SIGSTOPped
  shell with a >64KB frame in flight previously wedged the whole server,
  including `doctor`
- `warm_exec`: the begin marker is newline-guarded (stray background output
  can no longer glue onto it), `cwd` rejects newline characters (a
  wrong-directory execution fix), pre-write drain detects a dead shell and
  respawns instead of misreporting, failed spawns no longer leak file
  descriptors, and the reader caps line materialization at 1MB
- `batch_exec`: the `cwd` argument applies to the first command only,
  matching the documented state-flow semantics
- `doctor` no longer blocks behind a running `warm_exec`
- `fast_tree`: invalid glob patterns now error (they were silently matching
  nothing), `truncated` is set only when entries were actually omitted, and
  entries gain `is_symlink` with lstat semantics — dangling links included
- Python reference: line splitting aligned to `\n` with CRLF folded (was
  `str.splitlines`), the page budget is byte-counted, and the same
  `warm_exec` hardening applies
- Walk-lane `file_glob`: invalid patterns now error instead of silently
  matching nothing

## [1.1.0] - 2026-09-07

### Added

- `batch_search`: 1–16 content searches through one call, fanned out across
  goroutines in the Go server (each op spawns its own rg; the Python
  reference runs sequentially by design). Input order preserved, whole
  batch validated before anything runs (`op N invalid: ...`), per-op
  failures isolated, results raw-embedded like `batch_read`
- `context` param on `fast_search` (and `batch_search` ops): 0–5 lines of
  gutter around each hit (`L-line` before, `L+line` after), implemented for
  both the rg (`-C`) and walk engines; default 0 keeps envelopes
  byte-identical
- `fast_tree`: budgeted directory listing — deterministic dirs-first
  alphabetical order, depth (1–10) and entry (1–5000) budgets, basename
  pattern filter, shared skip list (`.git`, `node_modules`, `__pycache__`,
  `.venv`, `target`, `dist`, `build`, `*_cache`); no `.gitignore` parsing
- `batch_search` and `fast_tree` counters in `doctor` stats

### Fixed

- Single-file `fast_search` returned `total_hits` but no hits: rg omits the
  path prefix for single-file searches unless `--with-filename` is passed
- Python walk fallback now searches a single-file root (parity with the Go
  `WalkDir` engine)

## [1.0.1] - 2026-09-07

### Fixed

- `streamRead` no longer scans past its window on >64MB files
- Unbounded walk fallback is now capped (16MB/file, 60s deadline)
- Zero-size `/proc` files are no longer misreported as empty
- `isError` is now set on tool-call failures
- `fast_read` no longer panics on `offset=0`
- Silent 8MB output truncation is now flagged as `truncated:true`
- `rg` colon-containing paths are no longer misparsed
- Zombie shells and leaked file descriptors on shell exit are cleaned up
- Read cache now skips files larger than 4MB

## [1.0.0] - 2026-09-06

### Added

- Initial release: 6 tools (`fast_read`, `batch_read`, `fast_search`,
  `warm_exec`, `batch_exec`, `doctor`), Go implementation (stdlib only) and
  Python reference implementation, cross-language benchmarks.

[1.1.2]: https://github.com/dhava-gautama/toolrush-mcp/compare/v1.1.0...v1.1.2
[1.1.0]: https://github.com/dhava-gautama/toolrush-mcp/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/dhava-gautama/toolrush-mcp/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/dhava-gautama/toolrush-mcp/releases/tag/v1.0.0
