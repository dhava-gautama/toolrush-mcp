# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.1.0]: https://github.com/dhava-gautama/toolrush-mcp/compare/v1.0.1...v1.1.0
[1.0.1]: https://github.com/dhava-gautama/toolrush-mcp/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/dhava-gautama/toolrush-mcp/releases/tag/v1.0.0
