# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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

[1.0.1]: https://github.com/dhava-gautama/toolrush-mcp/compare/v1.0.0...v1.0.1
[1.0.0]: https://github.com/dhava-gautama/toolrush-mcp/releases/tag/v1.0.0
