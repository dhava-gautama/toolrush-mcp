# Security Policy

## Scope and threat model

toolrush-mcp is a **local stdio MCP server**. It is spawned by your agent
harness (Kimi Code CLI, Claude Code, Codex CLI, ...) as the same user, talks
JSON-RPC over stdin/stdout, and terminates with the harness. Concretely:

- **Same-user, harness-gated.** It runs with exactly the privileges of the
  user who launched the harness — no privilege boundary is created or
  claimed. Dangerous operations are meant to be gated by your harness's MCP
  approval rules, not by this server.
- **Read-only lanes by design.** `fast_read`, `batch_read`, and `fast_search`
  open files read-only. There is deliberately **no write tool**; writes
  belong behind your harness's own permission UI.
- **`warm_exec` is the sharp edge.** It holds a persistent bash with
  terminal-level privileges (the same ones your harness's shell tool has).
  Gate it with your harness's MCP approval rules for shell execution. It
  never retries a submitted command and kills its process group on timeout.
- **No network.** The server opens no sockets. The only subprocesses it
  launches are `rg` (search lane) and bash (exec lanes).
- **No persistence.** No state is written anywhere; the only caches live in
  process memory and die with the server.
- **Kill-switches are fail-closed.** `TOOLRUSH_FASTLANE=0`,
  `TOOLRUSH_SEARCH=0`, `TOOLRUSH_PERSIST=0`, `TOOLRUSH_PARALLEL=0` (set in
  the server's environment) disable the corresponding lanes with a clear
  error, so the agent falls back to its native tools.
- **stdout is protocol-only.** Diagnostics go to stderr so agent loops can
  never parse accidental output as tool results.

Not in scope: hardening against a malicious local user with your own
privileges, or against a harness that executes tools without approval. If
those are adversarial in your setup, no MCP server of this shape is the
right mitigation.

## Reporting a vulnerability

Use **GitHub Security Advisories** (private disclosure — no public issue,
no IRC):

1. Open https://github.com/dhava-gautama/toolrush-mcp/security/advisories/new
2. Fill in the advisory; include the tool lane, reproduction, and harness
   version if relevant.

If GitHub's private vulnerability reporting is not yet enabled on the repo,
the maintainer enables it under **Settings → Code security & analysis →
Private vulnerability reporting**; advisories can then be drafted against
the repo as above.

Please do not open public issues for anything exploitable.

## Supported versions

The **latest release only**. Older tags do not receive security patches —
upgrade to the newest `vX.Y.Z` release (or `go install ...@latest`) before
reporting.
