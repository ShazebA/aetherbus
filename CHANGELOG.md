# Changelog

All notable changes land here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Project follows SemVer once we ship v1.0; until then, sections are organized by milestone (PRD §7.1).

Entries under `## [Unreleased]` describe work merged to `main` since the last milestone tag, or work-in-progress on an open PR. When a milestone PR merges, its Unreleased entries get promoted to a new milestone heading dated with the merge day.

---

## [Unreleased]

### Tooling
- `.gitignore` excludes a local `.mcp.json` — the convenient way to wire `aetherbus-mcp` into Claude Code, but the command path is currently absolute. Stays out of git until we ship the binary in a way that works from `PATH` on any box.

### M3b — `https://` + MCP server adapter (in flight on `feature/m3-resolvers-mcp`)

**Added**
- `https://` resolver — httpx-backed; optional `AETHERBUS_HTTPS_AUTH` bearer.
- `aetherbus.mcp_adapter.AetherBusMCPServer` — bridges bus topics to MCP resources at `aetherbus://<topic>`. On every event the server pushes `notifications/resources/updated`; clients pull via `resources/read` (poke-then-read, see ADR-004).
- `aetherbus-mcp serve --topic <topic>` CLI entry, wired through Typer. Logs to stderr (stdout is the MCP transport). Documented snippet for `claude_desktop_config.json` in the CLI help.
- ADR-004 — MCP poke-then-read semantics.
- 5 new unit tests for `https://` (success, 404, bearer header on/off, transport errors via `respx`); 3 for the MCP adapter (event → latest + notification, latched without session, broken-session safety).
- `respx` added to dev deps; `mcp>=1.0` moved into dev so the adapter tests run by default.

**Changed**
- `aetherbus.resolver.__init__` imports `https` alongside `file` so both built-ins register on package import.

**Implementation notes**
- The MCP SDK doesn't expose the active `ServerSession` outside a request context, so we replicate `mcp.server.lowlevel.Server.run` (~25 LOC) to capture it. Couples to a single-underscore SDK method (`Server._handle_message`); pinned to `mcp>=1.0`. Move to a public hook when the SDK offers one.

### M3a — Resolver SDK + `file://` (in flight on `feature/m3-resolvers-mcp`)

**Added**
- `aetherbus.resolver` package: `Resolver` Protocol, `register_resolver` decorator, `resolve_uri` with TTL cache + per-URI single-flight (concurrent resolves of the same URI share one fetch), `ResolverError` with actionable install hints for missing schemes.
- `file://` resolver — RFC 8089 (empty netloc or `localhost` only); off-loop reads via `asyncio.to_thread` so large payloads don't block the event loop.
- Third-party resolver discovery via the `aetherbus.resolvers` entry-point group; failures during load log a WARNING rather than failing import.
- Tests: 13 unit tests covering registry, cache, single-flight, error-not-cached, file roundtrip, and `Event.resolve()` dataref paths (inline JSON, model validation, octet-stream raw bytes).

**Changed**
- `Event.resolve()` now actually fetches `dataref` payloads via the resolver SDK instead of raising `NotImplementedError`. JSON content-types auto-parse; `as_=Model` validates via `model_validate_json`; other content-types return raw bytes.

**Removed**
- Dropped the `[project.entry-points."aetherbus.resolvers"]` block from `pyproject.toml` — those entries pointed at modules that didn't exist yet. Built-ins register via direct import; the entry-point group is for third-party plugins.

---

## M2 — Core — 2026-05-17 (merged via [PR #1](https://github.com/ShazebA/aetherbus/pull/1))

### Added
- Public API per PRD §6.2: `Bus.from_env` / `Bus.from_config`, `publish`, `subscribe` (decorator), `run`, `close`, `register_schema`, async context manager (`async with Bus.from_env() as bus`).
- `Event`, `EventPayload` (Pydantic v2 marker). `event.resolve()` returns inline data or validates against a model.
- `Envelope` — CloudEvents 1.0 + AetherBus extension attrs; enforces data XOR dataref and the 4 KB inline guard.
- `Transport` Protocol + `NATSTransport` (JetStream durable consumers, deliver_group queue sharing, `replay_from` policies). Kafka adapter plugs into the same Protocol in M4.
- SQLite outbox (stdlib `sqlite3` in a thread executor) with exponential backoff + jitter and an `on_failure` hook after `max_attempts`.
- Auto-derived event source `aetherbus://agent/${HOSTNAME}-${pid}` with `AETHERBUS_SOURCE` override.
- `docker-compose.yml` with healthchecked NATS for local dev.
- ADRs **002** (CloudEvents structured-JSON envelope), **003** (Pydantic v2), **005** (UUIDv7, inlined — no third-party dep).
- Test suite scaffolding: 25 unit tests at **84% coverage** on `src/aetherbus/`; 3 integration tests against testcontainers-NATS (set `AETHERBUS_TEST_NATS_URL` to reuse a running broker). **Note:** the integration tests as merged were buggy — see Unreleased Fixed entries.

### Changed
- README rewritten with the verified install + docker-compose + pytest flow and a one-shot publisher quickstart via `async with`.
- `bus.replay()` stubs to `NotImplementedError` pointing at M5.
- `event.resolve()` raises `NotImplementedError` for `dataref` URIs until M3's resolver SDK lands.

### Removed
- `uuid7` third-party dependency — UUIDv7 generation inlined in `envelope.new_id()` (~10 LOC).

---

## M1 — NATS + CloudEvents spike — 2026-05-17

**Added**
- `examples/spike.py` — async pub/sub roundtrip over local NATS JetStream with a CloudEvent envelope; asserts envelope `id` roundtrips end-to-end.
- ADR-001 — NATS JetStream as the default transport (vs. Kafka and Redis Streams).

---

## M0 — Scaffold & workflow — 2026-05-17

**Added**
- Repo scaffold per PRD §7.5: `pyproject.toml` (Python 3.11+, Pydantic v2, NATS/Kafka/MCP/replay/adapter extras), Apache 2.0 `LICENSE`, `.gitignore`, package skeleton, stub `README.md`.
- [PRD_AetherBus.md](PRD_AetherBus.md) committed as the canonical spec.
- [CLAUDE.md](CLAUDE.md) — working agreement for Claude collaborators (design principles, non-goals, code/test conventions, ADR plan).
- Branching workflow rule in `CLAUDE.md`: `main` protected, every feature on `feature/<slug>`, plan files at `~/.claude/plans/<slug>.md` (not committed).
- Private GitHub remote: [ShazebA/aetherbus](https://github.com/ShazebA/aetherbus).

**Changed**
- Claude attribution disabled in `~/.claude/settings.json` (`attribution.commit` and `attribution.pr` set to empty strings) — commits and PRs no longer carry the `Co-Authored-By: Claude` trailer.
