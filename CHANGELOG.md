# Changelog

All notable changes land here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Project follows SemVer once we ship v1.0; until then, sections are organized by milestone (PRD §7.1).

Entries under `## [Unreleased]` describe work merged to `main` since the last milestone tag, or work-in-progress on an open PR. When a milestone PR merges, its Unreleased entries get promoted to a new milestone heading dated with the merge day.

---

## [Unreleased]

(no entries yet — next milestone is M4 Kafka transport per PRD §7.1)

---

## M3 — Resolvers + MCP adapter — 2026-05-17

Merged in three slices: M3a + M3b via [PR #3](https://github.com/ShazebA/aetherbus/pull/3) (`1147df8`), M3c on a follow-up PR.

### Added

**Resolver SDK and built-ins**
- `aetherbus.resolver` package — `Resolver` Protocol, `register_resolver` decorator, `resolve_uri` with TTL cache + per-URI single-flight (concurrent resolves of the same URI share one fetch). `ResolverError` carries actionable install hints for missing schemes.
- `file://` resolver — RFC 8089 (empty netloc or `localhost`); off-loop reads via `asyncio.to_thread`.
- `https://` resolver — httpx-backed; optional `AETHERBUS_HTTPS_AUTH` bearer.
- `mcp://` client resolver — reads MCP resources from a configured server. `register_mcp_server(name, "stdio:///cmd ..." | "https://.../sse" | argv)` populates the per-server registry; sessions open lazily and live for the process. Self-referential round-trip works: events published to the bus can carry `dataref="mcp://aetherbus/aetherbus://<topic>"` and resolve through our own MCP server adapter.
- `git://` resolver — `git://<owner>/<repo>[@<sha>]:<path>`. Bare clones cached under `~/.cache/aetherbus/git` (override via `AETHERBUS_GIT_CACHE`); per-repo origin URL override via `AETHERBUS_GIT_URL_<OWNER>_<REPO>` for tests / self-hosted forges. Subprocess `git`, no pygit2 dep.
- Third-party resolver discovery via the `aetherbus.resolvers` entry-point group; failures log a WARNING rather than failing package import.

**MCP server adapter**
- `aetherbus.mcp_adapter.AetherBusMCPServer` — bridges bus topics to MCP resources at `aetherbus://<topic>`. On every event the server emits `notifications/resources/updated`; clients pull via `resources/read` (**poke-then-read**, see ADR-004).
- `aetherbus-mcp serve --topic <topic>` CLI (Typer). Logs to stderr (stdout is the MCP transport). Docstring carries the Claude Desktop config snippet. `aetherbus-mcp version` subcommand prevents Typer from collapsing the single command.
- `.mcp.json` for project-level Claude Code integration (gitignored — command path is currently absolute).

**Event wiring**
- `Event.resolve()` now actually fetches `dataref` payloads via the resolver SDK instead of raising `NotImplementedError`. JSON content-types auto-parse; `as_=Model` validates via `model_validate_json`; other content-types return raw bytes.

**Examples and docs**
- `examples/poke.py` — one-event publisher used to drive the MCP wedge demo.
- README's top section now leads with the wedge demo (transcript of Claude reading a bus event as an MCP resource).
- **ADR-004** — MCP poke-then-read semantics (spec alignment, client-controlled rate, pointer-by-default keeps the MCP transport thin).

**Tests**
- 42 new unit tests across the slice (resolver registry, cache, single-flight, error-not-cached, file/https/git/mcp paths, `Event.resolve()` dataref handling, MCP adapter wiring including broken-session safety). Total at end of M3: **67 unit tests, all green**, ruff clean.

### Changed
- `bus.replay()` still stubs to `NotImplementedError` pointing at M5 (unchanged from M2; called out because M3 didn't ship it either).

### Removed
- Dropped the stale `[project.entry-points."aetherbus.resolvers"]` block from `pyproject.toml` — those entries referenced modules that didn't exist yet. Built-ins register via direct import; the entry-point group is for third-party plugins.

### Fixed
- `aetherbus-mcp` Typer app gained a `version` subcommand so `serve` stays a subcommand (single-command Typer apps collapse to root invocation otherwise).

### Implementation notes
- The MCP SDK doesn't expose the active `ServerSession` outside a request context, so the adapter mirrors `mcp.server.lowlevel.Server.run` (~25 LOC) to capture it. Couples to a single-underscore SDK method (`Server._handle_message`); pinned to `mcp>=1.0`. Move to a public hook the moment the SDK offers one.

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
