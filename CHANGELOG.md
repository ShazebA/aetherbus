# Changelog

All notable changes land here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Project follows SemVer once we ship v1.0; until then, sections are organized by milestone (PRD §7.1).

Entries under `## [Unreleased]` describe work merged to `main` since the last milestone tag, or work-in-progress on an open PR. When a milestone PR merges, its Unreleased entries get promoted to a new milestone heading dated with the merge day.

---

## [Unreleased]

(no entries yet — next is M6 framework adapters per PRD §7.1)

---

## M5 — Replay UI — 2026-05-20

Merged across three slices: **M5a** ([PR #6](https://github.com/ShazebA/aetherbus/pull/6) — backend + `bus.replay()`), **M5b** ([PR #7](https://github.com/ShazebA/aetherbus/pull/7) — React SPA + Kafka history), **M5c** (this PR — sandbox replay execution + `depends_on` UI + wheel bundling).

### Added

**`bus.replay()` and `Transport.history()`**
- `bus.replay(topic, since=, until=, into=, idle_timeout=)` — async generator over stored events. Pass `into=other_bus` to re-emit raw envelope bytes onto a sandbox bus (ids, timestamps, correlation ids preserved). Replaces the M2 `NotImplementedError` stub.
- `Transport.history()` Protocol method. NATS uses an ephemeral pull consumer with `DeliverPolicy.BY_START_TIME` / `ALL`; Kafka uses `offsets_for_times` + per-partition seek + end-of-snapshot detection via `end_offsets`. Reads are non-destructive on both transports.

**Backend (`aetherbus.replay_ui.backend`)**
- FastAPI app with `/`, `/api/streams`, `/api/streams/{name}/events`, `/api/events/{id}`, `/api/streams/{name}/tail` (WebSocket), `/api/replay`.
- `/api/replay` does both dry-run (returns IDs that would replay) and real sandbox-replay (re-emits raw envelopes into the sandbox bus, ids preserved). Sandbox setup failures wrap as 502.
- `aetherbus-ui serve [--port 7798] [--bus URL] [--config PATH] [--host 127.0.0.1]` CLI (Typer). Loopback-only by default per PRD §6.7.
- Backend probes `frontend/dist/` at startup: serves the React SPA when present, falls back to a single Jinja viewer otherwise. Either way the API surface is identical.

**Frontend (React SPA)**
- `src/aetherbus/replay_ui/frontend/` — Vite + React 18 + TypeScript + Tailwind + TanStack Query.
- Hand-rolled UI components (button, badge, input, dialog) styled in the same dark palette. Skipped the shadcn CLI / Radix / cva path to keep the dep tree small.
- Live tail via WebSocket — header status pill reflects connection state; new events prepend (capped at 200).
- Sandbox-URL input next to the Replay button — blank = dry run, populated = real re-emit into that bus. Result panel shows count + mode.
- Event detail dialog renders `depends_on` as clickable chips. Clicking pushes the current event onto a breadcrumb stack and jumps to the ancestor; back arrow pops. Full DAG layout deferred to a polish PR.
- Vite dev proxy forwards `/api` and the WebSocket so `npm run dev` hot-reloads against a running backend.

**Packaging**
- Wheel bundles the built SPA. `pyproject.toml` `[tool.hatch.build.targets.wheel.force-include]` adds `frontend/dist/` to the wheel when it exists; an `exclude` block strips the frontend source so the wheel stays small (~112 KB).
- `scripts/build-wheel.sh` — chains `npm install` + `npm run build` + `hatch build` for the release flow.

**Tests**
- 4 new unit tests for `bus.replay()` (in-order, sandbox re-publish preserves bytes, sandbox failure → outbox, caller-break).
- 8 new FastAPI tests via `TestClient` (HTML route, streams list, history paging + limit, event-by-id success + 404, replay dry-run, replay sandbox-failure → 502).
- New conformance test `test_bus_replay_iterates_history` parametrized over both transports — integration suite now 8 tests (4 scenarios × 2 transports).

### Changed
- `fastapi`, `uvicorn`, `websockets`, `jinja2` promoted to `dev` extras so the replay-ui tests run by default.

### Implementation notes
- `replay(into=)` calls the sandbox transport's `publish()` with the original bytes so re-emitted events keep their original ids and timestamps — the audit trail survives a fork. Sandbox transport failures route through the sandbox bus's outbox, mirroring the live-publish path.
- Kafka `history()` opens a fresh consumer group on every call so reads are isolated from production consumers and don't disturb commit cursors.

---

## M4 — Kafka transport — 2026-05-17 (merged via [PR #5](https://github.com/ShazebA/aetherbus/pull/5))

**Added**
- `aetherbus.transports.kafka.KafkaTransport` — `aiokafka`-backed implementation of the `Transport` Protocol. Selected when `settings.transport_type == "kafka"`. Semantic translations: topic name → Kafka topic (pass-through), `queue_group` → consumer `group_id`, `replay_from="earliest"|"latest"` → `auto_offset_reset`, `replay_from=datetime` → `offsets_for_times` + `seek`, `ack` → `commit({tp: offset+1})`, `nack` → `seek(tp, offset)`. `max_in_flight` enforced via an `asyncio.Semaphore` around dispatch (no native aiokafka knob).
- `_build_transport(settings)` in `bus.py` — instantiates NATS or Kafka based on `settings.transport_type`. ImportError on Kafka surfaces the right `pip install 'aetherbus[kafka]'` hint.
- `kafka` service in `docker-compose.yml` (Bitnami 3.7, KRaft mode — no ZooKeeper). `KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=true` so unit-scale tests don't need explicit topic creation.
- Integration suite parametrized over `["nats", "kafka"]` via a new `transport` fixture. `tests/integration/test_nats.py` renamed to `test_conformance.py`; the same three scenarios (roundtrip, queue-group exactly-one, replay-from-earliest) now run against both transports — 6 integration tests total. Kafka branch skips when `AETHERBUS_TEST_KAFKA_URL` isn't set so the NATS-only flow stays green for contributors without a Kafka container.
- 5 new unit tests for transport selection + Kafka helpers (`_has_wildcard`, `_normalize_url`).

**Changed**
- `aiokafka>=0.10` promoted to `dev` extras alongside `[kafka]` so the conformance tests run by default.
- README explains how to switch transports without touching agent code; updated docker-compose section covers both brokers.

**Implementation notes**
- `ensure_stream` is a no-op for wildcard subjects on Kafka (Kafka has no multi-subject container). Concrete topics get explicit `create_topics` with tolerance for `TopicAlreadyExistsError`; the rest rely on broker auto-create.
- `nack` semantics on Kafka are `seek(tp, msg.offset)` — the next poll re-fetches from the same offset. Buffered records on other partitions aren't affected.

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
