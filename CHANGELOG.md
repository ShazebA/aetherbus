# Changelog

All notable changes land here. Format: [Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Project follows SemVer once we ship v1.0; until then, sections are organized by milestone (PRD §7.1).

Entries under `## [Unreleased]` describe work merged to `main` since the last milestone tag, or work-in-progress on an open PR. When a milestone PR merges, its Unreleased entries get promoted to a new milestone heading dated with the merge day.

---

## [Unreleased]

### Added
- `CHANGELOG.md` (this file), history backfilled.
- `CLAUDE.md` "Changelog upkeep" rule: every commit gets an Unreleased entry.

### Fixed
- Integration tests use one session-shared JetStream (`TEST_AETHERBUS`) + a per-test `topic_ns` fixture instead of per-test streams, avoiding `subjects overlap with an existing stream` errors from JetStream.
- Conftest sweeps any pre-existing stream whose subjects start with `test.` at session start, surviving leftover state from earlier runs (including from the initial buggy M2 conftest that leaked streams into JetStream's data dir).
- NATS transport queue groups: when `queue_group` is set, pass the topic-scoped durable as both the queue name and the consumer's `deliver_group` to satisfy nats-py 2.14's `queue == durable` invariant (`nats/js/client.py:376`). Previously the second subscriber in a queue group always errored on attach.

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
