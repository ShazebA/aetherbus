# CLAUDE.md — working agreement for AetherBus

This file orients Claude (and any AI collaborator) to the repo. The authoritative spec is **[PRD_AetherBus.md](PRD_AetherBus.md)** — when this file and the PRD disagree, the PRD wins. Update the PRD first, then sync the relevant section here.

## What this project is

AetherBus is a Python library + reference broker that gives AI agents a Kafka-style pub/sub layer where **events carry pointers to context, not the context itself**. CloudEvents 1.0 envelopes over NATS JetStream (default) or Kafka, with a `dataref` URI that resolvers fetch on demand (MCP, vector DB, Git, file, HTTPS).

It is **not** a new protocol, framework, or hosted service. It is a convention layer over CloudEvents + AsyncAPI + MCP, with the developer experience as the differentiator.

## Status and target

- Pre-alpha. No working code yet — only the PRD and this skeleton.
- Ship target: 8 weekends to v1 (see PRD §7.1).
- Single repo, single package: `aetherbus`. Optional extras: `kafka`, `mcp`, `replay`, `langgraph`, `crewai`, `openai`, `vec`, `git`.

## Design principles (PRD §3.3 — keep these in mind on every change)

1. **Standards first, conventions second, code third.** If a concept can't be expressed in CloudEvents or AsyncAPI, reject it or contribute upstream.
2. **Pointers by default, inline only as escape hatch.** Events <4 KB may inline; everything else is a `dataref` URI.
3. **The broker is dumb; the resolver is smart.** No business logic in the bus. All intelligence lives in producers, consumers, and resolvers.
4. **Replayability is a first-class feature.** Every event must be replayable from the moment it is published.
5. **The Python DX is the moat.** Decorator API, `Bus.from_env()`, `<20 lines` quickstart. We will not win on protocol rigor — we win by being the library a developer actually wants to use.
6. **Boring infrastructure.** NATS JetStream, Kafka, CloudEvents, Pydantic v2, FastAPI, Typer. No exotic deps.

## Non-goals (don't drift into these)

- No hosted SaaS in v1.
- No new wire protocol — we layer on CloudEvents + AsyncAPI.
- No new agent framework — we adapt to LangGraph, CrewAI, OpenAI Agents SDK.
- No JS/Go SDK in v1 (Python only).
- No end-to-end encryption / per-event RBAC in v1 (defer to A2A + SLIM in v2).

## Architecture at a glance

```
Producer ──▶ Broker (NATS JetStream | Kafka) ──▶ Subscriber
                       │
                       ├──▶ MCP Adapter ──▶ Claude Desktop / Cursor / Cline
                       │
                       └──▶ Replay UI (FastAPI + React)

Events carry { id, source, type, time, dataref } — Resolver SDK fetches
the actual payload from mcp:// vec:// git:// file:// https://
```

Component inventory and the canonical envelope spec live in PRD §5.

## Repo layout

```
src/aetherbus/
├── bus.py              # Bus class — the public API
├── event.py            # Event, EventPayload (Pydantic v2)
├── envelope.py         # CloudEvents serialization
├── transports/         # nats.py, kafka.py — both implement transports/base.py
├── resolver/           # mcp / vec / git / file / https — registered via entry points
├── mcp_adapter/        # `aetherbus-mcp serve` — bridges topics to MCP resources
├── replay_ui/          # backend (FastAPI) + frontend (Vite + React)
├── adapters/           # langgraph.py, crewai.py, openai_agents.py (<100 LOC each)
└── cli.py              # Typer — `aetherbus init/tail/publish/replay/...`

tests/{unit,integration,e2e}/
examples/{codegen_pipeline,research_pipeline,replay_demo}/
docs/architecture/adrs/
```

## Conventions

### Event envelopes
- CloudEvents 1.0. Required attrs: `id` (UUIDv7), `source` (`aetherbus://agent/<name>`), `specversion`, `type`, `time`, `datacontenttype`.
- AetherBus extension attrs (lowercase letters/digits only — CloudEvents rule): `dataref`, `aetherbusschema`, `aetherbusttl`, `aetherbusdependencies`, `aetherbuscorrelationid`, `aetherbussignature`.
- Either `data` (inline, <4 KB) or `dataref` (URI). Not both.

### Topic names
`<domain>.<entity>.<action>.<version>` → e.g. `dev.aetherbus.research.finding.v1`.
NATS wildcards: `*` (one token), `>` (rest). Kafka adapter translates to regex.

### Code style
- Python 3.11+. `ruff` for lint+format, `mypy --strict` for types.
- Pydantic v2 for all payloads. Not dataclasses, not msgspec (ecosystem alignment — see ADR-003).
- Async-first. `anyio` for primitives where it matters.
- Decorator-style public API: `@bus.subscribe("...")`, `await bus.publish(...)`.

### Testing strategy (PRD §7.3)
- **Unit:** pure Pydantic / envelope / resolver-fallback tests, no broker.
- **Integration:** `testcontainers` for NATS and Kafka. Same scenario suite runs against both — that's how we catch transport drift.
- **E2E:** dockerized 3-agent codegen pipeline (planner / impl / tester) that must exit 0 in CI.
- **MCP conformance:** drive the adapter with the official MCP Inspector; assert `notifications/resources/updated` within 500ms of publish.
- **Property tests:** Hypothesis for envelope round-trips and schema validation.
- Do **not** mock the broker in integration tests — use testcontainers. Mocked tests pass while real semantics drift.

### ADRs
Architecture decisions go in `docs/architecture/adrs/` as `NNNN-title.md`. Open ADRs to write during build (PRD §7.2): ADR-001 (NATS as default), ADR-002 (CloudEvents + dataref), ADR-003 (Pydantic v2), ADR-004 (MCP poke-then-read semantics), ADR-005 (UUIDv7), ADR-006 (no JS SDK in v1), ADR-007 (no E2E encryption in v1).

### Commits / releases
- Conventional commits.
- SemVer. v0.x while the API churns; v1.0.0 gated on a stable MCP adapter.
- Releases via tag → GitHub Action → PyPI.

## Working with Claude on this repo

- **Read the PRD first** for any non-trivial task. Section 5 (Architecture) and 6 (API) are the most load-bearing.
- **When you add a feature, check it against §3.2 non-goals.** It's easy to drift into building a framework. Don't.
- **The Replay UI is the portfolio money shot (PRD §4.2 UC6, §7.1 M5).** If the schedule slips, cut Kafka before cutting the Replay UI.
- **MCP adapter is the strategic wedge.** Anything that improves it has outsized value vs. its LOC.
- **Don't invent new envelope fields.** Use CloudEvents extension attrs (lowercase letters/digits only) or `headers`.
- **Don't add a layer above NATS/Kafka ACLs.** Broker-native auth only in v1 (PRD §5.8).

## Quick refs

- Full spec: [PRD_AetherBus.md](PRD_AetherBus.md)
- Milestones: PRD §7.1
- Open questions to resolve in M1/M2: PRD §9
- Risks: PRD §8
- Launch plan: PRD §10
