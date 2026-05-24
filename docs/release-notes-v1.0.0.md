# AetherBus v1.0.0 — Release notes

*Use these notes for the `gh release create` body.*

---

## v1.0.0 — Public launch

The first public release of AetherBus — an event-driven context layer for AI agents. CloudEvents 1.0 envelopes over NATS JetStream (default) or Kafka, with `dataref` URIs that resolve content on demand. Includes an MCP server adapter so Claude Code / Claude Desktop / Cursor / Cline consume bus streams natively, and a Replay UI for fork-from-here debugging.

### Highlights

- **MCP `resources/subscribe` actually works.** `aetherbus-mcp serve` is a stdio MCP server that pushes `notifications/resources/updated` on every bus event. The primitive the spec defined but the ecosystem skipped.
- **Pointers, not payloads.** Events carry `dataref` URIs (`mcp://`, `vec://`, `git://`, `file://`, `https://`); subscribers resolve on demand. TTL cache + per-URI single-flight built in.
- **Replay UI.** FastAPI + React SPA. Watch events flow live, click for the envelope, sandbox-replay history with original event IDs preserved.
- **Two transports, one API.** NATS JetStream as default; Kafka adapter ships in v1. Conformance suite parametrized over both — semantic drift caught at CI.
- **Framework adapters.** Drop into LangGraph, CrewAI Flows, or the OpenAI Agents SDK in <30 lines. Each adapter is under 100 LOC.

### Surface

| | |
|---|---|
| Python | 3.11+ |
| Transports | NATS JetStream (default), Kafka via `aiokafka` |
| Envelope | CloudEvents 1.0 structured-JSON, AetherBus extension attrs |
| Resolvers | `file`, `https`, `mcp`, `git` (built-in); entry-point plugin group for third-party |
| MCP | Server adapter (stdio); `mcp://` client resolver |
| Replay | `bus.replay()` async generator + FastAPI/React UI |
| Tests | 95 unit tests, integration suite parametrized across NATS + Kafka |
| License | Apache 2.0 |

### Installing

```bash
pip install aetherbus                   # core
pip install 'aetherbus[mcp]'           # MCP adapter + mcp:// resolver
pip install 'aetherbus[kafka]'         # Kafka transport
pip install 'aetherbus[langgraph]'     # LangGraph adapter
pip install 'aetherbus[crewai]'        # CrewAI adapter
pip install 'aetherbus[openai]'        # OpenAI Agents SDK adapter
pip install 'aetherbus[replay]'        # Replay UI server
pip install 'aetherbus[all]'           # everything
```

### Three-line quickstart

```bash
docker compose up -d nats
pip install aetherbus
python -c "import asyncio; from aetherbus import Bus, EventPayload; \
asyncio.run((lambda: None)())"  # see README for the actual sub/pub examples
```

(Real quickstart in the [README](https://github.com/ShazebA/aetherbus#quickstart) and at [aetherbus.dev/quickstart](https://aetherbus.dev/quickstart/).)

### Notable design decisions

- **NATS JetStream as default** — single binary, native multi-tenancy, queue groups. ADR-001.
- **CloudEvents structured-JSON envelope** — one serialization across transports. ADR-002.
- **Pydantic v2 for typed payloads** — ecosystem alignment with LangChain / FastAPI. ADR-003.
- **MCP poke-then-read, not push** — spec-aligned; client controls the read cadence. ADR-004.
- **UUIDv7 event IDs** — time-sortable, no third-party dep. ADR-005.

### What's NOT in v1.0.0

- Hosted SaaS (PRD non-goal N1).
- E2E encryption / per-event RBAC (defer to A2A identity + SLIM transport in v2).
- JavaScript / Go SDKs (PRD non-goal N6).
- AsyncAPI schema strict-mode validation (warn-mode only in v1; strict in v2).
- DLQ topic convention (broker-level controls only for now).

### Acknowledgments

This was built solo over eight weekends. Most of the design ideas trace to:

- Anthropic's June 2025 [multi-agent research system](https://www.anthropic.com/news/multi-agent-research-system) post for the claim-check / dataref pattern.
- Cognition's [Don't Build Multi-Agents](https://cognition.ai/blog/dont-build-multi-agents) for the shared-traces argument.
- The MCP spec for the resources primitive nobody else implemented.
- Microsoft's [enterprise integration patterns](https://www.enterpriseintegrationpatterns.com/) reference for the original claim-check shape.

Issues and PRs welcome. The PRD lays out the v2 roadmap.
