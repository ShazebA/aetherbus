# AetherBus — Product Requirements Document

**Project:** AetherBus — an event-driven context layer for AI agents
**Version:** 1.0 (v1 spec, ambitious scope)
**Author:** Shazeb (with Claude)
**Status:** Draft for implementation
**Target ship:** 6–8 weekends of focused work
**Repo target:** `github.com/shazeb/aetherbus`
**License:** Apache 2.0

---

## Part 1 — Executive Summary

### What it is

AetherBus is a Python library and reference broker that gives AI agents a Kafka-style pub/sub layer where **events carry pointers to context, not the context itself**. Agents subscribe to topics, receive lightweight CloudEvents with a `dataref` URI, and resolve the full payload on demand from MCP resources, vector databases, Git, S3, or local files.

It is **not** a new agent framework, and it is **not** a competitor to MCP or A2A. It is the missing async, many-to-many, replayable layer between them — implemented in a way that any MCP-capable client (Claude Code, Cursor, Goose, Cline) can consume out of the box.

### Why now

Three independent forces converge in 2025–2026 and leave a precise gap:

1. **Multi-agent systems waste tokens at predictable, well-documented rates.** Anthropic's June 2025 engineering post benchmarks multi-agent systems at ~15× the tokens of a single chat. CrewAI's verbose role/goal/backstory pattern adds ~18% overhead vs. LangGraph on identical workloads. GitHub's official MCP server uses 17,600 tokens of tool definitions per request before the agent does any work.
2. **The pointer pattern has already won.** Anthropic's research-agent post: subagents *"write to a shared filesystem and return a lightweight reference. The lead agent does not re-read every detail; it gets a pointer and pulls what it needs."* Cognition's "Don't Build Multi-Agents" argues for shared traces. Microsoft's reference architecture mandates message brokers with correlation IDs. Everyone is doing claim-check by hand; nobody has shipped the library.
3. **MCP has the primitive but nobody uses it.** The MCP spec defines `resources/subscribe` and `notifications/resources/updated` — but they are marked optional and, per the open MCP discussion #301, neither Claude Desktop nor FastMCP implements them. This is the wedge.

### What v1 ships

A single `pip install aetherbus` that gives a Python developer:

- A **decorator-style pub/sub API** (`@bus.subscribe("research.findings.*")`) over **NATS JetStream** as the default broker, with a **Kafka adapter** for users on Confluent.
- **CloudEvents 1.0 envelopes** with a `dataref` URI, signed by the producer agent, with explicit `dependencies` for provenance.
- A **resolver SDK** that fetches the actual context from `mcp://`, `vec://`, `git://`, `file://`, and `https://` URIs, with TTL caching.
- An **MCP server adapter** that exposes any bus topic as an MCP resource, making AetherBus consumable from Claude Code, Cursor, etc. without any AetherBus-specific client code.
- A **replay UI** (single-page web app) that lets you scrub through an event stream, inspect resolved context, and re-fire events into a sandbox runtime — the killer demo for the portfolio narrative.
- **Reference adapters** for LangGraph, CrewAI Flows, and OpenAI Agents SDK so the bus drops into any of the three dominant agent frameworks in <30 lines.

### What it explicitly is not

- Not a new protocol. It is a **convention layer** over CloudEvents + AsyncAPI + MCP.
- Not a managed service. Self-hosted only in v1; SaaS is out of scope.
- Not a security/identity system. It defers identity to A2A Agent Cards in v2.
- Not a replacement for AGNTCY/SLIM. AetherBus targets developer-tooling DX; AGNTCY targets enterprise/internet-scale agent routing. The two can interoperate, and SLIM is a planned future transport.

### Success criteria

A v1 ship is successful if **two of three** are true 90 days after public launch:

1. ≥50 GitHub stars and ≥3 external contributors with merged PRs.
2. ≥1 external integration (someone other than the author uses AetherBus in a real agent project, documented in a public repo or blog post).
3. Featured in one community curation: Awesome MCP, LangChain blog, Anthropic cookbook, Hacker News front page, or equivalent.

If none are true at 90 days, the project pivots to **opinionated personal dev tooling** (a portfolio piece showing the architecture, not a general-purpose library).

### Strategic positioning for the portfolio

This is a portfolio project that demonstrates three signals a senior IC role looks for:

- **Systems literacy:** Kafka semantics, claim-check, CloudEvents, distributed-broker tradeoffs.
- **AI/agent literacy:** MCP, A2A, multi-agent failure modes, context engineering.
- **Product judgment:** Identifying a precise gap in a crowded space and shipping the smallest thing that addresses it.

The README should lead with the AGNTCY/Anthropic/Cognition convergence and end with the replay demo GIF. That framing matters more than line count.

---

## Part 2 — Background and Problem Statement

### 2.1 The token-cost problem

Anthropic's June 13, 2025 engineering blog post "How we built our multi-agent research system" states: *"agents typically use about 4× more tokens than chat interactions, and multi-agent systems use about 15× more tokens than chats."* The reason is mechanical: every subagent re-encodes shared context into its own prompt, and the orchestrator re-reads subagent outputs in full.

The current workarounds are:

- **Truncation:** keep the most recent N messages. Loses information.
- **Summarization:** LLM-compress old context. Loses fidelity; costs more tokens.
- **Shared filesystem:** Anthropic's approach — subagents write to disk, return paths. Works but is bespoke per system.
- **Vector retrieval:** Letta/MemGPT-style memory. Works but is one-agent-one-store.

None of these are reusable infrastructure. Every multi-agent team rebuilds the same pattern.

### 2.2 The communication problem

Direct agent-to-agent calls (A2A, framework-specific RPC) work when the topology is known: planner → implementer → tester. They fail when:

- The number of consumers is unknown (any agent that cares about `market.data.refresh`).
- The producer should not block (long-running research feeding multiple synthesis agents).
- A run needs to be reproduced (replay the exact stream that led to a bug).
- A consumer joins late (a new agent should catch up on the last N events).

These are the cases Kafka exists for in classical infrastructure. There is no agent-native equivalent that ships as a usable library.

### 2.3 The standards gap

| Layer | Standard | Direction | Verdict |
|---|---|---|---|
| Agent ↔ tools | MCP (Anthropic, Nov 2024) | Vertical, 1:1 | Has `resources/subscribe` but rarely implemented |
| Agent ↔ agent (RPC) | A2A (Google, Apr 2025; LF, Aug 2025) | Horizontal, task-based | Sync, no fan-out, no replay |
| Agent ↔ agent (transport) | AGNTCY/SLIM (Cisco/LF, 2025) | Horizontal, pub/sub | Rust/Go, enterprise framing, heavy stack |
| Framework-internal | AutoGen v0.4 runtime, CrewAI Flows, LangGraph | In-process or gRPC | Locked to one framework; AutoGen now in maintenance |

AetherBus sits at the intersection: **framework-agnostic, MCP-aware, lightweight Python**.

### 2.4 Why not just use Kafka / NATS directly

You can. Kai Waehner has a well-known blueprint for exactly this. The gap is in **convention and developer experience**:

- No standard schema for "agent context event" — every team invents one.
- No standard URI scheme for resolving context — every team wires fetchers by hand.
- No standard MCP bridge — every team writes a custom adapter or skips MCP integration.
- No standard replay UI — every team debugs blind.

AetherBus is the **convention** that lets a developer go from `pip install` to a working multi-agent pipeline in an afternoon, while remaining a thin layer over standards.

---

## Part 3 — Goals, Non-Goals, and Principles

### 3.1 Goals (v1)

1. **G1.** A Python developer can publish and subscribe to context events in fewer than 20 lines of code.
2. **G2.** Events carry context pointers; resolution is on-demand, pluggable, and cached.
3. **G3.** Any MCP-capable client can consume an AetherBus topic without AetherBus-specific code.
4. **G4.** Streams are persistent and replayable for at least 7 days by default.
5. **G5.** A working replay UI ships in v1 — this is the headline portfolio demo.
6. **G6.** Reference adapters exist for LangGraph, CrewAI, and OpenAI Agents SDK.
7. **G7.** Both NATS JetStream (default) and Kafka transports work end-to-end.

### 3.2 Non-goals (v1, deferred or never)

- **N1.** Hosted SaaS / managed AetherBus Cloud (never in v1; possible future).
- **N2.** Cross-organization identity / MLS encryption (defer to A2A Agent Cards in v2).
- **N3.** A new wire protocol (we use CloudEvents + AsyncAPI; we add no fields not derivable from these).
- **N4.** A new agent framework (we adapt to LangGraph, CrewAI, OpenAI Agents SDK).
- **N5.** Web-scale routing / federation (AGNTCY/SLIM territory).
- **N6.** A JavaScript or Go SDK (Python only in v1; community can add others).

### 3.3 Design principles

- **P1. Standards first, conventions second, code third.** Every novel concept must be expressible in CloudEvents or AsyncAPI. If it cannot, we either reject it or contribute upstream.
- **P2. Pointers by default, inline only as escape hatch.** Events <4 KB may inline; everything else is a URI.
- **P3. The broker is dumb; the resolver is smart.** No business logic in the bus. All intelligence lives in producers, consumers, and resolvers.
- **P4. Replayability is a first-class feature, not a debug afterthought.** Every event must be replayable from the moment it is published.
- **P5. The Python DX is the moat.** AGNTCY/SLIM will beat us on protocol rigor. We win by being the library a developer actually wants to use.
- **P6. Boring infrastructure.** NATS JetStream, Kafka, CloudEvents, Pydantic, FastAPI. No exotic dependencies.

---

## Part 4 — User Personas and Use Cases

### 4.1 Personas

**P1. Solo agent-tinkerer (primary).** Shazeb-shaped. Building a multi-agent personal-dev pipeline on a MacBook. Wants `docker-compose up` and decorator-style API. Will not tolerate Java, Rust, or YAML-heavy config. Cares about replay because they debug constantly.

**P2. Small-team agent platform engineer.** 3–10 person team building production agent workflows. Already has Kafka or NATS. Wants AetherBus to integrate without forcing a new broker. Cares about observability and schema evolution.

**P3. MCP server author.** Building MCP servers and wants subscribe semantics that actually work. AetherBus is the reference implementation they wrap.

**P4. Researcher / paper author.** Studying multi-agent coordination. AetherBus is the substrate they run experiments on because replay makes ablations reproducible.

### 4.2 Use cases (with concrete event flows)

#### UC1 — Code-generation pipeline (the headline demo)

Three agents: Planner, Implementer, Tester.

```
[Planner] --publish--> code.plan.v1        {dataref: mcp://repo/plan-abc.md}
[Implementer] subscribes to code.plan.*
  -> resolves the spec
  -> writes code
  -> --publish--> code.diff.v1            {dataref: git://repo@sha, depends_on: [plan-evt-id]}
[Tester] subscribes to code.diff.*
  -> resolves the diff
  -> runs tests
  -> --publish--> code.test.result.v1     {dataref: file:///tmp/junit.xml, depends_on: [diff-evt-id]}
[Planner] subscribes to code.test.result.* — if fail, re-plan
```

Why pub/sub beats direct calls: Implementer and Tester have no compile-time dependency. Adding a Linter agent that also subscribes to `code.diff.*` requires zero changes to anyone else. Replay lets you reproduce the exact failure.

#### UC2 — Long-running research pipeline

Mirrors Anthropic's research agent architecture but as a reusable component. N research subagents publish `research.findings.<query-hash>` with vector-DB URIs. A synthesis agent subscribes, resolves selectively (only the chunks relevant to its current section), and publishes `research.synthesis.v1`. Solves the 200K-token truncation problem because the synthesizer never holds all findings in context at once.

#### UC3 — Continuous-learning lessons ledger

Any agent in your system publishes `lessons.learned` events whenever a postmortem fires. New agents at startup replay the topic from the beginning, resolving only lessons tagged relevant to their role. The `dependencies` field lets agents trace why a lesson exists.

#### UC4 — Scheduled context-refresh fan-out

A cron producer publishes `market.data.refresh.v1` every minute. Three financial agents subscribe; each does its own analysis without polling. Canonical Kafka pattern, expressible in A2A only with effort.

#### UC5 — Decoupled tool-result sharing (the "cache" use case)

Agent A scrapes a URL, publishes `tool.result.scrape.<url-hash>` on a compacted topic. Agents B, C, D check the topic before scraping; if a recent event exists, they resolve from cache. Topic compaction (key = URL hash) is built into JetStream and Kafka. Directly addresses the documented "agents re-retrieve documents they have already read" pattern.

#### UC6 — Replay debugging (the portfolio money shot)

Production agent run fails. Open the AetherBus Replay UI, scrub to the failure, see all events leading up to it, click "fork from here." The same agents — pointed at a sandbox broker — re-execute from that point with the same events. This is something neither A2A nor MCP nor any current framework offers. Demo it on the README.

#### UC7 — Hierarchical team-of-agents coordination

Manager publishes `tasks.assigned` on a queue-group subject (exactly one worker claims each task — NATS native, exactly the case JetStream queue groups handle better than Kafka). Workers publish `tasks.completed.<id>`. Manager subscribes to completions, aggregates, reports.

---

## Part 5 — Architecture

### 5.1 System diagram (conceptual)

```
┌─────────────────────────────────────────────────────────────────┐
│                         AetherBus                                │
│                                                                  │
│  ┌────────────┐    ┌──────────────┐    ┌─────────────────┐     │
│  │ Producer   │───▶│   Broker     │───▶│ Subscriber       │     │
│  │ Agent      │    │ (JetStream   │    │ Agent            │     │
│  │            │    │  or Kafka)   │    │                  │     │
│  └────────────┘    └──────────────┘    └────────┬─────────┘     │
│       │                  │                       │               │
│       │                  ▼                       │               │
│       │           ┌──────────────┐               │               │
│       │           │ MCP Adapter  │◀──── Claude   │               │
│       │           │ (server)     │      Desktop  │               │
│       │           └──────────────┘      Cursor   │               │
│       │                                  Cline   │               │
│       │                                          │               │
│       ▼                                          ▼               │
│   Publishes context           Resolves dataref URI               │
│   (writes to vec-db,          via Resolver SDK:                  │
│    git, S3, MCP server)       mcp:// vec:// git://               │
│                               file:// https://                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
                          ┌──────────────┐
                          │ Replay UI    │
                          │ (FastAPI +   │
                          │  React SPA)  │
                          └──────────────┘
```

### 5.2 Component inventory

| # | Component | Role | Language | Status |
|---|---|---|---|---|
| C1 | `aetherbus-core` | Bus client, decorators, event types | Python 3.11+ | v1 |
| C2 | `aetherbus-transport-nats` | NATS JetStream adapter | Python | v1 |
| C3 | `aetherbus-transport-kafka` | Kafka adapter (`aiokafka`) | Python | v1 |
| C4 | `aetherbus-resolver` | URI resolution + caching | Python | v1 |
| C5 | `aetherbus-mcp` | MCP server adapter | Python (`mcp` SDK) | v1 |
| C6 | `aetherbus-replay-ui` | Replay web UI | FastAPI + React (Vite) | v1 |
| C7 | `aetherbus-adapters` | LangGraph / CrewAI / Agents SDK glue | Python | v1 |
| C8 | `aetherbus-cli` | `aetherbus init`, `aetherbus tail`, `aetherbus replay` | Python (Typer) | v1 |

Single repo, single package (`aetherbus`), namespaced submodules. Optional extras: `pip install aetherbus[kafka,mcp,replay,langgraph,crewai,openai]`.

### 5.3 Event envelope (CloudEvents 1.0 + AetherBus extensions)

Required CloudEvents attributes:
- `id` (UUIDv7 — sortable, time-ordered)
- `source` (URI: `aetherbus://agent/<agent-name>`)
- `specversion` (`"1.0"`)
- `type` (reverse-DNS topic: `dev.aetherbus.research.finding.v1`)
- `time` (RFC 3339, UTC)
- `datacontenttype` (`"application/json"` or `"application/octet-stream"`)

AetherBus extensions (CloudEvents extension attributes):
- `dataref` (string, URI) — pointer to the actual payload. Required if no inline `data`.
- `aetherbusschema` (string, URI) — JSON Schema for the resolved payload.
- `aetherbusttl` (int, seconds) — cache hint for resolvers.
- `aetherbusdependencies` (string, comma-separated event IDs) — provenance DAG. CloudEvents extension attribute names must be lowercase letters/digits only.
- `aetherbuscorrelationid` (string) — workflow/trace ID, OpenTelemetry-compatible.
- `aetherbussignature` (string, optional, JWS) — producer signature over the envelope.

Either `data` (inline, <4 KB) or `dataref` (URI) is present. Not both.

### 5.4 Topic naming convention

`<domain>.<entity>.<action>.<version>`

Examples:
- `dev.aetherbus.research.finding.v1`
- `dev.aetherbus.code.plan.v1`
- `dev.aetherbus.tool.result.scrape.v1`

Wildcards: NATS uses `*` (one token) and `>` (rest). Kafka adapter translates to regex consumer filters. The CLI normalizes both.

### 5.5 URI schemes for `dataref`

| Scheme | Example | Resolver behavior |
|---|---|---|
| `mcp://` | `mcp://server-name/resources/abc` | Calls MCP `resources/read` on the named server |
| `vec://` | `vec://chroma/coll-x/chunk-id` | Fetches from Chroma/Qdrant/Pinecone (config maps name → backend) |
| `git://` | `git://owner/repo@sha:path` | Reads via local clone or GitHub API |
| `file://` | `file:///tmp/output.json` | Local filesystem (only for single-host use) |
| `https://` | `https://docs.example.com/...` | HTTP GET with optional auth |
| `s3://` | `s3://bucket/key` | Optional in v1, deferred to v1.1 if time runs out |

Resolvers register via entry points (`aetherbus.resolvers`) so users can add custom schemes.

### 5.6 Persistence and replay

- **Default retention:** 7 days OR 100,000 events per stream, whichever first. Configurable.
- **Compaction:** opt-in per topic via `aetherbus topic create --compact-by url-hash`.
- **Replay API:** `bus.replay(topic, since=<ts>, until=<ts>, into=<sandbox-bus>)`.
- **Sandbox runtime:** isolated NATS account (multi-tenancy primitive) so replay never collides with production traffic.

### 5.7 Schema and contract management

Channel contracts in **AsyncAPI 3.0**, generated from Pydantic models via a small codegen step:

```python
class ResearchFinding(EventPayload):
    query: str
    source_url: HttpUrl
    summary: str
    confidence: float = Field(ge=0, le=1)

bus.register_schema("dev.aetherbus.research.finding.v1", ResearchFinding)
```

`aetherbus schema export` writes `asyncapi.yaml`. CI fails if a topic publishes an event whose resolved payload does not match the registered schema (warn-mode in v1, strict in v2).

### 5.8 Security model (v1 scope)

- **Authentication:** NATS bearer tokens (NKey or JWT) or Kafka SASL. No new auth system.
- **Authorization:** topic-level ACLs delegated to the broker (NATS account ACLs, Kafka ACLs). AetherBus does not add a layer above this in v1.
- **Producer identity:** the `source` URI is informational only in v1. v2 will bind it to A2A Agent Card keys with `aetherbussignature`.
- **Content integrity:** events are immutable once published (broker enforces). Resolved payloads are not signed in v1.

Out of scope for v1: end-to-end encryption (defer to MLS via SLIM transport in v2), per-event RBAC, audit log export.

### 5.9 Observability

- Every event emits OpenTelemetry spans (`aetherbus.publish`, `aetherbus.subscribe.handle`, `aetherbus.resolve`).
- `aetherbuscorrelationid` becomes the trace ID for cross-agent flows.
- The Replay UI surfaces traces inline.
- Prometheus metrics: events/sec per topic, resolve latency p50/p99, handler error rate.

### 5.10 Failure modes and recovery

| Failure | Behavior |
|---|---|
| Broker unreachable on publish | Local outbox (SQLite); retry with exponential backoff; surface to caller after N attempts |
| Resolver fetch fails | Configurable: raise, return `None`, or return last-cached value |
| Handler raises | Negative-ack on JetStream; redelivery with `max_deliver`; dead-letter topic after exceeded |
| Schema mismatch | Warn in v1 (log + metric); strict-fail option |
| Cache stampede on resolver | Single-flight per `dataref` URI per process |

---

## Part 6 — Detailed API Specification

### 6.1 Quickstart (what the README opens with)

```python
from aetherbus import Bus, EventPayload
from pydantic import HttpUrl

bus = Bus.from_env()  # AETHERBUS_URL=nats://localhost:4222

class Finding(EventPayload):
    query: str
    source_url: HttpUrl
    summary: str

@bus.subscribe("dev.aetherbus.research.finding.v1")
async def on_finding(event):
    finding: Finding = await event.resolve(as_=Finding)
    print(f"Got: {finding.summary}")

await bus.publish(
    "dev.aetherbus.research.finding.v1",
    payload=Finding(query="kafka for agents", source_url="https://...", summary="..."),
    # payload >4KB? AetherBus writes it to the configured store and produces a dataref automatically.
)

await bus.run()  # blocks; runs the handler loop
```

### 6.2 Core API surface

```python
class Bus:
    @classmethod
    def from_env(cls) -> "Bus": ...
    @classmethod
    def from_config(cls, path: str) -> "Bus": ...

    async def publish(
        self,
        topic: str,
        payload: EventPayload | bytes | None = None,
        *,
        dataref: str | None = None,
        depends_on: list[str] | None = None,
        correlation_id: str | None = None,
        ttl: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:  # returns event id
        ...

    def subscribe(
        self,
        topic: str,
        *,
        queue_group: str | None = None,
        replay_from: datetime | str | None = None,  # "earliest", "latest", or timestamp
        max_in_flight: int = 16,
    ) -> Callable[[Handler], Handler]:
        ...

    async def replay(
        self,
        topic: str,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        into: "Bus | None" = None,  # sandbox bus
    ) -> AsyncIterator["Event"]:
        ...

    async def run(self) -> None: ...
    async def close(self) -> None: ...


class Event:
    id: str
    source: str
    type: str
    time: datetime
    dataref: str | None
    inline_data: Any | None
    correlation_id: str | None
    depends_on: list[str]

    async def resolve(self, as_: type[T] | None = None) -> T | bytes | dict:
        """Fetch the actual context via the configured resolver chain."""
        ...
```

### 6.3 Resolver SDK

```python
from aetherbus.resolver import Resolver, register_resolver

@register_resolver("vec")
class VectorResolver(Resolver):
    async def fetch(self, uri: str) -> bytes:
        # uri: vec://chroma/collection/id
        ...
```

Built-ins: `mcp`, `vec` (Chroma + Qdrant + Pinecone), `git`, `file`, `https`. Each is a separate optional extra.

### 6.4 MCP server adapter

`aetherbus-mcp serve --bus nats://localhost:4222 --topics 'dev.aetherbus.>'` starts an MCP server that:

- Lists each subscribed topic as an MCP Resource.
- Honors `resources/subscribe`. When a new event arrives, it emits `notifications/resources/updated`.
- `resources/read` returns the resolved payload (so MCP clients never see the raw envelope; they see content).

This is the **single most important component for portfolio framing**, because it makes AetherBus consumable from Claude Desktop, Cursor, Goose, and Cline with zero AetherBus-aware code on the client side.

### 6.5 CLI

```
aetherbus init                  # scaffold a project with example agents
aetherbus tail <topic>          # real-time event stream to stdout
aetherbus publish <topic> ...   # one-shot publish
aetherbus replay <topic> ...    # replay into sandbox
aetherbus topic create ...      # create topic with retention/compaction policy
aetherbus schema export         # emit asyncapi.yaml
aetherbus mcp serve             # start the MCP adapter
aetherbus ui                    # start the Replay UI
```

### 6.6 Framework adapters

Each adapter is a thin wrapper (target: <100 lines each).

**LangGraph:**

```python
from aetherbus.adapters.langgraph import bus_node

graph.add_node("publish_plan", bus_node(bus, topic="dev.aetherbus.code.plan.v1"))
```

**CrewAI Flows:**

```python
from aetherbus.adapters.crewai import bus_listen

class CodeFlow(Flow):
    @bus_listen(bus, "dev.aetherbus.code.plan.v1")
    async def implement(self, event):
        ...
```

**OpenAI Agents SDK:**

```python
from aetherbus.adapters.openai_agents import as_tool

agent = Agent(tools=[as_tool(bus, "dev.aetherbus.code.diff.v1")])
```

### 6.7 Replay UI specification

A single-page React app (Vite) served by a FastAPI backend that talks to the bus. Pages:

1. **Streams.** List of topics with retention, depth, throughput.
2. **Stream detail.** Timeline of events; click an event for envelope + resolved payload; provenance graph showing `depends_on` lineage.
3. **Replay.** Pick a time range, configure sandbox broker URL, hit Replay. Live progress; per-event status (delivered / handler-errored / not-yet-consumed).
4. **Schemas.** AsyncAPI rendering of registered channels.
5. **Trace.** OTel trace view for a `correlation_id`.

Auth: HTTP basic over loopback in v1, JWT later. Not exposed to public internet by default.

The replay flow is the README GIF. It must be polished.

### 6.8 Configuration

`aetherbus.yaml`:

```yaml
transport:
  type: nats              # or "kafka"
  url: nats://localhost:4222
  jetstream:
    default_retention: 7d
    default_max_msgs: 100000

resolvers:
  mcp:
    servers:
      filesystem: stdio:///usr/local/bin/mcp-filesystem
      github: https://mcp.github.com/sse
  vec:
    backends:
      chroma: http://localhost:8000
      qdrant: http://localhost:6333
  https:
    auth: ${BEARER_TOKEN}

mcp_adapter:
  port: 7799
  topics:
    - "dev.aetherbus.>"

replay_ui:
  port: 7798
  auth: basic
```

---

## Part 7 — Implementation Plan

### 7.1 Milestones

**M1 — Spike (1 weekend)**
- Single-file `examples/spike.py`: publish + subscribe over local NATS JetStream with CloudEvents.
- Decision log: confirm NATS as default, confirm CloudEvents shape, confirm Pydantic for payloads.
- Deliverable: working spike committed; ADR-001 written.

**M2 — Core (1 weekend)**
- `Bus`, `Event`, `EventPayload`, NATS transport, publish + subscribe + run.
- Local outbox for publish reliability.
- Test suite (pytest + testcontainers-NATS). 80%+ coverage target on core.
- Deliverable: `pip install aetherbus` works locally; quickstart in README runs.

**M3 — Resolvers + MCP adapter (1 weekend)**
- Resolver SDK + `mcp`, `file`, `https`, `git` resolvers.
- MCP server adapter (`aetherbus-mcp serve`).
- Manual test: subscribe from Claude Desktop / Cursor end-to-end.
- Deliverable: Claude Desktop receives a `notifications/resources/updated` when an event publishes.

**M4 — Kafka transport (1 weekend)**
- Kafka adapter using `aiokafka`. Same `Bus` API.
- Conformance test suite that runs the same scenarios against NATS and Kafka.
- Deliverable: `transport.type: kafka` works for the quickstart.

**M5 — Replay UI (2 weekends)**
- FastAPI backend (REST + WebSocket for live tail).
- React SPA (Vite, TanStack Query, shadcn/ui, recharts for throughput plots).
- Sandbox replay (isolated NATS account).
- Deliverable: replay GIF for the README.

**M6 — Framework adapters (1 weekend)**
- LangGraph, CrewAI, OpenAI Agents SDK adapters.
- One example app per adapter in `examples/`.
- Deliverable: three working integration examples.

**M7 — Polish + launch (1 weekend)**
- Docs site (mkdocs-material).
- README with GIF, architecture diagram, three quickstarts (raw, MCP, LangGraph).
- HN / r/LocalLLaMA / LinkedIn launch post drafted.
- Deliverable: public repo, public launch.

Total: 8 weekends. If schedule slips, drop M4 (Kafka) to v1.1 — the lean MVP path. Do not drop the Replay UI; it is the portfolio centerpiece.

### 7.2 Decision log (ADRs to write during build)

- **ADR-001:** Choose NATS JetStream as default transport.
- **ADR-002:** Choose CloudEvents 1.0 envelope; rationale for `dataref` over inline.
- **ADR-003:** Choose Pydantic v2 over dataclasses / msgspec for payload schemas.
- **ADR-004:** MCP `resources/subscribe` poke-then-read semantics (not push) — matches spec.
- **ADR-005:** UUIDv7 for event IDs.
- **ADR-006:** Reject building a JS SDK in v1.
- **ADR-007:** Reject end-to-end encryption in v1; defer to A2A identity in v2.

### 7.3 Testing strategy

- **Unit:** Pydantic models, envelope construction, resolver fallbacks.
- **Integration:** testcontainers for NATS and Kafka. Same scenario suite runs against both.
- **End-to-end:** dockerized 3-agent code-gen pipeline (planner / impl / tester) that exits 0 on success. Runs in CI.
- **MCP conformance:** spin up the MCP adapter, drive it with the official MCP Inspector, assert `notifications/resources/updated` arrives within 500ms of publish.
- **Property tests (Hypothesis):** envelope round-trips, schema validation.
- **Load:** k6 or locust hammering 10k events/sec on a single-node NATS to characterize p99 publish latency. Document numbers in `BENCHMARKS.md`.

### 7.4 CI/CD

- GitHub Actions: lint (ruff), type-check (mypy strict), test (pytest + coverage), build wheel, publish to PyPI on tag.
- Pre-commit hooks: ruff, mypy, conventional commits.
- Renovate for dependency updates.
- Codecov for coverage trend.

### 7.5 Repository layout

```
aetherbus/
├── README.md
├── PRD_AetherBus.md
├── LICENSE                       # Apache 2.0
├── pyproject.toml
├── docs/
│   ├── index.md
│   ├── architecture/
│   │   ├── adrs/
│   │   └── diagrams/
│   ├── quickstart.md
│   └── reference/
├── src/aetherbus/
│   ├── __init__.py
│   ├── bus.py                    # Bus class
│   ├── event.py                  # Event, EventPayload
│   ├── envelope.py               # CloudEvents serialization
│   ├── transports/
│   │   ├── base.py
│   │   ├── nats.py
│   │   └── kafka.py
│   ├── resolver/
│   │   ├── base.py
│   │   ├── mcp.py
│   │   ├── vec.py
│   │   ├── git.py
│   │   ├── file.py
│   │   └── https.py
│   ├── mcp_adapter/
│   ├── replay_ui/
│   │   ├── backend/              # FastAPI
│   │   └── frontend/             # Vite + React
│   ├── adapters/
│   │   ├── langgraph.py
│   │   ├── crewai.py
│   │   └── openai_agents.py
│   └── cli.py                    # Typer
├── tests/
│   ├── unit/
│   ├── integration/
│   └── e2e/
└── examples/
    ├── codegen_pipeline/
    ├── research_pipeline/
    └── replay_demo/
```

### 7.6 Versioning and release

- SemVer. v0.x while API churns. v1.0.0 only when MCP adapter is stable.
- Releases via tag → GitHub Action → PyPI.
- CHANGELOG.md follows Keep a Changelog.

---

## Part 8 — Risks and Mitigations

| # | Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|---|
| R1 | AGNTCY/SLIM ships polished Python SDK before AetherBus | Medium | High | Position as DX layer; add SLIM transport in v2; do not compete on protocol |
| R2 | Anthropic / FastMCP add `resources/subscribe` natively | Medium | Medium | Welcome it — AetherBus becomes the multi-source broker behind it |
| R3 | Cognition's anti-multi-agent thesis is widely correct | Medium | Medium | Frame use cases for wide-shallow / async / scheduled, which are out of Cognition's scope |
| R4 | Replay UI eats half the schedule | High | Medium | Time-box to 2 weekends; cut features (no graph view, table only) before cutting the UI entirely |
| R5 | Kafka adapter has long-tail bugs from semantic differences | Medium | Low | Ship behind a feature flag; mark "beta" in v1.0; conformance suite catches drift |
| R6 | "Yet another event bus" framing on launch | High | Medium | Lead with the MCP gap and the replay demo, not the broker |
| R7 | No external adopters in 90 days | Medium | Medium | Active PR campaign: open PRs into CrewAI/LangGraph examples; write at LangChain blog |
| R8 | NATS Python client gaps (e.g., per-message ack control) | Low | Medium | Vendor a thin shim; contribute back; document workarounds |

---

## Part 9 — Open Questions

These should be resolved during M1 (the spike) or M2 (core):

1. **Q1.** Inline threshold — is 4 KB right? Anthropic's research subagents use ~few-KB references; check actual sizes once we instrument an example.
2. **Q2.** Default JetStream retention — 7 days fine for solo dev; what about CI environments? Maybe ship a `--ephemeral` profile.
3. **Q3.** UUIDv7 vs. KSUID — both sortable; UUIDv7 is RFC-standard and Postgres-native. Going with UUIDv7 unless ecosystem gaps appear.
4. **Q4.** Should `EventPayload` be Pydantic or `attrs`? Pydantic v2 chosen for ecosystem alignment with LangChain / FastAPI. Reconsider only if perf is an issue.
5. **Q5.** MCP adapter — push the resolved content in `resources/read`, or push the raw envelope? Push resolved content; raw envelope is exposed via a separate `aetherbus.envelope` resource type.
6. **Q6.** Replay UI auth — basic over loopback v1, or skip auth entirely and document "do not expose"? Going with basic; trivial to add, signals seriousness.
7. **Q7.** Multi-tenancy in v1 — NATS accounts per project, or single account? Single account in v1; doc multi-tenant pattern.

---

## Part 10 — Launch Plan

### 10.1 Pre-launch (the week before public release)

- Repo public, README polished, replay GIF embedded.
- `mkdocs` site live at `aetherbus.dev` or `shazeb.ca/aetherbus`.
- PyPI package published, install instructions verified on macOS + Linux + WSL.
- Discord or GitHub Discussions enabled.
- Three blog posts drafted (see 10.2).

### 10.2 Launch artifacts

1. **Substack / personal site post:** "MCP has `resources/subscribe` but nobody uses it." Frames the gap. Links AGNTCY, Anthropic research blog, Cognition piece, AetherBus.
2. **HN submission:** title `Show HN: AetherBus — event-driven context layer for AI agents, MCP-native`. Body is short; links to the post + repo.
3. **r/LocalLLaMA + r/AI_Agents:** focused on the replay demo.
4. **LinkedIn (Shazeb's audience):** career-narrative framing — what was learned, how the AGNTCY/Anthropic/Cognition convergence shaped the design, link to the post.

### 10.3 Post-launch (first 30 days)

- Triage issues within 48 hours.
- Ship a 0.1.1 fast-follow within the first week to signal momentum.
- Open one PR per week into upstream agent frameworks (CrewAI examples, LangGraph cookbook, Anthropic cookbook) demoing AetherBus.
- Submit talks to: PyCon, AI Engineer Summit, MCP DevDay (if it happens), LangChain Interrupt.

### 10.4 The 90-day decision gate

Re-check the three success criteria from Part 1. Two-of-three pass → continue to v2 (A2A identity, SLIM transport, JS SDK). Otherwise → reframe as personal-tooling portfolio piece, freeze the public surface, document in the README.

---

## Part 11 — Future (v2 and beyond, non-binding)

- **A2A Agent Card identity:** bind `source` URIs to A2A keys; sign envelopes with JWS.
- **SLIM transport:** AGNTCY/SLIM as an alternative pluggable transport.
- **JS/TS SDK:** for browser-side agents and Node tools.
- **Hosted broker:** if and only if external adoption justifies it.
- **MLS encryption:** group-encrypted topics for multi-tenant deployments.
- **Schema registry:** Confluent-compatible registry adapter.
- **Agent marketplace integration:** publish/discover topics across orgs via AGNTCY Directory.

---

## Appendix A — Glossary

| Term | Meaning |
|---|---|
| Event | A CloudEvents envelope published to a topic |
| Topic | A named, persistent stream of events (NATS subject / Kafka topic) |
| Dataref | A URI inside an event that points to the actual context payload |
| Resolver | A pluggable component that fetches the content at a `dataref` URI |
| Replay | Re-emitting historical events into a (typically sandbox) bus |
| Claim-check | The design pattern of replacing large payloads with pointers (origin: Microsoft messaging patterns; now standard in CloudEvents via `dataref`) |
| Provenance DAG | The directed graph of `depends_on` relationships across events |
| Compaction | Retaining only the most recent event per key on a topic |
| Queue group | A set of subscribers where each event is delivered to exactly one member (NATS); equivalent to Kafka consumer groups |

---

## Appendix B — Relationship to Existing Standards

| Standard | AetherBus position |
|---|---|
| **MCP** | First-class. AetherBus implements MCP `resources/subscribe` properly via the MCP adapter; `mcp://` is a built-in `dataref` scheme. |
| **A2A** | Complementary. A2A is RPC; AetherBus is pub/sub. Recommended pattern: A2A task completions can publish to AetherBus; AetherBus events can trigger A2A tasks. v2 will use A2A Agent Cards for producer identity. |
| **AGNTCY / SLIM** | Strategic friend. SLIM is a planned future transport. AetherBus does not duplicate SLIM's protocol-rigor work; it sits above it. |
| **CloudEvents** | Wire format. AetherBus envelopes ARE CloudEvents envelopes with documented extension attributes. |
| **AsyncAPI** | Contract format. `aetherbus schema export` writes AsyncAPI 3.0. |
| **OpenTelemetry** | Observability. All spans use OTel conventions; `aetherbuscorrelationid` is the trace ID. |

---

*End of PRD v1.0.*
