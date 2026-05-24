# Architecture

AetherBus is a thin convention layer over [CloudEvents 1.0](https://github.com/cloudevents/spec) + [AsyncAPI 3.0](https://www.asyncapi.com/) + [MCP](https://modelcontextprotocol.io). It does not invent new wire formats; it ships sensible defaults, a pluggable resolver, and an MCP bridge.

## System diagram

```mermaid
flowchart LR
    A[Producer Agent] -->|publish CloudEvent| B[Broker]
    B -->|push| C[Subscriber Agent]
    B -->|push| D[MCP Adapter]
    D -->|notifications/resources/updated| E[Claude Code / Desktop / Cursor]
    E -->|resources/read| D
    B -->|history| F[Replay UI]
    C -->|resolve dataref| G[(mcp:// vec:// git:// file:// https://)]

    subgraph Broker[NATS JetStream &nbsp;|&nbsp; Kafka]
      B
    end
    subgraph Resolvers[Resolver SDK]
      G
    end

    style Broker fill:#1a1e27,stroke:#4cc9f0,color:#e6e8ec
    style Resolvers fill:#1a1e27,stroke:#4cc9f0,color:#e6e8ec
```

The key idea: **events carry pointers, not payloads**. Each CloudEvent has a `dataref` URI; subscribers resolve content on demand from any registered resolver. Inline data is the escape hatch for <4 KB payloads.

## Components

| Component | What it does | Where |
|---|---|---|
| `Bus` | Public API — `publish`, `subscribe`, `run`, `replay`, async context manager. | `aetherbus.bus` |
| `Envelope` | CloudEvents 1.0 + AetherBus extension attrs. Enforces data XOR dataref, 4 KB inline guard. | `aetherbus.envelope` |
| `Event` / `EventPayload` | Wrapper over a received envelope; Pydantic v2 base for typed payloads. | `aetherbus.event` |
| `Transport` Protocol | Two concrete impls: NATS JetStream (default), Kafka (via `aiokafka`). Identical agent API. | `aetherbus.transports.{nats,kafka}` |
| Resolver SDK | Process-wide registry, TTL cache + per-URI single-flight. Built-ins: `file://`, `https://`, `mcp://`, `git://`. | `aetherbus.resolver` |
| MCP Adapter | Bridges bus topics to MCP resources; emits `notifications/resources/updated`. | `aetherbus.mcp_adapter` |
| Replay UI | FastAPI backend + React SPA + sandbox replay. | `aetherbus.replay_ui` |
| Framework Adapters | LangGraph node, CrewAI Flow decorator, OpenAI Agents tool wrapper. <100 LOC each. | `aetherbus.adapters` |
| Outbox | SQLite-backed publish-side reliability. Exponential backoff + jitter. | `aetherbus.outbox` |

## Design principles

1. **Standards first, conventions second, code third.** Every novel concept expresses in CloudEvents or AsyncAPI — otherwise it doesn't land.
2. **Pointers by default, inline only as escape hatch.** Events <4 KB may inline; everything else is a `dataref` URI.
3. **The broker is dumb; the resolver is smart.** No business logic in the bus. Everything lives in producers, consumers, and resolvers.
4. **Replayability is a first-class feature, not a debug afterthought.** Every event replays from the moment it's published.
5. **Boring infrastructure.** NATS, Kafka, CloudEvents, Pydantic v2, FastAPI, Typer. No exotic dependencies.

## Deeper dives

- [Envelope](envelope.md) — what's in a CloudEvent and the AetherBus extension attributes.
- [Transports](transports.md) — semantic translation between NATS and Kafka.
- [ADRs](adrs/index.md) — five architecture decisions that drive the rest of the design.
