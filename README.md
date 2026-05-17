# AetherBus

> Event-driven context layer for AI agents. CloudEvents over NATS/Kafka, with pointers — not payloads — and an MCP adapter so any MCP client can consume the stream.

**Status:** pre-alpha. The full v1 spec lives in [PRD_AetherBus.md](PRD_AetherBus.md).

## What it is

AetherBus is a Python library and reference broker that gives AI agents a Kafka-style pub/sub layer where **events carry pointers to context, not the context itself**. Agents subscribe to topics, receive lightweight CloudEvents with a `dataref` URI, and resolve the full payload on demand from MCP resources, vector databases, Git, S3, or local files.

It is not a new agent framework, and not a competitor to MCP or A2A. It is the missing async, many-to-many, replayable layer between them.

## Quickstart (target API)

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
)

await bus.run()
```

## Roadmap

See [PRD_AetherBus.md §7.1](PRD_AetherBus.md) for the full milestone plan. Target: 8 weekends to v1.

- M1 — Spike (NATS + CloudEvents publish/subscribe)
- M2 — Core (`Bus`, `Event`, outbox, tests)
- M3 — Resolvers + MCP server adapter
- M4 — Kafka transport
- M5 — Replay UI
- M6 — Framework adapters (LangGraph, CrewAI, OpenAI Agents SDK)
- M7 — Polish + launch

## License

Apache 2.0 — see [LICENSE](LICENSE).
