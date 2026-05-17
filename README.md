# AetherBus

> Event-driven context layer for AI agents. CloudEvents over NATS/Kafka, with pointers — not payloads — and an MCP adapter so any MCP client can consume the stream.

**Status:** pre-alpha. The full v1 spec lives in [PRD_AetherBus.md](PRD_AetherBus.md). M3 (resolvers + MCP adapter — the strategic wedge) just landed.

## The wedge — bus stream → MCP resource → any MCP client

```bash
docker compose up -d nats                                             # broker
aetherbus-mcp serve --topic dev.aetherbus.research.finding.v1 &      # adapter
python examples/poke.py "kafka for agents"                            # publish one event
```

Wire `aetherbus-mcp` into Claude Code's `.mcp.json` (or Claude Desktop's `claude_desktop_config.json`) and the bus stream shows up as a live MCP resource. Every published event triggers `notifications/resources/updated`; the client pulls the resolved payload via `resources/read` (poke-then-read — see [ADR-004](docs/architecture/adrs/0004-mcp-poke-then-read-semantics.md)).

```
> read the aetherbus://dev.aetherbus.research.finding.v1 resource
Here's the latest event on dev.aetherbus.research.finding.v1:

{
  "query": "kafka for agents",
  "summary": "claim-check works"
}
```

That's a Claude session consuming a Kafka-style pub/sub stream **with zero AetherBus-specific code on the client side** — MCP `resources/subscribe` and `notifications/resources/updated` doing the work. The protocol gap from PRD §2.3 ("MCP has the primitive but nobody uses it") closing in real time.

## Install

```bash
git clone git@github.com:ShazebA/aetherbus.git
cd aetherbus
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

## Run a broker

```bash
docker compose up -d nats
```

NATS JetStream listens on `localhost:4222`. Stop with `docker compose down`.

## Quickstart

Two terminals, one publisher, one subscriber, both pointing at the same NATS:

```python
# subscriber.py
import asyncio
from aetherbus import Bus, EventPayload


class Finding(EventPayload):
    query: str
    summary: str


async def main() -> None:
    bus = Bus.from_env()  # reads AETHERBUS_URL, defaults to nats://localhost:4222
    bus.register_schema("dev.aetherbus.research.finding.v1", Finding)

    @bus.subscribe("dev.aetherbus.research.finding.v1")
    async def on_finding(event):
        finding = await event.resolve(as_=Finding)
        print(f"got: {finding.summary}")

    await bus.run()


asyncio.run(main())
```

```python
# publisher.py
import asyncio
from aetherbus import Bus, EventPayload


class Finding(EventPayload):
    query: str
    summary: str


async def main() -> None:
    async with Bus.from_env() as bus:
        eid = await bus.publish(
            "dev.aetherbus.research.finding.v1",
            payload=Finding(query="kafka for agents", summary="claim-check works"),
        )
        print(f"published {eid}")


asyncio.run(main())
```

> Events larger than 4 KB serialized must use `dataref` instead of inline payloads. As of M3 the resolver SDK supports `file://`, `https://`, `mcp://`, and `git://` schemes out of the box (`mcp://` and `git://` need the `[mcp]` extra and a `git` binary on `$PATH` respectively).

## Develop

```bash
pip install -e ".[dev]"
ruff check src tests
mypy src
docker compose up -d nats
pytest -q
```

Integration tests use `testcontainers` to spin up NATS automatically; set `AETHERBUS_TEST_NATS_URL=nats://localhost:4222` to reuse the compose-managed instance and skip the container start.

## Roadmap

See [PRD_AetherBus.md §7.1](PRD_AetherBus.md) for the full milestone plan. Target: 8 weekends to v1.

- ✅ M1 — Spike (NATS + CloudEvents publish/subscribe) → see [examples/spike.py](examples/spike.py)
- ✅ M2 — Core (`Bus`, `Event`, outbox, tests)
- ✅ M3 — Resolvers + MCP server adapter — *strategic wedge above* → see [examples/poke.py](examples/poke.py)
- M4 — Kafka transport
- M5 — Replay UI
- M6 — Framework adapters (LangGraph, CrewAI, OpenAI Agents SDK)
- M7 — Polish + launch

## License

Apache 2.0 — see [LICENSE](LICENSE).
