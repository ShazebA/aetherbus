# AetherBus

> Event-driven context layer for AI agents. CloudEvents over NATS/Kafka, with pointers — not payloads — and an MCP adapter so any MCP client can consume the stream.

**Status:** pre-alpha. The full v1 spec lives in [PRD_AetherBus.md](PRD_AetherBus.md). M2 (core API, NATS transport, outbox, tests) is the current tip.

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

> M2 ships inline payloads only — `dataref` resolution arrives in M3. Events larger than 4 KB serialized will error; that's where `dataref` (and the resolver SDK) will take over.

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
- ✅ M2 — Core (`Bus`, `Event`, outbox, tests) — *this milestone*
- M3 — Resolvers + MCP server adapter
- M4 — Kafka transport
- M5 — Replay UI
- M6 — Framework adapters (LangGraph, CrewAI, OpenAI Agents SDK)
- M7 — Polish + launch

## License

Apache 2.0 — see [LICENSE](LICENSE).
