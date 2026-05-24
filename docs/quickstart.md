# Quickstart

Three working examples. Pick the one closest to where you are.

## Install

```bash
git clone git@github.com:ShazebA/aetherbus.git
cd aetherbus
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
```

A broker — NATS by default:

```bash
docker compose up -d nats
```

## 1 — Raw bus

The smallest useful program: one publisher, one subscriber, full envelope round-trip.

=== "subscriber.py"

    ```python
    import asyncio
    from aetherbus import Bus, EventPayload


    class Finding(EventPayload):
        query: str
        summary: str


    async def main() -> None:
        bus = Bus.from_env()
        bus.register_schema("dev.aetherbus.research.finding.v1", Finding)

        @bus.subscribe("dev.aetherbus.research.finding.v1")
        async def on_finding(event):
            finding = await event.resolve(as_=Finding)
            print(f"got: {finding.summary}")

        await bus.run()


    asyncio.run(main())
    ```

=== "publisher.py"

    ```python
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

Two terminals:

```bash
python subscriber.py                  # terminal A — blocks
python publisher.py                   # terminal B — publishes one event
# Terminal A prints: got: claim-check works
```

Events ≤4 KB serialize inline. Larger payloads must carry a `dataref` URI — see the [resolver SDK](reference/resolver.md).

## 2 — MCP wedge

Drop AetherBus into Claude Code or Claude Desktop as a live MCP server. Zero AetherBus-specific code on the client side.

### Project-level `.mcp.json`

```json
{
  "mcpServers": {
    "aetherbus": {
      "command": "/abs/path/to/.venv/bin/aetherbus-mcp",
      "args": [
        "serve",
        "--topic", "dev.aetherbus.research.finding.v1",
        "--topic", "dev.aetherbus.code.plan.v1"
      ],
      "env": { "AETHERBUS_URL": "nats://localhost:4222" }
    }
  }
}
```

### What happens

1. Start a Claude Code session in the project directory; it picks up `.mcp.json` and prompts to enable `aetherbus`.
2. Publish an event from anywhere: `python examples/poke.py "kafka for agents"`.
3. The MCP server emits `notifications/resources/updated`. The model picks it up and reads the resource via `resources/read`.

This is the strategic wedge — see [ADR-004: MCP poke-then-read semantics](architecture/adrs/0004-mcp-poke-then-read-semantics.md) for the design rationale.

## 3 — LangGraph node

Plug the bus into a LangGraph graph in one node.

```python
import asyncio
from typing import TypedDict
from langgraph.graph import END, START, StateGraph
from aetherbus import Bus, EventPayload
from aetherbus.adapters.langgraph import bus_node


class Plan(EventPayload):
    summary: str
    steps: list[str]


class State(TypedDict, total=False):
    payload: Plan
    _aetherbus_event_id: str


async def planner(state: State) -> dict:
    steps = ["draft schema", "wire transport", "publish plan"]
    return {"payload": Plan(summary="design pub/sub layer", steps=steps)}


async def main() -> None:
    bus = Bus.from_env()
    graph = StateGraph(State)
    graph.add_node("plan", planner)
    graph.add_node("publish_plan", bus_node(bus, topic="dev.aetherbus.code.plan.v1"))
    graph.add_edge(START, "plan")
    graph.add_edge("plan", "publish_plan")
    graph.add_edge("publish_plan", END)

    async with bus:
        result = await graph.compile().ainvoke({})
        print(f"published as {result['_aetherbus_event_id']}")


asyncio.run(main())
```

Install with `pip install 'aetherbus[langgraph]'` first. Equivalent examples for CrewAI Flows and the OpenAI Agents SDK live in [`examples/`](https://github.com/ShazebA/aetherbus/tree/main/examples).

## Next

- [Replay UI](replay-ui.md) — watch events flow live; sandbox-replay history.
- [Envelope format](architecture/envelope.md) — what's in a CloudEvent + AetherBus extensions.
- [Transports](architecture/transports.md) — switching between NATS and Kafka without changing agent code.
