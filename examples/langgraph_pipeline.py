"""LangGraph + AetherBus — three-node graph publishes a plan through the bus.

Run:

.. code-block:: bash

    pip install 'aetherbus[langgraph]'
    docker compose up -d nats
    python examples/langgraph_pipeline.py

The planner builds a plan in graph state; ``bus_node`` publishes it onto the
bus; a downstream consumer (running anywhere on the same broker) can react.
"""
from __future__ import annotations

import asyncio
from typing import TypedDict

from langgraph.graph import END, START, StateGraph

from aetherbus import Bus, EventPayload
from aetherbus.adapters.langgraph import bus_node


class PlanPayload(EventPayload):
    summary: str
    steps: list[str]


class State(TypedDict, total=False):
    summary: str
    steps: list[str]
    payload: PlanPayload
    _aetherbus_event_id: str


async def main() -> None:
    bus = Bus.from_env()

    async def planner(state: State) -> dict:
        steps = ["draft schema", "wire transport", "publish plan"]
        return {
            "summary": "design AetherBus pub/sub layer",
            "steps": steps,
            "payload": PlanPayload(summary="design AetherBus pub/sub layer", steps=steps),
        }

    async def confirm(state: State) -> dict:
        eid = state.get("_aetherbus_event_id")
        print(f"published plan as event {eid}")
        return {}

    graph = StateGraph(State)
    graph.add_node("plan", planner)
    graph.add_node("publish_plan", bus_node(bus, topic="dev.aetherbus.code.plan.v1"))
    graph.add_node("confirm", confirm)
    graph.add_edge(START, "plan")
    graph.add_edge("plan", "publish_plan")
    graph.add_edge("publish_plan", "confirm")
    graph.add_edge("confirm", END)

    async with bus:
        await graph.compile().ainvoke({})


if __name__ == "__main__":
    asyncio.run(main())
