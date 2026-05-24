"""CrewAI Flows + AetherBus — Flow methods react to bus events.

Run:

.. code-block:: bash

    pip install 'aetherbus[crewai]'
    docker compose up -d nats
    python examples/crewai_pipeline.py            # subscriber
    python examples/poke.py "kafka for agents"    # publisher (separate shell)

Pure-Python equivalent that doesn't require ``crewai`` itself — the adapter
only needs decorated methods, not a real ``Flow`` subclass. Swap ``object``
below for ``crewai.flow.Flow`` once you wire your actual flow.
"""
from __future__ import annotations

import asyncio

from aetherbus import Bus, EventPayload
from aetherbus.adapters.crewai import bus_listen, wire_bus_listeners


class Finding(EventPayload):
    query: str
    summary: str


async def main() -> None:
    bus = Bus.from_env()

    class CodeFlow:
        # Replace `object` with `crewai.flow.Flow` for the real thing.

        @bus_listen(bus, "dev.aetherbus.research.finding.v1")
        async def on_finding(self, event):
            finding = await event.resolve(as_=Finding)
            print(f"flow saw: {finding.query!r} → {finding.summary!r}")

        @bus_listen(bus, "dev.aetherbus.code.plan.v1")
        async def on_plan(self, event):
            print(f"flow saw plan event {event.id}")

    flow = CodeFlow()
    wired = wire_bus_listeners(flow)
    print(f"wired {len(wired)} bus listener(s): {wired}")
    await bus.run()  # blocks; publish from another terminal


if __name__ == "__main__":
    asyncio.run(main())
