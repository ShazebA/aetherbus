"""poke — publish a single CloudEvent. Used to drive the MCP wedge demo.

Run:
    docker compose up -d nats           # broker
    python examples/poke.py             # publishes one event

Pair with ``aetherbus-mcp serve --topic dev.aetherbus.research.finding.v1``
wired into Claude Code / Desktop and watch the resource update.
"""
from __future__ import annotations

import asyncio
import sys

from aetherbus import Bus, EventPayload


class Finding(EventPayload):
    query: str
    summary: str


async def main() -> None:
    topic = "dev.aetherbus.research.finding.v1"
    payload = Finding(
        query=" ".join(sys.argv[1:]) or "kafka for agents",
        summary="claim-check works",
    )
    async with Bus.from_env() as bus:
        eid = await bus.publish(topic, payload=payload)
        print(f"published {eid} → {topic}")


if __name__ == "__main__":
    asyncio.run(main())
