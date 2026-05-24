"""OpenAI Agents SDK + AetherBus — agent publishes via a bus tool.

Run:

.. code-block:: bash

    pip install 'aetherbus[openai]'
    export OPENAI_API_KEY=sk-...
    docker compose up -d nats
    python examples/openai_agents_demo.py

The agent is wired with a single tool that publishes its arguments onto a
bus topic. The model decides when to call it — when a separate subscriber is
attached you'll see the agent's published payloads flow through.
"""
from __future__ import annotations

import asyncio
import os

from aetherbus import Bus


async def main() -> None:
    try:
        from agents import Agent, Runner
    except ImportError as e:
        raise SystemExit(
            "openai-agents not installed. Run: pip install 'aetherbus[openai]'"
        ) from e

    from aetherbus.adapters.openai_agents import as_tool

    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("Set OPENAI_API_KEY before running this example.")

    bus = Bus.from_env()
    async with bus:
        agent = Agent(
            name="planner",
            instructions=(
                "When the user requests a plan, draft 2-3 short bullet steps "
                "and publish them via the publish_plan tool."
            ),
            tools=[
                as_tool(
                    bus,
                    "dev.aetherbus.code.plan.v1",
                    name="publish_plan",
                    description="Publish a plan onto the AetherBus.",
                ),
            ],
        )
        result = await Runner.run(
            agent,
            "Draft a plan for migrating the auth subsystem to OAuth.",
        )
        print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main())
