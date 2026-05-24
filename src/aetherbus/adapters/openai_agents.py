"""OpenAI Agents SDK adapter — wrap a bus topic as an Agent tool.

Usage::

    from agents import Agent
    from aetherbus.adapters.openai_agents import as_tool

    agent = Agent(
        name="planner",
        tools=[as_tool(bus, "dev.aetherbus.code.diff.v1")],
    )

The returned object is a ``function_tool`` the Agent can call. The model
chooses when to invoke it; on invocation we publish the model-supplied payload
onto the bus and return the resulting event id (which the model often quotes
back so subsequent tool calls can reference it via ``depends_on``).
"""
from __future__ import annotations

from typing import Any


def as_tool(
    bus: Any,
    topic: str,
    *,
    name: str | None = None,
    description: str | None = None,
) -> Any:
    """Return an openai-agents tool that publishes a dict payload onto ``topic``.

    Raises :class:`ImportError` if the ``[openai]`` extra isn't installed.
    """
    try:
        from agents import function_tool
    except ImportError as e:  # pragma: no cover — guarded by env in tests
        raise ImportError(
            "openai_agents adapter requires the [openai] extra: "
            "pip install 'aetherbus[openai]'"
        ) from e

    tool_name = name or topic.replace(".", "_").replace("-", "_")
    tool_description = description or (
        f"Publish a JSON payload onto AetherBus topic {topic!r}. "
        "Returns the event id."
    )

    @function_tool(
        name_override=tool_name,
        description_override=tool_description,
    )
    async def _publish(payload: dict[str, Any]) -> str:
        return await bus.publish(topic, payload=payload)

    return _publish
