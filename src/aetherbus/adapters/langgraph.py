"""LangGraph adapter — a graph node that publishes state onto the bus.

Usage::

    from aetherbus.adapters.langgraph import bus_node

    graph.add_node(
        "publish_plan",
        bus_node(bus, topic="dev.aetherbus.code.plan.v1"),
    )

The node passes state through unchanged and stamps the published event id at
``state["_aetherbus_event_id"]`` so downstream nodes can correlate.

LangGraph itself isn't imported — the adapter only needs ``state`` to be a
dict or attr-accessible object. That keeps the surface stable across the rapid
LangGraph API churn and avoids pulling the framework into the default install.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

LangGraphState = Any  # opaque — LangGraph passes whatever the user typed


def bus_node(
    bus: Any,
    topic: str,
    *,
    payload_key: str = "payload",
    pass_through: bool = True,
) -> Callable[[LangGraphState], Awaitable[LangGraphState | dict[str, Any]]]:
    """Build a LangGraph node that publishes ``state[payload_key]`` onto ``topic``.

    Parameters:
        bus: An ``aetherbus.Bus`` instance.
        topic: AetherBus topic to publish on.
        payload_key: Where in ``state`` to find the payload (dict key or attr).
        pass_through: If ``True`` (default) return the input state plus the
            published event id at ``_aetherbus_event_id``. If ``False``, return
            only ``{"_aetherbus_event_id": ...}`` — useful when the graph
            merges partial dicts and you don't want to echo state.
    """

    async def _node(state: LangGraphState) -> LangGraphState | dict[str, Any]:
        if isinstance(state, dict):
            payload = state.get(payload_key)
        else:
            payload = getattr(state, payload_key, None)
        if payload is None:
            raise KeyError(
                f"bus_node({topic!r}): state has no {payload_key!r} (got "
                f"{list(state) if isinstance(state, dict) else type(state).__name__})"
            )
        event_id = await bus.publish(topic, payload=payload)
        if not pass_through:
            return {"_aetherbus_event_id": event_id}
        if isinstance(state, dict):
            return {**state, "_aetherbus_event_id": event_id}
        # Non-dict state — return the partial dict; LangGraph merges it.
        return {"_aetherbus_event_id": event_id}

    return _node
