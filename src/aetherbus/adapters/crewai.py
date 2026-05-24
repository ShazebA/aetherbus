"""CrewAI Flows adapter — decorator + wire helper.

Usage::

    from crewai.flow import Flow
    from aetherbus.adapters.crewai import bus_listen, wire_bus_listeners

    class CodeFlow(Flow):
        def __init__(self, bus):
            super().__init__()
            self.bus = bus

        @bus_listen(bus, "dev.aetherbus.code.plan.v1")
        async def implement(self, event):
            spec = await event.resolve()
            ...

    flow = CodeFlow(bus)
    wire_bus_listeners(flow)
    await bus.run()

The decorator is metadata-only — actual ``bus.subscribe`` registration happens
in :func:`wire_bus_listeners`, which is called after the Flow instance exists
so ``self`` is bound. CrewAI Flow integration stays loose; we don't import
``crewai`` here, so this adapter works against any class that has decorated
methods.
"""
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

# Sentinel attribute names — set by @bus_listen, read by wire_bus_listeners.
_TOPIC_ATTR = "_aetherbus_topic"
_QUEUE_ATTR = "_aetherbus_queue_group"
_BUS_ATTR = "_aetherbus_bus"

Handler = Callable[..., Awaitable[None]]


def bus_listen(
    bus: Any,
    topic: str,
    *,
    queue_group: str | None = None,
) -> Callable[[Handler], Handler]:
    """Mark a Flow method as an AetherBus subscriber.

    The wire happens at :func:`wire_bus_listeners` time, not at decoration —
    methods need a bound ``self`` first.
    """

    def deco(fn: Handler) -> Handler:
        setattr(fn, _TOPIC_ATTR, topic)
        setattr(fn, _QUEUE_ATTR, queue_group)
        setattr(fn, _BUS_ATTR, bus)
        return fn

    return deco


def wire_bus_listeners(flow_instance: Any, bus: Any | None = None) -> list[str]:
    """Walk ``flow_instance`` and subscribe every ``@bus_listen``-decorated method.

    Returns the list of topics that got wired so callers can log / assert.
    The optional ``bus`` overrides the one captured at decoration time —
    handy if you build the Flow class before the Bus exists.
    """
    wired: list[str] = []
    for name in dir(flow_instance):
        method = getattr(flow_instance, name, None)
        if method is None or not callable(method):
            continue
        topic = getattr(method, _TOPIC_ATTR, None)
        if not topic:
            continue
        target_bus = bus if bus is not None else getattr(method, _BUS_ATTR)
        queue_group = getattr(method, _QUEUE_ATTR, None)
        target_bus.subscribe(topic, queue_group=queue_group)(method)
        wired.append(topic)
    return wired
