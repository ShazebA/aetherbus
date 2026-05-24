"""CrewAI adapter — bus_listen decorator + wire_bus_listeners helper.

Uses a fake Bus with the same surface as the real one. No CrewAI install.
"""
from __future__ import annotations

from typing import Any

from aetherbus.adapters.crewai import bus_listen, wire_bus_listeners


class _FakeBus:
    def __init__(self) -> None:
        self.subscriptions: list[tuple[str, str | None, Any]] = []

    def subscribe(self, topic: str, *, queue_group: str | None = None):
        def deco(fn):
            self.subscriptions.append((topic, queue_group, fn))
            return fn
        return deco


def test_bus_listen_stamps_metadata() -> None:
    bus = _FakeBus()

    class _Flow:
        @bus_listen(bus, "t.plan.v1")
        async def handle(self, event):
            return None

    method = _Flow.handle
    assert method._aetherbus_topic == "t.plan.v1"
    assert method._aetherbus_queue_group is None
    assert method._aetherbus_bus is bus


def test_wire_bus_listeners_subscribes_each_method() -> None:
    bus = _FakeBus()

    class _Flow:
        @bus_listen(bus, "t.plan.v1")
        async def implement(self, event):
            return None

        @bus_listen(bus, "t.diff.v1", queue_group="testers")
        async def test_it(self, event):
            return None

        async def unrelated(self, event):
            return None

    flow = _Flow()
    wired = wire_bus_listeners(flow)
    topics = sorted(wired)
    assert topics == ["t.diff.v1", "t.plan.v1"]
    sub_topics = sorted(t for t, _, _ in bus.subscriptions)
    assert sub_topics == ["t.diff.v1", "t.plan.v1"]
    # Queue group is forwarded.
    diff_sub = next(s for s in bus.subscriptions if s[0] == "t.diff.v1")
    assert diff_sub[1] == "testers"


def test_wire_bus_listeners_bus_override() -> None:
    bus_a = _FakeBus()
    bus_b = _FakeBus()

    class _Flow:
        @bus_listen(bus_a, "t.v1")
        async def handle(self, event):
            return None

    wire_bus_listeners(_Flow(), bus=bus_b)
    assert len(bus_b.subscriptions) == 1
    assert bus_a.subscriptions == []


def test_wire_bus_listeners_with_no_marked_methods_is_noop() -> None:
    bus = _FakeBus()

    class _Empty:
        async def nothing(self, event):
            return None

    assert wire_bus_listeners(_Empty(), bus=bus) == []
    assert bus.subscriptions == []
