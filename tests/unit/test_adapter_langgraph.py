"""LangGraph adapter — exercised against a fake Bus (no LangGraph install)."""
from __future__ import annotations

from typing import Any

import pytest

from aetherbus.adapters.langgraph import bus_node


class _FakeBus:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    async def publish(self, topic: str, *, payload: Any) -> str:
        self.calls.append((topic, payload))
        return f"evt-{len(self.calls)}"


@pytest.mark.asyncio
async def test_bus_node_publishes_dict_payload() -> None:
    bus = _FakeBus()
    node = bus_node(bus, "code.plan.v1")

    state = {"payload": {"steps": ["a", "b"]}, "other": "ignored"}
    result = await node(state)

    assert bus.calls == [("code.plan.v1", {"steps": ["a", "b"]})]
    assert result == {
        "payload": {"steps": ["a", "b"]},
        "other": "ignored",
        "_aetherbus_event_id": "evt-1",
    }


@pytest.mark.asyncio
async def test_bus_node_with_pass_through_false_returns_only_event_id() -> None:
    bus = _FakeBus()
    node = bus_node(bus, "t.v1", pass_through=False)

    result = await node({"payload": {"x": 1}})
    assert result == {"_aetherbus_event_id": "evt-1"}


@pytest.mark.asyncio
async def test_bus_node_custom_payload_key() -> None:
    bus = _FakeBus()
    node = bus_node(bus, "t.v1", payload_key="finding")

    await node({"finding": {"q": "kafka"}, "payload": "decoy"})
    assert bus.calls == [("t.v1", {"q": "kafka"})]


@pytest.mark.asyncio
async def test_bus_node_missing_payload_key_raises() -> None:
    bus = _FakeBus()
    node = bus_node(bus, "t.v1")
    with pytest.raises(KeyError) as ei:
        await node({"other": "stuff"})
    assert "payload" in str(ei.value)
    assert "t.v1" in str(ei.value)


@pytest.mark.asyncio
async def test_bus_node_handles_attr_state() -> None:
    bus = _FakeBus()
    node = bus_node(bus, "t.v1")

    class _State:
        def __init__(self) -> None:
            self.payload = {"x": 9}

    result = await node(_State())
    assert bus.calls == [("t.v1", {"x": 9})]
    # Attr state — return value is the partial dict (LangGraph merges it).
    assert result == {"_aetherbus_event_id": "evt-1"}
