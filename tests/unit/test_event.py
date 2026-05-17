from __future__ import annotations

import pytest

from aetherbus import EventPayload
from aetherbus.envelope import Envelope
from aetherbus.event import Event


class Greeting(EventPayload):
    text: str
    n: int


@pytest.mark.asyncio
async def test_resolve_returns_inline_dict_by_default() -> None:
    env = Envelope.new(source="x", type="t.v1", data={"text": "hi", "n": 2})
    event = Event(envelope=env)
    val = await event.resolve()
    assert val == {"text": "hi", "n": 2}


@pytest.mark.asyncio
async def test_resolve_with_model_validates() -> None:
    env = Envelope.new(source="x", type="t.v1", data={"text": "hi", "n": 2})
    event = Event(envelope=env)
    g = await event.resolve(as_=Greeting)
    assert isinstance(g, Greeting)
    assert g.text == "hi"
    assert g.n == 2


@pytest.mark.asyncio
async def test_resolve_dataref_raises_not_implemented() -> None:
    env = Envelope.new(source="x", type="t.v1", dataref="mcp://server/abc")
    event = Event(envelope=env)
    with pytest.raises(NotImplementedError):
        await event.resolve()


def test_event_exposes_envelope_fields() -> None:
    env = Envelope.new(
        source="aetherbus://agent/test",
        type="t.v1",
        data={"n": 1},
        correlation_id="abc",
        depends_on=["e1"],
    )
    event = Event(envelope=env)
    assert event.id == env.id
    assert event.source == "aetherbus://agent/test"
    assert event.type == "t.v1"
    assert event.correlation_id == "abc"
    assert event.depends_on == ["e1"]
    assert event.dataref is None
    assert event.inline_data == {"n": 1}
