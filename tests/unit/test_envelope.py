from __future__ import annotations

import json

import pytest
from hypothesis import given
from hypothesis import strategies as st

from aetherbus.envelope import Envelope, EnvelopeError, new_id


def test_new_id_is_uuid_v7_shaped() -> None:
    eid = new_id()
    parts = eid.split("-")
    assert len(parts) == 5
    assert [len(p) for p in parts] == [8, 4, 4, 4, 12]
    # version nibble at index 12 of the hex string (== first char of 3rd group)
    assert parts[2].startswith("7")


def test_new_id_is_time_sortable() -> None:
    a = new_id()
    b = new_id()
    assert a < b or a[:8] == b[:8]  # adjacent calls share or order within ms


def test_roundtrip_inline_data() -> None:
    env = Envelope.new(
        source="aetherbus://agent/test",
        type="test.event.v1",
        data={"x": 42, "msg": "hi"},
        correlation_id="trace-abc",
    )
    parsed = Envelope.from_bytes(env.to_bytes())
    assert parsed.id == env.id
    assert parsed.source == env.source
    assert parsed.type == env.type
    assert parsed.data == {"x": 42, "msg": "hi"}
    assert parsed.aetherbuscorrelationid == "trace-abc"


def test_roundtrip_dataref_only() -> None:
    env = Envelope.new(
        source="x",
        type="y.v1",
        dataref="mcp://server/resources/abc",
    )
    parsed = Envelope.from_bytes(env.to_bytes())
    assert parsed.dataref == "mcp://server/resources/abc"
    assert parsed.data is None


def test_both_data_and_dataref_rejected() -> None:
    with pytest.raises(EnvelopeError):
        Envelope(
            id="01",
            source="x",
            type="y",
            time="2026-01-01T00:00:00+00:00",
            data={"a": 1},
            dataref="mcp://x",
        )


def test_depends_on_roundtrip() -> None:
    env = Envelope.new(
        source="x",
        type="y.v1",
        data={"n": 1},
        depends_on=["evt-1", "evt-2"],
    )
    parsed = Envelope.from_bytes(env.to_bytes())
    assert parsed.depends_on == ["evt-1", "evt-2"]


def test_depends_on_empty_when_unset() -> None:
    env = Envelope.new(source="x", type="y.v1", data={"n": 1})
    assert env.depends_on == []


def test_unsupported_specversion_rejected() -> None:
    raw = json.dumps(
        {
            "specversion": "0.3",
            "id": "x",
            "source": "y",
            "type": "z",
            "time": "2026-01-01T00:00:00+00:00",
        }
    ).encode()
    with pytest.raises(EnvelopeError):
        Envelope.from_bytes(raw)


@given(
    payload=st.dictionaries(
        st.text(min_size=1, max_size=20),
        st.one_of(st.integers(), st.text(max_size=100), st.booleans(), st.none()),
        max_size=10,
    )
)
def test_arbitrary_dict_payload_roundtrip(payload: dict[str, object]) -> None:
    env = Envelope.new(source="x", type="y.v1", data=payload)
    parsed = Envelope.from_bytes(env.to_bytes())
    assert parsed.data == payload
