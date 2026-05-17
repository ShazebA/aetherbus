"""Wiring tests: Event.resolve() now actually fetches dataref payloads."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from aetherbus import EventPayload
from aetherbus.envelope import Envelope
from aetherbus.event import Event
from aetherbus.resolver import clear_cache


class Finding(EventPayload):
    query: str
    summary: str


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    clear_cache()


@pytest.mark.asyncio
async def test_resolve_dataref_returns_parsed_json(tmp_path: Path) -> None:
    body = json.dumps({"query": "x", "summary": "y"}).encode()
    target = tmp_path / "payload.json"
    target.write_bytes(body)

    env = Envelope.new(
        source="aetherbus://agent/test",
        type="research.finding.v1",
        dataref=f"file://{target}",
    )
    event = Event(envelope=env)
    result = await event.resolve()
    assert result == {"query": "x", "summary": "y"}


@pytest.mark.asyncio
async def test_resolve_dataref_with_model_validates(tmp_path: Path) -> None:
    target = tmp_path / "f.json"
    target.write_bytes(b'{"query":"q","summary":"s"}')

    env = Envelope.new(
        source="x",
        type="t.v1",
        dataref=f"file://{target}",
    )
    event = Event(envelope=env)
    f = await event.resolve(as_=Finding)
    assert isinstance(f, Finding)
    assert f.query == "q"
    assert f.summary == "s"


@pytest.mark.asyncio
async def test_resolve_dataref_octet_stream_returns_raw_bytes(tmp_path: Path) -> None:
    target = tmp_path / "blob.bin"
    target.write_bytes(b"\x00\x01\x02\x03binary")

    env = Envelope(
        id="01",
        source="x",
        type="t.v1",
        time="2026-01-01T00:00:00+00:00",
        datacontenttype="application/octet-stream",
        dataref=f"file://{target}",
    )
    event = Event(envelope=env)
    result = await event.resolve()
    assert result == b"\x00\x01\x02\x03binary"


@pytest.mark.asyncio
async def test_resolve_inline_path_unchanged() -> None:
    env = Envelope.new(source="x", type="t.v1", data={"a": 1})
    event = Event(envelope=env)
    assert await event.resolve() == {"a": 1}
