"""bus.replay() — unit tests against a fake transport with canned history."""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from aetherbus import Bus
from aetherbus.config import Settings
from aetherbus.envelope import Envelope
from aetherbus.transports.base import (
    Msg,
    Subscription,
    TransportError,
)


class _FakeSub:
    async def unsubscribe(self) -> None:
        return None


class HistoryFakeTransport:
    """Fake transport with a deterministic ``history`` iterator."""

    def __init__(self, msgs: list[Msg] | None = None, *, fail_publish: bool = False) -> None:
        self.msgs = msgs or []
        self.fail_publish = fail_publish
        self.published: list[tuple[str, bytes, dict[str, str]]] = []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def ensure_stream(self, name: str, subjects: list[str]) -> None:
        return None

    async def publish(self, topic: str, payload: bytes, headers: dict[str, str]) -> None:
        if self.fail_publish:
            raise TransportError("simulated")
        self.published.append((topic, payload, headers))

    async def subscribe(self, *args: Any, **kwargs: Any) -> Subscription:
        return _FakeSub()

    async def history(
        self,
        topic: str,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        idle_timeout: float = 1.0,
    ) -> AsyncIterator[Msg]:
        for msg in self.msgs:
            yield msg


def _msg_for(env: Envelope) -> Msg:
    async def _noop() -> None:
        return None

    return Msg(data=env.to_bytes(), headers={}, _ack_fn=_noop, _nack_fn=_noop)


def _settings(tmp_path: Path, *, suffix: str = "src") -> Settings:
    return Settings(
        transport_url="nats://unused",
        outbox_path=str(tmp_path / f"ob_{suffix}.sqlite"),
        stream_subjects=("test.>",),
    )


@pytest.mark.asyncio
async def test_replay_yields_events_in_order(tmp_path: Path) -> None:
    envs = [
        Envelope.new(source="x", type="t.v1", data={"n": i})
        for i in range(3)
    ]
    ft = HistoryFakeTransport([_msg_for(e) for e in envs])
    bus = Bus(_settings(tmp_path), transport=ft)

    seen: list[int] = []
    async for ev in bus.replay("t.v1"):
        seen.append(ev.inline_data["n"])
    assert seen == [0, 1, 2]


@pytest.mark.asyncio
async def test_replay_into_sandbox_republishes_raw_bytes(tmp_path: Path) -> None:
    env = Envelope.new(source="x", type="t.v1", data={"hello": "world"})
    src = HistoryFakeTransport([_msg_for(env)])
    sandbox = HistoryFakeTransport()

    src_bus = Bus(_settings(tmp_path), transport=src)
    sandbox_bus = Bus(_settings(tmp_path, suffix="sandbox"), transport=sandbox)

    collected: list[str] = []
    async for ev in src_bus.replay("t.v1", into=sandbox_bus):
        collected.append(ev.id)

    assert collected == [env.id]
    # Sandbox transport received the raw envelope bytes verbatim — same id,
    # same source, same time, no remint.
    assert len(sandbox.published) == 1
    topic, payload, _headers = sandbox.published[0]
    assert topic == "t.v1"
    assert Envelope.from_bytes(payload).id == env.id


@pytest.mark.asyncio
async def test_replay_sandbox_publish_failure_writes_to_outbox(tmp_path: Path) -> None:
    env = Envelope.new(source="x", type="t.v1", data={"a": 1})
    src = HistoryFakeTransport([_msg_for(env)])
    sandbox = HistoryFakeTransport(fail_publish=True)

    src_bus = Bus(_settings(tmp_path), transport=src)
    sandbox_bus = Bus(_settings(tmp_path, suffix="sandbox"), transport=sandbox)

    async for _ in src_bus.replay("t.v1", into=sandbox_bus):
        pass

    assert await sandbox_bus._outbox.pending_count() == 1


@pytest.mark.asyncio
async def test_replay_stops_on_caller_break(tmp_path: Path) -> None:
    envs = [Envelope.new(source="x", type="t.v1", data={"n": i}) for i in range(10)]
    ft = HistoryFakeTransport([_msg_for(e) for e in envs])
    bus = Bus(_settings(tmp_path), transport=ft)

    seen: list[int] = []
    async for ev in bus.replay("t.v1"):
        seen.append(ev.inline_data["n"])
        if len(seen) == 2:
            break
    assert seen == [0, 1]
