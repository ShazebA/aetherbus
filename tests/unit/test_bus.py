from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime
from pathlib import Path

import pytest

from aetherbus import Bus, EventPayload
from aetherbus.config import Settings
from aetherbus.envelope import Envelope, EnvelopeError
from aetherbus.transports.base import (
    DeliverPolicy,
    Msg,
    Subscription,
    TransportError,
)


class _FakeSub:
    async def unsubscribe(self) -> None:
        return None


class FakeTransport:
    def __init__(self, *, fail_publish: bool = False) -> None:
        self.connected = False
        self.streams: list[tuple[str, list[str]]] = []
        self.published: list[tuple[str, bytes, dict[str, str]]] = []
        self.subscriptions: list[dict[str, object]] = []
        self.fail_publish = fail_publish
        self._cb: Callable[[Msg], Awaitable[None]] | None = None

    async def connect(self) -> None:
        self.connected = True

    async def close(self) -> None:
        self.connected = False

    async def ensure_stream(self, name: str, subjects: list[str]) -> None:
        self.streams.append((name, subjects))

    async def publish(self, topic: str, payload: bytes, headers: dict[str, str]) -> None:
        if self.fail_publish:
            raise TransportError("simulated outage")
        self.published.append((topic, payload, headers))

    async def subscribe(
        self,
        topic: str,
        *,
        durable: str,
        queue_group: str | None,
        max_in_flight: int,
        deliver_policy: DeliverPolicy,
        opt_start_time: datetime | None,
        callback: Callable[[Msg], Awaitable[None]],
    ) -> Subscription:
        self.subscriptions.append(
            {
                "topic": topic,
                "durable": durable,
                "queue_group": queue_group,
                "max_in_flight": max_in_flight,
                "deliver_policy": deliver_policy,
                "opt_start_time": opt_start_time,
            }
        )
        self._cb = callback
        return _FakeSub()

    async def deliver(self, payload: bytes, headers: dict[str, str] | None = None) -> None:
        assert self._cb is not None
        acks: list[str] = []

        async def _ack() -> None:
            acks.append("ack")

        async def _nack() -> None:
            acks.append("nack")

        await self._cb(Msg(data=payload, headers=headers or {}, _ack_fn=_ack, _nack_fn=_nack))
        self.last_acks = acks


class Ping(EventPayload):
    n: int


def _settings(tmp_path: Path, **over: object) -> Settings:
    base = {
        "transport_url": "nats://test",
        "outbox_path": str(tmp_path / "ob.sqlite"),
        "stream_subjects": ("test.>",),
    }
    base.update(over)
    return Settings(**base)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_publish_payload_and_dataref_together_rejected(tmp_path: Path) -> None:
    bus = Bus(_settings(tmp_path), transport=FakeTransport())
    with pytest.raises(ValueError):
        await bus.publish("test.t.v1", payload={"a": 1}, dataref="mcp://x")


@pytest.mark.asyncio
async def test_publish_dataref_only_succeeds(tmp_path: Path) -> None:
    ft = FakeTransport()
    bus = Bus(_settings(tmp_path), transport=ft)
    await bus.connect()
    eid = await bus.publish("test.t.v1", dataref="mcp://srv/abc")
    assert eid
    assert len(ft.published) == 1


@pytest.mark.asyncio
async def test_publish_inline_size_limit(tmp_path: Path) -> None:
    bus = Bus(_settings(tmp_path, inline_size_limit=200), transport=FakeTransport())
    await bus.connect()
    with pytest.raises(EnvelopeError):
        await bus.publish("test.t.v1", payload={"blob": "x" * 1000})


@pytest.mark.asyncio
async def test_publish_routes_to_outbox_on_transport_failure(tmp_path: Path) -> None:
    ft = FakeTransport(fail_publish=True)
    bus = Bus(_settings(tmp_path), transport=ft)
    await bus.connect()
    eid = await bus.publish("test.t.v1", payload={"n": 1})
    assert eid  # publish returns success — pointer pattern applies to "did it happen"
    assert await bus._outbox.pending_count() == 1


@pytest.mark.asyncio
async def test_subscribe_dispatches_to_handler(tmp_path: Path) -> None:
    ft = FakeTransport()
    bus = Bus(_settings(tmp_path), transport=ft)
    bus.register_schema("test.ping.v1", Ping)

    received: list[Ping] = []

    @bus.subscribe("test.ping.v1")
    async def on_ping(event):
        received.append(await event.resolve(as_=Ping))

    run_task = asyncio.create_task(bus.run())
    try:
        # let bus.run() wire the subscription
        for _ in range(20):
            if ft.subscriptions:
                break
            await asyncio.sleep(0.01)

        env = Envelope.new(source="x", type="test.ping.v1", data={"n": 42})
        await ft.deliver(env.to_bytes())

        assert received == [Ping(n=42)]
        assert ft.last_acks == ["ack"]
    finally:
        await bus.close()
        await asyncio.wait_for(run_task, timeout=2)


@pytest.mark.asyncio
async def test_handler_exception_results_in_nack(tmp_path: Path) -> None:
    ft = FakeTransport()
    bus = Bus(_settings(tmp_path), transport=ft)

    @bus.subscribe("test.bad.v1")
    async def boom(event):
        raise RuntimeError("expected")

    run_task = asyncio.create_task(bus.run())
    try:
        for _ in range(20):
            if ft.subscriptions:
                break
            await asyncio.sleep(0.01)
        env = Envelope.new(source="x", type="test.bad.v1", data={"n": 1})
        await ft.deliver(env.to_bytes())
        assert ft.last_acks == ["nack"]
    finally:
        await bus.close()
        await asyncio.wait_for(run_task, timeout=2)


@pytest.mark.asyncio
async def test_subscribe_validates_replay_from(tmp_path: Path) -> None:
    bus = Bus(_settings(tmp_path), transport=FakeTransport())
    with pytest.raises(ValueError):
        @bus.subscribe("test.t.v1", replay_from="some-day")
        async def _(event):
            return None


def test_from_env_uses_env_vars(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AETHERBUS_URL", "nats://test-host:1234")
    monkeypatch.delenv("AETHERBUS_SOURCE", raising=False)
    bus = Bus.from_env()
    assert bus._settings.transport_url == "nats://test-host:1234"
    assert bus._settings.source.startswith("aetherbus://agent/")


def test_from_env_honors_explicit_source(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AETHERBUS_SOURCE", "aetherbus://agent/foo")
    bus = Bus.from_env()
    assert bus._settings.source == "aetherbus://agent/foo"
