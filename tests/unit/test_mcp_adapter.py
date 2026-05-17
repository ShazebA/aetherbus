"""Unit tests for the MCP adapter — exercise the bridge without speaking MCP.

A real Claude Desktop / MCP Inspector roundtrip is the manual verification step
for M3 (PRD §7.1 M3 deliverable). These tests cover the bus → adapter wiring:
events update `_latest` and trigger `send_resource_updated` when a session
is attached.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pytest
from pydantic import AnyUrl

from aetherbus import EventPayload
from aetherbus.bus import Bus
from aetherbus.config import Settings
from aetherbus.envelope import Envelope
from aetherbus.mcp_adapter import URI_PREFIX, AetherBusMCPServer
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
    """Minimal transport that lets a test deliver framed envelopes to handlers."""

    def __init__(self) -> None:
        self._cbs: dict[str, Any] = {}

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def ensure_stream(self, name: str, subjects: list[str]) -> None:
        return None

    async def publish(self, topic: str, payload: bytes, headers: dict[str, str]) -> None:
        raise TransportError("publish not used in this test")

    async def subscribe(
        self,
        topic: str,
        *,
        durable: str,
        queue_group: str | None,
        max_in_flight: int,
        deliver_policy: DeliverPolicy,
        opt_start_time: Any,
        callback: Any,
    ) -> Subscription:
        self._cbs[topic] = callback
        return _FakeSub()

    async def deliver_to(self, topic: str, envelope_bytes: bytes) -> None:
        cb = self._cbs[topic]
        acked: list[str] = []

        async def _ack() -> None:
            acked.append("ack")

        async def _nack() -> None:
            acked.append("nack")

        await cb(Msg(data=envelope_bytes, headers={}, _ack_fn=_ack, _nack_fn=_nack))


class Ping(EventPayload):
    n: int


def _settings(tmp_path: Path) -> Settings:
    return Settings(
        transport_url="nats://unused",
        outbox_path=str(tmp_path / "ob.sqlite"),
        stream_subjects=("test.>",),
    )


class _FakeSession:
    """Stand-in for `mcp.server.session.ServerSession` — captures notifications."""

    def __init__(self) -> None:
        self.updated: list[str] = []

    async def send_resource_updated(self, uri: AnyUrl) -> None:
        self.updated.append(str(uri))


@pytest.mark.asyncio
async def test_event_arrives_caches_latest_and_pushes_notification(tmp_path: Path) -> None:
    ft = FakeTransport()
    bus = Bus(_settings(tmp_path), transport=ft)
    topic = "test.research.finding.v1"
    bus.register_schema(topic, Ping)
    mcp_server = AetherBusMCPServer(bus, [topic])

    fake_session = _FakeSession()
    mcp_server._session = fake_session  # type: ignore[assignment]

    run_task = asyncio.create_task(bus.run())
    try:
        # Wait for bus to register the subscription on the fake transport.
        for _ in range(20):
            if topic in ft._cbs:
                break
            await asyncio.sleep(0.01)

        env = Envelope.new(source="x", type=topic, data={"n": 7})
        await ft.deliver_to(topic, env.to_bytes())

        # Bus dispatches in a task — give it a beat to settle.
        for _ in range(20):
            if topic in mcp_server._latest:
                break
            await asyncio.sleep(0.01)

        assert mcp_server._latest[topic].id == env.id
        assert fake_session.updated == [f"{URI_PREFIX}{topic}"]
    finally:
        await bus.close()
        await asyncio.wait_for(run_task, timeout=3)


@pytest.mark.asyncio
async def test_event_without_attached_session_is_silently_skipped(tmp_path: Path) -> None:
    ft = FakeTransport()
    bus = Bus(_settings(tmp_path), transport=ft)
    topic = "test.silent.v1"
    mcp_server = AetherBusMCPServer(bus, [topic])
    # _session is None by default — no client attached yet.

    run_task = asyncio.create_task(bus.run())
    try:
        for _ in range(20):
            if topic in ft._cbs:
                break
            await asyncio.sleep(0.01)

        env = Envelope.new(source="x", type=topic, data={"k": "v"})
        await ft.deliver_to(topic, env.to_bytes())

        for _ in range(20):
            if topic in mcp_server._latest:
                break
            await asyncio.sleep(0.01)

        # Latest still updates so a late-attaching client sees current state on
        # first resources/read.
        assert mcp_server._latest[topic].id == env.id
    finally:
        await bus.close()
        await asyncio.wait_for(run_task, timeout=3)


@pytest.mark.asyncio
async def test_session_failure_does_not_break_bus_subscriber(tmp_path: Path) -> None:
    """An MCP client disconnection must not kill the underlying bus subscriber."""
    ft = FakeTransport()
    bus = Bus(_settings(tmp_path), transport=ft)
    topic = "test.flaky.v1"
    mcp_server = AetherBusMCPServer(bus, [topic])

    class _BoomSession:
        async def send_resource_updated(self, uri: AnyUrl) -> None:
            raise RuntimeError("client gone")

    mcp_server._session = _BoomSession()  # type: ignore[assignment]

    run_task = asyncio.create_task(bus.run())
    try:
        for _ in range(20):
            if topic in ft._cbs:
                break
            await asyncio.sleep(0.01)

        env = Envelope.new(source="x", type=topic, data={"a": 1})
        await ft.deliver_to(topic, env.to_bytes())

        for _ in range(20):
            if topic in mcp_server._latest:
                break
            await asyncio.sleep(0.01)

        # Bus subscriber still latched the event despite the broken session.
        assert mcp_server._latest[topic].id == env.id
    finally:
        await bus.close()
        await asyncio.wait_for(run_task, timeout=3)
