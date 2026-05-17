from __future__ import annotations

import asyncio

import pytest

from aetherbus import EventPayload

pytestmark = pytest.mark.integration


class Ping(EventPayload):
    n: int


@pytest.mark.asyncio
async def test_roundtrip(bus_factory) -> None:
    bus = bus_factory()
    bus.register_schema("test.ping.v1", Ping)

    received: list[Ping] = []
    done = asyncio.Event()

    @bus.subscribe("test.ping.v1")
    async def handler(event):
        received.append(await event.resolve(as_=Ping))
        done.set()

    run_task = asyncio.create_task(bus.run())
    try:
        await asyncio.sleep(0.3)  # let subscription attach
        await bus.publish("test.ping.v1", payload=Ping(n=42))
        await asyncio.wait_for(done.wait(), timeout=5)
        assert received == [Ping(n=42)]
    finally:
        await bus.close()
        await asyncio.wait_for(run_task, timeout=3)


@pytest.mark.asyncio
async def test_queue_group_exactly_one_consumer(bus_factory) -> None:
    bus_a = bus_factory()
    bus_b = bus_factory()

    counts = {"a": 0, "b": 0}
    received = asyncio.Event()

    @bus_a.subscribe("test.work.v1", queue_group="workers")
    async def handler_a(event):
        counts["a"] += 1
        received.set()

    @bus_b.subscribe("test.work.v1", queue_group="workers")
    async def handler_b(event):
        counts["b"] += 1
        received.set()

    run_a = asyncio.create_task(bus_a.run())
    run_b = asyncio.create_task(bus_b.run())
    try:
        await asyncio.sleep(0.5)
        # need a third bus to publish; use bus_a's transport
        await bus_a.publish("test.work.v1", payload={"job": 1})
        await asyncio.wait_for(received.wait(), timeout=5)
        await asyncio.sleep(0.3)  # window for an accidental second delivery
        assert counts["a"] + counts["b"] == 1
    finally:
        await bus_a.close()
        await bus_b.close()
        await asyncio.wait_for(run_a, timeout=3)
        await asyncio.wait_for(run_b, timeout=3)


@pytest.mark.asyncio
async def test_replay_from_earliest(bus_factory) -> None:
    # Publish first (with a transient bus), then subscribe with replay_from=earliest.
    pub_bus = bus_factory(stream_name="TEST_REPLAY")
    async with pub_bus:
        for n in range(3):
            await pub_bus.publish("test.replay.v1", payload={"n": n})

    sub_bus = bus_factory(stream_name="TEST_REPLAY")
    received: list[dict] = []
    enough = asyncio.Event()

    @sub_bus.subscribe("test.replay.v1", replay_from="earliest")
    async def handler(event):
        received.append(await event.resolve())
        if len(received) >= 3:
            enough.set()

    run_task = asyncio.create_task(sub_bus.run())
    try:
        await asyncio.wait_for(enough.wait(), timeout=5)
        ns = sorted(r["n"] for r in received)
        assert ns == [0, 1, 2]
    finally:
        await sub_bus.close()
        await asyncio.wait_for(run_task, timeout=3)
