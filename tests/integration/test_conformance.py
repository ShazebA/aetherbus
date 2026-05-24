from __future__ import annotations

import asyncio

import pytest

from aetherbus import EventPayload

pytestmark = pytest.mark.integration


class Ping(EventPayload):
    n: int


@pytest.mark.asyncio
async def test_roundtrip(bus_factory, topic_ns) -> None:
    bus = bus_factory()
    topic = f"{topic_ns}.ping.v1"
    bus.register_schema(topic, Ping)

    received: list[Ping] = []
    done = asyncio.Event()

    @bus.subscribe(topic)
    async def handler(event):
        received.append(await event.resolve(as_=Ping))
        done.set()

    run_task = asyncio.create_task(bus.run())
    try:
        await asyncio.sleep(0.3)  # let subscription attach
        await bus.publish(topic, payload=Ping(n=42))
        await asyncio.wait_for(done.wait(), timeout=5)
        assert received == [Ping(n=42)]
    finally:
        await bus.close()
        await asyncio.wait_for(run_task, timeout=3)


@pytest.mark.asyncio
async def test_queue_group_exactly_one_consumer(bus_factory, topic_ns) -> None:
    bus_a = bus_factory()
    bus_b = bus_factory()
    topic = f"{topic_ns}.work.v1"

    counts = {"a": 0, "b": 0}
    received = asyncio.Event()

    @bus_a.subscribe(topic, queue_group="workers")
    async def handler_a(event):
        counts["a"] += 1
        received.set()

    @bus_b.subscribe(topic, queue_group="workers")
    async def handler_b(event):
        counts["b"] += 1
        received.set()

    run_a = asyncio.create_task(bus_a.run())
    run_b = asyncio.create_task(bus_b.run())
    try:
        await asyncio.sleep(0.5)
        await bus_a.publish(topic, payload={"job": 1})
        await asyncio.wait_for(received.wait(), timeout=5)
        await asyncio.sleep(0.3)  # window for an accidental second delivery
        assert counts["a"] + counts["b"] == 1
    finally:
        await bus_a.close()
        await bus_b.close()
        await asyncio.wait_for(run_a, timeout=3)
        await asyncio.wait_for(run_b, timeout=3)


@pytest.mark.asyncio
async def test_bus_replay_iterates_history(bus_factory, topic_ns) -> None:
    """``bus.replay()`` yields stored events on both NATS and Kafka."""
    topic = f"{topic_ns}.history.v1"

    pub_bus = bus_factory()
    async with pub_bus:
        for n in range(4):
            await pub_bus.publish(topic, payload={"n": n})

    sub_bus = bus_factory()
    seen: list[int] = []
    async with sub_bus:
        async for ev in sub_bus.replay(topic, idle_timeout=2.0):
            seen.append(ev.inline_data["n"])
            if len(seen) >= 4:
                break

    assert sorted(seen) == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_replay_into_same_broker_terminates(bus_factory, topic_ns) -> None:
    """Same-broker sandbox replay must not feedback-loop on its own re-emissions.

    Regression for the JetStream pull-consumer bug: history() needs to snapshot
    the end-of-stream BEFORE iterating so events we publish during the iteration
    don't extend the loop. Kafka uses end_offsets; NATS pins to stream.state.last_seq.
    """
    topic = f"{topic_ns}.feedback.v1"

    # Publish exactly one event into the broker.
    pub_bus = bus_factory()
    async with pub_bus:
        await pub_bus.publish(topic, payload={"only": "event"})

    # Source bus replays history into a sandbox pointed at the *same* broker.
    # Without the snapshot fix this loops indefinitely (or until JetStream's
    # max_msgs cap), publishing geometric copies.
    src_bus = bus_factory()
    sandbox_bus = bus_factory()

    replayed: list[str] = []
    async with sandbox_bus:
        async for ev in src_bus.replay(topic, into=sandbox_bus, idle_timeout=1.0):
            replayed.append(ev.id)
            # Abort fast if the bug ever resurfaces; without this the test
            # would hang on idle_timeout each iteration.
            if len(replayed) > 5:
                pytest.fail(
                    f"replay did not terminate at end-of-history snapshot; "
                    f"yielded {len(replayed)}+ events after publishing 1"
                )

    assert len(replayed) == 1


@pytest.mark.asyncio
async def test_replay_from_earliest(bus_factory, topic_ns) -> None:
    # Publish first (with a transient bus), then subscribe with replay_from=earliest.
    # The shared stream covers `test.>`; the unique topic_ns keeps this test's
    # events isolated from any other test's via consumer filter_subject.
    topic = f"{topic_ns}.replay.v1"

    pub_bus = bus_factory()
    async with pub_bus:
        for n in range(3):
            await pub_bus.publish(topic, payload={"n": n})

    sub_bus = bus_factory()
    received: list[dict] = []
    enough = asyncio.Event()

    @sub_bus.subscribe(topic, replay_from="earliest")
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
