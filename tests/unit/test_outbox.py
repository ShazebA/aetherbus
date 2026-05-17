from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from aetherbus.outbox import Outbox, OutboxRow


async def _drain_for(outbox: Outbox, publish, on_failure=None, seconds: float = 0.5) -> None:
    task = asyncio.create_task(outbox.run(publish, on_failure))
    try:
        await asyncio.sleep(seconds)
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_enqueue_then_drain_delivers(tmp_path: Path) -> None:
    ob = Outbox(tmp_path / "ob.sqlite", initial_backoff=0.01)
    await ob.enqueue("evt-1", "test.t.v1", b"payload", {"h": "v"})

    delivered: list[tuple[str, bytes, dict[str, str]]] = []

    async def pub(topic: str, payload: bytes, headers: dict[str, str]) -> None:
        delivered.append((topic, payload, headers))

    await _drain_for(ob, pub, seconds=0.3)

    assert delivered == [("test.t.v1", b"payload", {"h": "v"})]
    assert await ob.pending_count() == 0


@pytest.mark.asyncio
async def test_transient_failure_then_success(tmp_path: Path) -> None:
    ob = Outbox(tmp_path / "ob.sqlite", initial_backoff=0.01, max_attempts=5)
    await ob.enqueue("evt-1", "t.v1", b"p", {})

    attempts: list[int] = []

    async def pub(topic: str, payload: bytes, headers: dict[str, str]) -> None:
        attempts.append(1)
        if len(attempts) < 3:
            raise RuntimeError("transient")

    await _drain_for(ob, pub, seconds=1.0)

    assert len(attempts) >= 3  # at least one success after retries
    assert await ob.pending_count() == 0


@pytest.mark.asyncio
async def test_max_attempts_calls_on_failure_then_drops(tmp_path: Path) -> None:
    ob = Outbox(tmp_path / "ob.sqlite", initial_backoff=0.01, max_attempts=2)
    await ob.enqueue("evt-1", "t.v1", b"p", {})

    async def pub(*_: object, **__: object) -> None:
        raise RuntimeError("permanent")

    failed: list[OutboxRow] = []

    async def on_failure(row: OutboxRow) -> None:
        failed.append(row)

    await _drain_for(ob, pub, on_failure, seconds=0.6)

    assert len(failed) == 1
    assert failed[0].id == "evt-1"
    assert await ob.pending_count() == 0
