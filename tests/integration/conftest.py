from __future__ import annotations

import asyncio
import os
import socket
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import nats
import pytest

from aetherbus import Bus
from aetherbus.config import Settings

# One stream covers the whole session — JetStream rejects overlapping subject
# claims across streams, so we share. Tests isolate via unique topic prefixes
# (see `topic_ns` below) so consumer `filter_subject` keeps them apart.
SESSION_STREAM = "TEST_AETHERBUS"
SESSION_SUBJECTS: tuple[str, ...] = ("test.>",)


def _wait_port(host: str, port: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"{host}:{port} not reachable within {timeout}s")


async def _clean_test_streams_async(url: str) -> None:
    """Delete any pre-existing JetStream that overlaps our test subjects.

    JetStream rejects new streams whose subjects overlap an existing one. If a
    prior test run (or a buggy commit) leaked streams claiming `test.>`, the
    session-shared TEST_AETHERBUS stream can't be created. We sweep them here.
    """
    nc = await nats.connect(url)
    try:
        js = nc.jetstream()
        streams = await js.streams_info()
        for s in streams:
            subjects = list(s.config.subjects or [])
            if any(subj.startswith("test.") or subj == "test.>" for subj in subjects):
                try:
                    await js.delete_stream(s.config.name)
                except Exception:
                    pass
    finally:
        await nc.drain()


def _clean_test_streams(url: str) -> None:
    last_err: Exception | None = None
    for _ in range(5):
        try:
            asyncio.run(_clean_test_streams_async(url))
            return
        except Exception as e:
            last_err = e
            time.sleep(0.5)
    raise RuntimeError(f"failed to clean test streams at {url}: {last_err}")


@pytest.fixture(scope="session")
def nats_url() -> Iterator[str]:
    """Reuse `AETHERBUS_TEST_NATS_URL`; else start a one-off NATS container."""
    url = os.environ.get("AETHERBUS_TEST_NATS_URL")
    if url:
        host, port_str = url.removeprefix("nats://").split(":")
        _wait_port(host, int(port_str), timeout=10)
        _clean_test_streams(url)
        yield url
        return

    try:
        from testcontainers.core.container import DockerContainer
    except ImportError:
        pytest.skip(
            "testcontainers not installed; "
            "set AETHERBUS_TEST_NATS_URL=nats://localhost:4222 to use a running NATS"
        )

    container = (
        DockerContainer("nats:latest")
        .with_command("-js")
        .with_exposed_ports(4222)
    )
    container.start()
    try:
        host = container.get_container_host_ip()
        port = int(container.get_exposed_port(4222))
        _wait_port(host, port)
        url = f"nats://{host}:{port}"
        _clean_test_streams(url)
        yield url
    finally:
        container.stop()


@pytest.fixture
def topic_ns() -> str:
    """Per-test topic prefix so durables and filter_subjects don't collide."""
    return f"test.{uuid.uuid4().hex[:12]}"


@pytest.fixture
def bus_factory(nats_url: str, tmp_path: Path):
    """Build Buses on the shared session stream."""
    counter = {"n": 0}

    def _build(**override: object) -> Bus:
        counter["n"] += 1
        defaults: dict[str, object] = {
            "transport_url": nats_url,
            "outbox_path": str(tmp_path / f"ob_{counter['n']}.sqlite"),
            "stream_name": SESSION_STREAM,
            "stream_subjects": SESSION_SUBJECTS,
        }
        defaults.update(override)
        return Bus(Settings(**defaults))  # type: ignore[arg-type]

    return _build
