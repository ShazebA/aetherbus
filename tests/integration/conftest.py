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

# Session-shared NATS JetStream — JetStream rejects overlapping subject claims
# across streams, so we share one. Per-test `topic_ns` keeps consumers isolated
# via filter_subject. Kafka has no stream concept; topics are auto-created.
SESSION_STREAM = "TEST_AETHERBUS"
SESSION_SUBJECTS: tuple[str, ...] = ("test.>",)


# ---------- broker reachability ---------------------------------------------


def _wait_port(host: str, port: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError:
            time.sleep(0.2)
    raise TimeoutError(f"{host}:{port} not reachable within {timeout}s")


# ---------- NATS fixtures ---------------------------------------------------


async def _clean_test_streams_async(url: str) -> None:
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
    """Reuse `AETHERBUS_TEST_NATS_URL`; else spin up a one-off NATS container."""
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


# ---------- Kafka fixture ---------------------------------------------------


@pytest.fixture(scope="session")
def kafka_url() -> Iterator[str | None]:
    """Reuse `AETHERBUS_TEST_KAFKA_URL`; skip if not set (no testcontainer fallback yet).

    Kafka testcontainer images are heavy enough that we don't auto-start one —
    bring up `docker compose up -d kafka` and set the env var.
    """
    url = os.environ.get("AETHERBUS_TEST_KAFKA_URL")
    if not url:
        yield None
        return
    host, port_str = url.split(":")
    _wait_port(host, int(port_str), timeout=30)
    yield url


# ---------- transport parametrization ---------------------------------------


@pytest.fixture(params=["nats", "kafka"])
def transport(request: pytest.FixtureRequest, nats_url: str, kafka_url: str | None
              ) -> tuple[str, str]:
    """Parametrized fixture: every test runs against both transports.

    Skips the kafka branch when `AETHERBUS_TEST_KAFKA_URL` isn't set so the
    NATS-only flow stays green for contributors without a Kafka container.
    """
    if request.param == "kafka":
        if kafka_url is None:
            pytest.skip(
                "AETHERBUS_TEST_KAFKA_URL not set; "
                "run `docker compose up -d kafka` and export the URL to enable Kafka conformance"
            )
        return ("kafka", kafka_url)
    return ("nats", nats_url)


# ---------- per-test scaffolding --------------------------------------------


@pytest.fixture
def topic_ns() -> str:
    """Per-test topic prefix so durables / consumer groups / filter_subjects don't collide."""
    return f"test.{uuid.uuid4().hex[:12]}"


@pytest.fixture
def bus_factory(transport: tuple[str, str], tmp_path: Path):
    """Build Buses on whichever transport this test is parametrized over."""
    kind, url = transport
    counter = {"n": 0}

    def _build(**override: object) -> Bus:
        counter["n"] += 1
        defaults: dict[str, object] = {
            "transport_type": kind,
            "transport_url": url,
            "outbox_path": str(tmp_path / f"ob_{counter['n']}.sqlite"),
            "stream_name": SESSION_STREAM,
            "stream_subjects": SESSION_SUBJECTS,
        }
        defaults.update(override)
        return Bus(Settings(**defaults))  # type: ignore[arg-type]

    return _build
