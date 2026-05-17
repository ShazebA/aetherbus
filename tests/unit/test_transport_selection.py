"""Bus picks a Transport based on settings.transport_type. No broker contact."""
from __future__ import annotations

from pathlib import Path

import pytest

from aetherbus import Bus
from aetherbus.config import Settings
from aetherbus.transports.kafka import KafkaTransport, _has_wildcard, _normalize_url
from aetherbus.transports.nats import NATSTransport


def _settings(tmp_path: Path, **over: object) -> Settings:
    base = {
        "transport_url": "irrelevant",
        "outbox_path": str(tmp_path / "ob.sqlite"),
    }
    base.update(over)
    return Settings(**base)  # type: ignore[arg-type]


def test_default_transport_is_nats(tmp_path: Path) -> None:
    bus = Bus(_settings(tmp_path, transport_url="nats://localhost:4222"))
    assert isinstance(bus._transport, NATSTransport)


def test_kafka_selected_when_type_is_kafka(tmp_path: Path) -> None:
    bus = Bus(_settings(tmp_path, transport_type="kafka", transport_url="localhost:9092"))
    assert isinstance(bus._transport, KafkaTransport)


def test_unknown_transport_type_errors(tmp_path: Path) -> None:
    with pytest.raises(ValueError) as ei:
        Bus(_settings(tmp_path, transport_type="redis"))
    assert "redis" in str(ei.value)


def test_normalize_url_strips_kafka_scheme() -> None:
    assert _normalize_url("kafka://broker:9092") == "broker:9092"
    assert _normalize_url("broker:9092") == "broker:9092"


def test_has_wildcard_detects_star_and_gt() -> None:
    assert _has_wildcard("a.b.*")
    assert _has_wildcard("a.b.>")
    assert _has_wildcard("a.>")
    assert not _has_wildcard("a.b.c.v1")
