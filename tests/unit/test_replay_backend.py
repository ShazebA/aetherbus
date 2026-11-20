"""Replay UI backend — REST surface tests against a fake Bus."""
from __future__ import annotations

import sys
from collections.abc import AsyncIterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from aetherbus import Bus
from aetherbus.config import Settings
from aetherbus.envelope import Envelope
from aetherbus.replay_ui.backend.app import create_app

# The real app *module*, for monkeypatching the SPA-detection globals. Reached
# via sys.modules because `backend/__init__` rebinds the `app` attribute to the
# FastAPI instance, shadowing the submodule for both attribute and dotted-string
# access.
_app_mod = sys.modules["aetherbus.replay_ui.backend.app"]


class _FakeSub:
    async def unsubscribe(self) -> None:
        return None


class StubTransport:
    """Fake transport: canned ``history`` iterator and no-op publish/subscribe."""

    def __init__(self, envelopes: list[Envelope] | None = None) -> None:
        self.envelopes = envelopes or []

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def ensure_stream(self, name: str, subjects: list[str]) -> None:
        return None

    async def publish(self, *args: Any, **kwargs: Any) -> None:
        return None

    async def subscribe(self, *args: Any, **kwargs: Any) -> Any:
        return _FakeSub()

    async def history(
        self,
        topic: str,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        idle_timeout: float = 1.0,
    ) -> AsyncIterator[Any]:
        from aetherbus.transports.base import Msg

        async def _noop() -> None:
            return None

        for env in self.envelopes:
            yield Msg(data=env.to_bytes(), headers={}, _ack_fn=_noop, _nack_fn=_noop)


@pytest.fixture
def client(tmp_path: Path) -> TestClient:
    envs = [
        Envelope.new(source="x", type="t.v1", data={"n": 0}),
        Envelope.new(source="x", type="t.v1", data={"n": 1}),
        Envelope.new(
            source="x",
            type="t.v1",
            dataref="file:///nowhere",
        ),
    ]
    settings = Settings(
        transport_url="nats://unused",
        outbox_path=str(tmp_path / "ob.sqlite"),
        stream_name="TEST_REPLAY_UI",
        stream_subjects=("t.>",),
    )
    bus = Bus(settings, transport=StubTransport(envs))
    app = create_app(bus)
    return TestClient(app)


def test_index_serves_html(client: TestClient) -> None:
    resp = client.get("/")
    assert resp.status_code == 200
    assert "AetherBus Replay" in resp.text


def _stub_client(tmp_path: Path) -> TestClient:
    settings = Settings(
        transport_url="nats://unused",
        outbox_path=str(tmp_path / "ob.sqlite"),
        stream_name="TEST_REPLAY_UI",
        stream_subjects=("t.>",),
    )
    return TestClient(create_app(Bus(settings, transport=StubTransport())))


def test_index_falls_back_to_jinja_when_spa_absent(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """No built SPA (a clean/git/sdist install) → `/` serves the Jinja viewer.

    This is the packaging fallback the wheel fix relies on: the wheel can ship
    without frontend/dist/, and the backend still serves a usable page.
    """
    missing = tmp_path / "no-such-dist"
    monkeypatch.setattr(_app_mod, "SPA_INDEX", missing / "index.html")
    monkeypatch.setattr(_app_mod, "SPA_ASSETS", missing / "assets")

    resp = _stub_client(tmp_path).get("/")
    assert resp.status_code == 200
    # Marker unique to the Jinja template, absent from the built SPA index.
    assert "M5a — minimal viewer" in resp.text


def test_index_serves_spa_when_built(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A present dist/ (the release wheel) → `/` serves the built SPA instead."""
    dist = tmp_path / "dist"
    (dist / "assets").mkdir(parents=True)
    (dist / "index.html").write_text("<!doctype html><title>BUILT SPA</title>")
    monkeypatch.setattr(_app_mod, "SPA_INDEX", dist / "index.html")
    monkeypatch.setattr(_app_mod, "SPA_ASSETS", dist / "assets")

    resp = _stub_client(tmp_path).get("/")
    assert resp.status_code == 200
    assert "BUILT SPA" in resp.text
    assert "M5a — minimal viewer" not in resp.text


def test_list_streams_returns_configured_stream(client: TestClient) -> None:
    resp = client.get("/api/streams")
    assert resp.status_code == 200
    streams = resp.json()
    assert len(streams) == 1
    assert streams[0]["name"] == "TEST_REPLAY_UI"
    assert streams[0]["subjects"] == ["t.>"]


def test_list_events_returns_history(client: TestClient) -> None:
    resp = client.get("/api/streams/TEST_REPLAY_UI/events?topic=t.v1")
    assert resp.status_code == 200
    events = resp.json()
    assert len(events) == 3
    assert events[0]["type"] == "t.v1"
    assert events[2]["has_dataref"] is True
    assert events[0]["has_dataref"] is False


def test_list_events_respects_limit(client: TestClient) -> None:
    resp = client.get("/api/streams/TEST_REPLAY_UI/events?topic=t.v1&limit=1")
    assert resp.status_code == 200
    assert len(resp.json()) == 1


def test_get_event_by_id(client: TestClient) -> None:
    listing = client.get("/api/streams/TEST_REPLAY_UI/events?topic=t.v1").json()
    target_id = listing[1]["id"]
    resp = client.get(f"/api/events/{target_id}?topic=t.v1")
    assert resp.status_code == 200
    detail = resp.json()
    assert detail["envelope"]["id"] == target_id
    assert detail["inline_data"] == {"n": 1}


def test_get_event_not_found(client: TestClient) -> None:
    resp = client.get("/api/events/nope?topic=t.v1")
    assert resp.status_code == 404


def test_replay_endpoint_returns_event_ids(client: TestClient) -> None:
    resp = client.post("/api/replay", json={"topic": "t.v1"})
    assert resp.status_code == 200
    body = resp.json()
    assert body["count"] == 3
    assert len(body["event_ids"]) == 3
    assert body["dry_run"] is True
    assert body["sandbox_url"] is None


def test_replay_endpoint_with_bad_sandbox_returns_502(client: TestClient) -> None:
    # An unknown transport_type fails fast at Bus(_) construction; the route
    # wraps any sandbox setup error as a 502.
    resp = client.post(
        "/api/replay",
        json={
            "topic": "t.v1",
            "sandbox_url": "redis://localhost:6379",
            "sandbox_transport_type": "redis-not-a-real-transport",
        },
    )
    assert resp.status_code == 502
    assert "sandbox replay failed" in resp.json()["detail"]
