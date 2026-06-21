"""Run-centric REST routes (/api/runs*) against a fake Bus.

Mirrors ``test_replay_backend.py``: a StubTransport with a canned ``history``
feeds the lifespan backfill, which populates the run index. The TestClient is
used as a context manager so the lifespan (and thus the backfill) actually runs.
"""
from __future__ import annotations

from collections.abc import AsyncIterator, Iterator
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from aetherbus import Bus
from aetherbus.config import Settings
from aetherbus.envelope import Envelope
from aetherbus.replay_ui.backend.app import create_app


class _FakeSub:
    async def unsubscribe(self) -> None:
        return None


class StubTransport:
    """Canned ``history`` iterator; no-op publish/subscribe."""

    def __init__(self, envelopes: list[Envelope]) -> None:
        self.envelopes = envelopes

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


# A two-run history plus one orphan (no correlation id):
#   run-1: started -> step -> completed   (status ok, linear DAG)
#   run-2: a single error event           (status errored)
#   orphan: no correlation id             (single-event run, running)
_E0 = Envelope.new(source="aetherbus://agent/a", type="dev.app.run.started.v1",
                   correlation_id="run-1")
_E1 = Envelope.new(source="aetherbus://agent/b", type="dev.app.step.v1",
                   correlation_id="run-1", depends_on=[_E0.id])
_E2 = Envelope.new(source="aetherbus://agent/c", type="dev.app.run.completed.v1",
                   correlation_id="run-1", depends_on=[_E1.id])
_E3 = Envelope.new(source="aetherbus://agent/d", type="dev.app.task.error.v1",
                   correlation_id="run-2")
_ORPHAN = Envelope.new(source="aetherbus://agent/e", type="dev.app.note.v1")


@pytest.fixture
def client(tmp_path: Path) -> Iterator[TestClient]:
    envs = [_E0, _E1, _E2, _E3, _ORPHAN]
    settings = Settings(
        transport_url="nats://unused",
        outbox_path=str(tmp_path / "ob.sqlite"),
        run_index_path=str(tmp_path / "runs.sqlite"),
        stream_name="TEST_RUNS",
        stream_subjects=("t.>",),
    )
    bus = Bus(settings, transport=StubTransport(envs))
    app = create_app(bus)
    with TestClient(app) as c:  # `with` runs the lifespan → backfill populates the index
        yield c


def test_list_runs_returns_all_runs(client: TestClient) -> None:
    runs = client.get("/api/runs").json()
    by_cid = {r["correlation_id"]: r for r in runs}
    assert {"run-1", "run-2"} <= set(by_cid)
    assert by_cid["run-1"]["status"] == "ok"
    assert by_cid["run-1"]["event_count"] == 3
    assert by_cid["run-2"]["status"] == "errored"
    assert by_cid["run-2"]["error_count"] == 1


def test_list_runs_filter_by_status(client: TestClient) -> None:
    runs = client.get("/api/runs?status=errored").json()
    assert [r["correlation_id"] for r in runs] == ["run-2"]


def test_get_run_detail_lists_ordered_events(client: TestClient) -> None:
    detail = client.get("/api/runs/run-1").json()
    assert detail["status"] == "ok"
    assert detail["event_count"] == 3
    assert [e["topic"] for e in detail["events"]] == [
        "dev.app.run.started.v1",
        "dev.app.step.v1",
        "dev.app.run.completed.v1",
    ]


def test_get_run_not_found(client: TestClient) -> None:
    assert client.get("/api/runs/nope").status_code == 404


def test_get_run_graph(client: TestClient) -> None:
    graph = client.get("/api/runs/run-1/graph").json()
    assert len(graph["nodes"]) == 3
    edge_pairs = {(e["source"], e["target"]) for e in graph["edges"]}
    assert edge_pairs == {(_E0.id, _E1.id), (_E1.id, _E2.id)}


def test_get_run_graph_not_found(client: TestClient) -> None:
    assert client.get("/api/runs/nope/graph").status_code == 404


def test_orphan_event_is_single_event_running_run(client: TestClient) -> None:
    runs = client.get("/api/runs").json()
    orphans = [
        r for r in runs
        if r["event_count"] == 1 and r["status"] == "running"
        and r["last_topic"] == "dev.app.note.v1"
    ]
    assert len(orphans) == 1
    assert orphans[0]["correlation_id"] == _ORPHAN.id
