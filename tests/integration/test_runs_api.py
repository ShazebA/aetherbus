"""Run-centric API over a live NATS broker.

Publishes a correlated sequence, then serves the replay-ui app and asserts the
run is reconstructed from history. NATS-only: the backfill walks the wildcard
stream subject (``test.>``), which Kafka has no equivalent for.

The session NATS stream is shared across integration tests, so we assert on this
test's unique correlation id (via ``/api/runs/{cid}``) rather than the total
run count — other tests' events live under ``test.>`` too.
"""
from __future__ import annotations

import asyncio
import uuid
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from aetherbus import Bus
from aetherbus.config import Settings
from aetherbus.replay_ui.backend.app import create_app

pytestmark = pytest.mark.integration

# Mirrors conftest's session stream/subjects (kept local to avoid importing the
# conftest module directly).
SESSION_STREAM = "TEST_AETHERBUS"
SESSION_SUBJECTS: tuple[str, ...] = ("test.>",)


def test_runs_api_groups_correlated_events_over_nats(
    nats_url: str, tmp_path: Path
) -> None:
    cid = f"run-{uuid.uuid4().hex[:12]}"
    topic = f"test.{uuid.uuid4().hex[:8]}.step.v1"

    async def _publish() -> list[str]:
        bus = Bus(
            Settings(
                transport_url=nats_url,
                outbox_path=str(tmp_path / "pub.sqlite"),
                stream_name=SESSION_STREAM,
                stream_subjects=SESSION_SUBJECTS,
            )
        )
        ids: list[str] = []
        async with bus:
            for i in range(3):
                eid = await bus.publish(
                    topic,
                    payload={"i": i},
                    correlation_id=cid,
                    depends_on=[ids[-1]] if ids else None,  # linear chain
                )
                ids.append(eid)
        return ids

    ids = asyncio.run(_publish())

    app_bus = Bus(
        Settings(
            transport_url=nats_url,
            outbox_path=str(tmp_path / "app.sqlite"),
            stream_name=SESSION_STREAM,
            stream_subjects=SESSION_SUBJECTS,
            run_index_path=str(tmp_path / "runs.sqlite"),
        )
    )
    app = create_app(app_bus)
    with TestClient(app) as client:  # lifespan runs the history backfill
        detail = client.get(f"/api/runs/{cid}")
        assert detail.status_code == 200
        body = detail.json()
        assert body["event_count"] == 3
        assert {e["event_id"] for e in body["events"]} == set(ids)

        graph = client.get(f"/api/runs/{cid}/graph").json()
        assert len(graph["nodes"]) == 3
        assert len(graph["edges"]) == 2  # e0 <- e1 <- e2
