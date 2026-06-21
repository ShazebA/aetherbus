"""RunIndex — grouping events into runs by correlation id (no broker)."""
from __future__ import annotations

from pathlib import Path

from aetherbus.envelope import Envelope
from aetherbus.observability import RunIndex


def _env(
    *,
    type_: str,
    cid: str | None = None,
    depends_on: list[str] | None = None,
    time: str | None = None,
    source: str = "aetherbus://agent/x",
) -> Envelope:
    env = Envelope.new(
        source=source, type=type_, correlation_id=cid, depends_on=depends_on
    )
    if time is not None:
        env.time = time
    return env


async def test_groups_events_by_correlation_id(tmp_path: Path) -> None:
    idx = RunIndex(tmp_path / "r.sqlite")
    await idx.ingest(_env(type_="dev.app.step.v1", cid="run-1"))
    await idx.ingest(_env(type_="dev.app.step.v1", cid="run-1"))

    run = await idx.get_run("run-1")
    assert run is not None
    assert run.event_count == 2
    assert run.status == "running"
    assert len(await idx.list_runs()) == 1


async def test_no_correlation_id_becomes_single_event_run(tmp_path: Path) -> None:
    idx = RunIndex(tmp_path / "r.sqlite")
    e = _env(type_="dev.app.note.v1")
    await idx.ingest(e)

    # Keyed by the event's own id so nothing is invisible.
    run = await idx.get_run(e.id)
    assert run is not None
    assert run.event_count == 1
    assert run.status == "running"


async def test_error_topic_marks_run_errored(tmp_path: Path) -> None:
    idx = RunIndex(tmp_path / "r.sqlite")
    await idx.ingest(_env(type_="dev.app.step.v1", cid="run-1"))
    await idx.ingest(_env(type_="dev.app.task.error.v1", cid="run-1"))

    run = await idx.get_run("run-1")
    assert run is not None
    assert run.status == "errored"
    assert run.error_count == 1


async def test_terminal_ok_marks_run_ok(tmp_path: Path) -> None:
    idx = RunIndex(tmp_path / "r.sqlite")
    await idx.ingest(_env(type_="dev.app.step.v1", cid="run-1"))
    await idx.ingest(_env(type_="dev.app.run.completed.v1", cid="run-1"))

    run = await idx.get_run("run-1")
    assert run is not None
    assert run.status == "ok"


async def test_error_wins_over_terminal_ok(tmp_path: Path) -> None:
    idx = RunIndex(tmp_path / "r.sqlite")
    await idx.ingest(_env(type_="dev.app.run.completed.v1", cid="run-1"))
    await idx.ingest(_env(type_="dev.app.task.error.v1", cid="run-1"))

    run = await idx.get_run("run-1")
    assert run is not None
    assert run.status == "errored"


async def test_ingest_is_idempotent_on_event_id(tmp_path: Path) -> None:
    idx = RunIndex(tmp_path / "r.sqlite")
    e = _env(type_="dev.app.step.v1", cid="run-1")
    await idx.ingest(e)
    await idx.ingest(e)  # same event id — must not double-count

    run = await idx.get_run("run-1")
    assert run is not None
    assert run.event_count == 1


async def test_run_events_ordered_by_time_with_aggregates(tmp_path: Path) -> None:
    idx = RunIndex(tmp_path / "r.sqlite")
    e1 = _env(type_="dev.app.a.v1", cid="run-1", time="2026-01-01T00:00:01+00:00",
              source="aetherbus://agent/first")
    e2 = _env(type_="dev.app.b.v1", cid="run-1", time="2026-01-01T00:00:02+00:00")
    e3 = _env(type_="dev.app.c.v1", cid="run-1", time="2026-01-01T00:00:03+00:00")
    # Ingest out of chronological order — ordering must come from event time.
    await idx.ingest(e2)
    await idx.ingest(e3)
    await idx.ingest(e1)

    events = await idx.run_events("run-1")
    assert [ev.topic for ev in events] == ["dev.app.a.v1", "dev.app.b.v1", "dev.app.c.v1"]

    run = await idx.get_run("run-1")
    assert run is not None
    assert run.started_at == "2026-01-01T00:00:01+00:00"
    assert run.ended_at == "2026-01-01T00:00:03+00:00"
    assert run.first_source == "aetherbus://agent/first"
    assert run.last_topic == "dev.app.c.v1"


async def test_graph_built_from_depends_on(tmp_path: Path) -> None:
    idx = RunIndex(tmp_path / "r.sqlite")
    e0 = _env(type_="dev.app.start.v1", cid="run-1")
    e1 = _env(type_="dev.app.mid.v1", cid="run-1", depends_on=[e0.id])
    e2 = _env(type_="dev.app.end.v1", cid="run-1", depends_on=[e1.id, e0.id])
    for e in (e0, e1, e2):
        await idx.ingest(e)

    nodes, edges = await idx.run_graph("run-1")
    assert {n.id for n in nodes} == {e0.id, e1.id, e2.id}
    assert {(x.source, x.target) for x in edges} == {
        (e0.id, e1.id),
        (e1.id, e2.id),
        (e0.id, e2.id),
    }


async def test_graph_drops_cross_run_dependencies(tmp_path: Path) -> None:
    idx = RunIndex(tmp_path / "r.sqlite")
    external = "00000000-0000-0000-0000-000000000000"
    e0 = _env(type_="dev.app.start.v1", cid="run-1", depends_on=[external])
    await idx.ingest(e0)

    nodes, edges = await idx.run_graph("run-1")
    assert len(nodes) == 1
    assert edges == []  # parent outside the run is dropped


async def test_list_runs_filter_by_status_and_limit(tmp_path: Path) -> None:
    idx = RunIndex(tmp_path / "r.sqlite")
    await idx.ingest(_env(type_="dev.app.run.completed.v1", cid="ok-1",
                          time="2026-01-01T00:00:01+00:00"))
    await idx.ingest(_env(type_="dev.app.task.error.v1", cid="err-1",
                          time="2026-01-01T00:00:02+00:00"))
    await idx.ingest(_env(type_="dev.app.step.v1", cid="run-1",
                          time="2026-01-01T00:00:03+00:00"))

    errored = await idx.list_runs(status="errored")
    assert [r.correlation_id for r in errored] == ["err-1"]

    # Newest-first by started_at, capped at the limit.
    recent = await idx.list_runs(limit=2)
    assert [r.correlation_id for r in recent] == ["run-1", "err-1"]
