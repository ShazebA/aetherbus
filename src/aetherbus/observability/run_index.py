"""SQLite-backed index that groups bus events into *runs* by correlation id.

This is the M8 data layer for the run-centric observability pivot: where the
Replay UI is stream-centric (events grouped by topic), the index is run-centric
(events grouped by ``aetherbuscorrelationid``). It mirrors the outbox's
stdlib-sqlite-in-thread-executor pattern (see :mod:`aetherbus.outbox`): one
connection per call, WAL journal, all DB work pushed off the event loop via
``asyncio.to_thread``.

Status heuristic (M8 — deliberately simple, documented per the plan):

- ``errored`` — any event in the run has an error (topic ends with an
  :data:`ERROR_SUFFIXES` suffix). Error always wins.
- ``ok`` — a terminal-success event was seen (topic ends with a
  :data:`TERMINAL_OK_SUFFIXES` suffix) and no error.
- ``running`` — no terminal event yet.

``bus.replay()`` (the backfill path) drops transport headers, so the topic
``type`` is the only status signal available. M9 spans will set status
explicitly via ``aetherbus.span.v1`` payloads. A no-correlation-id,
non-terminal event becomes a single-event run that stays ``running`` forever —
expected under this heuristic, fixed in M9.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from ..envelope import Envelope

# Topic-suffix conventions for deriving run status. Lowercased ``<...>.action.vN``
# per the repo's topic naming rule (CLAUDE.md).
ERROR_SUFFIXES: tuple[str, ...] = (".error.v1", ".failed.v1", ".failure.v1")
TERMINAL_OK_SUFFIXES: tuple[str, ...] = (
    ".completed.v1",
    ".complete.v1",
    ".done.v1",
    ".finished.v1",
    ".succeeded.v1",
)

STATUS_RUNNING = "running"
STATUS_OK = "ok"
STATUS_ERRORED = "errored"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
    correlation_id TEXT PRIMARY KEY,
    status         TEXT NOT NULL,
    started_at     TEXT,
    ended_at       TEXT,
    event_count    INTEGER NOT NULL DEFAULT 0,
    error_count    INTEGER NOT NULL DEFAULT 0,
    first_source   TEXT,
    last_topic     TEXT
);
CREATE TABLE IF NOT EXISTS run_events (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id       TEXT NOT NULL UNIQUE,
    correlation_id TEXT NOT NULL,
    topic          TEXT NOT NULL,
    source         TEXT,
    time           TEXT,
    has_error      INTEGER NOT NULL DEFAULT 0,
    depends_on     TEXT NOT NULL DEFAULT '[]'
);
CREATE INDEX IF NOT EXISTS run_events_cid ON run_events(correlation_id, seq);
"""


def _is_error_topic(topic: str) -> bool:
    return any(topic.endswith(s) for s in ERROR_SUFFIXES)


def _is_terminal_ok_topic(topic: str) -> bool:
    return any(topic.endswith(s) for s in TERMINAL_OK_SUFFIXES)


@dataclass
class RunSummaryRow:
    correlation_id: str
    status: str
    started_at: str | None
    ended_at: str | None
    event_count: int
    error_count: int
    first_source: str | None
    last_topic: str | None


@dataclass
class RunEventRow:
    event_id: str
    correlation_id: str
    topic: str
    source: str | None
    time: str | None
    has_error: bool
    depends_on: list[str]


@dataclass
class GraphNode:
    id: str
    topic: str
    source: str | None
    time: str | None
    has_error: bool


@dataclass
class GraphEdge:
    source: str  # parent event id (a depends_on entry)
    target: str  # the dependent event id


class RunIndex:
    """Groups events into runs by correlation id; serves the run-explorer API.

    Events with no ``aetherbuscorrelationid`` become single-event runs keyed by
    their own event id, so nothing is invisible.
    """

    def __init__(self, path: str | Path) -> None:
        # Cheap: store the path only. The module-level FastAPI ``app`` builds a
        # RunIndex at import time, so DB/schema work is deferred to ``init()``.
        self._path = Path(path)
        self._initialized = False

    def _run_sync(self, fn: Callable[[sqlite3.Connection], object]) -> object:
        with sqlite3.connect(str(self._path), isolation_level=None) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            # Unlike the outbox (single drainer), the index is written from two
            # call sites — the startup backfill and the tail callback — and the
            # tail callback can fire concurrently. busy_timeout makes a second
            # writer wait rather than raise "database is locked"; recompute is
            # idempotent so no explicit transaction is needed.
            conn.execute("PRAGMA busy_timeout=5000")
            return fn(conn)

    async def _exec(self, fn: Callable[[sqlite3.Connection], object]) -> object:
        return await asyncio.to_thread(self._run_sync, fn)

    async def init(self) -> None:
        if self._initialized:
            return
        await self._exec(lambda c: c.executescript(_SCHEMA))
        self._initialized = True

    # ----- write path -------------------------------------------------------

    async def ingest(self, envelope: Envelope) -> None:
        """Record one event into its run. Idempotent on event id.

        Safe to call from both the startup backfill and the live tail path; a
        repeat of the same ``event_id`` is a no-op.
        """
        await self.init()
        cid = envelope.aetherbuscorrelationid or envelope.id
        topic = envelope.type
        has_error = 1 if _is_error_topic(topic) else 0
        depends_on = json.dumps(envelope.depends_on)

        def _ingest(conn: sqlite3.Connection) -> None:
            cur = conn.execute(
                "INSERT OR IGNORE INTO run_events"
                "(event_id, correlation_id, topic, source, time, has_error, depends_on)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (envelope.id, cid, topic, envelope.source, envelope.time, has_error, depends_on),
            )
            if cur.rowcount == 0:
                return  # already indexed — idempotent re-ingest
            self._recompute(conn, cid, incoming_topic=topic)

        await self._exec(_ingest)

    def _recompute(self, conn: sqlite3.Connection, cid: str, *, incoming_topic: str) -> None:
        """Recompute a run's aggregate row from its events. Cheap: runs are small."""
        count, error_count, started_at, ended_at = conn.execute(
            "SELECT COUNT(*), COALESCE(SUM(has_error), 0), MIN(time), MAX(time)"
            " FROM run_events WHERE correlation_id = ?",
            (cid,),
        ).fetchone()
        first_source = conn.execute(
            "SELECT source FROM run_events WHERE correlation_id = ? ORDER BY time, seq LIMIT 1",
            (cid,),
        ).fetchone()[0]
        last_topic = conn.execute(
            "SELECT topic FROM run_events WHERE correlation_id = ?"
            " ORDER BY time DESC, seq DESC LIMIT 1",
            (cid,),
        ).fetchone()[0]
        prior = conn.execute(
            "SELECT status FROM runs WHERE correlation_id = ?", (cid,)
        ).fetchone()
        prior_status = prior[0] if prior else None

        if error_count > 0:
            status = STATUS_ERRORED
        elif _is_terminal_ok_topic(incoming_topic) or prior_status == STATUS_OK:
            # Terminal-ok is monotonic: once a run is ok it stays ok unless an
            # error later flips it (error_count is re-evaluated above).
            status = STATUS_OK
        else:
            status = STATUS_RUNNING

        conn.execute(
            "INSERT OR REPLACE INTO runs"
            "(correlation_id, status, started_at, ended_at, event_count, error_count,"
            " first_source, last_topic) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (cid, status, started_at, ended_at, count, error_count, first_source, last_topic),
        )

    # ----- read path --------------------------------------------------------

    async def list_runs(
        self, *, limit: int = 50, status: str | None = None
    ) -> list[RunSummaryRow]:
        await self.init()

        def _q(conn: sqlite3.Connection) -> list[RunSummaryRow]:
            cols = (
                "correlation_id, status, started_at, ended_at, event_count, error_count,"
                " first_source, last_topic"
            )
            if status is not None:
                cur = conn.execute(
                    f"SELECT {cols} FROM runs WHERE status = ?"
                    " ORDER BY started_at DESC, correlation_id DESC LIMIT ?",
                    (status, limit),
                )
            else:
                cur = conn.execute(
                    f"SELECT {cols} FROM runs"
                    " ORDER BY started_at DESC, correlation_id DESC LIMIT ?",
                    (limit,),
                )
            return [RunSummaryRow(*r) for r in cur]

        return await self._exec(_q)  # type: ignore[return-value]

    async def get_run(self, cid: str) -> RunSummaryRow | None:
        await self.init()

        def _q(conn: sqlite3.Connection) -> RunSummaryRow | None:
            r = conn.execute(
                "SELECT correlation_id, status, started_at, ended_at, event_count,"
                " error_count, first_source, last_topic FROM runs WHERE correlation_id = ?",
                (cid,),
            ).fetchone()
            return RunSummaryRow(*r) if r else None

        return await self._exec(_q)  # type: ignore[return-value]

    async def run_events(self, cid: str) -> list[RunEventRow]:
        await self.init()

        def _q(conn: sqlite3.Connection) -> list[RunEventRow]:
            cur = conn.execute(
                "SELECT event_id, correlation_id, topic, source, time, has_error, depends_on"
                " FROM run_events WHERE correlation_id = ? ORDER BY time, seq",
                (cid,),
            )
            return [
                RunEventRow(
                    event_id=r[0],
                    correlation_id=r[1],
                    topic=r[2],
                    source=r[3],
                    time=r[4],
                    has_error=bool(r[5]),
                    depends_on=json.loads(r[6]),
                )
                for r in cur
            ]

        return await self._exec(_q)  # type: ignore[return-value]

    async def run_graph(self, cid: str) -> tuple[list[GraphNode], list[GraphEdge]]:
        """Build the provenance DAG for a run from ``depends_on``.

        Nodes are the run's events. Edges point parent → child for each
        ``depends_on`` entry whose parent is also in this run (cross-run
        dependencies are dropped so the graph stays self-contained).
        """
        events = await self.run_events(cid)
        node_ids = {e.event_id for e in events}
        nodes = [
            GraphNode(
                id=e.event_id,
                topic=e.topic,
                source=e.source,
                time=e.time,
                has_error=e.has_error,
            )
            for e in events
        ]
        edges = [
            GraphEdge(source=parent, target=e.event_id)
            for e in events
            for parent in e.depends_on
            if parent in node_ids
        ]
        return nodes, edges
