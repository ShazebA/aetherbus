"""Replay UI backend — FastAPI app exposing the bus over REST + WebSocket.

Routes (PRD §6.7):
- ``GET  /``                          — minimal HTML viewer (Jinja, vanilla JS).
- ``GET  /api/streams``               — list configured streams.
- ``GET  /api/streams/{name}/events`` — page history; ``since``/``until``/``limit`` query params.
- ``GET  /api/events/{event_id}``     — envelope + resolved payload for one event.
- ``WS   /api/streams/{name}/tail``   — push live events as they arrive.
- ``POST /api/replay``                — return the event IDs that would replay (M5a stub).

Run-centric routes (M8 — the observability pivot, grouped by correlation id):
- ``GET  /api/runs``                  — list runs; ``status``/``limit`` query params.
- ``GET  /api/runs/{cid}``            — run summary + ordered event list.
- ``GET  /api/runs/{cid}/graph``      — provenance DAG (nodes + edges from depends_on).

M5b swapped the HTML for a React SPA; the stream API surface is unchanged. M10
rebuilds the SPA around these run routes.
"""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from ...bus import Bus
from ...config import Settings
from ...envelope import Envelope
from ...event import Event
from ...observability import RunIndex, RunSummaryRow
from ...transports.base import Msg

logger = logging.getLogger(__name__)

TEMPLATES_DIR = Path(__file__).parent / "templates"
FRONTEND_DIR = Path(__file__).resolve().parents[1] / "frontend"
SPA_DIST = FRONTEND_DIR / "dist"
SPA_INDEX = SPA_DIST / "index.html"
SPA_ASSETS = SPA_DIST / "assets"


# ----- response models ------------------------------------------------------


class StreamSummary(BaseModel):
    name: str
    subjects: list[str]


class EventSummary(BaseModel):
    id: str
    source: str
    type: str
    time: str
    has_dataref: bool


class EventDetail(BaseModel):
    envelope: dict[str, Any]
    inline_data: Any | None
    dataref: str | None


class RunSummary(BaseModel):
    correlation_id: str
    status: str
    started_at: str | None
    ended_at: str | None
    duration_ms: float | None
    event_count: int
    error_count: int
    first_source: str | None
    last_topic: str | None


class RunEventModel(BaseModel):
    event_id: str
    topic: str
    source: str | None
    time: str | None
    has_error: bool
    depends_on: list[str]


class RunDetail(RunSummary):
    events: list[RunEventModel]


class RunGraphNode(BaseModel):
    id: str
    topic: str
    source: str | None
    time: str | None
    has_error: bool


class RunGraphEdge(BaseModel):
    source: str
    target: str


class RunGraph(BaseModel):
    correlation_id: str
    nodes: list[RunGraphNode]
    edges: list[RunGraphEdge]


class ReplayRequest(BaseModel):
    topic: str
    since: datetime | None = None
    until: datetime | None = None
    sandbox_url: str | None = None  # if None: dry run; else re-emit into that bus
    sandbox_transport_type: str = "nats"


class ReplayResponse(BaseModel):
    event_ids: list[str]
    count: int
    sandbox_url: str | None = None
    dry_run: bool = True


# ----- app builder ----------------------------------------------------------


def create_app(bus: Bus | None = None) -> FastAPI:
    """Build a FastAPI app wired to ``bus`` (or a default Bus.from_env())."""
    owned_bus = bus
    if owned_bus is None:
        owned_bus = Bus.from_env()

    run_index = RunIndex(owned_bus._settings.run_index_path)

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await owned_bus.connect()
        await run_index.init()
        # Backfill the run index from stored history. Backfill is the workhorse;
        # the tail WebSocket path ingests live events best-effort while a client
        # is connected. Always-on live capture lands with span streams in M9.
        for subject in owned_bus._settings.stream_subjects:
            try:
                async for ev in owned_bus.replay(subject):
                    await run_index.ingest(ev.envelope)
            except Exception:
                logger.exception("run index backfill failed for subject %s", subject)
        try:
            yield
        finally:
            await owned_bus._transport.close()

    fastapi_app = FastAPI(
        title="AetherBus Replay UI",
        version="0.1.0",
        lifespan=lifespan,
    )

    spa_built = SPA_INDEX.exists() and SPA_ASSETS.exists()
    if spa_built:
        fastapi_app.mount(
            "/assets", StaticFiles(directory=str(SPA_ASSETS)), name="spa-assets"
        )

    @fastapi_app.get("/", response_class=HTMLResponse, include_in_schema=False)
    async def index(request: Request):
        if spa_built:
            return FileResponse(SPA_INDEX, media_type="text/html")
        return templates.TemplateResponse(
            request, "index.html", {"title": "AetherBus Replay"}
        )

    @fastapi_app.get("/api/streams")
    async def list_streams() -> list[StreamSummary]:
        settings: Settings = owned_bus._settings
        return [
            StreamSummary(
                name=settings.stream_name,
                subjects=list(settings.stream_subjects),
            )
        ]

    @fastapi_app.get("/api/streams/{name}/events")
    async def list_events(
        name: str,
        topic: str,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 50,
    ) -> list[EventSummary]:
        events: list[EventSummary] = []
        async for ev in owned_bus.replay(topic, since=since, until=until):
            events.append(_summary_for(ev))
            if len(events) >= limit:
                break
        return events

    @fastapi_app.get("/api/events/{event_id}")
    async def get_event(event_id: str, topic: str) -> EventDetail:
        async for ev in owned_bus.replay(topic):
            if ev.id == event_id:
                return _detail_for(ev)
        raise HTTPException(status_code=404, detail=f"event {event_id!r} not found in {topic!r}")

    @fastapi_app.websocket("/api/streams/{name}/tail")
    async def tail(ws: WebSocket, name: str, topic: str) -> None:
        await ws.accept()
        queue: asyncio.Queue[Msg] = asyncio.Queue(maxsize=256)

        async def _enqueue(msg: Msg) -> None:
            # Capture into the run index on delivery (before any WS-queue drop)
            # so live events still group into runs while a client tails.
            try:
                await run_index.ingest(Envelope.from_bytes(msg.data))
            except Exception:
                logger.exception("tail: run index ingest failed")
            try:
                queue.put_nowait(msg)
            except asyncio.QueueFull:
                logger.warning("tail queue full; dropping event")
            await msg.ack()

        sub = await owned_bus._transport.subscribe(
            topic,
            durable=f"tail_{name}_{id(ws)}",
            queue_group=None,
            max_in_flight=16,
            deliver_policy=__import__(
                "aetherbus.transports.base", fromlist=["DeliverPolicy"]
            ).DeliverPolicy.LATEST,
            opt_start_time=None,
            callback=_enqueue,
        )

        try:
            while True:
                msg = await queue.get()
                env = Envelope.from_bytes(msg.data)
                await ws.send_json(_summary_for(Event(envelope=env)).model_dump())
        except WebSocketDisconnect:
            pass
        finally:
            try:
                await sub.unsubscribe()
            except Exception:
                logger.exception("tail: unsubscribe failed")

    @fastapi_app.post("/api/replay")
    async def replay(req: ReplayRequest) -> ReplayResponse:
        # Dry run: just iterate history and return the IDs that would replay.
        if not req.sandbox_url:
            ids: list[str] = []
            async for ev in owned_bus.replay(
                req.topic, since=req.since, until=req.until
            ):
                ids.append(ev.id)
            return ReplayResponse(
                event_ids=ids, count=len(ids), sandbox_url=None, dry_run=True
            )

        # Real replay: re-emit raw envelope bytes into the sandbox bus. ID,
        # time, source, correlation_id all preserved (see bus.replay docstring).
        replayed: list[str] = []
        try:
            # Reuse the production stream NAME so a same-broker "sandbox"
            # (the common dev case where sandbox_url == AETHERBUS_URL) doesn't
            # try to claim overlapping subjects on a second stream — JetStream
            # rejects that with `subjects overlap with an existing stream`.
            # On a truly isolated broker that stream doesn't exist yet, so
            # ensure_stream creates it the same way it would for production.
            # Per PRD §5.6 the sandbox primitive presumes a different broker
            # (account isolation); same-broker replay is effectively re-emit.
            sandbox_settings = Settings(
                transport_type=req.sandbox_transport_type,
                transport_url=req.sandbox_url,
                outbox_path="/tmp/aetherbus-sandbox-outbox.sqlite",
                stream_name=owned_bus._settings.stream_name,
                stream_subjects=tuple(owned_bus._settings.stream_subjects),
            )
            sandbox = Bus(sandbox_settings)
            async with sandbox:
                async for ev in owned_bus.replay(
                    req.topic,
                    since=req.since,
                    until=req.until,
                    into=sandbox,
                ):
                    replayed.append(ev.id)
        except Exception as e:
            raise HTTPException(
                status_code=502,
                detail=f"sandbox replay failed: {e}",
            ) from e
        return ReplayResponse(
            event_ids=replayed,
            count=len(replayed),
            sandbox_url=req.sandbox_url,
            dry_run=False,
        )

    # ----- run-centric routes (M8) ------------------------------------------

    @fastapi_app.get("/api/runs")
    async def list_runs(status: str | None = None, limit: int = 50) -> list[RunSummary]:
        rows = await run_index.list_runs(limit=limit, status=status)
        return [_run_summary(r) for r in rows]

    @fastapi_app.get("/api/runs/{cid}")
    async def get_run(cid: str) -> RunDetail:
        row = await run_index.get_run(cid)
        if row is None:
            raise HTTPException(status_code=404, detail=f"run {cid!r} not found")
        events = await run_index.run_events(cid)
        return RunDetail(
            **_run_summary(row).model_dump(),
            events=[
                RunEventModel(
                    event_id=e.event_id,
                    topic=e.topic,
                    source=e.source,
                    time=e.time,
                    has_error=e.has_error,
                    depends_on=e.depends_on,
                )
                for e in events
            ],
        )

    @fastapi_app.get("/api/runs/{cid}/graph")
    async def get_run_graph(cid: str) -> RunGraph:
        row = await run_index.get_run(cid)
        if row is None:
            raise HTTPException(status_code=404, detail=f"run {cid!r} not found")
        nodes, edges = await run_index.run_graph(cid)
        return RunGraph(
            correlation_id=cid,
            nodes=[
                RunGraphNode(
                    id=n.id, topic=n.topic, source=n.source, time=n.time, has_error=n.has_error
                )
                for n in nodes
            ],
            edges=[RunGraphEdge(source=e.source, target=e.target) for e in edges],
        )

    return fastapi_app


def _summary_for(event: Event) -> EventSummary:
    return EventSummary(
        id=event.id,
        source=event.source,
        type=event.type,
        time=event.envelope.time,
        has_dataref=event.dataref is not None,
    )


def _detail_for(event: Event) -> EventDetail:
    return EventDetail(
        envelope=json.loads(event.envelope.to_bytes()),
        inline_data=event.inline_data,
        dataref=event.dataref,
    )


def _duration_ms(started: str | None, ended: str | None) -> float | None:
    if not started or not ended:
        return None
    try:
        delta = datetime.fromisoformat(ended) - datetime.fromisoformat(started)
    except ValueError:
        return None
    return round(delta.total_seconds() * 1000.0, 3)


def _run_summary(r: RunSummaryRow) -> RunSummary:
    return RunSummary(
        correlation_id=r.correlation_id,
        status=r.status,
        started_at=r.started_at,
        ended_at=r.ended_at,
        duration_ms=_duration_ms(r.started_at, r.ended_at),
        event_count=r.event_count,
        error_count=r.error_count,
        first_source=r.first_source,
        last_topic=r.last_topic,
    )


# Module-level app for `uvicorn aetherbus.replay_ui.backend.app:app`.
app = create_app()
