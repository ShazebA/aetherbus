"""Replay UI backend — FastAPI app exposing the bus over REST + WebSocket.

Routes (PRD §6.7):
- ``GET  /``                          — minimal HTML viewer (Jinja, vanilla JS).
- ``GET  /api/streams``               — list configured streams.
- ``GET  /api/streams/{name}/events`` — page history; ``since``/``until``/``limit`` query params.
- ``GET  /api/events/{event_id}``     — envelope + resolved payload for one event.
- ``WS   /api/streams/{name}/tail``   — push live events as they arrive.
- ``POST /api/replay``                — return the event IDs that would replay (M5a stub).

M5b will swap the HTML for a real React SPA; the API surface stays the same.
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

    templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        await owned_bus.connect()
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


# Module-level app for `uvicorn aetherbus.replay_ui.backend.app:app`.
app = create_app()
