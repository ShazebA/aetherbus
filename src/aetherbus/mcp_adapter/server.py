"""MCP server adapter — exposes bus topics as MCP resources.

Each subscribed topic becomes an MCP resource at ``aetherbus://<topic>``. On
every published event the server pushes a ``notifications/resources/updated``
to subscribed MCP clients (poke-then-read per ADR-004); the client then calls
``resources/read`` to retrieve the resolved payload.

Wedge use case: drop ``aetherbus-mcp serve`` into Claude Desktop's
``claude_desktop_config.json`` and the bus stream shows up as a live MCP
resource — zero AetherBus-specific code on the client side.
"""
from __future__ import annotations

import asyncio
import json
import logging
from contextlib import AsyncExitStack
from typing import Any

import anyio
from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.session import ServerSession
from mcp.server.stdio import stdio_server
from pydantic import AnyUrl

from ..bus import Bus
from ..event import Event

logger = logging.getLogger(__name__)

URI_PREFIX = "aetherbus://"


class AetherBusMCPServer:
    """Bridges AetherBus topic subscriptions to MCP resource notifications."""

    def __init__(self, bus: Bus, topics: list[str], *, name: str = "aetherbus") -> None:
        self.bus = bus
        self.topics = list(topics)
        self._latest: dict[str, Event] = {}
        self._session: ServerSession | None = None
        self.server: Server = Server(name)
        self._wire_resource_handlers()
        self._wire_bus_subscriptions()

    # ----- MCP server handlers ------------------------------------------------

    def _wire_resource_handlers(self) -> None:
        @self.server.list_resources()
        async def _list() -> list[types.Resource]:
            return [
                types.Resource(
                    uri=AnyUrl(f"{URI_PREFIX}{topic}"),
                    name=topic,
                    description=f"Latest event published to bus topic {topic!r}",
                    mimeType="application/json",
                )
                for topic in self.topics
            ]

        @self.server.read_resource()
        async def _read(uri: AnyUrl) -> str:
            topic = str(uri).removeprefix(URI_PREFIX).rstrip("/")
            event = self._latest.get(topic)
            if event is None:
                # MCP clients shouldn't crash on "nothing yet" — return an empty
                # JSON document. They'll see updated content on the next poke.
                return "{}"
            payload = await event.resolve()
            if isinstance(payload, bytes):
                try:
                    return payload.decode("utf-8")
                except UnicodeDecodeError:
                    return ""
            return json.dumps(payload)

    # ----- Bus subscriptions --------------------------------------------------

    def _wire_bus_subscriptions(self) -> None:
        for topic in self.topics:
            self._register_handler_for(topic)

    def _register_handler_for(self, topic: str) -> None:
        # Capture `topic` via a default argument so each registration is bound
        # to its own subject (closure-over-loop pitfall otherwise).
        @self.bus.subscribe(topic)
        async def _handler(event: Event, _topic: str = topic) -> None:
            self._latest[_topic] = event
            session = self._session
            if session is None:
                return
            try:
                await session.send_resource_updated(AnyUrl(f"{URI_PREFIX}{_topic}"))
            except Exception:
                # An MCP client disconnection mid-flight shouldn't kill the
                # bus subscriber. Log and move on.
                logger.exception("send_resource_updated failed for %s", _topic)

    # ----- Lifecycle ---------------------------------------------------------

    async def _run_message_loop(self, read_stream: Any, write_stream: Any) -> None:
        """Run the MCP message loop, exposing the session as ``self._session``.

        Mirrors ``mcp.server.lowlevel.Server.run`` but captures the session so
        bus handlers can push ``resources/updated`` notifications outside a
        request context. Pinned to ``mcp>=1.0`` — relies on the stable shapes
        of ``Server.lifespan`` and ``Server._handle_message`` (single-underscore,
        not technically public; we accept the coupling for the strategic win).
        """
        init_opts = self.server.create_initialization_options()
        async with AsyncExitStack() as stack:
            lifespan_ctx = await stack.enter_async_context(self.server.lifespan(self.server))
            session = await stack.enter_async_context(
                ServerSession(read_stream, write_stream, init_opts, stateless=False)
            )
            self._session = session
            try:
                async with anyio.create_task_group() as tg:
                    try:
                        async for message in session.incoming_messages:
                            tg.start_soon(
                                self.server._handle_message,
                                message,
                                session,
                                lifespan_ctx,
                                False,  # raise_exceptions
                            )
                    finally:
                        tg.cancel_scope.cancel()
            finally:
                self._session = None

    async def run_stdio(self) -> None:
        """Serve over stdio (the Claude Desktop transport)."""
        bus_task = asyncio.create_task(self.bus.run())
        # Give the bus a moment to attach its subscriptions before the MCP
        # session starts; otherwise the first events can land before we
        # publish list_changed.
        await asyncio.sleep(0.1)
        try:
            async with stdio_server() as (read, write):
                await self._run_message_loop(read, write)
        finally:
            await self.bus.close()
            try:
                await asyncio.wait_for(bus_task, timeout=3)
            except (TimeoutError, asyncio.CancelledError, Exception):
                bus_task.cancel()
