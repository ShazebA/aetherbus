"""``mcp://`` client resolver — read a resource from a configured MCP server.

URI shape: ``mcp://<server-name>/<resource-uri-on-that-server>``. The
``<server-name>`` is a label the user previously registered via
:func:`register_mcp_server`. The rest of the URI is the resource URI to read
on that server.

Example:

.. code-block:: python

    from aetherbus.resolver.mcp import register_mcp_server
    register_mcp_server(
        "filesystem",
        "stdio:/// usr/local/bin/mcp-filesystem --root /tmp",
    )
    # Now an envelope with dataref="mcp://filesystem/file:///tmp/foo.json"
    # resolves to the file's content.

Sessions are opened lazily on first use and reused for the process lifetime.
"""
from __future__ import annotations

import asyncio
import base64
import logging
import shlex
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any

from pydantic import AnyUrl

from .base import ResolverError, register_resolver

logger = logging.getLogger(__name__)


@dataclass
class _ServerEntry:
    name: str
    stdio_command: list[str] | None = None
    sse_url: str | None = None

    _session: Any | None = field(default=None, repr=False)
    _stack: AsyncExitStack | None = field(default=None, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)


_servers: dict[str, _ServerEntry] = {}


def register_mcp_server(name: str, spec: str | list[str]) -> None:
    """Register an MCP server under ``name`` for ``mcp://<name>/...`` URIs.

    ``spec`` accepts:

    - ``"stdio:///path/to/cmd --arg ..."`` — launch via stdio.
    - ``["/path/to/cmd", "--arg", ...]`` — argv form; also stdio.
    - ``"http(s)://host/sse"`` — connect via SSE.
    """
    if isinstance(spec, list):
        if not spec:
            raise ValueError("stdio argv must be non-empty")
        _servers[name] = _ServerEntry(name=name, stdio_command=list(spec))
        return

    if not isinstance(spec, str):
        raise TypeError(f"spec must be str or list[str]; got {type(spec).__name__}")

    if spec.startswith("stdio://"):
        # URI shape: stdio://<empty-authority>/<absolute-path> [args...]
        # After removing the scheme + '//' we keep the leading '/' on the path.
        argv = shlex.split(spec[len("stdio://"):])
        if not argv:
            raise ValueError(f"stdio: spec missing command: {spec!r}")
        _servers[name] = _ServerEntry(name=name, stdio_command=argv)
    elif spec.startswith(("http://", "https://")):
        _servers[name] = _ServerEntry(name=name, sse_url=spec)
    else:
        raise ValueError(
            f"unrecognized mcp server spec: {spec!r}. "
            "Use 'stdio:///cmd args' or 'http(s)://host/sse'."
        )


_pending_closes: set[asyncio.Task[Any]] = set()


def unregister_mcp_server(name: str) -> None:
    """Drop a registration and tear down any open session."""
    entry = _servers.pop(name, None)
    if entry is None or entry._stack is None:
        return
    stack = entry._stack
    entry._stack = None
    entry._session = None
    try:
        loop = asyncio.get_event_loop()
        if loop.is_running():
            # Keep a strong reference so the GC doesn't drop the coroutine mid-flight.
            task = asyncio.ensure_future(stack.aclose())
            _pending_closes.add(task)
            task.add_done_callback(_pending_closes.discard)
        else:
            loop.run_until_complete(stack.aclose())
    except Exception:
        logger.exception("failed to close MCP session for %s during unregister", name)


def registered_mcp_servers() -> list[str]:
    return sorted(_servers)


async def _ensure_session(entry: _ServerEntry) -> Any:
    if entry._session is not None:
        return entry._session
    async with entry._lock:
        if entry._session is not None:
            return entry._session

        # Lazy import — the [mcp] extra may not be installed in every env that
        # touches the resolver package.
        try:
            from mcp import ClientSession
            from mcp.client.stdio import StdioServerParameters, stdio_client
        except ImportError as e:
            raise ResolverError(
                "mcp:// resolver requires the [mcp] extra: pip install 'aetherbus[mcp]'"
            ) from e

        stack = AsyncExitStack()
        try:
            if entry.stdio_command is not None:
                params = StdioServerParameters(
                    command=entry.stdio_command[0],
                    args=entry.stdio_command[1:],
                )
                read, write = await stack.enter_async_context(stdio_client(params))
            elif entry.sse_url is not None:
                try:
                    from mcp.client.sse import sse_client
                except ImportError as e:
                    raise ResolverError(
                        f"SSE client unavailable for mcp server {entry.name!r}: {e}"
                    ) from e
                read, write = await stack.enter_async_context(sse_client(entry.sse_url))
            else:
                raise ResolverError(
                    f"mcp server {entry.name!r} has neither stdio nor sse transport"
                )

            session = await stack.enter_async_context(ClientSession(read, write))
            await session.initialize()
        except Exception:
            await stack.aclose()
            raise

        entry._stack = stack
        entry._session = session
        return session


def _split(uri: str) -> tuple[str, str]:
    """Split ``mcp://<server>/<rest>`` into ``(server, rest)``.

    ``rest`` may itself be a URI containing ``://`` (e.g.
    ``mcp://aetherbus/aetherbus://topic``).
    """
    if not uri.startswith("mcp://"):
        raise ResolverError(f"expected mcp:// URI, got {uri!r}")
    body = uri[len("mcp://"):]
    slash = body.find("/")
    if slash < 0:
        raise ResolverError(f"mcp:// URI missing resource: {uri!r}")
    server = body[:slash]
    rest = body[slash + 1:]
    if not server:
        raise ResolverError(f"mcp:// URI missing server name: {uri!r}")
    if not rest:
        raise ResolverError(f"mcp:// URI missing resource: {uri!r}")
    return server, rest


@register_resolver("mcp")
class MCPResolver:
    scheme = "mcp"

    async def fetch(self, uri: str) -> bytes:
        server_name, resource_uri = _split(uri)
        if server_name not in _servers:
            raise ResolverError(
                f"unknown MCP server {server_name!r}. "
                "Register with aetherbus.resolver.mcp.register_mcp_server(name, spec). "
                f"Currently registered: {sorted(_servers) or '(none)'}."
            )

        try:
            session = await _ensure_session(_servers[server_name])
        except ResolverError:
            raise
        except Exception as e:
            raise ResolverError(
                f"failed to open session to mcp server {server_name!r}: {e}"
            ) from e

        try:
            result = await session.read_resource(AnyUrl(resource_uri))
        except Exception as e:
            raise ResolverError(
                f"mcp://{server_name} resources/read failed for {resource_uri!r}: {e}"
            ) from e

        bufs: list[bytes] = []
        for content in result.contents:
            text = getattr(content, "text", None)
            if text is not None:
                bufs.append(text.encode("utf-8"))
                continue
            blob = getattr(content, "blob", None)
            if blob is not None:
                # MCP encodes binary content as base64 strings in ResourceContents.
                bufs.append(base64.b64decode(blob))
        return b"".join(bufs)
