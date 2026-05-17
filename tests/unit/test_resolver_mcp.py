"""Tests for the mcp:// client resolver.

Uses a fake ClientSession injected directly into the server registry to avoid
spinning up a real MCP transport. The end-to-end loop (Bus → AetherBus MCP
adapter → mcp:// resolver → ResourceContents) is verified by integration
tests when Docker is available.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest
from pydantic import AnyUrl

from aetherbus.resolver import ResolverError, clear_cache, resolve_uri
from aetherbus.resolver.mcp import (
    _ServerEntry,
    _servers,
    _split,
    register_mcp_server,
    registered_mcp_servers,
    unregister_mcp_server,
)


@pytest.fixture(autouse=True)
def _reset_cache_and_registry() -> None:
    clear_cache()
    # Snapshot + restore to keep other tests' registrations intact.
    snapshot = dict(_servers)
    _servers.clear()
    yield
    _servers.clear()
    _servers.update(snapshot)


# --- _split -----------------------------------------------------------------


def test_split_simple() -> None:
    assert _split("mcp://srv/some/resource") == ("srv", "some/resource")


def test_split_nested_uri_resource() -> None:
    # The resource itself can be a URI — the AetherBus adapter exposes
    # `aetherbus://<topic>`, so a fully-qualified roundtrip looks like:
    server, rest = _split("mcp://aetherbus/aetherbus://dev.aetherbus.research.finding.v1")
    assert server == "aetherbus"
    assert rest == "aetherbus://dev.aetherbus.research.finding.v1"


def test_split_missing_resource_errors() -> None:
    with pytest.raises(ResolverError):
        _split("mcp://just-a-server")


def test_split_missing_server_errors() -> None:
    with pytest.raises(ResolverError):
        _split("mcp:///just-a-resource")


# --- register / unregister --------------------------------------------------


def test_register_stdio_string_form() -> None:
    register_mcp_server("alpha", "stdio:///bin/echo hello")
    assert "alpha" in registered_mcp_servers()
    assert _servers["alpha"].stdio_command == ["/bin/echo", "hello"]


def test_register_stdio_argv_form() -> None:
    register_mcp_server("alpha", ["/bin/echo", "hi"])
    assert _servers["alpha"].stdio_command == ["/bin/echo", "hi"]


def test_register_sse_form() -> None:
    register_mcp_server("remote", "https://mcp.example.com/sse")
    assert _servers["remote"].sse_url == "https://mcp.example.com/sse"


def test_register_unknown_form_rejected() -> None:
    with pytest.raises(ValueError):
        register_mcp_server("bad", "garbage://something")


def test_unregister_drops_entry() -> None:
    register_mcp_server("alpha", "stdio:///bin/echo x")
    assert "alpha" in registered_mcp_servers()
    unregister_mcp_server("alpha")
    assert "alpha" not in registered_mcp_servers()


# --- fetch ------------------------------------------------------------------


@dataclass
class _FakeContent:
    text: str | None = None
    blob: str | None = None


@dataclass
class _FakeResult:
    contents: list[Any] = field(default_factory=list)


@dataclass
class _FakeSession:
    """Mimics enough of mcp.ClientSession for the resolver to call read_resource."""

    answers: dict[str, _FakeResult] = field(default_factory=dict)
    seen: list[str] = field(default_factory=list)

    async def read_resource(self, uri: AnyUrl) -> _FakeResult:
        self.seen.append(str(uri))
        try:
            return self.answers[str(uri)]
        except KeyError as e:
            raise RuntimeError(f"no canned answer for {uri}") from e


def _install_fake(server_name: str, session: _FakeSession) -> None:
    """Skip the real connect path by pre-installing a session on the entry."""
    entry = _ServerEntry(name=server_name, stdio_command=["/bin/false"])
    entry._session = session  # type: ignore[assignment]
    _servers[server_name] = entry


@pytest.mark.asyncio
async def test_fetch_returns_concatenated_text() -> None:
    fake = _FakeSession(
        answers={
            "file:///doc.txt": _FakeResult(
                contents=[
                    _FakeContent(text="line one\n"),
                    _FakeContent(text="line two\n"),
                ]
            )
        }
    )
    _install_fake("alpha", fake)
    data = await resolve_uri("mcp://alpha/file:///doc.txt")
    assert data == b"line one\nline two\n"
    assert fake.seen == ["file:///doc.txt"]


@pytest.mark.asyncio
async def test_fetch_decodes_blob_contents() -> None:
    import base64

    payload = b"\x00\x01\x02binary"
    blob_b64 = base64.b64encode(payload).decode()
    fake = _FakeSession(
        answers={
            "data://blob": _FakeResult(contents=[_FakeContent(blob=blob_b64)]),
        }
    )
    _install_fake("alpha", fake)
    data = await resolve_uri("mcp://alpha/data://blob")
    assert data == payload


@pytest.mark.asyncio
async def test_fetch_unknown_server_actionable_error() -> None:
    with pytest.raises(ResolverError) as ei:
        await resolve_uri("mcp://nope/x")
    msg = str(ei.value)
    assert "nope" in msg
    assert "register_mcp_server" in msg


@pytest.mark.asyncio
async def test_fetch_session_failure_wrapped() -> None:
    class _Broken:
        async def read_resource(self, uri: AnyUrl) -> Any:
            raise RuntimeError("server crashed")

    _install_fake("alpha", _Broken())  # type: ignore[arg-type]
    with pytest.raises(ResolverError) as ei:
        await resolve_uri("mcp://alpha/x")
    assert "resources/read failed" in str(ei.value)
