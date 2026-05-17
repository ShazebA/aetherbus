from __future__ import annotations

import httpx
import pytest
import respx

from aetherbus.resolver import ResolverError, clear_cache, resolve_uri


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    clear_cache()


@pytest.mark.asyncio
async def test_https_resolver_returns_body() -> None:
    with respx.mock(base_url="https://example.com") as router:
        router.get("/payload.json").mock(
            return_value=httpx.Response(200, content=b'{"ok":true}')
        )
        data = await resolve_uri("https://example.com/payload.json")
        assert data == b'{"ok":true}'


@pytest.mark.asyncio
async def test_https_resolver_404_raises_resolver_error() -> None:
    with respx.mock(base_url="https://example.com") as router:
        router.get("/missing").mock(return_value=httpx.Response(404))
        with pytest.raises(ResolverError) as ei:
            await resolve_uri("https://example.com/missing")
        assert "404" in str(ei.value)


@pytest.mark.asyncio
async def test_https_resolver_sends_bearer_when_env_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AETHERBUS_HTTPS_AUTH", "secret-token")
    seen: list[str] = []

    def _capture(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("Authorization", ""))
        return httpx.Response(200, content=b"ok")

    with respx.mock() as router:
        router.get("https://example.com/auth").mock(side_effect=_capture)
        await resolve_uri("https://example.com/auth")
        assert seen == ["Bearer secret-token"]


@pytest.mark.asyncio
async def test_https_resolver_omits_bearer_when_env_unset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AETHERBUS_HTTPS_AUTH", raising=False)
    seen: list[str | None] = []

    def _capture(request: httpx.Request) -> httpx.Response:
        seen.append(request.headers.get("Authorization"))
        return httpx.Response(200, content=b"ok")

    with respx.mock() as router:
        router.get("https://example.com/noauth").mock(side_effect=_capture)
        await resolve_uri("https://example.com/noauth")
        assert seen == [None]


@pytest.mark.asyncio
async def test_https_resolver_transport_error_wrapped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AETHERBUS_HTTPS_AUTH", raising=False)

    def _boom(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route", request=request)

    with respx.mock() as router:
        router.get("https://example.com/dead").mock(side_effect=_boom)
        with pytest.raises(ResolverError) as ei:
            await resolve_uri("https://example.com/dead")
        assert "https fetch failed" in str(ei.value)
