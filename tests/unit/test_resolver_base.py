from __future__ import annotations

import asyncio

import pytest

from aetherbus.resolver import (
    ResolverError,
    clear_cache,
    get_resolver,
    register_resolver,
    registered_schemes,
    resolve_uri,
)
from aetherbus.resolver.base import _registry


@pytest.fixture(autouse=True)
def _reset_cache_between_tests() -> None:
    clear_cache()


def test_register_resolver_adds_to_registry() -> None:
    before = set(registered_schemes())

    @register_resolver("custom-scheme-xyz")
    class XYZ:
        async def fetch(self, uri: str) -> bytes:
            return b"xyz"

    try:
        assert "custom-scheme-xyz" in registered_schemes()
        r = get_resolver("custom-scheme-xyz")
        assert r.scheme == "custom-scheme-xyz"
    finally:
        _registry.pop("custom-scheme-xyz", None)
        assert set(registered_schemes()) == before


def test_unknown_scheme_error_is_actionable() -> None:
    with pytest.raises(ResolverError) as ei:
        get_resolver("definitely-not-a-real-scheme")
    msg = str(ei.value)
    assert "definitely-not-a-real-scheme" in msg
    assert "pip install aetherbus[" in msg  # mentions the extras hint


@pytest.mark.asyncio
async def test_resolve_uri_caches_within_ttl() -> None:
    calls = []

    @register_resolver("countscheme")
    class _Counter:
        async def fetch(self, uri: str) -> bytes:
            calls.append(uri)
            return b"hello"

    try:
        a = await resolve_uri("countscheme://x", ttl=60)
        b = await resolve_uri("countscheme://x", ttl=60)
        assert a == b == b"hello"
        assert len(calls) == 1  # second call hit cache
    finally:
        _registry.pop("countscheme", None)


@pytest.mark.asyncio
async def test_resolve_uri_single_flight_dedupes_concurrent() -> None:
    calls = []
    gate = asyncio.Event()

    @register_resolver("slow")
    class _Slow:
        async def fetch(self, uri: str) -> bytes:
            calls.append(uri)
            await gate.wait()
            return b"value"

    try:
        # Two concurrent resolves of the same URI -> one underlying fetch.
        t1 = asyncio.create_task(resolve_uri("slow://x"))
        t2 = asyncio.create_task(resolve_uri("slow://x"))
        # Give both tasks time to enter resolve_uri and find the in-flight future.
        await asyncio.sleep(0.05)
        gate.set()
        r1, r2 = await asyncio.gather(t1, t2)
        assert r1 == r2 == b"value"
        assert len(calls) == 1
    finally:
        _registry.pop("slow", None)


@pytest.mark.asyncio
async def test_resolve_uri_failures_are_not_cached() -> None:
    calls = []

    @register_resolver("flake")
    class _Flake:
        async def fetch(self, uri: str) -> bytes:
            calls.append(uri)
            if len(calls) == 1:
                raise ResolverError("nope")
            return b"ok"

    try:
        with pytest.raises(ResolverError):
            await resolve_uri("flake://x")
        # Second attempt actually fetches again (no negative cache).
        assert await resolve_uri("flake://x") == b"ok"
        assert len(calls) == 2
    finally:
        _registry.pop("flake", None)


@pytest.mark.asyncio
async def test_resolve_uri_rejects_no_scheme() -> None:
    with pytest.raises(ResolverError):
        await resolve_uri("just-a-path-no-scheme")
