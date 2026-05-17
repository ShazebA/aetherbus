"""Resolver SDK — protocol, registry, TTL cache, single-flight.

Resolvers fetch the actual payload for a CloudEvents ``dataref`` URI. The bus
itself doesn't know about resolution — when ``event.resolve()`` is called and
the envelope carries a ``dataref``, this module dispatches to the registered
resolver for that URI's scheme.

Built-ins register on import (see ``aetherbus.resolver.__init__``). Third-
party resolvers register via the Python entry-point group ``aetherbus.resolvers``.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Protocol, TypeVar, runtime_checkable
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


class ResolverError(Exception):
    """Raised when a resolver fails to fetch a dataref."""


@runtime_checkable
class Resolver(Protocol):
    """A pluggable fetcher for a single URI scheme.

    Implementations expose a ``scheme`` class attribute (or instance attribute
    on register) and an async ``fetch(uri)`` returning the raw payload bytes.
    """

    scheme: str

    async def fetch(self, uri: str) -> bytes: ...


_registry: dict[str, Resolver] = {}

T = TypeVar("T")


def register_resolver(scheme: str) -> Callable[[type[T]], type[T]]:
    """Decorator: instantiate a Resolver class and register it under ``scheme``."""

    def deco(cls: type[T]) -> type[T]:
        instance = cls()  # type: ignore[call-arg]
        # Stamp the scheme so Resolver instances expose it even if the class
        # didn't declare it as a class attribute.
        instance.scheme = scheme  # type: ignore[attr-defined]
        _registry[scheme] = instance  # type: ignore[assignment]
        return cls

    return deco


def get_resolver(scheme: str) -> Resolver:
    if scheme not in _registry:
        available = sorted(_registry)
        raise ResolverError(
            f"no resolver registered for scheme {scheme!r}. "
            f"Available: {available or '(none)'}. "
            "For https://, mcp://, git://, vec://: install the matching extra "
            "(e.g. `pip install aetherbus[mcp]`)."
        )
    return _registry[scheme]


def registered_schemes() -> list[str]:
    return sorted(_registry)


# --- TTL cache + single-flight ----------------------------------------------

DEFAULT_TTL_SECONDS = 300


@dataclass
class _CacheEntry:
    data: bytes
    expires_at: float


_cache: dict[str, _CacheEntry] = {}
_in_flight: dict[str, asyncio.Future[bytes]] = {}


async def resolve_uri(uri: str, *, ttl: int | None = None) -> bytes:
    """Resolve ``uri`` via the registered resolver, with TTL cache + single-flight.

    Concurrent resolves of the same URI share one underlying fetch. A successful
    fetch is cached for ``ttl`` seconds (or ``DEFAULT_TTL_SECONDS`` if unset).
    Failed fetches are not cached — the next caller retries.
    """
    now = time.monotonic()

    cached = _cache.get(uri)
    if cached is not None and cached.expires_at > now:
        return cached.data

    # Single-flight: if another task is already fetching this URI, await its result.
    in_flight = _in_flight.get(uri)
    if in_flight is not None:
        return await in_flight

    parsed = urlparse(uri)
    scheme = parsed.scheme
    if not scheme:
        raise ResolverError(f"dataref has no scheme: {uri!r}")
    resolver = get_resolver(scheme)

    loop = asyncio.get_running_loop()
    fut: asyncio.Future[bytes] = loop.create_future()
    _in_flight[uri] = fut
    try:
        try:
            data = await resolver.fetch(uri)
        except Exception as e:
            if not fut.done():
                fut.set_exception(e)
            raise

        effective_ttl = ttl if ttl is not None else DEFAULT_TTL_SECONDS
        _cache[uri] = _CacheEntry(data=data, expires_at=time.monotonic() + effective_ttl)
        if not fut.done():
            fut.set_result(data)
        return data
    finally:
        _in_flight.pop(uri, None)


def clear_cache() -> None:
    """Wipe the resolver cache. Test helper; not part of the production API."""
    _cache.clear()


def _load_entry_point_resolvers() -> None:
    """Load third-party resolvers declared under the ``aetherbus.resolvers`` group.

    Failures are logged at WARNING; one broken plugin must not prevent the bus
    from starting.
    """
    try:
        from importlib.metadata import entry_points
    except ImportError:  # pragma: no cover — stdlib since 3.10
        return
    try:
        eps = entry_points(group="aetherbus.resolvers")
    except Exception:  # pragma: no cover
        return
    for ep in eps:
        try:
            ep.load()  # decorator side-effect registers the resolver
        except Exception as e:
            logger.warning("failed to load resolver %s from entry point: %s", ep.name, e)


__all__ = [
    "Resolver",
    "ResolverError",
    "_load_entry_point_resolvers",
    "clear_cache",
    "get_resolver",
    "register_resolver",
    "registered_schemes",
    "resolve_uri",
]


# Re-export Awaitable so type-checkers don't complain about the protocol
_ = Awaitable
