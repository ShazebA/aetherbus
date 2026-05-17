"""``https://`` resolver — HTTP GET via httpx.

Optional bearer auth from ``AETHERBUS_HTTPS_AUTH`` env var. Per-host auth maps
live in v2 once we have a real use case; M3 keeps the surface minimal.
"""
from __future__ import annotations

import os

import httpx

from .base import ResolverError, register_resolver

DEFAULT_TIMEOUT_SECONDS = 30.0


@register_resolver("https")
class HTTPSResolver:
    scheme = "https"

    async def fetch(self, uri: str) -> bytes:
        headers: dict[str, str] = {}
        token = os.environ.get("AETHERBUS_HTTPS_AUTH")
        if token:
            headers["Authorization"] = f"Bearer {token}"

        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT_SECONDS) as client:
            try:
                resp = await client.get(uri, headers=headers)
            except httpx.HTTPError as e:
                raise ResolverError(f"https fetch failed for {uri}: {e}") from e

            if resp.status_code >= 400:
                raise ResolverError(
                    f"https fetch returned {resp.status_code} for {uri}"
                )
            return resp.content
