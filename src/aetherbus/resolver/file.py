"""``file://`` resolver — local filesystem reads.

Intended for single-host development and the codegen-pipeline-style examples
(planner writes to ``file:///tmp/plan.md``, implementer subscribes and reads).
No sandboxing beyond OS permissions — do not enable on a bus that accepts
events from untrusted producers.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from urllib.parse import unquote, urlparse

from .base import ResolverError, register_resolver


def _read_sync(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except FileNotFoundError as e:
        raise ResolverError(f"file not found: {path}") from e
    except OSError as e:
        raise ResolverError(f"file read failed for {path}: {e}") from e


@register_resolver("file")
class FileResolver:
    scheme = "file"

    async def fetch(self, uri: str) -> bytes:
        parsed = urlparse(uri)
        if parsed.scheme != "file":
            raise ResolverError(f"expected file:// URI, got {uri!r}")
        # RFC 8089: empty netloc or 'localhost' both mean local FS.
        if parsed.netloc and parsed.netloc != "localhost":
            raise ResolverError(
                f"file:// netloc must be empty or 'localhost'; got {parsed.netloc!r}"
            )
        path = Path(unquote(parsed.path))
        # Off the event loop — small files won't notice, large ones won't block.
        return await asyncio.to_thread(_read_sync, path)
