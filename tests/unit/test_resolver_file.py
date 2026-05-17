from __future__ import annotations

from pathlib import Path

import pytest

from aetherbus.resolver import ResolverError, clear_cache, resolve_uri


@pytest.fixture(autouse=True)
def _reset_cache() -> None:
    clear_cache()


@pytest.mark.asyncio
async def test_file_resolver_reads_local_file(tmp_path: Path) -> None:
    target = tmp_path / "payload.json"
    target.write_bytes(b'{"hello":"world"}')

    data = await resolve_uri(f"file://{target}")
    assert data == b'{"hello":"world"}'


@pytest.mark.asyncio
async def test_file_resolver_localhost_netloc_allowed(tmp_path: Path) -> None:
    target = tmp_path / "x.txt"
    target.write_text("ok")
    data = await resolve_uri(f"file://localhost{target}")
    assert data == b"ok"


@pytest.mark.asyncio
async def test_file_resolver_remote_netloc_rejected() -> None:
    with pytest.raises(ResolverError) as ei:
        await resolve_uri("file://example.com/etc/hosts")
    assert "netloc" in str(ei.value)


@pytest.mark.asyncio
async def test_file_resolver_missing_file_errors(tmp_path: Path) -> None:
    with pytest.raises(ResolverError) as ei:
        await resolve_uri(f"file://{tmp_path}/does-not-exist")
    assert "not found" in str(ei.value)
