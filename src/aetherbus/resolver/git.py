"""``git://`` resolver — fetch a file at a specific commit.

URI grammar (matches PRD §5.5):

- ``git://<owner>/<repo>@<sha>:<path>`` — exact commit.
- ``git://<owner>/<repo>:<path>`` — current ``HEAD``.

Default origin is ``https://github.com/<owner>/<repo>.git``. For tests or
self-hosted forges, override per-repo via env:
``AETHERBUS_GIT_URL_<OWNER>_<REPO>`` (uppercase, ``-`` → ``_``).

Bare clones are cached under ``$AETHERBUS_GIT_CACHE`` (default
``~/.cache/aetherbus/git``) and reused across calls.
"""
from __future__ import annotations

import asyncio
import os
import re
from pathlib import Path

from .base import ResolverError, register_resolver

DEFAULT_CACHE_DIR = "~/.cache/aetherbus/git"

# git://owner/repo@sha:path  (sha optional → HEAD)
_URI_RE = re.compile(
    r"^git://(?P<owner>[^/]+)/(?P<repo>[^@:]+)(?:@(?P<sha>[^:]+))?:(?P<path>.+)$"
)


def _cache_dir() -> Path:
    return Path(os.environ.get("AETHERBUS_GIT_CACHE", DEFAULT_CACHE_DIR)).expanduser()


def _origin_url_for(owner: str, repo: str) -> str:
    env_key = f"AETHERBUS_GIT_URL_{owner}_{repo}".upper().replace("-", "_")
    override = os.environ.get(env_key)
    if override:
        return override
    return f"https://github.com/{owner}/{repo}.git"


async def _run(*argv: str, cwd: str | None = None) -> bytes:
    proc = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
    )
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        msg = stderr.decode(errors="replace").strip() or f"exit {proc.returncode}"
        raise ResolverError(f"git {' '.join(argv[1:])} failed: {msg}")
    return stdout


def _parse(uri: str) -> tuple[str, str, str | None, str]:
    m = _URI_RE.match(uri)
    if not m:
        raise ResolverError(
            f"unrecognized git:// URI: {uri!r}. Expected git://<owner>/<repo>[@<sha>]:<path>"
        )
    return m.group("owner"), m.group("repo"), m.group("sha"), m.group("path")


@register_resolver("git")
class GitResolver:
    scheme = "git"

    async def fetch(self, uri: str) -> bytes:
        owner, repo, sha, path = _parse(uri)
        bare = _cache_dir() / f"{owner}-{repo}.git"
        bare.parent.mkdir(parents=True, exist_ok=True)

        if not bare.exists():
            await _run("git", "clone", "--bare", _origin_url_for(owner, repo), str(bare))

        ref = sha or "HEAD"
        if sha:
            # If the specific sha isn't local yet, fetch it. Requires the remote
            # to allow fetching by sha (GitHub does for repos with
            # uploadpack.allowReachableSHA1InWant or via refs/pull/...).
            try:
                await _run("git", "-C", str(bare), "cat-file", "-e", sha)
            except ResolverError:
                await _run("git", "-C", str(bare), "fetch", "origin", sha)

        return await _run("git", "-C", str(bare), "cat-file", "-p", f"{ref}:{path}")
