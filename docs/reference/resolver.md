# Resolver SDK

Resolvers fetch the actual payload for a `dataref` URI. Each handles one scheme; the SDK dispatches by parsing the URI prefix.

## Built-ins

| Scheme | Module | Extras needed | Notes |
|---|---|---|---|
| `file://` | `aetherbus.resolver.file` | (stdlib only) | RFC 8089. Off-loop reads. Single-host trust model. |
| `https://` | `aetherbus.resolver.https` | (httpx in core deps) | Optional bearer auth from `AETHERBUS_HTTPS_AUTH`. |
| `mcp://` | `aetherbus.resolver.mcp` | `[mcp]` | `mcp://<server>/<resource-uri>`. Register MCP servers with `register_mcp_server`. |
| `git://` | `aetherbus.resolver.git` | (`git` on `$PATH`) | `git://<owner>/<repo>[@<sha>]:<path>`. Bare-clone cache. |

## Resolving from an event

You don't call resolvers directly in most code; `event.resolve()` dispatches for you:

```python
@bus.subscribe("dev.aetherbus.research.finding.v1")
async def handler(event):
    payload = await event.resolve(as_=Finding)
```

## Cache and single-flight

The SDK keeps a process-wide TTL cache keyed by URI. Default TTL is 300 s; override per-event via the envelope's `aetherbusttl` attribute or globally via the resolver settings.

**Single-flight**: concurrent resolves of the same URI share one underlying fetch. Two handlers racing on the same `dataref` won't double-call the origin. Failed fetches are not cached — the next caller retries.

## Writing your own resolver

```python
from aetherbus.resolver import register_resolver, Resolver

@register_resolver("s3")
class S3Resolver:
    scheme = "s3"

    async def fetch(self, uri: str) -> bytes:
        # uri: s3://bucket/key
        bucket, key = uri.removeprefix("s3://").split("/", 1)
        async with self._client() as s3:
            obj = await s3.get_object(Bucket=bucket, Key=key)
            return await obj["Body"].read()
```

Two options for distribution:

1. **In your application** — register at import time before `Bus.run()` reaches a `resolve()` call.
2. **As a third-party package** — declare under the `aetherbus.resolvers` entry-point group:

    ```toml
    [project.entry-points."aetherbus.resolvers"]
    s3 = "my_package.s3_resolver:S3Resolver"
    ```

    The SDK loads entry-point resolvers on first import. Failures log a WARNING but don't fail the package import.

## `mcp://` — server registry

`mcp://<server-name>/<resource-uri-rest>` resolves by calling MCP `resources/read` on the named server. Register servers before calling `event.resolve()`:

```python
from aetherbus.resolver.mcp import register_mcp_server

register_mcp_server("filesystem", "stdio:///usr/local/bin/mcp-filesystem --root /tmp")
register_mcp_server("github",      "https://mcp.github.com/sse")
```

Sessions open lazily on first use; one session per server, reused for the process lifetime.

## `git://` — bare-clone cache

`git://<owner>/<repo>[@<sha>]:<path>` clones the repo as a bare repo under `~/.cache/aetherbus/git/` and runs `git cat-file -p <sha>:<path>` to read content at a specific commit.

Environment overrides:

- `AETHERBUS_GIT_CACHE` — change the bare-clone cache directory.
- `AETHERBUS_GIT_URL_<OWNER>_<REPO>` — override the origin URL (used in tests and for self-hosted forges).

## `ResolverError`

Every failure normalizes to `aetherbus.resolver.ResolverError`. Messages carry actionable install hints — for example, resolving `mcp://...` without the `[mcp]` extra installed produces:

```
ResolverError: no resolver registered for scheme 'mcp'. Available: ['file', 'git', 'https'].
For https://, mcp://, git://, vec://: install the matching extra (e.g. `pip install aetherbus[mcp]`).
```
