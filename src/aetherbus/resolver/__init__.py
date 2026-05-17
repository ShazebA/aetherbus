"""AetherBus resolver SDK + built-in resolvers.

Public surface:

- :class:`Resolver` — Protocol every resolver implements (a ``scheme`` and an
  async ``fetch(uri) -> bytes``).
- :func:`register_resolver` — decorator that registers a resolver class.
- :func:`resolve_uri` — TTL-cached, single-flight entry point used by
  :meth:`aetherbus.Event.resolve`.
- :class:`ResolverError` — raised for any resolution failure.

Built-ins land here on import. Third-party resolvers register via the
``aetherbus.resolvers`` entry-point group (loaded once, on first import).
"""
from __future__ import annotations

# Import built-ins so their @register_resolver decorators run.
from . import file as _file  # noqa: F401 — import for side effect
from . import https as _https  # noqa: F401 — import for side effect
from .base import (
    Resolver,
    ResolverError,
    clear_cache,
    get_resolver,
    register_resolver,
    registered_schemes,
    resolve_uri,
)
from .base import _load_entry_point_resolvers as __load_eps

# Load third-party resolvers exactly once on first import of the package.
__load_eps()

__all__ = [
    "Resolver",
    "ResolverError",
    "clear_cache",
    "get_resolver",
    "register_resolver",
    "registered_schemes",
    "resolve_uri",
]
