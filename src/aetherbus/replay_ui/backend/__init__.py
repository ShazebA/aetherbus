"""Replay UI backend — FastAPI app + Typer CLI.

Public surface:
- :func:`create_app` — build the FastAPI app for a given Bus.
- :data:`app` — module-level app instance configured from ``AETHERBUS_*`` env.
"""
from __future__ import annotations

from .app import app, create_app

__all__ = ["app", "create_app"]
