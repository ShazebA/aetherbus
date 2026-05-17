"""MCP server adapter — exposes AetherBus topics as MCP resources.

CLI entry: ``aetherbus-mcp serve`` (see ``aetherbus.mcp_adapter.cli``).
"""
from __future__ import annotations

from .server import URI_PREFIX, AetherBusMCPServer

__all__ = ["URI_PREFIX", "AetherBusMCPServer"]
