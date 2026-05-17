"""``aetherbus-mcp serve`` — Typer entry for the MCP adapter."""
from __future__ import annotations

import asyncio
import logging
import sys
from typing import Annotated

import typer

from ..bus import Bus
from ..config import Settings

try:
    from .server import AetherBusMCPServer
except ImportError as e:  # pragma: no cover — runtime install guard
    print(
        "aetherbus-mcp requires the [mcp] extra. Install with:\n"
        "  pip install 'aetherbus[mcp]'\n"
        f"Underlying error: {e}",
        file=sys.stderr,
    )
    raise SystemExit(2) from e

app = typer.Typer(help="MCP server adapter for AetherBus.", no_args_is_help=True)


@app.command()
def serve(
    topics: Annotated[
        list[str],
        typer.Option(
            "--topic",
            "-t",
            help="Bus topic to expose as an MCP resource. Repeat for multiple.",
        ),
    ],
    bus_url: Annotated[
        str | None,
        typer.Option("--bus", help="NATS URL. Falls back to AETHERBUS_URL env."),
    ] = None,
    config: Annotated[
        str | None,
        typer.Option("--config", help="Path to aetherbus.yaml (overrides --bus)."),
    ] = None,
    verbose: Annotated[bool, typer.Option("-v", "--verbose")] = False,
) -> None:
    """Start the MCP server over stdio.

    Drop into Claude Desktop's ``claude_desktop_config.json``:

    .. code-block:: json

        {
          "mcpServers": {
            "aetherbus": {
              "command": "aetherbus-mcp",
              "args": ["serve", "--topic", "dev.aetherbus.research.finding.v1"]
            }
          }
        }
    """
    # MCP uses stdio for the protocol — log to stderr only.
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if not topics:
        raise typer.BadParameter("at least one --topic is required")

    if config is not None:
        settings = Settings.from_yaml(config)
    else:
        settings = Settings.from_env()
        if bus_url:
            settings.transport_url = bus_url

    bus = Bus(settings)
    server = AetherBusMCPServer(bus, topics)
    asyncio.run(server.run_stdio())


@app.command()
def version() -> None:
    """Print the installed aetherbus version."""
    from .. import __version__

    typer.echo(__version__)


def main() -> None:  # pragma: no cover — wired through pyproject scripts
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
