"""``aetherbus-ui serve`` — Typer CLI for the Replay UI backend."""
from __future__ import annotations

import logging
import os
import sys
from typing import Annotated

import typer

try:
    import uvicorn  # noqa: F401 — checked here so the error is friendly
except ImportError as e:  # pragma: no cover
    print(
        "aetherbus-ui requires the [replay] extra. Install with:\n"
        "  pip install 'aetherbus[replay]'\n"
        f"Underlying error: {e}",
        file=sys.stderr,
    )
    raise SystemExit(2) from e

app = typer.Typer(help="Replay UI backend for AetherBus.", no_args_is_help=True)


@app.command()
def serve(
    port: Annotated[int, typer.Option("--port", "-p", help="HTTP port.")] = 7798,
    bus_url: Annotated[
        str | None,
        typer.Option("--bus", help="NATS URL. Falls back to AETHERBUS_URL env."),
    ] = None,
    config: Annotated[
        str | None,
        typer.Option("--config", help="Path to aetherbus.yaml."),
    ] = None,
    host: Annotated[
        str,
        typer.Option("--host", help="Bind host. Defaults to loopback."),
    ] = "127.0.0.1",
    verbose: Annotated[bool, typer.Option("-v", "--verbose")] = False,
) -> None:
    """Run the Replay UI backend.

    PRD §6.7 specifies loopback-only by default (no auth in v1). Override
    ``--host`` only if you've put a reverse proxy with basic auth in front.
    """
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stderr,
    )

    if config:
        os.environ["AETHERBUS_CONFIG"] = config
    if bus_url:
        os.environ["AETHERBUS_URL"] = bus_url

    import uvicorn

    uvicorn.run(
        "aetherbus.replay_ui.backend.app:app",
        host=host,
        port=port,
        log_level="debug" if verbose else "info",
    )


@app.command()
def version() -> None:
    """Print the installed aetherbus version."""
    from ... import __version__

    typer.echo(__version__)


def main() -> None:  # pragma: no cover — wired through pyproject scripts
    app()


if __name__ == "__main__":  # pragma: no cover
    main()
