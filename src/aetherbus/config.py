from __future__ import annotations

import os
import socket
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

DEFAULT_STREAM_NAME = "AETHERBUS"
DEFAULT_STREAM_SUBJECTS: tuple[str, ...] = ("aetherbus.>", "dev.aetherbus.>")


def _auto_source() -> str:
    return f"aetherbus://agent/{socket.gethostname()}-{os.getpid()}"


@dataclass
class Settings:
    transport_type: str = "nats"
    transport_url: str = "nats://localhost:4222"
    source: str = field(default_factory=_auto_source)
    stream_name: str = DEFAULT_STREAM_NAME
    stream_subjects: tuple[str, ...] = DEFAULT_STREAM_SUBJECTS
    outbox_path: str = ".outbox.sqlite"
    outbox_max_attempts: int = 8
    outbox_initial_backoff: float = 0.2
    outbox_max_backoff: float = 30.0
    inline_size_limit: int = 4096
    run_index_path: str = ".runs.sqlite"

    @classmethod
    def from_env(cls) -> Settings:
        kwargs: dict[str, Any] = {
            "transport_url": os.environ.get("AETHERBUS_URL", "nats://localhost:4222"),
            "outbox_path": os.environ.get("AETHERBUS_OUTBOX", ".outbox.sqlite"),
            "run_index_path": os.environ.get("AETHERBUS_RUN_INDEX", ".runs.sqlite"),
        }
        src = os.environ.get("AETHERBUS_SOURCE")
        if src:
            kwargs["source"] = src
        return cls(**kwargs)

    @classmethod
    def from_yaml(cls, path: str | Path) -> Settings:
        data: dict[str, Any] = yaml.safe_load(Path(path).read_text()) or {}
        transport = data.get("transport") or {}
        stream = data.get("stream") or {}
        outbox = data.get("outbox") or {}
        kwargs: dict[str, Any] = {
            "transport_type": transport.get("type", "nats"),
            "transport_url": transport.get("url", "nats://localhost:4222"),
            "stream_name": stream.get("name", DEFAULT_STREAM_NAME),
            "stream_subjects": tuple(stream.get("subjects") or DEFAULT_STREAM_SUBJECTS),
            "outbox_path": outbox.get("path", ".outbox.sqlite"),
        }
        if data.get("source"):
            kwargs["source"] = data["source"]
        if data.get("run_index_path"):
            kwargs["run_index_path"] = data["run_index_path"]
        return cls(**kwargs)
