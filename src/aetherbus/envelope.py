from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


class EnvelopeError(ValueError):
    """Raised when an envelope violates CloudEvents or AetherBus rules."""


def new_id() -> str:
    """UUIDv7. Time-sortable, no external dep — see ADR-005."""
    unix_ms = int(time.time() * 1000) & 0xFFFFFFFFFFFF
    rand_a = int.from_bytes(os.urandom(2), "big") & 0xFFF
    rand_b = int.from_bytes(os.urandom(8), "big") & 0x3FFFFFFFFFFFFFFF
    val = (
        (unix_ms << 80)
        | (0x7 << 76)               # version 7
        | (rand_a << 64)
        | (0b10 << 62)              # variant (RFC 4122)
        | rand_b
    )
    h = f"{val:032x}"
    return f"{h[0:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"


@dataclass
class Envelope:
    """CloudEvents 1.0 envelope with AetherBus extension attributes.

    Either ``data`` (inline JSON-serializable) or ``dataref`` (URI). Both unset
    is allowed (e.g. a tick/heartbeat). Both set is not.
    """

    id: str
    source: str
    type: str
    time: str
    datacontenttype: str = "application/json"
    data: Any | None = None
    dataref: str | None = None
    aetherbusschema: str | None = None
    aetherbusttl: int | None = None
    aetherbusdependencies: str | None = None
    aetherbuscorrelationid: str | None = None
    aetherbussignature: str | None = None

    def __post_init__(self) -> None:
        if self.data is not None and self.dataref is not None:
            raise EnvelopeError("envelope has both inline data and dataref")

    @classmethod
    def new(
        cls,
        *,
        source: str,
        type: str,
        data: Any | None = None,
        dataref: str | None = None,
        correlation_id: str | None = None,
        depends_on: list[str] | None = None,
        ttl: int | None = None,
        schema_uri: str | None = None,
    ) -> Envelope:
        return cls(
            id=new_id(),
            source=source,
            type=type,
            time=datetime.now(UTC).isoformat(),
            data=data,
            dataref=dataref,
            aetherbuscorrelationid=correlation_id,
            aetherbusdependencies=",".join(depends_on) if depends_on else None,
            aetherbusttl=ttl,
            aetherbusschema=schema_uri,
        )

    def to_bytes(self) -> bytes:
        out: dict[str, Any] = {
            "specversion": "1.0",
            "id": self.id,
            "source": self.source,
            "type": self.type,
            "time": self.time,
            "datacontenttype": self.datacontenttype,
        }
        if self.data is not None:
            out["data"] = self.data
        if self.dataref is not None:
            out["dataref"] = self.dataref
        for k in (
            "aetherbusschema",
            "aetherbusttl",
            "aetherbusdependencies",
            "aetherbuscorrelationid",
            "aetherbussignature",
        ):
            v = getattr(self, k)
            if v is not None:
                out[k] = v
        return json.dumps(out, separators=(",", ":")).encode("utf-8")

    @classmethod
    def from_bytes(cls, raw: bytes) -> Envelope:
        d = json.loads(raw.decode("utf-8"))
        spec = d.get("specversion")
        if spec != "1.0":
            raise EnvelopeError(f"unsupported specversion: {spec!r}")
        return cls(
            id=d["id"],
            source=d["source"],
            type=d["type"],
            time=d["time"],
            datacontenttype=d.get("datacontenttype", "application/json"),
            data=d.get("data"),
            dataref=d.get("dataref"),
            aetherbusschema=d.get("aetherbusschema"),
            aetherbusttl=d.get("aetherbusttl"),
            aetherbusdependencies=d.get("aetherbusdependencies"),
            aetherbuscorrelationid=d.get("aetherbuscorrelationid"),
            aetherbussignature=d.get("aetherbussignature"),
        )

    @property
    def depends_on(self) -> list[str]:
        if not self.aetherbusdependencies:
            return []
        return [s for s in self.aetherbusdependencies.split(",") if s]
