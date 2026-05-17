from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any, TypeVar, overload

from pydantic import BaseModel

from .envelope import Envelope

T = TypeVar("T", bound="EventPayload")


class EventPayload(BaseModel):
    """Marker base class for typed event payloads.

    Subclass with Pydantic v2 fields. Pass instances to ``bus.publish``;
    consumers deserialize via ``event.resolve(as_=YourModel)``.
    """


@dataclass
class Event:
    """A received event. Thin wrapper around a CloudEvents envelope."""

    envelope: Envelope

    @property
    def id(self) -> str:
        return self.envelope.id

    @property
    def source(self) -> str:
        return self.envelope.source

    @property
    def type(self) -> str:
        return self.envelope.type

    @property
    def time(self) -> datetime:
        return datetime.fromisoformat(self.envelope.time)

    @property
    def dataref(self) -> str | None:
        return self.envelope.dataref

    @property
    def inline_data(self) -> Any:
        return self.envelope.data

    @property
    def correlation_id(self) -> str | None:
        return self.envelope.aetherbuscorrelationid

    @property
    def depends_on(self) -> list[str]:
        return self.envelope.depends_on

    @overload
    async def resolve(self, as_: type[T]) -> T: ...
    @overload
    async def resolve(self, as_: None = None) -> Any: ...
    async def resolve(self, as_: type[T] | None = None) -> Any:
        if self.envelope.dataref is not None:
            raise NotImplementedError(
                "resolver SDK ships in M3; this event has "
                f"dataref={self.envelope.dataref!r} and cannot be resolved yet."
            )
        data = self.envelope.data
        if as_ is None:
            return data
        return as_.model_validate(data)


Handler = Callable[[Event], Awaitable[None]]
