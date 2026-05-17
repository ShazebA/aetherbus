from __future__ import annotations

from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Protocol, runtime_checkable


class DeliverPolicy(Enum):
    EARLIEST = "earliest"
    LATEST = "latest"


class TransportError(Exception):
    """Raised when an underlying broker operation fails."""


@dataclass
class Msg:
    data: bytes
    headers: dict[str, str]
    _ack_fn: Callable[[], Awaitable[None]]
    _nack_fn: Callable[[], Awaitable[None]]

    async def ack(self) -> None:
        await self._ack_fn()

    async def nack(self) -> None:
        await self._nack_fn()


@runtime_checkable
class Subscription(Protocol):
    async def unsubscribe(self) -> None: ...


@runtime_checkable
class Transport(Protocol):
    async def connect(self) -> None: ...
    async def close(self) -> None: ...
    async def ensure_stream(self, name: str, subjects: list[str]) -> None: ...
    async def publish(
        self, topic: str, payload: bytes, headers: dict[str, str]
    ) -> None: ...
    async def subscribe(
        self,
        topic: str,
        *,
        durable: str,
        queue_group: str | None,
        max_in_flight: int,
        deliver_policy: DeliverPolicy,
        opt_start_time: datetime | None,
        callback: Callable[[Msg], Awaitable[None]],
    ) -> Subscription: ...

    def history(
        self,
        topic: str,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        idle_timeout: float = 1.0,
    ) -> AsyncIterator[Msg]:
        """Iterate stored messages for ``topic`` between ``since`` and ``until``.

        Returns an async generator. The generator stops on three conditions:
        ``until`` passed, the broker is idle for ``idle_timeout`` seconds (no
        more history to read), or the caller breaks out of the loop. Cleanup
        of broker-side cursors / consumers happens on close.
        """
        ...
