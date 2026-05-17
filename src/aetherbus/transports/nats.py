from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any

import nats
from nats.aio.client import Client as NATSClient
from nats.errors import NoServersError
from nats.js import JetStreamContext
from nats.js.api import ConsumerConfig, StreamConfig
from nats.js.api import DeliverPolicy as NatsDeliver
from nats.js.errors import NotFoundError

from .base import DeliverPolicy, Msg, Subscription, TransportError


class NATSTransport:
    """JetStream-backed Transport. Default in v1."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._nc: NATSClient | None = None
        self._js: JetStreamContext | None = None

    async def connect(self) -> None:
        try:
            self._nc = await nats.connect(self._url)
        except NoServersError as e:
            raise TransportError(f"could not reach NATS at {self._url}: {e}") from e
        self._js = self._nc.jetstream()

    async def close(self) -> None:
        if self._nc is not None:
            try:
                await self._nc.drain()
            finally:
                self._nc = None
                self._js = None

    async def ensure_stream(self, name: str, subjects: list[str]) -> None:
        assert self._js is not None, "transport not connected"
        try:
            await self._js.stream_info(name)
        except NotFoundError:
            await self._js.add_stream(StreamConfig(name=name, subjects=subjects))

    async def publish(
        self, topic: str, payload: bytes, headers: dict[str, str]
    ) -> None:
        assert self._js is not None, "transport not connected"
        try:
            await self._js.publish(topic, payload, headers=headers or None)
        except Exception as e:  # nats raises various subclasses; normalize
            raise TransportError(f"publish failed for {topic}: {e}") from e

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
    ) -> Subscription:
        assert self._js is not None, "transport not connected"

        if opt_start_time is not None:
            nats_policy = NatsDeliver.BY_START_TIME
        elif deliver_policy is DeliverPolicy.EARLIEST:
            nats_policy = NatsDeliver.ALL
        else:
            nats_policy = NatsDeliver.NEW

        cfg = ConsumerConfig(
            durable_name=durable,
            deliver_group=queue_group,
            deliver_policy=nats_policy,
            opt_start_time=opt_start_time.isoformat() if opt_start_time else None,
            max_ack_pending=max_in_flight,
            filter_subject=topic,
            ack_wait=30,
        )

        async def _cb(nm: Any) -> None:
            msg = Msg(
                data=nm.data,
                headers=dict(nm.headers or {}),
                _ack_fn=nm.ack,
                _nack_fn=nm.nak,
            )
            await callback(msg)

        psub = await self._js.subscribe(
            topic,
            queue=queue_group,
            cb=_cb,
            durable=durable,
            config=cfg,
        )

        class _Sub:
            async def unsubscribe(self) -> None:
                await psub.unsubscribe()

        return _Sub()
