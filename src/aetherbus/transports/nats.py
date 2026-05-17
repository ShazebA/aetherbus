from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import datetime
from typing import Any

import nats
from nats.aio.client import Client as NATSClient
from nats.errors import NoServersError
from nats.errors import TimeoutError as NatsTimeoutError
from nats.js import JetStreamContext
from nats.js.api import AckPolicy, ConsumerConfig, StreamConfig
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

        # nats-py 2.14 requires queue == durable when both are passed, and uses
        # the queue string as the consumer's deliver_group. We encode the user-
        # facing queue_group into the topic-scoped durable, then reuse it as
        # both the queue name and the deliver_group so the wire-level check at
        # nats/js/client.py:375 succeeds for second-and-later subscribers.
        effective_queue = durable if queue_group else None

        cfg = ConsumerConfig(
            durable_name=durable,
            deliver_group=effective_queue,
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
            queue=effective_queue,
            cb=_cb,
            durable=durable,
            config=cfg,
        )

        class _Sub:
            async def unsubscribe(self) -> None:
                await psub.unsubscribe()

        return _Sub()

    async def history(
        self,
        topic: str,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        idle_timeout: float = 1.0,
    ) -> AsyncIterator[Msg]:
        """JetStream pull-consumer iteration over stored events on ``topic``.

        Creates an ephemeral durable consumer scoped to this iteration and
        deletes it on close. Ack policy is EXPLICIT — we ack each message so
        the consumer's "delivered" cursor advances but won't redeliver.
        """
        assert self._js is not None, "transport not connected"

        durable = f"replay_{uuid.uuid4().hex[:12]}"
        if since is not None:
            policy = NatsDeliver.BY_START_TIME
            opt_start_time: str | None = since.isoformat()
        else:
            policy = NatsDeliver.ALL
            opt_start_time = None

        cfg = ConsumerConfig(
            durable_name=durable,
            deliver_policy=policy,
            opt_start_time=opt_start_time,
            ack_policy=AckPolicy.EXPLICIT,
            filter_subject=topic,
        )

        psub = await self._js.pull_subscribe(topic, durable=durable, config=cfg)
        stream_name = await self._js.find_stream_name_by_subject(topic)
        try:
            while True:
                try:
                    batch = await psub.fetch(batch=32, timeout=idle_timeout)
                except (TimeoutError, NatsTimeoutError):
                    return  # idle — caught up

                if not batch:
                    return

                for nm in batch:
                    if until is not None and nm.metadata is not None:
                        ts = nm.metadata.timestamp
                        if ts is not None and ts > until:
                            await nm.ack()
                            return
                    yield Msg(
                        data=nm.data,
                        headers=dict(nm.headers or {}),
                        _ack_fn=nm.ack,
                        _nack_fn=nm.nak,
                    )
                    # Replay reads are non-destructive but we still ack so the
                    # ephemeral consumer's pending count doesn't grow.
                    await nm.ack()
        finally:
            try:
                await self._js.delete_consumer(stream_name, durable)
            except Exception:
                pass
