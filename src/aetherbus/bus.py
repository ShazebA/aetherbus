from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .config import Settings
from .envelope import Envelope, EnvelopeError
from .event import Event, EventPayload, Handler
from .outbox import Outbox
from .transports.base import DeliverPolicy, Msg, Subscription, Transport, TransportError
from .transports.nats import NATSTransport

logger = logging.getLogger(__name__)

_NAME_DISALLOWED = re.compile(r"[^A-Za-z0-9_-]+")


def _sanitize(name: str) -> str:
    return _NAME_DISALLOWED.sub("_", name)[:200]


def _build_transport(settings: Settings) -> Transport:
    """Resolve ``settings.transport_type`` to a concrete Transport instance."""
    kind = settings.transport_type
    if kind == "nats":
        return NATSTransport(settings.transport_url)
    if kind == "kafka":
        try:
            from .transports.kafka import KafkaTransport
        except ImportError as e:
            raise ImportError(
                "Kafka transport requires the [kafka] extra: pip install 'aetherbus[kafka]'"
            ) from e
        return KafkaTransport(settings.transport_url)
    raise ValueError(
        f"unknown transport_type {kind!r}; expected 'nats' or 'kafka'"
    )


@dataclass
class _Registration:
    topic: str
    handler: Handler
    queue_group: str | None
    replay_from: object  # "earliest" | "latest" | datetime | None
    max_in_flight: int
    index: int


class Bus:
    """Public AetherBus client. See PRD §6.2."""

    def __init__(self, settings: Settings, transport: Transport | None = None) -> None:
        self._settings = settings
        self._transport: Transport = transport or _build_transport(settings)
        self._regs: list[_Registration] = []
        self._schemas: dict[str, type[EventPayload]] = {}
        self._outbox = Outbox(
            settings.outbox_path,
            max_attempts=settings.outbox_max_attempts,
            initial_backoff=settings.outbox_initial_backoff,
            max_backoff=settings.outbox_max_backoff,
        )
        self._subs_active: list[Subscription] = []
        self._tasks: list[asyncio.Task[Any]] = []
        self._stop = asyncio.Event()
        self._connected = False

    @classmethod
    def from_env(cls) -> Bus:
        return cls(Settings.from_env())

    @classmethod
    def from_config(cls, path: str) -> Bus:
        return cls(Settings.from_yaml(path))

    def register_schema(self, topic: str, model: type[EventPayload]) -> None:
        self._schemas[topic] = model

    async def publish(
        self,
        topic: str,
        payload: EventPayload | dict[str, Any] | None = None,
        *,
        dataref: str | None = None,
        depends_on: list[str] | None = None,
        correlation_id: str | None = None,
        ttl: int | None = None,
        headers: dict[str, str] | None = None,
    ) -> str:
        if payload is not None and dataref is not None:
            raise ValueError("publish: provide either payload or dataref, not both")

        data: Any | None = None
        if isinstance(payload, EventPayload):
            registered = self._schemas.get(topic)
            if registered is not None and not isinstance(payload, registered):
                logger.warning(
                    "publish: %s expects %s; got %s (warn-mode, not blocking)",
                    topic,
                    registered.__name__,
                    type(payload).__name__,
                )
            data = payload.model_dump(mode="json")
        elif isinstance(payload, dict):
            data = payload
        elif payload is not None:
            got = type(payload).__name__
            raise TypeError(
                f"publish: payload must be EventPayload, dict, or None; got {got}"
            )

        env = Envelope.new(
            source=self._settings.source,
            type=topic,
            data=data,
            dataref=dataref,
            correlation_id=correlation_id,
            depends_on=depends_on,
            ttl=ttl,
        )
        raw = env.to_bytes()
        if data is not None and len(raw) > self._settings.inline_size_limit:
            raise EnvelopeError(
                f"inline payload size {len(raw)}B exceeds limit "
                f"{self._settings.inline_size_limit}B for {topic} — use dataref instead"
            )

        hdrs = headers or {}
        try:
            await self._transport.publish(topic, raw, hdrs)
        except TransportError:
            logger.warning("publish: transport unavailable for %s; queued in outbox", topic)
            await self._outbox.enqueue(env.id, topic, raw, hdrs)
        return env.id

    def subscribe(
        self,
        topic: str,
        *,
        queue_group: str | None = None,
        replay_from: Any = None,
        max_in_flight: int = 16,
    ) -> Callable[[Handler], Handler]:
        if replay_from is not None and not (
            isinstance(replay_from, datetime) or replay_from in ("earliest", "latest")
        ):
            raise ValueError(
                f"replay_from must be 'earliest', 'latest', or a datetime; got {replay_from!r}"
            )

        def deco(fn: Handler) -> Handler:
            self._regs.append(
                _Registration(
                    topic=topic,
                    handler=fn,
                    queue_group=queue_group,
                    replay_from=replay_from,
                    max_in_flight=max_in_flight,
                    index=len(self._regs),
                )
            )
            return fn

        return deco

    async def replay(
        self,
        topic: str,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        into: Bus | None = None,
        idle_timeout: float = 1.0,
    ):
        """Async-iterate stored events on ``topic`` between ``since`` and ``until``.

        Optionally re-emit each event onto a separate ``into`` bus (the
        sandbox-replay pattern from PRD §5.6). Re-publish preserves the
        original envelope bytes — same id, same time, same correlation_id —
        so downstream subscribers see history exactly as it happened.

        Yields :class:`Event` instances. Iteration stops when the broker is
        idle for ``idle_timeout`` seconds (no more history) or when ``until``
        passes.
        """
        await self.connect()
        if into is not None:
            await into.connect()

        async for msg in self._transport.history(
            topic, since=since, until=until, idle_timeout=idle_timeout
        ):
            env = Envelope.from_bytes(msg.data)
            if into is not None:
                # Re-emit the raw envelope on the sandbox transport — preserves
                # id, time, source, correlation_id, depends_on, signature, …
                try:
                    await into._transport.publish(topic, msg.data, msg.headers)
                except TransportError:
                    logger.warning(
                        "replay: sandbox publish failed for %s; queued in sandbox outbox",
                        topic,
                    )
                    await into._outbox.enqueue(env.id, topic, msg.data, msg.headers)
            yield Event(envelope=env)

    async def connect(self) -> None:
        if self._connected:
            return
        await self._transport.connect()
        await self._transport.ensure_stream(
            self._settings.stream_name,
            list(self._settings.stream_subjects),
        )
        self._connected = True

    async def __aenter__(self) -> Bus:
        await self.connect()
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self._transport.close()
        self._connected = False

    async def run(self) -> None:
        await self.connect()

        for reg in self._regs:
            await self._start_subscription(reg)

        outbox_task = asyncio.create_task(self._outbox.run(self._transport.publish))
        self._tasks.append(outbox_task)

        try:
            await self._stop.wait()
        finally:
            for s in self._subs_active:
                try:
                    await s.unsubscribe()
                except Exception:
                    logger.exception("error unsubscribing")
            for t in self._tasks:
                t.cancel()
            for t in self._tasks:
                try:
                    await t
                except (asyncio.CancelledError, Exception):
                    pass
            await self._transport.close()
            self._connected = False

    async def close(self) -> None:
        self._stop.set()

    async def _start_subscription(self, reg: _Registration) -> None:
        if isinstance(reg.replay_from, datetime):
            policy = DeliverPolicy.EARLIEST
            start_time: datetime | None = reg.replay_from
        elif reg.replay_from == "earliest":
            policy = DeliverPolicy.EARLIEST
            start_time = None
        else:
            policy = DeliverPolicy.LATEST
            start_time = None

        if reg.queue_group:
            durable = _sanitize(f"aetherbus_{reg.topic}_{reg.queue_group}")
        else:
            durable = _sanitize(f"aetherbus_{reg.topic}_{reg.handler.__name__}_{reg.index}")

        async def _dispatch(msg: Msg) -> None:
            try:
                env = Envelope.from_bytes(msg.data)
                event = Event(envelope=env)
                await reg.handler(event)
                await msg.ack()
            except Exception:
                logger.exception("handler error on %s", reg.topic)
                await msg.nack()

        psub = await self._transport.subscribe(
            reg.topic,
            durable=durable,
            queue_group=reg.queue_group,
            max_in_flight=reg.max_in_flight,
            deliver_policy=policy,
            opt_start_time=start_time,
            callback=_dispatch,
        )
        self._subs_active.append(psub)
