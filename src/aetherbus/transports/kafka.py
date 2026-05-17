"""Kafka transport — ``aiokafka``-backed implementation of the Transport Protocol.

Maps the AetherBus surface onto Kafka semantics (see ADR-008 for the full table):

- topic name → Kafka topic (dots are valid; pass-through)
- ``ensure_stream`` → ensure topic via admin client (idempotent)
- ``queue_group`` → consumer ``group_id``
- ``replay_from="earliest"`` / ``"latest"`` → ``auto_offset_reset``
- ``replay_from=datetime`` → ``offsets_for_times`` + ``seek``
- ``ack`` → commit offset; ``nack`` → ``seek`` back to the same offset (re-poll)
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError, TopicAlreadyExistsError

from .base import DeliverPolicy, Msg, Subscription, TransportError

logger = logging.getLogger(__name__)


def _normalize_url(url: str) -> str:
    """Accept ``kafka://host:port`` or bare ``host:port``."""
    if url.startswith("kafka://"):
        return url[len("kafka://"):]
    return url


class _KafkaSubscription:
    """A running consumer + dispatch task. Cancel on ``unsubscribe``."""

    def __init__(self, consumer: AIOKafkaConsumer, task: asyncio.Task[None]) -> None:
        self._consumer = consumer
        self._task = task

    async def unsubscribe(self) -> None:
        self._task.cancel()
        try:
            await self._task
        except (asyncio.CancelledError, Exception):
            pass
        try:
            await self._consumer.stop()
        except Exception:
            logger.exception("error stopping kafka consumer")


class KafkaTransport:
    """aiokafka-backed Transport. Selected when ``settings.transport_type == 'kafka'``."""

    def __init__(self, url: str) -> None:
        self._bootstrap = _normalize_url(url)
        self._producer: AIOKafkaProducer | None = None
        self._admin: AIOKafkaAdminClient | None = None

    async def connect(self) -> None:
        try:
            self._producer = AIOKafkaProducer(bootstrap_servers=self._bootstrap)
            await self._producer.start()
            self._admin = AIOKafkaAdminClient(bootstrap_servers=self._bootstrap)
            await self._admin.start()
        except KafkaError as e:
            raise TransportError(f"could not reach Kafka at {self._bootstrap}: {e}") from e

    async def close(self) -> None:
        if self._producer is not None:
            try:
                await self._producer.stop()
            finally:
                self._producer = None
        if self._admin is not None:
            try:
                await self._admin.close()
            finally:
                self._admin = None

    async def ensure_stream(self, name: str, subjects: list[str]) -> None:
        """Ensure each subject exists as a Kafka topic.

        Kafka has no "stream" container — subjects map 1:1 to topics. The
        ``name`` parameter is purely cosmetic on Kafka and is ignored except
        for logging. Wildcard subjects (``foo.>`` or ``foo.*``) are skipped
        here; topics for those land on first publish (auto-create) or via a
        separate explicit creation call.
        """
        assert self._admin is not None, "transport not connected"
        concrete = [s for s in subjects if not _has_wildcard(s)]
        if not concrete:
            logger.debug("ensure_stream %s: only wildcards, nothing to pre-create", name)
            return
        new_topics = [
            NewTopic(name=s, num_partitions=1, replication_factor=1) for s in concrete
        ]
        try:
            await self._admin.create_topics(new_topics)
        except TopicAlreadyExistsError:
            pass
        except KafkaError as e:
            # Some brokers respond with TopicAlreadyExistsError wrapped in a
            # different exception class depending on protocol version. Don't
            # blow up on benign re-creation.
            if "already exists" in str(e).lower():
                return
            raise TransportError(f"ensure_stream failed for {concrete}: {e}") from e

    async def publish(
        self, topic: str, payload: bytes, headers: dict[str, str]
    ) -> None:
        assert self._producer is not None, "transport not connected"
        # aiokafka headers are list[tuple[str, bytes]].
        kheaders = [(k, v.encode("utf-8")) for k, v in headers.items()]
        try:
            await self._producer.send_and_wait(topic, payload, headers=kheaders or None)
        except KafkaError as e:
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
        # Group id: the user's queue_group if set, else a per-durable group
        # so each solo subscriber has its own offset cursor (NATS parity).
        group_id = queue_group or f"aetherbus_solo_{durable}"

        # Translate replay_from to auto_offset_reset.
        auto_offset_reset = (
            "earliest" if deliver_policy is DeliverPolicy.EARLIEST else "latest"
        )

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._bootstrap,
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset=auto_offset_reset,
        )
        try:
            await consumer.start()
        except KafkaError as e:
            raise TransportError(f"subscribe start failed for {topic}: {e}") from e

        # If the user asked for a wall-clock start, seek each partition to
        # that timestamp once the consumer has its assignment.
        if opt_start_time is not None:
            try:
                await _seek_to_time(consumer, opt_start_time)
            except Exception:
                logger.exception("seek to %s failed; consumer will use defaults", opt_start_time)

        flight_sema = asyncio.Semaphore(max_in_flight)
        task = asyncio.create_task(
            _run_dispatch(consumer, callback, flight_sema),
            name=f"kafka-dispatch-{topic}",
        )
        return _KafkaSubscription(consumer, task)


def _has_wildcard(subject: str) -> bool:
    return any(token in ("*", ">") for token in subject.split("."))


async def _seek_to_time(consumer: AIOKafkaConsumer, when: datetime) -> None:
    """Block briefly waiting for assignment, then seek each partition."""
    # Force assignment by polling once with a tiny timeout.
    for _ in range(50):
        if consumer.assignment():
            break
        await asyncio.sleep(0.05)
    ts_ms = int(when.timestamp() * 1000)
    partitions = list(consumer.assignment())
    if not partitions:
        return
    offsets = await consumer.offsets_for_times({tp: ts_ms for tp in partitions})
    for tp, ot in offsets.items():
        if ot is not None:
            consumer.seek(tp, ot.offset)


async def _run_dispatch(
    consumer: AIOKafkaConsumer,
    callback: Callable[[Msg], Awaitable[None]],
    flight_sema: asyncio.Semaphore,
) -> None:
    """Pull messages, dispatch with bounded in-flight, commit on ack."""
    pending: set[asyncio.Task[None]] = set()
    try:
        async for record in consumer:
            tp = TopicPartition(record.topic, record.partition)
            offset = record.offset

            await flight_sema.acquire()

            async def _ack(_consumer: AIOKafkaConsumer = consumer,
                           _tp: TopicPartition = tp,
                           _offset: int = offset) -> None:
                try:
                    await _consumer.commit({_tp: _offset + 1})
                finally:
                    flight_sema.release()

            async def _nack(_consumer: AIOKafkaConsumer = consumer,
                            _tp: TopicPartition = tp,
                            _offset: int = offset) -> None:
                try:
                    _consumer.seek(_tp, _offset)
                finally:
                    flight_sema.release()

            headers = {k: (v.decode("utf-8") if isinstance(v, bytes) else v)
                       for k, v in (record.headers or [])}
            msg = Msg(
                data=record.value,
                headers=headers,
                _ack_fn=_ack,
                _nack_fn=_nack,
            )

            # Run the callback in a task so we keep pulling. Hold strong refs
            # so the GC doesn't drop in-flight handlers.
            task = asyncio.create_task(callback(msg))
            pending.add(task)
            task.add_done_callback(pending.discard)
    except asyncio.CancelledError:
        return
    except Exception:
        logger.exception("kafka dispatch loop errored")
    finally:
        for t in pending:
            t.cancel()
