from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import sqlite3
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

logger = logging.getLogger(__name__)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS outbox (
    id              TEXT PRIMARY KEY,
    topic           TEXT NOT NULL,
    payload         BLOB NOT NULL,
    headers         TEXT NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    next_attempt_at REAL NOT NULL,
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS outbox_next_attempt ON outbox(next_attempt_at);
"""


@dataclass
class OutboxRow:
    id: str
    topic: str
    payload: bytes
    headers: dict[str, str]
    attempts: int
    next_attempt_at: float
    created_at: float


PublishFn = Callable[[str, bytes, dict[str, str]], Awaitable[None]]
FailureFn = Callable[[OutboxRow], Awaitable[None]]


class Outbox:
    """SQLite-backed publish outbox. Single-process safe; one drainer per Bus."""

    def __init__(
        self,
        path: str | Path,
        *,
        max_attempts: int = 8,
        initial_backoff: float = 0.2,
        max_backoff: float = 30.0,
    ) -> None:
        self._path = Path(path)
        self._max_attempts = max_attempts
        self._initial_backoff = initial_backoff
        self._max_backoff = max_backoff
        self._wake = asyncio.Event()
        self._initialized = False

    def _run_sync(self, fn: Callable[[sqlite3.Connection], object]) -> object:
        with sqlite3.connect(str(self._path), isolation_level=None) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            return fn(conn)

    async def _exec(self, fn: Callable[[sqlite3.Connection], object]) -> object:
        return await asyncio.to_thread(self._run_sync, fn)

    async def init(self) -> None:
        if self._initialized:
            return
        await self._exec(lambda c: c.executescript(_SCHEMA))
        self._initialized = True

    async def enqueue(
        self, event_id: str, topic: str, payload: bytes, headers: dict[str, str]
    ) -> None:
        await self.init()
        now = time.time()

        def _ins(conn: sqlite3.Connection) -> None:
            conn.execute(
                "INSERT OR REPLACE INTO outbox"
                "(id, topic, payload, headers, attempts, next_attempt_at, created_at)"
                " VALUES (?, ?, ?, ?, 0, ?, ?)",
                (event_id, topic, payload, json.dumps(headers), now, now),
            )

        await self._exec(_ins)
        self._wake.set()

    async def pending_count(self) -> int:
        await self.init()

        def _q(conn: sqlite3.Connection) -> int:
            return int(conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0])

        return await asyncio.to_thread(self._run_sync, _q)  # type: ignore[return-value]

    async def _next_due(self, limit: int = 32) -> list[OutboxRow]:
        now = time.time()

        def _q(conn: sqlite3.Connection) -> list[OutboxRow]:
            cur = conn.execute(
                "SELECT id, topic, payload, headers, attempts, next_attempt_at, created_at"
                " FROM outbox WHERE next_attempt_at <= ? ORDER BY next_attempt_at LIMIT ?",
                (now, limit),
            )
            return [
                OutboxRow(
                    id=r[0],
                    topic=r[1],
                    payload=r[2],
                    headers=json.loads(r[3]),
                    attempts=r[4],
                    next_attempt_at=r[5],
                    created_at=r[6],
                )
                for r in cur
            ]

        return await asyncio.to_thread(self._run_sync, _q)  # type: ignore[return-value]

    async def _delete(self, event_id: str) -> None:
        await self._exec(lambda c: c.execute("DELETE FROM outbox WHERE id = ?", (event_id,)))

    async def _reschedule(self, event_id: str, attempts: int) -> None:
        delay = min(self._initial_backoff * (2 ** (attempts - 1)), self._max_backoff)
        delay += random.uniform(0, delay * 0.1)
        next_at = time.time() + delay

        def _u(conn: sqlite3.Connection) -> None:
            conn.execute(
                "UPDATE outbox SET attempts = ?, next_attempt_at = ? WHERE id = ?",
                (attempts, next_at, event_id),
            )

        await self._exec(_u)

    async def run(self, publish: PublishFn, on_failure: FailureFn | None = None) -> None:
        """Drain loop. Cancel the task to stop."""
        await self.init()
        try:
            while True:
                rows = await self._next_due()
                if not rows:
                    # Short idle poll — keeps backoff retries snappy. The wake
                    # event short-circuits when enqueue lands a new row.
                    with contextlib.suppress(asyncio.TimeoutError):
                        await asyncio.wait_for(self._wake.wait(), timeout=0.05)
                    self._wake.clear()
                    continue
                for row in rows:
                    try:
                        await publish(row.topic, row.payload, row.headers)
                        await self._delete(row.id)
                    except Exception:
                        attempts = row.attempts + 1
                        if attempts >= self._max_attempts:
                            logger.error(
                                "outbox: %s on %s exhausted attempts; abandoning",
                                row.id,
                                row.topic,
                            )
                            if on_failure is not None:
                                row.attempts = attempts
                                with contextlib.suppress(Exception):
                                    await on_failure(row)
                            await self._delete(row.id)
                        else:
                            await self._reschedule(row.id, attempts)
        except asyncio.CancelledError:
            return
