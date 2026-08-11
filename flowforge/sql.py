"""Checkpoints in a SQL table, one row per run.

Redis is the right default for a paused run: short-lived, keyed, expiring on its
own. SQL earns its place when the checkpoint has to outlive a cache — a run
nobody answers until next week, or a record of what ran that survives a
``FLUSHALL``. Both stores implement the same ``RunStore`` protocol, so the engine
cannot tell them apart and either can be swapped in at the call site.

Two things SQL does not give you for free, and this module has to supply:

*expiry* — Redis has ``EX``; here ``expires_at`` is a column, filtered on read
and cleaned opportunistically, so a lapsed row can never be resumed even if it
is still physically present.

*upsert* — resuming rewrites a run's row, and the syntax for "insert or replace"
is per-dialect. That is what :class:`Dialect` carries: placeholder style, the
DDL types, and the conflict clause. Adding Postgres means adding one of these.

The drivers are synchronous (``sqlite3`` in the standard library, PyMySQL for
MySQL), so every call runs in a worker thread via ``asyncio.to_thread`` and the
event loop is never blocked on a socket it cannot see. Connections are checked
out of a small bounded pool, because a wide wave writing its frontier issues
those calls all at once — the same reason ``pool.py`` exists for Redis.
"""

from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass
from time import time
from typing import Any, Callable

from .checkpoint import Checkpoint
from .errors import FlowForgeError


class SQLStoreError(FlowForgeError):
    """The store could not talk to its database."""


@dataclass(frozen=True)
class Dialect:
    """The handful of places SQL dialects actually differ for this table."""

    name: str
    placeholder: str
    text_type: str
    id_type: str
    upsert: str

    def sql(self, statement: str) -> str:
        """Rewrite ``?`` placeholders into whatever this driver expects."""
        if self.placeholder == "?":
            return statement
        return statement.replace("?", self.placeholder)


SQLITE = Dialect(
    name="sqlite",
    placeholder="?",
    text_type="TEXT",
    id_type="TEXT",
    # sqlite needs the conflict target named; MySQL infers it from the key.
    upsert=(
        "ON CONFLICT(run_id) DO UPDATE SET "
        "workflow_id=excluded.workflow_id, payload=excluded.payload, "
        "updated_at=excluded.updated_at, expires_at=excluded.expires_at"
    ),
)

MYSQL = Dialect(
    name="mysql",
    placeholder="%s",
    # LONGTEXT, not TEXT: MySQL's TEXT caps at 64 KiB, and a checkpoint carries
    # a record per node plus the variable pool. A 500-node run would be
    # truncated — silently, on a non-strict server.
    text_type="LONGTEXT",
    # A prefix length is mandatory for a VARCHAR primary key on utf8mb4.
    id_type="VARCHAR(191)",
    upsert=(
        "ON DUPLICATE KEY UPDATE "
        "workflow_id=VALUES(workflow_id), payload=VALUES(payload), "
        "updated_at=VALUES(updated_at), expires_at=VALUES(expires_at)"
    ),
)


class SQLRunStore:
    """A ``RunStore`` backed by any DB-API 2.0 driver.

    Args:
        connect: zero-argument callable returning a fresh DB-API connection.
        dialect: :data:`SQLITE` or :data:`MYSQL`.
        table: table name, created on first use unless ``create_table`` is off.
        ttl_s: rows older than this are treated as absent and deleted when
            noticed. ``None`` keeps them forever. Saving again refreshes it, so
            an active run keeps extending its own life — same policy as
            :class:`~flowforge.store.RedisRunStore`.
        max_connections: bound on connections held open.
    """

    def __init__(
        self,
        connect: Callable[[], Any],
        dialect: Dialect = SQLITE,
        table: str = "flowforge_runs",
        ttl_s: int | None = 86_400,
        max_connections: int = 5,
        create_table: bool = True,
    ) -> None:
        if max_connections < 1:
            raise ValueError("max_connections must be >= 1")
        if not table.replace("_", "").isalnum():
            # Interpolated into the DDL, so it cannot be a placeholder.
            raise ValueError(f"table name must be alphanumeric: {table!r}")
        self._connect = connect
        self.dialect = dialect
        self.table = table
        self.ttl_s = ttl_s
        self._idle: list[Any] = []
        self._capacity = asyncio.Semaphore(max_connections)
        self._lock = asyncio.Lock()
        self._closed = False
        self._ready = not create_table

    # ------------------------------------------------------------- factories

    @classmethod
    def sqlite(cls, path: str = ":memory:", **kwargs: Any) -> "SQLRunStore":
        """A sqlite-backed store.

        ``:memory:`` gives *each connection* its own private database, so an
        in-memory store is pinned to a single connection — two writes through
        two connections would land in two different databases. Asking for more
        is refused rather than quietly ignored.
        """
        if path == ":memory:" and kwargs.setdefault("max_connections", 1) > 1:
            raise ValueError(
                "':memory:' is private to one connection; pass a file path to "
                "use max_connections > 1"
            )

        def connect() -> Any:
            # The connection is handed to worker threads one at a time, never
            # concurrently — the pool guarantees exclusive checkout.
            return sqlite3.connect(path, check_same_thread=False)

        return cls(connect, dialect=SQLITE, **kwargs)

    @classmethod
    def mysql(
        cls,
        host: str = "127.0.0.1",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "flowforge",
        **kwargs: Any,
    ) -> "SQLRunStore":
        try:
            import pymysql
        except ImportError as exc:  # pragma: no cover - driver is optional
            raise SQLStoreError(
                "MySQL support needs a driver: pip install 'flowforge[mysql]'"
            ) from exc

        def connect() -> Any:
            return pymysql.connect(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                autocommit=False,
                charset="utf8mb4",
            )

        return cls(connect, dialect=MYSQL, **kwargs)

    # ------------------------------------------------------------ plumbing

    def _ddl(self) -> str:
        d = self.dialect
        return (
            f"CREATE TABLE IF NOT EXISTS {self.table} ("
            f"  run_id {d.id_type} NOT NULL PRIMARY KEY,"
            f"  workflow_id {d.id_type} NOT NULL,"
            f"  payload {d.text_type} NOT NULL,"
            f"  updated_at DOUBLE NOT NULL,"
            f"  expires_at DOUBLE NULL"
            f")"
        )

    async def _checkout(self) -> Any:
        if self._closed:
            raise SQLStoreError("store is closed")
        await self._capacity.acquire()
        try:
            async with self._lock:
                if self._idle:
                    return self._idle.pop()
            return await asyncio.to_thread(self._connect)
        except BaseException:
            self._capacity.release()
            raise

    async def _release(self, connection: Any, reuse: bool = True) -> None:
        if reuse and not self._closed:
            async with self._lock:
                self._idle.append(connection)
        else:
            await asyncio.to_thread(_close_quietly, connection)
        self._capacity.release()

    async def _run(self, work: Callable[[Any], Any]) -> Any:
        """Hand a checked-out connection to a worker thread."""
        if not self._ready:
            await self._create_table()
        connection = await self._checkout()
        reuse = True
        try:
            return await asyncio.to_thread(_in_transaction, connection, work)
        except Exception as exc:
            # A driver error may have left the connection unusable; a fresh one
            # costs a handshake, a poisoned one costs every later call.
            reuse = False
            raise SQLStoreError(f"{self.dialect.name}: {exc}") from exc
        finally:
            await self._release(connection, reuse=reuse)

    async def _create_table(self) -> None:
        connection = await self._checkout()
        try:
            await asyncio.to_thread(
                _in_transaction, connection, lambda cursor: cursor.execute(self._ddl())
            )
            self._ready = True
        except Exception as exc:
            raise SQLStoreError(f"could not create {self.table}: {exc}") from exc
        finally:
            await self._release(connection)

    def _expiry(self) -> float | None:
        return time() + self.ttl_s if self.ttl_s else None

    # ---------------------------------------------------------------- RunStore

    async def save(self, checkpoint: Checkpoint) -> None:
        statement = self.dialect.sql(
            f"INSERT INTO {self.table} "
            f"(run_id, workflow_id, payload, updated_at, expires_at) "
            f"VALUES (?, ?, ?, ?, ?) {self.dialect.upsert}"
        )
        row = (
            checkpoint.run_id,
            checkpoint.workflow_id,
            checkpoint.to_json(),
            time(),
            self._expiry(),
        )
        await self._run(lambda cursor: cursor.execute(statement, row))

    async def load(self, run_id: str) -> Checkpoint | None:
        statement = self.dialect.sql(
            f"SELECT payload, expires_at FROM {self.table} WHERE run_id = ?"
        )

        def read(cursor: Any) -> tuple[Any, Any] | None:
            cursor.execute(statement, (run_id,))
            return cursor.fetchone()

        row = await self._run(read)
        if row is None:
            return None
        payload, expires_at = row[0], row[1]
        if expires_at is not None and expires_at <= time():
            # Lapsed but still on disk. Absent as far as callers are concerned,
            # and swept now that something has looked at it.
            await self.delete(run_id)
            return None
        text = payload.decode("utf-8") if isinstance(payload, bytes) else str(payload)
        return Checkpoint.from_json(text)

    async def delete(self, run_id: str) -> bool:
        statement = self.dialect.sql(f"DELETE FROM {self.table} WHERE run_id = ?")

        def remove(cursor: Any) -> int:
            cursor.execute(statement, (run_id,))
            return cursor.rowcount

        return bool(await self._run(remove))

    async def list_ids(self) -> list[str]:
        """Live runs only, sorted — same contract as the Redis store."""
        statement = self.dialect.sql(
            f"SELECT run_id FROM {self.table} "
            f"WHERE expires_at IS NULL OR expires_at > ? ORDER BY run_id"
        )

        def read(cursor: Any) -> list[str]:
            cursor.execute(statement, (time(),))
            return [str(row[0]) for row in cursor.fetchall()]

        return await self._run(read)

    async def purge_expired(self) -> int:
        """Delete every lapsed row. For a cron job; ``load`` already ignores them."""
        statement = self.dialect.sql(
            f"DELETE FROM {self.table} WHERE expires_at IS NOT NULL AND expires_at <= ?"
        )

        def remove(cursor: Any) -> int:
            cursor.execute(statement, (time(),))
            return cursor.rowcount

        return int(await self._run(remove))

    async def ping(self) -> bool:
        await self._run(lambda cursor: cursor.execute("SELECT 1"))
        return True

    async def close(self) -> None:
        self._closed = True
        async with self._lock:
            idle, self._idle = self._idle, []
        for connection in idle:
            await asyncio.to_thread(_close_quietly, connection)


def _in_transaction(connection: Any, work: Callable[[Any], Any]) -> Any:
    """Run ``work`` against a cursor, committing or rolling back as it goes.

    Every statement here is a single row touch, so the transaction exists to make
    the failure case clean rather than to group anything: a driver error must not
    leave the connection sitting inside an open transaction that the next
    checkout inherits.
    """
    cursor = connection.cursor()
    try:
        result = work(cursor)
        connection.commit()
        return result
    except BaseException:
        try:
            connection.rollback()
        except Exception:
            pass
        raise
    finally:
        _close_quietly(cursor)


def _close_quietly(closeable: Any) -> None:
    try:
        closeable.close()
    except Exception:
        pass
