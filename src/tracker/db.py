"""SQLite persistence for per-subscriber watchlists — the ONLY thing this project stores.

A single ``subscriptions(chat_id, address, label)`` table keyed by ``(chat_id, address)``: each
Telegram subscriber keeps their own labelled list of wallets. Addresses are stored lowercase
(the caller normalizes). WAL + a busy timeout keep concurrent command/reconcile writes from
tripping ``SQLITE_BUSY``. No trade/position/PnL data is ever written here.
"""

from dataclasses import dataclass
from pathlib import Path

import aiosqlite

_SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    chat_id INTEGER NOT NULL,
    address TEXT    NOT NULL,
    label   TEXT    NOT NULL,
    PRIMARY KEY (chat_id, address)
) STRICT;
"""


@dataclass(frozen=True, slots=True)
class Subscription:
    """One subscriber tracking one wallet, under their own label."""

    chat_id: int
    address: str
    label: str


class WatchlistDB:
    """Async CRUD over the ``subscriptions`` table."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: aiosqlite.Connection | None = None

    async def connect(self) -> None:
        """Open the connection, apply pragmas, and create the table if absent."""
        conn = await aiosqlite.connect(self._path)
        await conn.execute("PRAGMA journal_mode = WAL")
        await conn.execute("PRAGMA busy_timeout = 5000")
        await conn.execute("PRAGMA synchronous = NORMAL")
        conn.row_factory = aiosqlite.Row
        await conn.execute(_SCHEMA)
        await conn.commit()
        self._conn = conn

    @property
    def _db(self) -> aiosqlite.Connection:
        if self._conn is None:
            raise RuntimeError("WatchlistDB used before connect()")
        return self._conn

    async def all(self) -> list[Subscription]:
        """Return every subscription across all subscribers (for startup load).

        Addresses are lower-cased defensively: the listener's filter requires lowercase (it
        lowercases each trade's counterparties), and this keeps that invariant true even if a row
        was written out-of-band, without trusting the writer.
        """
        async with self._db.execute("SELECT chat_id, address, label FROM subscriptions") as cur:
            rows = await cur.fetchall()
        return [Subscription(r["chat_id"], r["address"].lower(), r["label"]) for r in rows]

    async def list_for(self, chat_id: int) -> dict[str, str]:
        """Return one subscriber's watchlist as ``{address: label}``."""
        async with self._db.execute(
            "SELECT address, label FROM subscriptions WHERE chat_id = ?", (chat_id,)
        ) as cur:
            rows = await cur.fetchall()
        return {r["address"]: r["label"] for r in rows}

    # INVARIANT: every write below is a single DML + commit. The connection is shared with the
    # reconcile loop, so a MULTI-statement write here could have its partial DML committed by an
    # unrelated coroutine's commit(); never make these multi-statement without serializing them
    # under a private asyncio.Lock or a dedicated write connection.

    async def add(self, chat_id: int, address: str, label: str) -> None:
        """Insert or relabel one subscriber's wallet (idempotent upsert)."""
        async with self._db.execute(
            "INSERT INTO subscriptions (chat_id, address, label) VALUES (?, ?, ?) "
            "ON CONFLICT(chat_id, address) DO UPDATE SET label = excluded.label",
            (chat_id, address, label),
        ):
            pass
        await self._db.commit()

    async def delete(self, chat_id: int, address: str) -> bool:
        """Remove one subscriber's wallet. Returns ``True`` if a row was deleted."""
        async with self._db.execute(
            "DELETE FROM subscriptions WHERE chat_id = ? AND address = ?", (chat_id, address)
        ) as cur:
            deleted = cur.rowcount > 0
        await self._db.commit()
        return deleted

    async def rename(self, chat_id: int, address: str, label: str) -> bool:
        """Relabel one subscriber's wallet. Returns ``True`` if it existed."""
        async with self._db.execute(
            "UPDATE subscriptions SET label = ? WHERE chat_id = ? AND address = ?",
            (label, chat_id, address),
        ) as cur:
            renamed = cur.rowcount > 0
        await self._db.commit()
        return renamed

    async def aclose(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None
