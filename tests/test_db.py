"""SQLite per-subscriber watchlist persistence CRUD."""

from pathlib import Path

from tracker.db import Subscription, WatchlistDB


async def _open(tmp_path: Path) -> WatchlistDB:
    db = WatchlistDB(tmp_path / "tracker.db")
    await db.connect()
    return db


async def test_subscriptions_are_scoped_per_chat(tmp_path: Path):
    db = await _open(tmp_path)
    try:
        await db.add(1, "0xa", "Whale")
        await db.add(1, "0xb", "Other")
        await db.add(2, "0xa", "MyWhale")  # different subscriber, same wallet, own label
        assert await db.list_for(1) == {"0xa": "Whale", "0xb": "Other"}
        assert await db.list_for(2) == {"0xa": "MyWhale"}

        every = await db.all()
        assert len(every) == 3
        assert Subscription(1, "0xa", "Whale") in every
        assert Subscription(2, "0xa", "MyWhale") in every
    finally:
        await db.aclose()


async def test_upsert_rename_delete_are_per_chat(tmp_path: Path):
    db = await _open(tmp_path)
    try:
        await db.add(1, "0xa", "Whale")
        await db.add(2, "0xa", "MyWhale")

        await db.add(1, "0xa", "relabel-via-upsert")  # ON CONFLICT relabels chat 1 only
        assert (await db.list_for(1))["0xa"] == "relabel-via-upsert"
        assert (await db.list_for(2))["0xa"] == "MyWhale"

        assert await db.rename(1, "0xa", "Whale-One") is True
        assert await db.rename(9, "0xa", "nope") is False

        assert await db.delete(1, "0xa") is True  # only chat 1's subscription
        assert await db.delete(1, "0xa") is False
        assert await db.list_for(1) == {}
        assert await db.list_for(2) == {"0xa": "MyWhale"}  # chat 2 unaffected
    finally:
        await db.aclose()


async def test_subscriptions_persist_across_reopen(tmp_path: Path):
    db = await _open(tmp_path)
    await db.add(1, "0xa", "Whale")
    await db.aclose()

    reopened = await _open(tmp_path)
    try:
        assert await reopened.list_for(1) == {"0xa": "Whale"}
    finally:
        await reopened.aclose()
