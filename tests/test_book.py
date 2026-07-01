"""In-memory book: ingest lifecycle, cold-start seeding, leverage cache, tid de-dupe."""

from datetime import UTC, datetime
from decimal import Decimal

from tracker.book import InMemoryBook, SeenTids
from tracker.resolve import seed_state_from_row
from tracker.state import EVENT_ADD, EVENT_OPEN

D = Decimal
TS = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)


def _ingest(book, address, coin, delta, px):
    return book.ingest(address=address, coin=coin, delta=D(delta), px=D(px), ts=TS)


def test_first_fill_on_empty_book_opens():
    book = InMemoryBook()
    events = _ingest(book, "0xa", "BTC", "2", "100")
    assert [e.kind for e in events] == [EVENT_OPEN]
    pos = book.position("0xa", "BTC")
    assert pos is not None and pos.szi == D(2)


def test_cold_start_seeded_position_makes_next_fill_an_add_not_open():
    # The crux: a wallet opened a long "yesterday"; with no storage we seed from the chain.
    book = InMemoryBook()
    seeded = seed_state_from_row("0xa", "BTC", D(5), D(90), fallback_ts=TS)
    book.reseed_wallet("0xa", [seeded], {"BTC": 10})

    events = _ingest(book, "0xa", "BTC", "1", "100")  # buys 1 more today
    assert [e.kind for e in events] == [EVENT_ADD]  # NOT a false "Started trade"
    pos = book.position("0xa", "BTC")
    assert pos is not None and pos.szi == D(6)
    assert book.leverage("0xa", "BTC") == 10


def test_close_drops_the_position_from_the_book():
    book = InMemoryBook()
    _ingest(book, "0xa", "BTC", "2", "100")
    _ingest(book, "0xa", "BTC", "-2", "110")
    assert book.position("0xa", "BTC") is None
    assert book.open_position_count == 0


def test_reseed_replaces_and_drops_stale_coins():
    book = InMemoryBook()
    _ingest(book, "0xa", "BTC", "2", "100")
    _ingest(book, "0xa", "ETH", "3", "50")
    # Chain now only reports ETH (BTC closed while we were disconnected).
    book.reseed_wallet("0xa", [seed_state_from_row("0xa", "ETH", D(3), D(50), fallback_ts=TS)], {})
    assert book.position("0xa", "BTC") is None
    eth = book.position("0xa", "ETH")
    assert eth is not None and eth.szi == D(3)


def test_drop_wallet_forgets_positions_and_leverage():
    book = InMemoryBook()
    _ingest(book, "0xa", "BTC", "2", "100")
    book.reseed_wallet(
        "0xa", [seed_state_from_row("0xa", "BTC", D(2), D(100), fallback_ts=TS)], {"BTC": 5}
    )
    book.drop_wallet("0xa")
    assert book.position("0xa", "BTC") is None
    assert book.leverage("0xa", "BTC") is None


def test_reconcile_reseed_skips_when_a_fill_landed_since_snapshot():
    # Models the reconcile lost-update guard: capture the epoch (as the reconcile does BEFORE its
    # clearinghouseState await), let a live fill land during the "snapshot window", then a reseed
    # with the now-stale snapshot must be skipped rather than clobber the fresher live state.
    book = InMemoryBook()
    book.reseed_wallet("0xa", [seed_state_from_row("0xa", "BTC", D(10), D(90), fallback_ts=TS)], {})
    epoch = book.fill_epoch("0xa")  # captured before the (simulated) await
    _ingest(book, "0xa", "BTC", "5", "100")  # live +5 lands during the window → now long 15

    stale = [seed_state_from_row("0xa", "BTC", D(10), D(90), fallback_ts=TS)]
    applied = book.reseed_wallet("0xa", stale, {}, expected_epoch=epoch)

    assert applied is False  # skipped
    pos = book.position("0xa", "BTC")
    assert pos is not None and pos.szi == D(15)  # live +5 preserved, not reverted to 10


def test_reconcile_reseed_applies_when_no_fill_since_snapshot():
    book = InMemoryBook()
    _ingest(book, "0xa", "BTC", "2", "100")
    epoch = book.fill_epoch("0xa")
    applied = book.reseed_wallet(
        "0xa",
        [seed_state_from_row("0xa", "BTC", D(3), D(120), fallback_ts=TS)],
        {},
        expected_epoch=epoch,
    )
    assert applied is True
    pos = book.position("0xa", "BTC")
    assert pos is not None and pos.szi == D(3)  # snapshot applied


def test_seen_tids_dedupes_and_evicts():
    seen = SeenTids(maxlen=2)
    assert seen.check_and_add(1) is False
    assert seen.check_and_add(1) is True  # duplicate
    assert seen.check_and_add(2) is False
    assert seen.check_and_add(3) is False  # evicts tid 1 (oldest)
    assert seen.check_and_add(1) is False  # 1 was evicted, so it's "new" again
