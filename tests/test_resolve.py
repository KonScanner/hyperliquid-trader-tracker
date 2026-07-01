"""Trade→signed-delta resolution, perp-coin extraction, and the seed helper."""

from datetime import UTC, datetime
from decimal import Decimal

from tracker.resolve import perp_coins_from_meta, resolve_deltas, seed_state_from_row
from tracker.state import DIRECTION_LONG, DIRECTION_SHORT

D = Decimal
WATCH = frozenset({"0xbuyer", "0xseller"})
TS = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)


def _trade(**kw):
    base = {
        "coin": "BTC",
        "side": "B",
        "px": "100",
        "sz": "2",
        "time": 1_700_000_000_000,
        "tid": 42,
        "users": ["0xBUYER", "0xSELLER"],
    }
    base.update(kw)
    return base


def test_buyer_gets_positive_delta_seller_negative():
    by_addr = {f.address: f for f in resolve_deltas(_trade(), WATCH)}
    assert by_addr["0xbuyer"].delta == D(2)
    assert by_addr["0xseller"].delta == D(-2)
    assert by_addr["0xbuyer"].coin == "BTC"
    assert by_addr["0xbuyer"].px == D(100)


def test_addresses_are_lowercased_to_match_watchlist():
    fills = resolve_deltas(_trade(users=["0xBUYER", "0xUNTRACKED"]), WATCH)
    assert [f.address for f in fills] == ["0xbuyer"]


def test_no_watched_wallet_yields_nothing():
    assert resolve_deltas(_trade(users=["0xx", "0xy"]), WATCH) == []


def test_zero_size_and_malformed_yield_nothing():
    assert resolve_deltas(_trade(sz="0"), WATCH) == []
    assert resolve_deltas(_trade(users=["only-one"]), WATCH) == []
    assert resolve_deltas({"coin": "BTC"}, WATCH) == []
    assert resolve_deltas(_trade(px="not-a-number"), WATCH) == []


def test_trade_time_is_parsed_to_utc():
    fill = resolve_deltas(_trade(time=1_700_000_000_000), WATCH)[0]
    assert fill.ts == datetime.fromtimestamp(1_700_000_000, tz=UTC)


def test_perp_coins_from_meta_extracts_names():
    meta = {"universe": [{"name": "BTC"}, {"name": "ETH"}, {"notname": "x"}, "junk"]}
    assert perp_coins_from_meta(meta) == ["BTC", "ETH"]


def test_seed_state_from_row_sets_direction_from_sign():
    long_state = seed_state_from_row("0xa", "BTC", D(3), D(100), fallback_ts=TS)
    assert long_state.direction == DIRECTION_LONG
    assert long_state.entry_qty_total == D(3)
    short_state = seed_state_from_row("0xa", "BTC", D(-3), D(100), fallback_ts=TS)
    assert short_state.direction == DIRECTION_SHORT
