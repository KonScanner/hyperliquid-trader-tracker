"""The pure position state machine: open / add / reduce / close / flip, PnL sign-correctness."""

from datetime import UTC, datetime
from decimal import Decimal

from tracker.state import (
    DIRECTION_LONG,
    DIRECTION_SHORT,
    EVENT_ADD,
    EVENT_CLOSE,
    EVENT_OPEN,
    EVENT_REDUCE,
    apply_fill,
)

D = Decimal
TS = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)


def _apply(state, delta, px, ts=TS):
    return apply_fill(state, address="0xa", coin="BTC", delta=D(delta), px=D(px), ts=ts)


def test_fill_from_flat_opens_a_long():
    result = _apply(None, "2", "100")
    assert result.state is not None
    assert result.state.szi == D(2)
    assert result.state.direction == DIRECTION_LONG
    assert [e.kind for e in result.events] == [EVENT_OPEN]
    assert result.closed_trade is None


def test_negative_fill_from_flat_opens_a_short():
    result = _apply(None, "-3", "100")
    assert result.state.szi == D(-3)
    assert result.state.direction == DIRECTION_SHORT
    assert result.events[0].kind == EVENT_OPEN


def test_same_direction_fill_adds_and_blends_avg_entry():
    opened = _apply(None, "2", "100").state
    result = _apply(opened, "2", "110")
    assert result.state.szi == D(4)
    assert result.state.avg_entry == D(105)  # (2*100 + 2*110) / 4
    assert result.events[0].kind == EVENT_ADD
    assert result.closed_trade is None


def test_partial_reduce_books_pnl_and_keeps_avg_entry():
    opened = _apply(None, "4", "100").state
    result = _apply(opened, "-1", "120")  # sell 1 of a long at 120
    assert result.state.szi == D(3)
    assert result.state.avg_entry == D(100)  # reduce does not move the basis
    assert result.events[0].kind == EVENT_REDUCE
    assert result.events[0].realized_pnl == D(20)  # (120-100)*1
    assert result.closed_trade is None


def test_exact_close_flattens_and_emits_completed_trade():
    opened = _apply(None, "2", "100").state
    result = _apply(opened, "-2", "150")
    assert result.state is None
    assert result.events[0].kind == EVENT_CLOSE
    assert result.events[0].realized_pnl == D(100)  # (150-100)*2
    assert result.closed_trade is not None
    assert result.closed_trade.net_pnl == D(100)


def test_short_close_pnl_sign_is_correct():
    opened = _apply(None, "-2", "100").state  # short at 100
    result = _apply(opened, "2", "90")  # buy back at 90 → profit
    assert result.state is None
    assert result.events[0].realized_pnl == D(20)  # (100-90)*2


def test_flip_closes_then_opens_residual():
    opened = _apply(None, "2", "100").state  # long 2
    result = _apply(opened, "-5", "120")  # sell 5 → close 2, open short 3
    assert [e.kind for e in result.events] == [EVENT_CLOSE, EVENT_OPEN]
    assert result.closed_trade.net_pnl == D(40)  # (120-100)*2
    assert result.state.szi == D(-3)
    assert result.state.direction == DIRECTION_SHORT
    assert result.state.avg_entry == D(120)


def test_zero_delta_is_a_noop():
    opened = _apply(None, "2", "100").state
    result = _apply(opened, "0", "100")
    assert result.state is opened
    assert result.events == []
