"""The position state machine — pure, I/O-free, fully unit-testable.

Given a wallet's current open position and one observed fill (a *signed* size delta at a
price), :func:`apply_fill` returns the next position, the lifecycle events to publish, and —
on a full close — one completed round-trip trade. It is decoupled from *how* the signed delta
was derived: the listener resolves "which side of this trade was the watched wallet on" (the
``users:[buyer,seller]`` convention) and hands this module a clean ``+sz`` / ``-sz``.

Accounting is **average-cost**: ``avg_entry`` is the volume-weighted average of every opening
fill in the current leg; reduces realize PnL against that average. PnL is approximate (the
public feed carries no per-fill fee or funding). A *flip* (a delta that crosses zero) closes
the current leg and opens a new one in the opposite direction with the residual size.

Everything is :class:`~decimal.Decimal` end-to-end (the feed sends decimal strings), so there
is no float drift. Ported verbatim (behaviour-wise) from the sibling ``hyperdash-crawl`` project.
"""

import dataclasses
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal

from tracker.models import TRADE_SOURCE_LIVE, CompletedTrade

# Leg direction labels.
DIRECTION_LONG = "Long"
DIRECTION_SHORT = "Short"

_ZERO = Decimal(0)

# Lifecycle event kinds, in order of "how much they matter".
EVENT_OPEN = "open"
EVENT_ADD = "add"
EVENT_REDUCE = "reduce"
EVENT_CLOSE = "close"


@dataclass(slots=True)
class PositionState:
    """A wallet's current open position in one coin, as a running average-cost leg.

    ``szi`` is the signed size (``> 0`` long, ``< 0`` short); it is never exactly zero for a
    live state (a position that reaches zero is closed and dropped). ``avg_entry`` is the
    average-cost basis of the *currently open* size — recomputed on each add, left UNCHANGED by
    a reduce. ``entry_qty_total`` is the running sum of every opening fill; ``exit_qty`` /
    ``exit_notional`` accumulate closing fills; ``realized_pnl`` is the PnL booked by reduces so
    far this leg.
    """

    address: str
    coin: str
    szi: Decimal
    direction: str
    opened_at: datetime
    last_added_at: datetime
    avg_entry: Decimal
    entry_qty_total: Decimal
    exit_qty: Decimal
    exit_notional: Decimal
    realized_pnl: Decimal

    @property
    def avg_entry_px(self) -> Decimal:
        return self.avg_entry

    @property
    def avg_exit_px(self) -> Decimal | None:
        return self.exit_notional / self.exit_qty if self.exit_qty else None


@dataclass(slots=True)
class LiveEvent:
    """One position-lifecycle event, for the notifier (transient, never persisted)."""

    kind: str
    address: str
    coin: str
    direction: str
    delta: Decimal  # signed size change that caused the event
    px: Decimal
    szi_after: Decimal  # resulting position size (0 when the leg closed)
    realized_pnl: Decimal | None  # set on reduce / close
    ts: datetime


@dataclass(slots=True)
class ApplyResult:
    """The outcome of folding one fill into a position.

    ``state`` is the next position, or ``None`` when the leg is now flat (the caller drops the
    key). ``events`` are the lifecycle events to publish (a flip yields close + open).
    ``closed_trade`` is the completed round-trip, set on a close or flip.
    """

    state: PositionState | None
    events: list[LiveEvent]
    closed_trade: CompletedTrade | None


def _sign(value: Decimal) -> int:
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def _open_leg(
    address: str, coin: str, *, delta: Decimal, px: Decimal, ts: datetime
) -> tuple[PositionState, LiveEvent]:
    """Start a fresh leg from flat with the (signed, non-zero) ``delta`` at ``px``."""
    qty = abs(delta)
    direction = DIRECTION_LONG if delta > 0 else DIRECTION_SHORT
    state = PositionState(
        address=address,
        coin=coin,
        szi=delta,
        direction=direction,
        opened_at=ts,
        last_added_at=ts,
        avg_entry=px,
        entry_qty_total=qty,
        exit_qty=_ZERO,
        exit_notional=_ZERO,
        realized_pnl=_ZERO,
    )
    event = LiveEvent(
        kind=EVENT_OPEN,
        address=address,
        coin=coin,
        direction=direction,
        delta=delta,
        px=px,
        szi_after=delta,
        realized_pnl=None,
        ts=ts,
    )
    return state, event


def _realized_chunk(direction: str, avg_entry: Decimal, px: Decimal, qty: Decimal) -> Decimal:
    """Average-cost realized PnL for closing ``qty`` of a ``direction`` leg at ``px``."""
    if direction == DIRECTION_LONG:
        return (px - avg_entry) * qty
    return (avg_entry - px) * qty


def _close_trade(state: PositionState, *, px: Decimal, ts: datetime) -> CompletedTrade:
    """Build the round-trip ``CompletedTrade`` for a leg that just reached flat at ``px``."""
    avg_entry = state.avg_entry
    closing_qty = abs(state.szi)  # the still-open size being closed by this final fill
    exit_notional = state.exit_notional + closing_qty * px
    exit_qty = state.exit_qty + closing_qty
    realized = state.realized_pnl + _realized_chunk(state.direction, avg_entry, px, closing_qty)
    duration_mins = max(0, int((ts - state.opened_at).total_seconds() // 60))
    return CompletedTrade(
        address=state.address,
        coin=state.coin,
        direction=state.direction,
        start_time=state.opened_at,
        end_time=ts.replace(microsecond=0),
        duration_mins=duration_mins,
        size=state.entry_qty_total,
        avg_entry_px=avg_entry,
        avg_exit_px=(exit_notional / exit_qty) if exit_qty else px,
        gross_pnl=realized,
        # The public feed carries no per-fill fee/funding, so net == gross (approximate).
        funding_pnl=None,
        total_fees=None,
        net_pnl=realized,
        source=TRADE_SOURCE_LIVE,
    )


def apply_fill(
    state: PositionState | None,
    *,
    address: str,
    coin: str,
    delta: Decimal,
    px: Decimal,
    ts: datetime,
) -> ApplyResult:
    """Fold one signed fill (``delta`` units at ``px``) into ``state``.

    ``state`` is ``None`` when the wallet has no open position in ``coin``. ``delta`` is the
    *signed* size change for this wallet (``+`` if it bought, ``-`` if it sold). Returns the
    next state (``None`` if now flat), the lifecycle events, and any completed round-trip.
    """
    if delta == _ZERO:  # defensive: a zero-size fill changes nothing
        return ApplyResult(state=state, events=[], closed_trade=None)

    # --- no open position: this fill opens one ---
    if state is None or state.szi == _ZERO:
        new_state, event = _open_leg(address, coin, delta=delta, px=px, ts=ts)
        return ApplyResult(state=new_state, events=[event], closed_trade=None)

    same_direction = _sign(delta) == _sign(state.szi)

    # --- ADD: same-direction fill grows the leg ---
    if same_direction:
        qty = abs(delta)
        open_now = abs(state.szi)
        new_avg = (state.avg_entry * open_now + px * qty) / (open_now + qty)
        new_state = dataclasses.replace(
            state,
            szi=state.szi + delta,
            last_added_at=ts,
            avg_entry=new_avg,
            entry_qty_total=state.entry_qty_total + qty,
        )
        event = LiveEvent(
            kind=EVENT_ADD,
            address=address,
            coin=coin,
            direction=state.direction,
            delta=delta,
            px=px,
            szi_after=new_state.szi,
            realized_pnl=None,
            ts=ts,
        )
        return ApplyResult(state=new_state, events=[event], closed_trade=None)

    # --- opposite-direction fill: reduce, close, or flip ---
    open_qty = abs(state.szi)
    close_qty = abs(delta)

    # REDUCE: closes part of the leg without reaching flat.
    if close_qty < open_qty:
        realized_chunk = _realized_chunk(state.direction, state.avg_entry_px, px, close_qty)
        new_state = dataclasses.replace(
            state,
            szi=state.szi + delta,
            exit_qty=state.exit_qty + close_qty,
            exit_notional=state.exit_notional + close_qty * px,
            realized_pnl=state.realized_pnl + realized_chunk,
        )
        event = LiveEvent(
            kind=EVENT_REDUCE,
            address=address,
            coin=coin,
            direction=state.direction,
            delta=delta,
            px=px,
            szi_after=new_state.szi,
            realized_pnl=realized_chunk,
            ts=ts,
        )
        return ApplyResult(state=new_state, events=[event], closed_trade=None)

    # CLOSE (exact) or FLIP (crosses zero): the current leg reaches flat either way.
    trade = _close_trade(state, px=px, ts=ts)
    close_event = LiveEvent(
        kind=EVENT_CLOSE,
        address=address,
        coin=coin,
        direction=state.direction,
        delta=delta,
        px=px,
        szi_after=_ZERO,
        realized_pnl=trade.net_pnl,
        ts=ts,
    )

    if close_qty == open_qty:  # exact close → flat
        return ApplyResult(state=None, events=[close_event], closed_trade=trade)

    # FLIP: open a new opposite-direction leg with the residual size.
    residual = delta + state.szi  # signed; same sign as delta (it dominated)
    new_state, open_event = _open_leg(address, coin, delta=residual, px=px, ts=ts)
    return ApplyResult(state=new_state, events=[close_event, open_event], closed_trade=trade)
