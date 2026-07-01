"""Pure trade-resolution helpers lifted from the sibling ``hyperdash-crawl`` listener.

These are the I/O-free parts: resolving a public ``WsTrade`` to per-wallet signed deltas,
listing perp coins from a ``meta`` response, and seeding a :class:`PositionState` from a
``clearinghouseState`` snapshot (the cold-start primitive).
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from tracker.state import DIRECTION_LONG, DIRECTION_SHORT, PositionState

_ZERO = Decimal(0)


def _now() -> datetime:
    return datetime.now(UTC)


@dataclass(slots=True)
class ResolvedFill:
    """One watched wallet's signed participation in a public trade."""

    address: str
    coin: str
    delta: Decimal  # +sz if the wallet bought, -sz if it sold
    px: Decimal
    ts: datetime


def perp_coins_from_meta(meta: Any) -> list[str]:
    """Extract the perp coin names from a Hyperliquid ``meta`` response's ``universe``."""
    universe = meta.get("universe", []) if isinstance(meta, dict) else []
    return [u["name"] for u in universe if isinstance(u, dict) and u.get("name")]


def resolve_deltas(trade: dict[str, Any], watchlist: frozenset[str]) -> list[ResolvedFill]:
    """Resolve one ``WsTrade`` to the signed deltas for any watched wallets it touched.

    Convention: ``users == [buyer, seller]`` — the buyer's signed size grows by ``+sz``, the
    seller's by ``-sz``, regardless of which side was the aggressor. Documented by Hyperliquid
    and VERIFIED on the live socket by the sibling project (2026-06-16). If this ever needs
    revisiting, it is the only function that changes. Malformed trades (missing coin/users,
    unparseable numbers, zero size) yield no fills.

    ``watchlist`` addresses MUST be lowercase; the trade's addresses are lowered here to match.
    """
    coin = trade.get("coin")
    users = trade.get("users")
    if not coin or not isinstance(users, list) or len(users) != 2:
        return []
    try:
        px = Decimal(str(trade["px"]))
        sz = Decimal(str(trade["sz"]))
    except KeyError, InvalidOperation:
        return []
    if sz == _ZERO:
        return []
    raw_time = trade.get("time")
    # `is not None` (not a falsy check): a legitimate epoch 0 must use the real ts; only a
    # missing `time` key falls back to the receive clock.
    ts = datetime.fromtimestamp(raw_time / 1000, tz=UTC) if raw_time is not None else _now()
    buyer = str(users[0]).lower()
    seller = str(users[1]).lower()
    fills: list[ResolvedFill] = []
    if buyer in watchlist:
        fills.append(ResolvedFill(address=buyer, coin=coin, delta=sz, px=px, ts=ts))
    if seller in watchlist:
        fills.append(ResolvedFill(address=seller, coin=coin, delta=-sz, px=px, ts=ts))
    return fills


def seed_state_from_row(
    address: str,
    coin: str,
    szi: Decimal,
    entry_px: Decimal | None,
    *,
    fallback_ts: datetime,
) -> PositionState:
    """Build a resume-state from a ``clearinghouseState`` position for cold-start seeding.

    Treats the existing position as a single open leg at its entry price (we don't have its
    constituent fills — those are never stored), so subsequent stream fills extend/close it
    correctly rather than mistaking the next fill for a brand-new open.
    """
    entry = entry_px if entry_px is not None else _ZERO
    return PositionState(
        address=address,
        coin=coin,
        szi=szi,
        direction=DIRECTION_LONG if szi > 0 else DIRECTION_SHORT,
        opened_at=fallback_ts,
        last_added_at=fallback_ts,
        avg_entry=entry,
        entry_qty_total=abs(szi),
        exit_qty=_ZERO,
        exit_notional=_ZERO,
        realized_pnl=_ZERO,
    )
