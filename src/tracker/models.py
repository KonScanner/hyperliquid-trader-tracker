"""Pydantic models + decimal coercion for Hyperliquid ``clearinghouseState`` positions.

A focused subset of the sibling ``hyperdash-crawl`` models: just what the tracker needs to
(a) parse an ``assetPositions`` entry into a flat :class:`AccountPosition` (for the cold-start
seed + leverage), and (b) carry a completed round-trip out of the state machine (for the
realized-PnL in close/reduce notifications). Everything monetary is :class:`~decimal.Decimal`.
"""

from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any

from pydantic import BaseModel, BeforeValidator, ConfigDict


def _to_decimal(value: object) -> Decimal | None:
    """Coerce ints, floats, and numeric strings to ``Decimal`` via ``str``.

    Going through ``str`` avoids binary-float artefacts (``Decimal(0.1)``); empty strings and
    ``None`` become ``None`` so missing money fields stay null. NaN/Infinity are rejected to
    ``None`` at this single source of truth.
    """
    if value is None or value == "":
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    try:
        d = Decimal(str(value))
    except (InvalidOperation, ValueError) as err:  # pragma: no cover - defensive
        raise ValueError(f"not a decimal: {value!r}") from err
    return d if d.is_finite() else None


Money = Annotated[Decimal | None, BeforeValidator(_to_decimal)]

# Provenance tag on a completed round-trip. The tracker only ever produces ``live`` trades
# (derived from the public feed, so PnL is approximate — no per-fill fee/funding).
TRADE_SOURCE_LIVE = "live"


class AccountPosition(BaseModel):
    """A normalized open perp position — one row per (wallet, coin).

    Hyperliquid's ``clearinghouseState.assetPositions[].position`` nests ``leverage`` and
    ``cumFunding`` objects; the fields the tracker uses are flattened here. ``szi`` is the
    signed size (``> 0`` long, ``< 0`` short).
    """

    model_config = ConfigDict(extra="forbid")

    address: str
    coin: str
    szi: Money = None
    entry_px: Money = None
    position_value: Money = None
    unrealized_pnl: Money = None
    liquidation_px: Money = None
    leverage_type: str | None = None
    leverage_value: int | None = None
    max_leverage: int | None = None


def build_position(address: str, raw: dict[str, Any]) -> AccountPosition | None:
    """Normalize one Hyperliquid ``assetPositions`` entry into a flat ``AccountPosition``.

    Returns ``None`` for a malformed entry (no nested ``position`` block / missing coin) so a
    bad row is skipped rather than fatal.
    """
    pos = raw.get("position") if isinstance(raw, dict) else None
    if not isinstance(pos, dict):
        return None
    coin = pos.get("coin")
    if not isinstance(coin, str) or not coin:
        return None
    leverage = pos.get("leverage") if isinstance(pos.get("leverage"), dict) else {}
    return AccountPosition(
        address=address,
        coin=coin,
        szi=pos.get("szi"),
        entry_px=pos.get("entryPx"),
        position_value=pos.get("positionValue"),
        unrealized_pnl=pos.get("unrealizedPnl"),
        liquidation_px=pos.get("liquidationPx"),
        leverage_type=leverage.get("type"),
        leverage_value=leverage.get("value"),
        max_leverage=pos.get("maxLeverage"),
    )


class CompletedTrade(BaseModel):
    """A round-trip trade emitted by the state machine on a close/flip.

    Transient (never persisted) — carried only so a close notification can report realized PnL,
    direction, and duration. PnL is approximate (the public feed carries no fee/funding), hence
    the ``source='live'`` tag.
    """

    model_config = ConfigDict(extra="forbid")

    address: str
    coin: str
    direction: str
    start_time: datetime
    end_time: datetime
    duration_mins: int
    size: Money = None
    avg_entry_px: Money = None
    avg_exit_px: Money = None
    gross_pnl: Money = None
    funding_pnl: Money = None
    total_fees: Money = None
    net_pnl: Money = None
    source: str = TRADE_SOURCE_LIVE
