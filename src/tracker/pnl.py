"""Authoritative close PnL: one REST ``userFillsByTime`` sweep of the just-closed leg.

The realized figure the state machine attaches to a close is an average-cost estimate over
the public ``trades`` feed — that feed carries no ``closedPnl``, a seeded leg's basis is
whatever ``entryPx`` the snapshot reported, and a restart collapses multi-add legs. The
exchange, however, publishes its own per-fill ``closedPnl``; summed over the leg's fills it
IS the leg's realized price PnL (opening fills contribute zero). So on a close/flip the
listener asks this module for that sum and swaps it into the event before dispatch.

Best-effort by design: the REST fill index can lag the trades feed by a moment, so the
lookup retries briefly until the closing fill is visible, and ANY failure falls back to the
local estimate rather than dropping or stalling the notification. Fees and funding stay out
of the number (Hyperliquid reports them separately) — the message remains price PnL, now
exchange-accurate. ``userFillsByTime`` is weight 20 and closes are rare, so this never
threatens the 1200/min IP budget.
"""

import asyncio
import dataclasses
import logging
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any

from tracker.config import Settings
from tracker.exceptions import ParseError, TrackerError
from tracker.hl_client import InfoClient
from tracker.models import CompletedTrade
from tracker.state import EVENT_CLOSE, LiveEvent

logger = logging.getLogger(__name__)

_ZERO = Decimal(0)
# Hyperliquid caps one userFillsByTime response at 2000 fills; a longer leg is time-walked.
_PAGE_LIMIT = 2000
# A leg spanning more pages than this costs more than one notification is worth — give up.
_MAX_PAGES = 5
# Ask past the close so an inclusive/exclusive endTime quibble can't hide the closing fill.
_END_SKEW_MS = 2_000


def _to_ms(ts: datetime) -> int:
    # round(), not int(): timestamp() is a float, and truncation can land 1ms early — which
    # would make the closing-fill visibility check below miss forever.
    return round(ts.timestamp() * 1000)


class ClosedPnlResolver:
    """Fetches the exchange's realized PnL for a leg that just closed."""

    def __init__(self, settings: Settings, client: InfoClient) -> None:
        self._client = client
        self._attempts = settings.closed_pnl_attempts
        self._retry_delay_s = settings.closed_pnl_retry_delay_s

    async def close_pnl(self, trade: CompletedTrade, *, closed_at: datetime) -> Decimal | None:
        """Sum of ``closedPnl`` over the leg's fills, or ``None`` (caller keeps its estimate).

        The window is ``(opened, closed]`` — strictly after the opening fill, because on a
        flip that fill's ``closedPnl`` belongs to the PREVIOUS leg (for a from-flat open it
        is just zero). ``closed_at`` must be the close event's exchange timestamp: a fill
        with exactly that time proves the REST index has caught up with the trades feed;
        until one appears the lookup waits and retries, then gives up.
        """
        opened_ms = _to_ms(trade.start_time)
        closed_ms = _to_ms(closed_at)
        for attempt in range(self._attempts):
            if attempt:
                await asyncio.sleep(self._retry_delay_s)
            try:
                fills = await self._window_fills(trade.address, opened_ms, closed_ms)
            except TrackerError as err:
                logger.warning(
                    "close-pnl: lookup failed for %s %s: %s", trade.address, trade.coin, err
                )
                return None
            if fills is None:  # window too long to page through — don't trust a partial sum
                return None
            leg = [
                f
                for f in fills
                if f.get("coin") == trade.coin
                and isinstance(f.get("time"), int)
                and opened_ms < f["time"] <= closed_ms
            ]
            if not any(f["time"] == closed_ms for f in leg):
                continue  # the closing fill isn't indexed yet — retry after a beat
            try:
                return sum((Decimal(str(f.get("closedPnl") or 0)) for f in leg), _ZERO)
            except InvalidOperation:
                logger.warning(
                    "close-pnl: unparseable closedPnl for %s %s", trade.address, trade.coin
                )
                return None
        logger.info(
            "close-pnl: closing fill for %s %s not visible after %d attempts; using estimate",
            trade.address,
            trade.coin,
            self._attempts,
        )
        return None

    async def _window_fills(
        self, address: str, start_ms: int, end_ms: int
    ) -> list[dict[str, Any]] | None:
        """Every fill for ``address`` in the window, time-walking full pages; None on overflow."""
        fills: list[dict[str, Any]] = []
        cursor = start_ms
        for _ in range(_MAX_PAGES):
            page = await self._client.info(
                {
                    "type": "userFillsByTime",
                    "user": address,
                    "startTime": cursor,
                    "endTime": end_ms + _END_SKEW_MS,
                }
            )
            if not isinstance(page, list):
                raise ParseError(f"userFillsByTime: expected a list, got {type(page).__name__}")
            fills.extend(f for f in page if isinstance(f, dict))
            if len(page) < _PAGE_LIMIT:
                return fills
            times = [
                f["time"] for f in page if isinstance(f, dict) and isinstance(f.get("time"), int)
            ]
            if not times:
                raise ParseError("userFillsByTime: full page with no usable timestamps")
            cursor = max(times) + 1  # the doc-sanctioned walk: advance past the last row's time
        logger.warning(
            "close-pnl: leg for %s spans more than %d pages of fills; skipping",
            address,
            _MAX_PAGES,
        )
        return None


async def with_authoritative_pnl(
    event: LiveEvent, trade: CompletedTrade | None, resolver: ClosedPnlResolver | None
) -> LiveEvent:
    """``event``, with the exchange's realized PnL swapped in when it applies and resolves.

    A no-op for non-close events, when no resolver is wired, or when the lookup comes back
    empty-handed — the event's local average-cost estimate is kept in those cases.
    """
    if resolver is None or trade is None or event.kind != EVENT_CLOSE:
        return event
    pnl = await resolver.close_pnl(trade, closed_at=event.ts)
    if pnl is None:
        return event
    return dataclasses.replace(event, realized_pnl=pnl)
