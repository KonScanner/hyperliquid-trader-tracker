"""Notification formatting + multi-tenant dispatch.

``format_event`` is a pure function (fully unit-tested); :class:`Notifier` applies the lifecycle
filter and fans one lifecycle event out to every subscriber of that wallet, each in their own
chat and with their own label. The sender is a Protocol so the Telegram binding lives entirely
in :mod:`tracker.bot` and the core stays dependency-free and testable with a fake.
"""

import logging
from decimal import Decimal
from typing import Protocol, runtime_checkable

from tracker.state import (
    EVENT_ADD,
    EVENT_CLOSE,
    EVENT_OPEN,
    EVENT_REDUCE,
    LiveEvent,
)

logger = logging.getLogger(__name__)


@runtime_checkable
class MessageSender(Protocol):
    """Anything that can deliver one rendered notification to one chat."""

    async def send(self, chat_id: int, text: str) -> None: ...


def _trim(d: Decimal) -> str:
    """Render a Decimal without trailing zeros or scientific notation (``2.50`` -> ``2.5``)."""
    return format(d.normalize(), "f")


def _pnl(d: Decimal | None) -> str:
    """Signed USD PnL, e.g. ``+$2,760.00`` / ``-$440.00``; ``?`` when unknown."""
    if d is None:
        return "?"
    sign = "+" if d >= 0 else "-"
    return f"{sign}${abs(d):,.2f}"


def format_event(
    event: LiveEvent,
    *,
    label: str,
    leverage: int | None,
    mark: Decimal | None,
) -> str:
    """Render one lifecycle event as a push message. Leads with the subscriber's ``label``."""
    lev = f"{leverage}x" if leverage is not None else "?x"
    coin, direction = event.coin, event.direction
    size = _trim(abs(event.delta))
    px = _trim(event.px)

    if event.kind == EVENT_OPEN:
        return f"🟢 Started trade {label}: {coin} {direction} {size} @ {px} ({lev})"
    if event.kind == EVENT_ADD:
        ntl = f" (~${abs(event.delta) * mark:,.2f})" if mark is not None else ""
        return f"➕ Added to position {label}: {coin} {direction} +{size}{ntl} @ {px} ({lev})"
    if event.kind == EVENT_REDUCE:
        remaining = _trim(abs(event.szi_after))
        return (
            f"➖ Reduced {label}: {coin} {direction} -{size} @ {px} "
            f"| realized {_pnl(event.realized_pnl)} | {remaining} left"
        )
    if event.kind == EVENT_CLOSE:
        return (
            f"🔴 Closed {label}: {coin} {direction} {size} @ {px} "
            f"| realized {_pnl(event.realized_pnl)}"
        )
    return f"{event.kind} {label}: {coin} {direction} {size} @ {px}"  # defensive


class LoggingSender:
    """A :class:`MessageSender` that logs instead of pushing — the no-token fallback."""

    async def send(self, chat_id: int, text: str) -> None:
        logger.info("NOTIFY chat=%s: %s", chat_id, text)


class Notifier:
    """Applies the reduce/close mute toggle, then renders + fans out to each subscriber."""

    def __init__(self, sender: MessageSender, *, notify_reduce_close: bool) -> None:
        self._sender = sender
        self._notify_reduce_close = notify_reduce_close

    async def dispatch(
        self,
        event: LiveEvent,
        *,
        recipients: dict[int, str],
        leverage: int | None,
        mark: Decimal | None,
    ) -> None:
        """Send ``event`` to every ``{chat_id: label}`` recipient (their own label per chat)."""
        if event.kind in (EVENT_REDUCE, EVENT_CLOSE) and not self._notify_reduce_close:
            return
        for chat_id, label in recipients.items():
            text = format_event(event, label=label, leverage=leverage, mark=mark)
            try:
                await self._sender.send(chat_id, text)
            except Exception:  # delivery is best-effort — a failed push must not kill the listener
                logger.exception("notification send failed; dropping (chat=%s): %s", chat_id, text)
