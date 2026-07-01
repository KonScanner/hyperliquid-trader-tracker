"""In-memory position book + trade-id de-dupe — the tracker's whole runtime state.

Replaces the sibling project's Redis+Postgres layer. Nothing here is persisted: on restart the
book is rebuilt from the chain via the ``clearinghouseState`` seed (see :mod:`tracker.enrich`).
Single-writer by construction — the listener's one event loop calls :meth:`ingest`,
:meth:`reseed_wallet`, and :meth:`drop_wallet` sequentially, so no lock is needed.
"""

from collections import deque
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal

from tracker.state import ApplyResult, PositionState, apply_fill

_Key = tuple[str, str]  # (address, coin), both lowercase / as-fed


class SeenTids:
    """A bounded FIFO set of recently-seen trade ``tid`` values (reconnect idempotency).

    A WS reconnect can redeliver trades; re-ingesting the same trade would both corrupt the
    position (double-counted delta) and double-notify. :meth:`check_and_add` records a ``tid``
    and reports whether it had already been seen, evicting the oldest once ``maxlen`` is hit.
    """

    def __init__(self, maxlen: int) -> None:
        self._maxlen = maxlen
        self._order: deque[int] = deque()
        self._set: set[int] = set()

    def check_and_add(self, tid: int) -> bool:
        """Return ``True`` if ``tid`` was already seen; otherwise record it and return ``False``."""
        if tid in self._set:
            return True
        self._set.add(tid)
        self._order.append(tid)
        if len(self._order) > self._maxlen:
            self._set.discard(self._order.popleft())
        return False


class InMemoryBook:
    """Per-``(address, coin)`` position state + a per-``(address, coin)`` leverage cache."""

    def __init__(self) -> None:
        self._positions: dict[_Key, PositionState] = {}
        self._leverage: dict[_Key, int] = {}
        # Per-address fill counter, bumped on every ingest. A reconcile reseed captures it BEFORE
        # its clearinghouseState await and passes it back, so a live fill that lands during that
        # await isn't clobbered by the now-stale snapshot. See reseed_wallet(expected_epoch=...).
        self._epoch: dict[str, int] = {}

    # --- live ingestion ----------------------------------------------------------------

    def ingest(
        self, *, address: str, coin: str, delta: Decimal, px: Decimal, ts: datetime
    ) -> ApplyResult:
        """Fold one resolved fill into the wallet's position and return the ``ApplyResult``.

        Its ``events`` are what the notifier pushes; ``closed_trade`` (set on a close/flip)
        carries the leg's time window so the close notification can fetch the exchange's own
        realized PnL. The book updates in place.
        """
        key = (address, coin)
        state = self._positions.get(key)
        result = apply_fill(state, address=address, coin=coin, delta=delta, px=px, ts=ts)
        if result.state is None:
            self._positions.pop(key, None)
        else:
            self._positions[key] = result.state
        self._epoch[address] = self._epoch.get(address, 0) + 1
        return result

    def fill_epoch(self, address: str) -> int:
        """The per-address fill counter — capture it before a reconcile snapshot's await."""
        return self._epoch.get(address, 0)

    # --- seeding / reconcile (SILENT — never emits) ------------------------------------

    def reseed_wallet(
        self,
        address: str,
        states: Iterable[PositionState],
        leverage: dict[str, int],
        *,
        expected_epoch: int | None = None,
    ) -> bool:
        """Replace all of ``address``'s positions with ``states`` and refresh its leverage.

        Used both for the cold-start seed and the periodic reconcile. Replacing (not merging)
        drops any coin the chain no longer reports — i.e. a position closed while we were
        disconnected self-heals. Emits NOTHING: these are corrections, not new activity.

        ``expected_epoch`` guards the reconcile-of-an-admitted-wallet case: if a live fill was
        folded in since the snapshot was requested (the fill epoch moved), the reseed is SKIPPED
        (returns ``False``) so a stale snapshot can't clobber the fresher live state — the next
        reconcile cycle corrects it. ``None`` (the seed-before-admit path, where no fills can land
        because the wallet isn't in the filter yet) always applies. Returns ``True`` when applied.
        """
        if expected_epoch is not None and self._epoch.get(address, 0) != expected_epoch:
            return False
        self.drop_wallet(address)
        for state in states:
            self._positions[(address, state.coin)] = state
        for coin, value in leverage.items():
            self._leverage[(address, coin)] = value
        return True

    def drop_wallet(self, address: str) -> None:
        """Forget every position + leverage entry for ``address`` (on watchlist removal)."""
        for key in [k for k in self._positions if k[0] == address]:
            del self._positions[key]
        for key in [k for k in self._leverage if k[0] == address]:
            del self._leverage[key]
        self._epoch.pop(address, None)

    # --- reads -------------------------------------------------------------------------

    def position(self, address: str, coin: str) -> PositionState | None:
        return self._positions.get((address, coin))

    def leverage(self, address: str, coin: str) -> int | None:
        return self._leverage.get((address, coin))

    @property
    def open_position_count(self) -> int:
        return len(self._positions)
