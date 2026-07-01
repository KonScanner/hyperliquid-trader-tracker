"""Cold-start seeding + periodic reconcile via Hyperliquid ``clearinghouseState``.

This is what makes "check whether the wallet ALREADY had a position" work with zero storage:
before a wallet is admitted to the live filter, we fetch its current on-chain positions and
install them silently, so the next same-direction fill is correctly an ADD, not a false OPEN.
``clearinghouseState`` is weight 2, so seeding even 1k wallets stays well within the IP budget.
"""

import asyncio
import logging
from datetime import UTC, datetime

from tracker.book import InMemoryBook
from tracker.config import Settings
from tracker.hl_client import InfoClient
from tracker.models import build_position
from tracker.resolve import seed_state_from_row
from tracker.state import PositionState

logger = logging.getLogger(__name__)


class Enricher:
    """Fetches ``clearinghouseState`` and folds it into the in-memory book (silently)."""

    def __init__(self, settings: Settings, book: InMemoryBook, client: InfoClient) -> None:
        self._settings = settings
        self._book = book
        self._client = client

    async def seed_wallet(self, address: str) -> bool:
        """Snapshot ``address``'s positions and reseed the book. Returns ``False`` on failure.

        A failure leaves the wallet UNSEEDED so the caller can quarantine it (not admit it to
        the filter) rather than risk mis-labelling a later add as a new open. Any failure — a
        transport/HTTP error OR a malformed snapshot that won't parse — returns ``False`` rather
        than propagating, so one bad wallet can never abort a whole seed sweep.
        """
        # Capture the fill epoch BEFORE the await so a live fill that lands during the snapshot
        # window makes the reconcile reseed skip (rather than clobber the fresher live state).
        epoch = self._book.fill_epoch(address)
        try:
            resp = await self._client.info({"type": "clearinghouseState", "user": address})
            now = datetime.now(UTC)
            states: list[PositionState] = []
            leverage: dict[str, int] = {}
            for entry in (resp or {}).get("assetPositions") or []:
                pos = build_position(address, entry)
                if pos is None or not pos.szi:  # skip malformed / zero-size
                    continue
                states.append(
                    seed_state_from_row(address, pos.coin, pos.szi, pos.entry_px, fallback_ts=now)
                )
                if pos.leverage_value is not None:
                    leverage[pos.coin] = pos.leverage_value
        except Exception:
            logger.exception("seed: clearinghouseState failed for %s", address)
            return False
        self._book.reseed_wallet(address, states, leverage, expected_epoch=epoch)
        return True

    async def seed_many(self, addresses: list[str]) -> tuple[list[str], list[str]]:
        """Seed every address with bounded concurrency. Returns ``(seeded, failed)``."""
        sem = asyncio.Semaphore(self._settings.seed_concurrency)

        async def _one(addr: str) -> tuple[str, bool]:
            async with sem:
                return addr, await self.seed_wallet(addr)

        results = await asyncio.gather(*(_one(a) for a in addresses))
        seeded = [a for a, ok in results if ok]
        failed = [a for a, ok in results if not ok]
        if failed:
            logger.warning("seed sweep: %d seeded, %d failed", len(seeded), len(failed))
        return seeded, failed
