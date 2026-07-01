"""The firehose listener: Hyperliquid public ``trades`` feed → watched-wallet notifications.

Subscribes to every perp coin's public ``trades`` feed (each trade carries both counterparty
addresses) plus ``allMids`` (for notional in messages), filters against the admitted watchlist,
folds the resulting signed fills through the in-memory book's state machine, and dispatches each
lifecycle event to the notifier. One reconnecting connection loop owns the socket; an app-level
ping keeps quiet sockets alive (HL closes any connection idle 60s). Trimmed from the sibling
``hyperdash-crawl`` listener — no Redis, no Postgres, no flush loop.
"""

import asyncio
import contextlib
import json
import logging
from decimal import Decimal, InvalidOperation
from typing import Any

import websockets

from tracker.book import InMemoryBook, SeenTids
from tracker.config import Settings
from tracker.hl_client import InfoClient
from tracker.notifier import Notifier
from tracker.registry import Registry
from tracker.resolve import perp_coins_from_meta, resolve_deltas

logger = logging.getLogger(__name__)


class Listener:
    """Owns the WebSocket connection, live marks, and per-trade dispatch."""

    def __init__(
        self,
        settings: Settings,
        registry: Registry,
        book: InMemoryBook,
        notifier: Notifier,
        client: InfoClient,
    ) -> None:
        self._settings = settings
        self._registry = registry
        self._book = book
        self._notifier = notifier
        self._client = client
        self._coins = settings.live_coins_list  # empty = all perps
        self._marks: dict[str, Decimal] = {}
        self._seen = SeenTids(settings.tid_dedup_maxlen)
        self._stop = asyncio.Event()

    def stop(self) -> None:
        """Signal a graceful shutdown (the reconnect loop exits)."""
        self._stop.set()

    async def run(self) -> None:
        await self._connection_loop()

    # --- connection (reconnecting) -----------------------------------------------------

    async def _connection_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self._run_connection()
                backoff = 1.0
            except* Exception as eg:  # TaskGroup wraps recv/heartbeat failures
                logger.warning(
                    "listener WS dropped (%s); reconnecting in %.1fs",
                    "; ".join(str(e) for e in eg.exceptions),
                    backoff,
                )
                await self._sleep_or_stop(backoff)
                backoff = min(backoff * 2, 30.0)

    async def _run_connection(self) -> None:
        coins = self._coins or await self._fetch_perp_coins()
        async with websockets.connect(
            self._settings.hl_ws_url, ping_interval=None, max_size=None
        ) as ws:
            await self._subscribe(ws, coins)
            logger.info("listener subscribed to %d coin trade feeds + allMids", len(coins))
            async with asyncio.TaskGroup() as tg:
                tg.create_task(self._recv_loop(ws))
                tg.create_task(self._heartbeat_loop(ws))

    async def _fetch_perp_coins(self) -> list[str]:
        meta = await self._client.info({"type": "meta"})
        coins = perp_coins_from_meta(meta)
        if not coins:
            raise RuntimeError("Hyperliquid meta returned no perp coins")
        return coins

    async def _subscribe(self, ws: Any, coins: list[str]) -> None:
        await ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "allMids"}}))
        for coin in coins:
            await ws.send(
                json.dumps(
                    {"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}
                )
            )

    async def _recv_loop(self, ws: Any) -> None:
        async for raw in ws:
            if self._stop.is_set():
                return
            await self._handle_message(raw)
        if not self._stop.is_set():  # socket closed on us → trigger a reconnect
            raise ConnectionError("Hyperliquid WebSocket closed")

    async def _heartbeat_loop(self, ws: Any) -> None:
        while not self._stop.is_set():
            await self._sleep_or_stop(self._settings.ws_heartbeat_s)
            if self._stop.is_set():
                return
            await ws.send(json.dumps({"method": "ping"}))

    async def _handle_message(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except ValueError, TypeError:
            return
        channel = msg.get("channel")
        if channel == "trades":
            await self._handle_trades(msg.get("data") or [])
        elif channel == "allMids":
            self._update_marks((msg.get("data") or {}).get("mids") or {})

    async def _handle_trades(self, trades: list[dict[str, Any]]) -> None:
        """For each public trade that touches a watched wallet, fold it in and notify."""
        for trade in trades:
            fills = resolve_deltas(trade, self._registry.addresses)
            if not fills:
                continue
            # De-dupe on the trade id only for watched trades (keeps the ring window meaningful):
            # a WS reconnect that redelivers this trade must not double-count or double-notify.
            tid = trade.get("tid")
            if isinstance(tid, int) and self._seen.check_and_add(tid):
                continue
            for fill in fills:
                events = self._book.ingest(
                    address=fill.address, coin=fill.coin, delta=fill.delta, px=fill.px, ts=fill.ts
                )
                if not events:
                    continue
                # Fan out to every subscriber of this wallet, each with their own label.
                recipients = self._registry.subscribers(fill.address)
                for event in events:
                    await self._notifier.dispatch(
                        event,
                        recipients=recipients,
                        leverage=self._book.leverage(event.address, event.coin),
                        mark=self._marks.get(event.coin),
                    )

    def _update_marks(self, mids: dict[str, Any]) -> None:
        """Refresh the live mark map from an ``allMids`` payload (drives the notional in adds)."""
        for coin, px in mids.items():
            try:
                self._marks[coin] = Decimal(str(px))
            except InvalidOperation:
                continue

    async def _sleep_or_stop(self, seconds: float) -> None:
        """Sleep ``seconds`` unless a stop is signalled first (so shutdown is prompt)."""
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(self._stop.wait(), timeout=seconds)
