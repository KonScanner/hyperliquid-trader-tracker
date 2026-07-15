"""Process entrypoint: wire the subscriptions DB, in-memory book, enricher, listener, and bot.

Startup ordering preserves cold-start correctness: the listener starts against an empty registry
(so it filters everything out harmlessly), then each wallet with a persisted subscriber is seeded
from ``clearinghouseState`` and admitted to the filter only once its seed lands — so no add on a
pre-existing position is mis-reported as a new open. This is a multi-tenant bot: one firehose
connection serves every subscriber; a lifecycle event fans out to all subscribers of that wallet.
Nothing but the per-subscriber watchlists is persisted; the book is rebuilt from the chain on
every start.
"""

import asyncio
import contextlib
import logging
import signal
from collections import defaultdict

from tracker.book import InMemoryBook
from tracker.config import Settings, load_env
from tracker.db import Subscription, WatchlistDB
from tracker.enrich import Enricher
from tracker.hl_client import HyperliquidClient
from tracker.listener import Listener
from tracker.notifier import LoggingSender, MessageSender, Notifier
from tracker.pnl import ClosedPnlResolver
from tracker.registry import Registry

logger = logging.getLogger(__name__)


async def _sleep_or_stop(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=seconds)


def _retain_allowed(subs: list[Subscription], allowed: frozenset[int]) -> list[Subscription]:
    """Drop persisted subscriptions from chats outside the allowlist (no-op when public).

    Command handlers are already gated per-update (bot._chat_id); this extends the same gate
    to delivery, so a watchlist row written while the bot was public can't keep fanning
    notifications out to a chat that ADMIN_CHAT_ID / TRACKER_ALLOWED_CHAT_IDS has since
    locked out. Rows stay in the DB untouched — lifting the gate restores them.
    """
    if not allowed:
        return subs
    kept = [sub for sub in subs if sub.chat_id in allowed]
    if len(kept) < len(subs):
        logger.info(
            "allowlist active: ignoring %d persisted subscription(s) from non-allowed chats",
            len(subs) - len(kept),
        )
    return kept


def _group_by_address(subs: list[Subscription]) -> dict[str, list[Subscription]]:
    grouped: dict[str, list[Subscription]] = defaultdict(list)
    for sub in subs:
        grouped[sub.address].append(sub)
    return grouped


async def _seed_and_admit(
    enricher: Enricher,
    registry: Registry,
    grouped: dict[str, list[Subscription]],
    concurrency: int,
) -> None:
    """Seed each unique wallet once, then admit all its subscribers (seed-before-admit)."""
    sem = asyncio.Semaphore(concurrency)

    async def _one(address: str, members: list[Subscription]) -> None:
        async with sem:
            if await enricher.seed_wallet(address):
                for member in members:
                    registry.subscribe(member.chat_id, address, member.label)

    await asyncio.gather(*(_one(a, m) for a, m in grouped.items()))


async def _reconcile_loop(
    settings: Settings,
    enricher: Enricher,
    db: WatchlistDB,
    registry: Registry,
    stop: asyncio.Event,
) -> None:
    """Periodically (a) seed+admit any persisted-but-untracked wallets (retry failed seeds) and
    (b) re-seed a rotating batch of tracked wallets to refresh leverage + correct size drift."""
    if settings.reconcile_interval_s <= 0:
        return
    cursor = 0
    allowed = settings.allowed_chat_ids_set
    while not stop.is_set():
        await _sleep_or_stop(stop, settings.reconcile_interval_s)
        if stop.is_set():
            return
        try:
            subs = _retain_allowed(await db.all(), allowed)
            pending = {
                addr: members
                for addr, members in _group_by_address(subs).items()
                if not registry.is_tracked(addr)
            }
            if pending:
                await _seed_and_admit(enricher, registry, pending, settings.seed_concurrency)
            tracked = sorted(registry.addresses)
            if tracked:
                start = cursor % len(tracked)
                batch = tracked[start : start + settings.reconcile_batch]
                cursor = start + settings.reconcile_batch
                await enricher.seed_many(batch)
        except Exception:  # a reconcile hiccup must not kill the loop
            logger.exception("reconcile cycle failed; retrying next interval")


async def _amain(settings: Settings) -> None:
    db = WatchlistDB(settings.db_path)
    await db.connect()
    book = InMemoryBook()
    registry = Registry()
    stop = asyncio.Event()

    async with HyperliquidClient(settings) as client:
        enricher = Enricher(settings, book, client)

        sender: MessageSender
        application = None
        settings_bot = None
        if settings.telegram_bot_token:
            from telegram.ext import Application

            from tracker.bot import SettingsBot, TelegramSender

            application = Application.builder().token(settings.telegram_bot_token).build()
            sender = TelegramSender(application)
            # Constructing it registers the command + button handlers on the app.
            settings_bot = SettingsBot(settings, application, db, book, registry, enricher)
        else:
            logger.warning("no TELEGRAM_BOT_TOKEN configured — notifications will be logged only")
            sender = LoggingSender()

        notifier = Notifier(sender, notify_reduce_close=settings.notify_reduce_close)
        # No resolver when close notifications are muted — the lookup's only consumer is the
        # close message, so fetching would just spend REST budget on a dropped event.
        pnl_resolver = (
            ClosedPnlResolver(settings, client)
            if settings.closed_pnl_lookup and settings.notify_reduce_close
            else None
        )
        listener = Listener(settings, registry, book, notifier, client, pnl_resolver=pnl_resolver)

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: (stop.set(), listener.stop()))

        if application is not None:
            await application.initialize()
            if settings_bot is not None:
                await settings_bot.configure()  # register the slash-command menu
            await application.start()
            if application.updater is not None:
                await application.updater.start_polling()

        subscriptions = _retain_allowed(await db.all(), settings.allowed_chat_ids_set)
        listener_task = asyncio.create_task(listener.run(), name="listener")
        # The listener only returns once stop is set; if it ever exits unexpectedly, trip stop so
        # the process shuts down cleanly instead of idling forever on `await stop.wait()`.
        listener_task.add_done_callback(lambda _t: stop.set())
        tasks = [
            listener_task,
            asyncio.create_task(
                _reconcile_loop(settings, enricher, db, registry, stop), name="reconcile"
            ),
            asyncio.create_task(
                _seed_and_admit(
                    enricher, registry, _group_by_address(subscriptions), settings.seed_concurrency
                ),
                name="startup-seed",
            ),
        ]
        try:
            await stop.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if application is not None:
                with contextlib.suppress(Exception):
                    if application.updater is not None:
                        await application.updater.stop()
                    await application.stop()
                    await application.shutdown()
            await db.aclose()


def main() -> None:
    """Console-script entry point (``hl-tracker``)."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    load_env()
    asyncio.run(_amain(Settings()))


if __name__ == "__main__":
    main()
