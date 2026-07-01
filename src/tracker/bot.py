"""Telegram delivery + settings UX (the only module that imports ``python-telegram-bot``).

A MULTI-TENANT public bot: anyone can message it and manage their own watchlist. Commands
operate on the sender's own chat: ``/add <address> <label…>``, ``/remove <address>``,
``/rename <address> <label…>``, ``/list``, ``/help``. Notifications for a wallet are fanned out
to every subscriber of that wallet in their own chat. The core stays Telegram-free (this module
is only imported by :mod:`tracker.app` when the ``telegram`` extra is installed).

The menu is presented with HTML formatting + an inline-button keyboard, and the persistent
slash-command menu (the Menu button) is registered via :meth:`SettingsBot.configure`. All
user-supplied text (labels, raw args) is HTML-escaped before interpolation so it can never break
the parse or inject markup.

The critical invariant lives in :meth:`SettingsBot.add_wallet`: a wallet is admitted to the live
filter only after its cold-start seed succeeds, so an add on a pre-existing position is never
mis-reported as a brand-new "Started trade". A wallet already tracked by another subscriber is
already seeded, so a new subscriber just joins it.
"""

import html
import logging

from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes

from tracker.book import InMemoryBook
from tracker.config import Settings
from tracker.db import WatchlistDB
from tracker.enrich import Enricher
from tracker.notifier import MessageSender
from tracker.registry import Registry, normalize_address

logger = logging.getLogger(__name__)

_h = html.escape  # escape user-supplied text before it goes into an HTML-parsed message

# The /start · /help · Help-button message. `<code>…</code>` renders the command syntax in
# monospace; the literal angle brackets in placeholders are escaped (&lt; / &gt;).
_HELP = (
    "🟢 <b>Hyperliquid Wallet Tracker</b>\n\n"
    "I'll DM you the moment a wallet you follow <b>opens</b>, adds to, reduces, "
    "or closes a Hyperliquid perp position.\n\n"
    "<b>Commands</b>\n"
    "➕ <code>/add &lt;address&gt; &lt;label&gt;</code> — follow a wallet\n"
    "➖ <code>/remove &lt;address&gt;</code> — stop following\n"
    "✏️ <code>/rename &lt;address&gt; &lt;label&gt;</code> — relabel\n"
    "📃 <code>/list</code> — show the wallets you follow\n\n"
    "Tip: tap a button below to get started."
)

# Tappable inline keyboard under the menu. The `callback_data` payloads are dispatched by
# SettingsBot._on_button. (/add & /rename need typed arguments, so they stay as commands.)
_MENU_KEYBOARD = InlineKeyboardMarkup(
    [
        [
            InlineKeyboardButton("📃 My wallets", callback_data="list"),
            InlineKeyboardButton("❓ Help", callback_data="help"),
        ]
    ]
)

# The persistent slash-command menu (the blue Menu button + "/" autocomplete). Descriptions are
# plain text — Telegram does not parse HTML here.
_COMMANDS = [
    BotCommand("add", "Follow a wallet — /add <address> <label>"),
    BotCommand("remove", "Stop following — /remove <address>"),
    BotCommand("rename", "Relabel a wallet — /rename <address> <label>"),
    BotCommand("list", "Show the wallets you follow"),
    BotCommand("help", "What this bot does + all commands"),
]


class TelegramSender(MessageSender):
    """A :class:`~tracker.notifier.MessageSender` backed by a Telegram bot."""

    def __init__(self, application: Application) -> None:
        self._app = application

    async def send(self, chat_id: int, text: str) -> None:
        await self._app.bot.send_message(chat_id=chat_id, text=text)


class SettingsBot:
    """Wires the Telegram command + button handlers to the DB + enricher + subscriber registry."""

    def __init__(
        self,
        settings: Settings,
        application: Application,
        db: WatchlistDB,
        book: InMemoryBook,
        registry: Registry,
        enricher: Enricher,
    ) -> None:
        self._settings = settings
        self._app = application
        self._db = db
        self._book = book
        self._registry = registry
        self._enricher = enricher
        self._allowed = settings.allowed_chat_ids_set
        self._register()

    def _register(self) -> None:
        self._app.add_handler(CommandHandler(["start", "help"], self._cmd_help))
        self._app.add_handler(CommandHandler("add", self._cmd_add))
        self._app.add_handler(CommandHandler("remove", self._cmd_remove))
        self._app.add_handler(CommandHandler("rename", self._cmd_rename))
        self._app.add_handler(CommandHandler("list", self._cmd_list))
        self._app.add_handler(CallbackQueryHandler(self._on_button))

    async def configure(self) -> None:
        """Register the persistent slash-command menu (the Menu button + "/" autocomplete).

        Called once after the Application is initialized. The manual initialize()/start()
        lifecycle in :mod:`tracker.app` does not fire ``post_init`` (only ``run_polling`` would),
        so the app invokes this explicitly.
        """
        await self._app.bot.set_my_commands(_COMMANDS)

    # --- shared logic --------------------------------------------------------------------

    async def add_wallet(self, chat_id: int, address: str, label: str) -> str:
        """Persist + (seed if newly tracked) + admit ``chat_id`` as a subscriber.

        Seed-before-admit: a wallet not yet tracked by anyone is seeded from ``clearinghouseState``
        before it enters the filter. On a seed failure it is persisted (so a restart/reconcile
        retries) but NOT admitted, so it can never mislabel a later add as a new open.
        """
        short = Registry.short(address)
        # Seed a wallet nobody tracks yet BEFORE it enters the filter; a wallet already tracked
        # by someone else is already seeded, so this short-circuits and the new subscriber joins.
        if not self._registry.is_tracked(address) and not await self._enricher.seed_wallet(address):
            await self._db.add(chat_id, address, label)
            return (
                f"💾 Saved <b>{_h(label)}</b> (<code>{short}</code>) but I couldn't read its "
                "current positions — it'll go live on the next reconcile."
            )
        await self._db.add(chat_id, address, label)
        self._registry.subscribe(chat_id, address, label)
        return f"✅ Now following <b>{_h(label)}</b> (<code>{short}</code>)."

    async def remove_wallet(self, chat_id: int, address: str) -> bool:
        """Delete the subscription; if it was the last subscriber, forget the wallet's book state."""
        existed = await self._db.delete(chat_id, address)
        _, orphan = self._registry.unsubscribe(chat_id, address)
        if orphan:
            self._book.drop_wallet(address)
        return existed

    # --- command plumbing ----------------------------------------------------------------

    def _chat_id(self, update: Update) -> int | None:
        """The sender's chat id, honoring the optional allowlist (None = ignore the command)."""
        chat = update.effective_chat
        if chat is None:
            return None
        if self._allowed and chat.id not in self._allowed:
            return None
        return chat.id

    async def _reply(self, update: Update, text: str, *, keyboard: bool = False) -> None:
        """Reply in the update's chat with HTML formatting (works for commands and button taps)."""
        message = update.effective_message
        if message is not None:
            await message.reply_text(
                text,
                parse_mode=ParseMode.HTML,
                reply_markup=_MENU_KEYBOARD if keyboard else None,
            )

    async def _cmd_help(self, update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
        if self._chat_id(update) is None:
            return
        await self._reply(update, _HELP, keyboard=True)

    async def _on_button(self, update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
        """Dispatch an inline-keyboard tap. Always answers first to clear the button spinner."""
        query = update.callback_query
        if query is None:
            return
        await query.answer()
        if self._chat_id(update) is None:
            return
        if query.data == "list":
            await self._show_list(update)
        else:  # "help" (and any unknown payload) falls back to the menu
            await self._reply(update, _HELP, keyboard=True)

    async def _resolve(
        self, update: Update, ctx: ContextTypes.DEFAULT_TYPE, *, min_args: int, usage: str
    ) -> tuple[int, str, list[str]] | None:
        """Shared command preamble: authorize, check arg count, normalize the address.

        Returns ``(chat_id, address, args)`` or ``None`` (having already replied) when the command
        is unauthorized, under-argumented, or the first arg isn't a valid address.
        """
        chat_id = self._chat_id(update)
        if chat_id is None:
            return None
        args = ctx.args or []
        if len(args) < min_args:
            await self._reply(update, usage)
            return None
        try:
            return chat_id, normalize_address(args[0]), args
        except ValueError:
            await self._reply(update, f"⚠️ Not a valid address: <code>{_h(args[0])}</code>")
            return None

    async def _cmd_add(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        resolved = await self._resolve(
            update, ctx, min_args=2, usage="Usage: <code>/add &lt;address&gt; &lt;label&gt;</code>"
        )
        if resolved is None:
            return
        chat_id, address, args = resolved
        label = " ".join(args[1:]).strip()
        await self._reply(update, await self.add_wallet(chat_id, address, label))

    async def _cmd_remove(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        resolved = await self._resolve(
            update, ctx, min_args=1, usage="Usage: <code>/remove &lt;address&gt;</code>"
        )
        if resolved is None:
            return
        chat_id, address, _ = resolved
        existed = await self.remove_wallet(chat_id, address)
        short = Registry.short(address)
        await self._reply(
            update,
            f"🗑️ Stopped following <code>{short}</code>."
            if existed
            else f"You weren't following <code>{short}</code>.",
        )

    async def _cmd_rename(self, update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
        resolved = await self._resolve(
            update,
            ctx,
            min_args=2,
            usage="Usage: <code>/rename &lt;address&gt; &lt;label&gt;</code>",
        )
        if resolved is None:
            return
        chat_id, address, args = resolved
        label = " ".join(args[1:]).strip()
        short = Registry.short(address)
        if await self._db.rename(chat_id, address, label):
            self._registry.rename(chat_id, address, label)
            await self._reply(update, f"✏️ Renamed to <b>{_h(label)}</b> (<code>{short}</code>).")
        else:
            await self._reply(update, f"You aren't following <code>{short}</code>.")

    async def _cmd_list(self, update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
        await self._show_list(update)

    async def _show_list(self, update: Update) -> None:
        """Render the sender's watchlist (shared by /list and the 📃 My wallets button)."""
        chat_id = self._chat_id(update)
        if chat_id is None:
            return
        wallets = await self._db.list_for(chat_id)
        if not wallets:
            await self._reply(
                update,
                "You're not following any wallets yet. Add one with "
                "<code>/add &lt;address&gt; &lt;label&gt;</code>.",
                keyboard=True,
            )
            return
        lines = [
            f"• <b>{_h(label)}</b> — <code>{Registry.short(addr)}</code>"
            for addr, label in sorted(wallets.items())
        ]
        await self._reply(update, "<b>You're following:</b>\n" + "\n".join(lines))
