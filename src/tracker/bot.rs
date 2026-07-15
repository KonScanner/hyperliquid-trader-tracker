//! Telegram delivery + settings UX (the only module that speaks the live Telegram Bot API).
//!
//! A MULTI-TENANT public bot: anyone can message it and manage their own watchlist — unless
//! `ADMIN_CHAT_ID` (or `TRACKER_ALLOWED_CHAT_IDS`) is set, in which case every command
//! (including `/start`/`/help`) and button tap from any other chat is silently ignored
//! (the [`SettingsBot::chat_id`] gate). Commands
//! operate on the sender's own chat: `/add <address> <label…>`, `/remove <address>`,
//! `/rename <address> <label…>`, `/list`, `/positions [address]`, `/help`. Notifications for
//! a wallet are fanned out to every subscriber of that wallet in their own chat. The core
//! stays Telegram-free (this module is only wired up by `tracker::app` when a bot token is
//! configured).
//!
//! The menu is presented with HTML formatting + an inline-button keyboard, and the persistent
//! slash-command menu (the Menu button) is registered via [`SettingsBot::configure`]. All
//! user-supplied text (labels, raw args) is HTML-escaped before interpolation so it can never
//! break the parse or inject markup.
//!
//! The critical invariant lives in [`SettingsBot::add_wallet`]: a wallet is admitted to the live
//! filter only after its cold-start seed succeeds, so an add on a pre-existing position is never
//! mis-reported as a brand-new "Started trade". A wallet already tracked by another subscriber is
//! already seeded, so a new subscriber just joins it.
//
// PORT NOTE: the Python module was "the only module that imports python-telegram-bot", and no
// Telegram crate is in the fixed dependency set (Cargo.toml pins reqwest/serde_json/tokio; cf.
// telegram_setup.rs, which already speaks the raw Bot API over reqwest). The PTB surface this
// module used — Application, bot.send_message / set_my_commands, Update.effective_chat /
// effective_message / callback_query, query.answer(), BotCommand, InlineKeyboardMarkup /
// InlineKeyboardButton, ParseMode.HTML — is therefore hand-rolled below over the raw Bot API
// JSON, in the same style as telegram_setup.rs. Runtime: tokio (fixed decision).
// PORT NOTE: `logger = logging.getLogger(__name__)` — defined but never USED in the Python
// module; nothing to port (tracing macros are free-standing anyway).

use std::collections::HashSet;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::book::InMemoryBook;
use crate::config::{self, Settings};
use crate::db::WatchlistDB;
use crate::enrich::Enricher;
use crate::models::AccountPosition;
use crate::notifier::{
    MessageSender, SendError, escape_html as h, format_live_position, linked_name, money_short,
    notional_short, update_pnl_keyboard,
};
use crate::registry::{Registry, normalize_address};

/// What a handler may fail with.
// PORT NOTE: structural addition — the Python module had NO error type: an exception escaping
// a handler bubbled into PTB's dispatcher, which logged it and kept polling. This enum
// materializes that propagation path: `handle_update` returns it, and Phase B's app.rs poll
// loop logs-and-continues, reproducing PTB's default error handling. Variants mirror what the
// handler bodies can actually raise: a Bot API call (reqwest), a watchlist DB call
// (tokio_rusqlite — db.rs returns its raw error per its own error-unification todo), and the
// allowed_chat_ids parse in `SettingsBot::new` (config::Error, was pydantic's ValueError).
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Telegram(#[from] reqwest::Error),
    #[error(transparent)]
    Db(#[from] tokio_rusqlite::Error),
    #[error(transparent)]
    Config(#[from] config::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

// ──────────────────────────────────────────────────────────────────────────
// The python-telegram-bot surface, hand-rolled over the raw Bot API.
// PORT NOTE: everything in this section replaces `from telegram... import ...` — it is the
// minimum of PTB this module consumed, not a general Telegram client.
// ──────────────────────────────────────────────────────────────────────────

// PORT NOTE: same base URL constant telegram_setup.rs defines for itself (module-private in
// both, matching the Python modules' independence).
const API: &str = "https://api.telegram.org";

/// `telegram.constants.ParseMode.HTML`.
const PARSE_MODE_HTML: &str = "HTML";

/// `telegram.BotCommand` — one entry of the persistent slash-command menu.
// PORT NOTE: fields narrowed to &'static str — only the module-level COMMANDS table ever
// constructs these.
pub struct BotCommand {
    pub command: &'static str,
    pub description: &'static str,
}

/// `telegram.Update` — a thin owned wrapper over one raw Bot API update object.
// PORT NOTE: PTB deserialized updates into typed objects; the accessors below reproduce just
// the PTB attributes this module reads (`effective_chat`, `effective_message`,
// `callback_query`) over serde_json::Value, matching telegram_setup.rs's raw-JSON handling.
#[derive(Debug, Clone)]
pub struct Update(pub Value);

impl Update {
    /// PTB's `Update.effective_message`, reduced to the update types this bot receives:
    /// a plain/edited message, a channel post, or the message an inline button hangs off.
    pub fn effective_message(&self) -> Option<&Value> {
        for key in [
            "message",
            "edited_message",
            "channel_post",
            "edited_channel_post",
        ] {
            if let Some(message) = self.0.get(key).filter(|m| m.is_object()) {
                return Some(message);
            }
        }
        self.callback_query()
            .and_then(|q| q.get("message"))
            .filter(|m| m.is_object())
    }

    /// PTB's `Update.effective_chat` — the chat of the effective message.
    // PORT NOTE: PTB also derives a chat from update types this bot never registers for
    // (inline queries, chat-member events, …); those arms are dropped.
    pub fn effective_chat(&self) -> Option<&Value> {
        self.effective_message()
            .and_then(|m| m.get("chat"))
            .filter(|c| c.is_object())
    }

    /// `update.callback_query`.
    pub fn callback_query(&self) -> Option<&Value> {
        self.0.get("callback_query").filter(|q| q.is_object())
    }

    /// `update.update_id` — the poll-offset cursor.
    // PORT NOTE: structural addition for Phase B's app.rs poll loop (PTB's Updater tracked
    // the offset internally).
    pub fn update_id(&self) -> Option<i64> {
        self.0.get("update_id").and_then(Value::as_i64)
    }
}

/// `application.bot` — the outbound Bot API client (sendMessage / setMyCommands / …).
pub struct Bot {
    client: reqwest::Client,
    token: String,
}

impl Bot {
    fn url(&self, method: &str) -> String {
        format!("{API}/bot{}/{method}", self.token)
    }

    /// `bot.send_message(...)`, returning the new message's id so a later fill can edit it.
    // PORT NOTE: PTB's keyword defaults (`parse_mode=None`, `reply_markup=None`) flattened to
    // required Option params (guide option (b): absence is semantic — plain text vs HTML,
    // keyboard vs none). PTB raised TelegramError on an `ok: false` payload; the Bot API sets
    // a non-2xx status on exactly those, so `error_for_status()` is the equivalent raise.
    // DIVERGENCE (edit-mode cards): the Python discarded the response; here we parse and return
    // `result.message_id` so the notifier can edit this card in place on the next fill.
    pub async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        parse_mode: Option<&str>,
        reply_markup: Option<&Value>,
    ) -> reqwest::Result<i64> {
        let mut body = json!({
            "chat_id": chat_id,
            "text": text,
            // Cards link the trader name and a "View TX"; without this the explorer URL spawns a
            // preview card that bloats every message (and every silent edit) — always suppress it.
            "link_preview_options": { "is_disabled": true },
        });
        if let Some(parse_mode) = parse_mode {
            body["parse_mode"] = json!(parse_mode);
        }
        if let Some(reply_markup) = reply_markup {
            body["reply_markup"] = reply_markup.clone();
        }
        let resp = self
            .client
            .post(self.url("sendMessage"))
            .json(&body)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(redact)?;
        // The Bot API always carries `result.message_id` on success; default to 0 defensively
        // (an untrackable card — a later edit just falls back to a fresh send).
        let payload: Value = resp.json().await.map_err(redact)?;
        Ok(payload
            .get("result")
            .and_then(|result| result.get("message_id"))
            .and_then(Value::as_i64)
            .unwrap_or(0))
    }

    /// `bot.edit_message_text(...)` — replace the text of a previously-sent message in place.
    /// Editing does not re-notify the chat, so it is the silent update the notifier relies on.
    // DIVERGENCE (edit-mode cards): no Python counterpart — the Python sent a new message per
    // fill. Fails (non-2xx) when the message is gone or past Telegram's edit window; the
    // notifier treats that as "resend a fresh card".
    pub async fn edit_message_text(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        parse_mode: Option<&str>,
        reply_markup: Option<&Value>,
    ) -> reqwest::Result<()> {
        let mut body = json!({
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            // Same reasoning as send_message: keep the edited card free of an explorer preview.
            "link_preview_options": { "is_disabled": true },
        });
        if let Some(parse_mode) = parse_mode {
            body["parse_mode"] = json!(parse_mode);
        }
        if let Some(reply_markup) = reply_markup {
            body["reply_markup"] = reply_markup.clone();
        }
        self.client
            .post(self.url("editMessageText"))
            .json(&body)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(redact)?;
        Ok(())
    }

    /// `bot.set_my_commands(...)` — registers the persistent slash-command menu.
    pub async fn set_my_commands(&self, commands: &[BotCommand]) -> reqwest::Result<()> {
        let commands: Vec<Value> = commands
            .iter()
            .map(|c| json!({ "command": c.command, "description": c.description }))
            .collect();
        self.client
            .post(self.url("setMyCommands"))
            .json(&json!({ "commands": commands }))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(redact)?;
        Ok(())
    }

    /// `query.answer()` — clears the inline-button spinner.
    pub async fn answer_callback_query(&self, callback_query_id: &str) -> reqwest::Result<()> {
        self.client
            .post(self.url("answerCallbackQuery"))
            .json(&json!({ "callback_query_id": callback_query_id }))
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(redact)?;
        Ok(())
    }

    /// One long-poll `getUpdates` call — the fetch primitive under PTB's `updater.start_polling()`.
    // TODO(port): the polling LOOP (offset bookkeeping, initialize/start/stop/shutdown
    // lifecycle) belonged to PTB's Application/Updater and moves to app.rs in Phase B:
    //   loop { for u in bot.get_updates(offset, 10).await? {
    //       offset = u.update_id().map(|id| id + 1); settings_bot.handle_update(&u).await; } }
    pub async fn get_updates(
        &self,
        offset: Option<i64>,
        timeout_s: u64,
    ) -> reqwest::Result<Vec<Update>> {
        let mut body = json!({ "timeout": timeout_s });
        if let Some(offset) = offset {
            body["offset"] = json!(offset);
        }
        let resp = self
            .client
            .post(self.url("getUpdates"))
            // long poll: the request deadline must outlive the server-side hold
            .timeout(Duration::from_secs(timeout_s + 10))
            .json(&body)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(redact)?;
        let payload: Value = resp.json().await.map_err(redact)?;
        Ok(payload
            .get("result")
            .and_then(Value::as_array)
            .map(|updates| updates.iter().cloned().map(Update).collect())
            .unwrap_or_default())
    }
}

/// Strip the request URL from a reqwest error before it can escape into a log line: the Bot
/// API URL embeds the bot token (`/bot<token>/<method>`), and PTB never logged the token.
// SECURITY NOTE (review follow-up): app.rs's poll loop logs these errors via Display; with
// the URL attached, one 409/timeout would print the credential.
fn redact(err: reqwest::Error) -> reqwest::Error {
    err.without_url()
}

/// `telegram.ext.Application`, reduced to what this project used of it: the [`Bot`] holder.
// PORT NOTE: PTB's Application bundled the Bot, a handler registry, and the polling
// lifecycle. The handler registry became [`SettingsBot::handle_update`] (see its PORT NOTE)
// and the lifecycle collapses into app.rs's own poll loop in Phase B; what remains is the
// shared Bot. `Application.builder().token(t).build()` → `Application::new(t)`.
pub struct Application {
    bot: Bot,
}

impl Application {
    // PORT NOTE: PTB's HTTPXRequest applied default timeouts (5s connect / 5s read) to every
    // Bot API call; an unbounded reqwest client would let one hung sendMessage stall the
    // listener's dispatch path and the command loop forever. get_updates overrides the
    // deadline per-request for the long poll (RequestBuilder::timeout wins over the client's).
    pub fn new(token: String) -> Self {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(15))
            .build()
            // Construction-time, pre-trading: only fails on a broken TLS backend.
            .expect("reqwest client construction failed (TLS backend misconfigured)");
        Self {
            bot: Bot { client, token },
        }
    }

    /// `application.bot`.
    pub fn bot(&self) -> &Bot {
        &self.bot
    }
}

// ──────────────────────────────────────────────────────────────────────────

// The HTML-escape helper (`_h = html.escape` in the Python) now lives in notifier.rs as
// `escape_html`, imported above as `h` — notifications are HTML-formatted too and the
// escaped label is theirs.

// The /start · /help · Help-button message. `<code>…</code>` renders the command syntax in
// monospace; the literal angle brackets in placeholders are escaped (&lt; / &gt;).
// PORT NOTE: `_HELP` → `HELP` (underscore dropped; privacy is the absence of `pub`); Python's
// adjacent-literal concatenation → one literal with `\` continuations.
const HELP: &str = "🟢 <b>Hyperliquid Wallet Tracker</b>\n\n\
    I'll DM you the moment a wallet you follow <b>opens</b>, adds to, reduces, \
    or closes a Hyperliquid perp position.\n\n\
    <b>Commands</b>\n\
    ➕ <code>/add &lt;address&gt; &lt;label&gt;</code> — follow a wallet\n\
    ➖ <code>/remove &lt;address&gt;</code> — stop following\n\
    ✏️ <code>/rename &lt;address&gt; &lt;label&gt;</code> — relabel\n\
    📃 <code>/list</code> — show the wallets you follow\n\
    📊 <code>/positions</code> — live open positions of a wallet you follow\n\n\
    Tip: tap a button below to get started.";

// Tappable inline keyboard under the menu. The `callback_data` payloads are dispatched by
// SettingsBot::on_button. (/add & /rename need typed arguments, so they stay as commands.)
// PORT NOTE: InlineKeyboardMarkup([[InlineKeyboardButton(...), ...]]) → the exact
// `reply_markup` JSON PTB would have serialized it to; LazyLock stands in for the
// module-level constructor call (same pattern as registry.rs's ADDR_RE).
static MENU_KEYBOARD: LazyLock<Value> = LazyLock::new(|| {
    json!({
        "inline_keyboard": [[
            { "text": "📃 My wallets", "callback_data": "list" },
            { "text": "📊 Positions", "callback_data": "positions" },
            { "text": "❓ Help", "callback_data": "help" },
        ]]
    })
});

// The persistent slash-command menu (the blue Menu button + "/" autocomplete). Descriptions are
// plain text — Telegram does not parse HTML here.
static COMMANDS: [BotCommand; 6] = [
    BotCommand {
        command: "add",
        description: "Follow a wallet — /add <address> <label>",
    },
    BotCommand {
        command: "remove",
        description: "Stop following — /remove <address>",
    },
    BotCommand {
        command: "rename",
        description: "Relabel a wallet — /rename <address> <label>",
    },
    BotCommand {
        command: "list",
        description: "Show the wallets you follow",
    },
    BotCommand {
        command: "positions",
        description: "Live open positions of a wallet you follow",
    },
    BotCommand {
        command: "help",
        description: "What this bot does + all commands",
    },
];

/// A [`MessageSender`] backed by a Telegram bot.
// PORT NOTE: holds Arc<Application> — Python held the same PTB object app.py also kept a name
// for; the Arc is that shared reference (same shape as Notifier's Arc<dyn MessageSender>).
pub struct TelegramSender {
    app: Arc<Application>,
}

impl TelegramSender {
    pub fn new(application: Arc<Application>) -> Self {
        Self { app: application }
    }
}

#[async_trait]
impl MessageSender for TelegramSender {
    // Notifications go out HTML-formatted (format_card escapes the user-supplied label); the
    // optional `reply_markup` is the 🔄 Update P&L button on live-position cards. reqwest errors
    // box into notifier.rs's type-erased SendError (its contract for Notifier.dispatch's
    // `except Exception`).
    async fn send(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<&Value>,
    ) -> std::result::Result<i64, SendError> {
        Ok(self
            .app
            .bot()
            .send_message(chat_id, text, Some(PARSE_MODE_HTML), reply_markup)
            .await?)
    }

    // Edit the live card in place (same HTML formatting) — the silent per-fill update that
    // replaces sending a new message each time; carries the same button while the card is open.
    async fn edit(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        reply_markup: Option<&Value>,
    ) -> std::result::Result<(), SendError> {
        self.app
            .bot()
            .edit_message_text(
                chat_id,
                message_id,
                text,
                Some(PARSE_MODE_HTML),
                reply_markup,
            )
            .await?;
        Ok(())
    }
}

/// Wires the Telegram command + button handlers to the DB + enricher + subscriber registry.
// PORT NOTE: field order matches __init__ (settings, app, db, book, registry, enricher,
// allowed); leading-underscore privacy → non-pub fields. Shared-state model (fixed
// decisions): the book is Arc<std::sync::Mutex<InMemoryBook>> exactly as enrich.rs holds it,
// and the registry gets the same treatment (the bot task mutates it while the listener task
// reads — Python shared the bare objects on one event loop). db and enricher are Arc-shared
// with the reconcile loop (their methods take &self); the Application is Arc-shared with
// TelegramSender.
pub struct SettingsBot {
    // PORT NOTE: stored but never read again — the Python kept `self._settings` unused after
    // __init__ too; kept for structural fidelity (the allowlist below is derived from it).
    // The underscore name matches the Python field and exempts it from dead_code.
    _settings: Settings,
    app: Arc<Application>,
    db: Arc<WatchlistDB>,
    book: Arc<Mutex<InMemoryBook>>,
    registry: Arc<Mutex<Registry>>,
    enricher: Arc<Enricher>,
    allowed: HashSet<i64>,
}

impl SettingsBot {
    // PORT NOTE: fallible where __init__ was — `settings.allowed_chat_ids_set` (a property)
    // raised ValueError on a malformed TRACKER_ALLOWED_CHAT_IDS; it is computed once here,
    // exactly as __init__ evaluated it once, and the config::Error surfaces via Error::Config.
    pub fn new(
        settings: Settings,
        application: Arc<Application>,
        db: Arc<WatchlistDB>,
        book: Arc<Mutex<InMemoryBook>>,
        registry: Arc<Mutex<Registry>>,
        enricher: Arc<Enricher>,
    ) -> Result<Self> {
        let allowed = settings.allowed_chat_ids_set()?;
        // PORT NOTE: __init__ tail-called `self._register()` to install bound-method handlers
        // on the PTB app; that registry is `handle_update`'s match below (see its PORT NOTE),
        // so construction has no side effect on the Application here.
        Ok(Self {
            _settings: settings,
            app: application,
            db,
            book,
            registry,
            enricher,
            allowed,
        })
    }

    /// Route one polled update to its handler — the port of `_register`'s handler table.
    // PORT NOTE: reshaped — PTB's `add_handler(CommandHandler(...))` stored `&self`-borrowing
    // bound methods inside the Application, which in Rust would make the app and the bot own
    // each other. The table becomes this match, and Phase B's app.rs feeds updates here from
    // its poll loop (Bot::get_updates). Row-for-row with the Python `_register`:
    //   CommandHandler(["start", "help"], _cmd_help)  → "start" | "help"
    //   CommandHandler("add", _cmd_add)               → "add"
    //   CommandHandler("remove", _cmd_remove)         → "remove"
    //   CommandHandler("rename", _cmd_rename)         → "rename"
    //   CommandHandler("list", _cmd_list)             → "list"
    //   CallbackQueryHandler(_on_button)              → the callback_query arm
    // PORT NOTE: PTB's CommandHandler required a bot_command entity at offset 0 and matched
    // case-insensitively; this draft approximates with "text starts with '/'" + lowercasing
    // (real clients always attach the entity). ctx.args (PTB's whitespace-split remainder)
    // becomes the `args` Vec handed to each handler.
    pub async fn handle_update(&self, update: &Update) -> Result<()> {
        if update.callback_query().is_some() {
            return self.on_button(update).await;
        }
        // PORT NOTE: PTB's CommandHandler only matched `update.message` — never edited
        // messages or channel posts (those fell through unhandled) — so commands dispatch
        // from the plain message only; effective_message() stays in use for REPLIES.
        let Some(message) = update.0.get("message").filter(|m| m.is_object()) else {
            return Ok(());
        };
        let text = message.get("text").and_then(Value::as_str).unwrap_or("");
        let mut parts = text.split_whitespace();
        let Some(command) = parts.next().filter(|c| c.starts_with('/')) else {
            return Ok(());
        };
        // TODO(port): PTB's CommandHandler only fired on `/cmd@ThisBot` (it verified the
        // @-suffix against the bot's own username via getMe); this draft strips any @-suffix
        // unconditionally — Phase B may fetch getMe once and verify.
        let name = command[1..].split('@').next().unwrap_or("").to_lowercase();
        let args: Vec<String> = parts.map(String::from).collect();
        match name.as_str() {
            "start" | "help" => self.cmd_help(update, &args).await,
            "add" => self.cmd_add(update, &args).await,
            "remove" => self.cmd_remove(update, &args).await,
            "rename" => self.cmd_rename(update, &args).await,
            "list" => self.cmd_list(update, &args).await,
            "positions" => self.cmd_positions(update, &args).await,
            _ => Ok(()),
        }
    }

    /// Register the persistent slash-command menu (the Menu button + "/" autocomplete).
    ///
    /// Called once after the bot is up. The Python noted that the manual initialize()/start()
    /// lifecycle in `tracker.app` did not fire PTB's `post_init` (only `run_polling` would), so
    /// the app invoked this explicitly — in this port app.rs likewise calls it once before its
    /// poll loop starts.
    pub async fn configure(&self) -> Result<()> {
        self.app.bot().set_my_commands(&COMMANDS).await?;
        Ok(())
    }

    // --- shared logic --------------------------------------------------------------------

    /// Persist + (seed if newly tracked) + admit `chat_id` as a subscriber.
    ///
    /// Seed-before-admit: a wallet not yet tracked by anyone is seeded from `clearinghouseState`
    /// before it enters the filter. On a seed failure it is persisted (so a restart/reconcile
    /// retries) but NOT admitted, so it can never mislabel a later add as a new open.
    // PORT NOTE: returns Result<String> — the reply text on success; the Python let db errors
    // raise into PTB's logger, `?` propagates them to handle_update's caller instead.
    pub async fn add_wallet(&self, chat_id: i64, address: &str, label: &str) -> Result<String> {
        // Seed a wallet nobody tracks yet BEFORE it enters the filter; a wallet already tracked
        // by someone else is already seeded, so this short-circuits and the new subscriber joins.
        // PORT NOTE: is_tracked is read under a lock captured into a local so the guard drops
        // before the seed await (a std Mutex must never be held across an await). The
        // interleaving window this opens matches the Python's own await point inside the same
        // condition (`and not await ...` suspended between the two operands too).
        let tracked = self
            .registry
            .lock()
            .expect("registry mutex poisoned")
            .is_tracked(address);
        if !tracked && !self.enricher.seed_wallet(address).await {
            self.db.add(chat_id, address, label).await?;
            return Ok(format!(
                "💾 Saved <b>{}</b> (<code>{address}</code>) but I couldn't read its \
                 current positions — it'll go live on the next reconcile.",
                h(label)
            ));
        }
        self.db.add(chat_id, address, label).await?;
        self.registry
            .lock()
            .expect("registry mutex poisoned")
            .subscribe(chat_id, address, label);
        Ok(format!(
            "✅ Now following <b>{}</b> (<code>{address}</code>).",
            h(label)
        ))
    }

    /// Delete the subscription; if it was the last subscriber, forget the wallet's book state.
    pub async fn remove_wallet(&self, chat_id: i64, address: &str) -> Result<bool> {
        let existed = self.db.delete(chat_id, address).await?;
        let (_, orphan) = self
            .registry
            .lock()
            .expect("registry mutex poisoned")
            .unsubscribe(chat_id, address);
        if orphan {
            self.book
                .lock()
                .expect("book mutex poisoned")
                .drop_wallet(address);
        }
        Ok(existed)
    }

    // --- command plumbing ----------------------------------------------------------------

    /// The sender's chat id, honoring the optional allowlist (None = ignore the command).
    // PORT NOTE: `_chat_id` → `chat_id` (underscore dropped). `if self._allowed and ...` —
    // frozenset truthiness → !is_empty().
    fn chat_id(&self, update: &Update) -> Option<i64> {
        let chat = update.effective_chat()?;
        // PORT NOTE: `chat.id` always exists on PTB's typed Chat; a malformed raw update
        // without an integer id is treated as "no chat" here (defensive divergence,
        // unreachable on the real Bot API).
        let id = chat.get("id").and_then(Value::as_i64)?;
        if !self.allowed.is_empty() && !self.allowed.contains(&id) {
            return None;
        }
        Some(id)
    }

    /// Reply in the update's chat with HTML formatting and an arbitrary inline keyboard
    /// (the /positions picker builds its own).
    // PORT NOTE: `message.reply_text(...)` → send_message to the message's own chat id; the
    // `if message is not None` guard folds into the chained Option (a chat/id-less raw message
    // also no-ops, which PTB's typed Message made impossible).
    async fn reply_with_markup(
        &self,
        update: &Update,
        text: &str,
        reply_markup: Option<&Value>,
    ) -> Result<()> {
        let Some(chat_id) = update
            .effective_message()
            .and_then(|m| m.get("chat"))
            .and_then(|c| c.get("id"))
            .and_then(Value::as_i64)
        else {
            return Ok(());
        };
        self.app
            .bot()
            .send_message(chat_id, text, Some(PARSE_MODE_HTML), reply_markup)
            .await?;
        Ok(())
    }

    /// Reply in the update's chat with HTML formatting (works for commands and button taps).
    // PORT NOTE: keyword-only `keyboard: bool = False` default flattened to a required
    // positional (guide option (a)) — every call site passes it explicitly.
    async fn reply(&self, update: &Update, text: &str, keyboard: bool) -> Result<()> {
        // `reply_markup=_MENU_KEYBOARD if keyboard else None`
        self.reply_with_markup(
            update,
            text,
            if keyboard {
                Some(&*MENU_KEYBOARD)
            } else {
                None
            },
        )
        .await
    }

    // PORT NOTE: `_ctx: ContextTypes.DEFAULT_TYPE` (unused) → `_args` — the dispatcher hands
    // every command handler the parsed ctx.args equivalent, keeping the uniform PTB handler
    // arity.
    async fn cmd_help(&self, update: &Update, _args: &[String]) -> Result<()> {
        if self.chat_id(update).is_none() {
            return Ok(());
        }
        self.reply(update, HELP, true).await
    }

    /// Dispatch an inline-keyboard tap. Always answers first to clear the button spinner.
    // PORT NOTE: `_ctx` param dropped entirely — a callback update carries no args.
    // `await query.answer()` (a method on PTB's CallbackQuery) → answer_callback_query with
    // the query's own id.
    async fn on_button(&self, update: &Update) -> Result<()> {
        let Some(query) = update.callback_query() else {
            return Ok(());
        };
        // PORT NOTE: `query.id` always exists on PTB's typed CallbackQuery; a malformed raw
        // update without one just skips the answer (defensive divergence).
        if let Some(id) = query.get("id").and_then(Value::as_str) {
            self.app.bot().answer_callback_query(id).await?;
        }
        if self.chat_id(update).is_none() {
            return Ok(());
        }
        match query.get("data").and_then(Value::as_str) {
            Some("list") => self.show_list(update).await,
            Some("positions") => self.positions_picker(update).await,
            // A wallet button from the /positions picker carries its full address.
            Some(data) if data.starts_with("pos:") => {
                self.show_positions(update, &data["pos:".len()..]).await
            }
            // The 🔄 Update P&L button on a live-position card carries `{address}:{coin}`.
            Some(data) if data.starts_with("upnl:") => {
                self.refresh_pnl(update, &data["upnl:".len()..]).await
            }
            // "help" (and any unknown payload) falls back to the menu
            _ => self.reply(update, HELP, true).await,
        }
    }

    /// Shared command preamble: authorize, check arg count, normalize the address.
    ///
    /// Returns `(chat_id, address, args)` or `None` (having already replied) when the command
    /// is unauthorized, under-argumented, or the first arg isn't a valid address.
    // PORT NOTE: keyword-only `*, min_args, usage` flattened to positional. The
    // `tuple[int, str, list[str]] | None` return grows a Result ring (the replies can fail);
    // the `list[str]` comes back as the same borrowed slice (Python returned the same list
    // object it was handed).
    async fn resolve<'a>(
        &self,
        update: &Update,
        args: &'a [String],
        min_args: usize,
        usage: &str,
    ) -> Result<Option<(i64, String, &'a [String])>> {
        let Some(chat_id) = self.chat_id(update) else {
            return Ok(None);
        };
        // PORT NOTE: `args = ctx.args or []` — handle_update always passes a (possibly empty)
        // slice, so PTB's None arm vanishes.
        if args.len() < min_args {
            self.reply(update, usage, false).await?;
            return Ok(None);
        }
        // PORT NOTE: try/except ValueError → match on normalize_address's Result
        // (registry::Error::InvalidAddress IS that ValueError).
        match normalize_address(&args[0]) {
            Ok(address) => Ok(Some((chat_id, address, args))),
            Err(_) => {
                self.reply(
                    update,
                    &format!("⚠️ Not a valid address: <code>{}</code>", h(&args[0])),
                    false,
                )
                .await?;
                Ok(None)
            }
        }
    }

    async fn cmd_add(&self, update: &Update, args: &[String]) -> Result<()> {
        let resolved = self
            .resolve(
                update,
                args,
                2,
                "Usage: <code>/add &lt;address&gt; &lt;label&gt;</code>",
            )
            .await?;
        let Some((chat_id, address, args)) = resolved else {
            return Ok(());
        };
        // `" ".join(args[1:]).strip()`
        let label = args[1..].join(" ").trim().to_string();
        let text = self.add_wallet(chat_id, &address, &label).await?;
        self.reply(update, &text, false).await
    }

    async fn cmd_remove(&self, update: &Update, args: &[String]) -> Result<()> {
        let resolved = self
            .resolve(
                update,
                args,
                1,
                "Usage: <code>/remove &lt;address&gt;</code>",
            )
            .await?;
        let Some((chat_id, address, _)) = resolved else {
            return Ok(());
        };
        let existed = self.remove_wallet(chat_id, &address).await?;
        // PORT NOTE: Python's conditional expression inside the call → a bound local.
        let text = if existed {
            format!("🗑️ Stopped following <code>{address}</code>.")
        } else {
            format!("You weren't following <code>{address}</code>.")
        };
        self.reply(update, &text, false).await
    }

    async fn cmd_rename(&self, update: &Update, args: &[String]) -> Result<()> {
        let resolved = self
            .resolve(
                update,
                args,
                2,
                "Usage: <code>/rename &lt;address&gt; &lt;label&gt;</code>",
            )
            .await?;
        let Some((chat_id, address, args)) = resolved else {
            return Ok(());
        };
        let label = args[1..].join(" ").trim().to_string();
        if self.db.rename(chat_id, &address, &label).await? {
            // PORT NOTE: the registry rename's bool is discarded, as the Python discarded it
            // (the DB row is the source of truth for "was subscribed").
            self.registry
                .lock()
                .expect("registry mutex poisoned")
                .rename(chat_id, &address, &label);
            self.reply(
                update,
                &format!(
                    "✏️ Renamed to <b>{}</b> (<code>{address}</code>).",
                    h(&label)
                ),
                false,
            )
            .await
        } else {
            self.reply(
                update,
                &format!("You aren't following <code>{address}</code>."),
                false,
            )
            .await
        }
    }

    async fn cmd_list(&self, update: &Update, _args: &[String]) -> Result<()> {
        self.show_list(update).await
    }

    /// `/positions [address]` — live open positions of a wallet the sender follows.
    /// Without an address it offers a tappable picker of the sender's wallets.
    async fn cmd_positions(&self, update: &Update, args: &[String]) -> Result<()> {
        if self.chat_id(update).is_none() {
            return Ok(());
        }
        match args.first() {
            None => self.positions_picker(update).await,
            Some(raw) => match normalize_address(raw) {
                Ok(address) => self.show_positions(update, &address).await,
                Err(_) => {
                    self.reply(
                        update,
                        &format!("⚠️ Not a valid address: <code>{}</code>", h(raw)),
                        false,
                    )
                    .await
                }
            },
        }
    }

    /// One tappable button per followed wallet (shared by /positions and the 📊 button).
    /// A single-wallet watchlist skips the picker and answers directly.
    async fn positions_picker(&self, update: &Update) -> Result<()> {
        let Some(chat_id) = self.chat_id(update) else {
            return Ok(());
        };
        let wallets = self.db.list_for(chat_id).await?;
        if wallets.is_empty() {
            self.reply(
                update,
                "You're not following any wallets yet. Add one with \
                 <code>/add &lt;address&gt; &lt;label&gt;</code>.",
                true,
            )
            .await?;
            return Ok(());
        }
        let mut items: Vec<(String, String)> = wallets.into_iter().collect();
        if let [(address, _)] = items.as_slice() {
            let address = address.clone();
            return self.show_positions(update, &address).await;
        }
        // Sorted by (label, address) so the buttons keep a stable order across taps.
        items.sort_by(|a, b| a.1.cmp(&b.1).then_with(|| a.0.cmp(&b.0)));
        let rows: Vec<Value> = items
            .iter()
            .map(|(addr, label)| json!([{ "text": label, "callback_data": format!("pos:{addr}") }]))
            .collect();
        self.reply_with_markup(
            update,
            "📊 Whose positions?",
            Some(&json!({ "inline_keyboard": rows })),
        )
        .await
    }

    /// Fetch + render one followed wallet's live open positions.
    async fn show_positions(&self, update: &Update, address: &str) -> Result<()> {
        let Some(chat_id) = self.chat_id(update) else {
            return Ok(());
        };
        // Only wallets the sender follows — their own label doubles as the header.
        let wallets = self.db.list_for(chat_id).await?;
        let Some(label) = wallets.get(address) else {
            self.reply(
                update,
                &format!("You aren't following <code>{address}</code>."),
                false,
            )
            .await?;
            return Ok(());
        };
        match self.enricher.account_positions(address).await {
            Ok(positions) => {
                self.reply(update, &format_positions(label, address, &positions), false)
                    .await
            }
            // Best-effort read: a transient clearinghouseState failure shouldn't bubble out
            // of the handler — log it and tell the user instead.
            Err(err) => {
                tracing::error!("/positions: clearinghouseState failed for {address}: {err:?}");
                self.reply(
                    update,
                    &format!(
                        "⚠️ Couldn't fetch positions for <b>{}</b> right now — try again in a \
                         moment.",
                        h(label)
                    ),
                    false,
                )
                .await
            }
        }
    }

    /// The 🔄 Update P&L button: re-fetch the wallet's clearinghouse snapshot for one coin and
    /// edit the card in place with live uPnL/ROI + the liquidation price. `payload` is
    /// `{address}:{coin}`. The fetch and the edit are best-effort — a transient fetch failure or an
    /// identical same-minute re-tap ("message is not modified") just leaves the card as-is; only
    /// the watchlist lookup propagates an error.
    async fn refresh_pnl(&self, update: &Update, payload: &str) -> Result<()> {
        let Some(chat_id) = self.chat_id(update) else {
            return Ok(());
        };
        // callback_data is `{address}:{coin}`; the address is 0x-hex (no ':'), so split on the
        // first ':' — a coin can't contain one either.
        let Some((raw_addr, coin)) = payload.split_once(':') else {
            return Ok(());
        };
        let Ok(address) = normalize_address(raw_addr) else {
            return Ok(());
        };
        // The card lives in the tapping user's own chat; their label heads the refreshed card.
        let wallets = self.db.list_for(chat_id).await?;
        let Some(label) = wallets.get(&address) else {
            return Ok(()); // no longer following — leave the stale card untouched
        };
        // Edit the very message the button hangs off.
        let Some(message_id) = update
            .callback_query()
            .and_then(|q| q.get("message"))
            .and_then(|m| m.get("message_id"))
            .and_then(Value::as_i64)
        else {
            return Ok(());
        };
        match self.enricher.account_positions(&address).await {
            Ok(positions) => {
                let (text, keyboard) = match positions.iter().find(|p| p.coin.as_str() == coin) {
                    Some(p) => (
                        format_live_position(label, &address, p, Utc::now()),
                        update_pnl_keyboard(&address, coin),
                    ),
                    // Closed since the card was shown — finalize it and drop the button.
                    None => (
                        format!("⚪ <b>{}</b> · {} — position closed", h(label), h(coin)),
                        None,
                    ),
                };
                // Best-effort: a since-deleted card or an identical same-minute re-tap makes the
                // Bot API return a 4xx; log it rather than bubbling it into the poll loop.
                if let Err(err) = self
                    .app
                    .bot()
                    .edit_message_text(
                        chat_id,
                        message_id,
                        &text,
                        Some(PARSE_MODE_HTML),
                        keyboard.as_ref(),
                    )
                    .await
                {
                    tracing::warn!(
                        "refresh P&L: edit failed (chat={chat_id} msg={message_id}): {err}"
                    );
                }
            }
            // Transient clearinghouseState failure — log and leave the card as-is.
            Err(err) => {
                tracing::warn!("refresh P&L: clearinghouseState failed for {address}: {err:?}");
            }
        }
        Ok(())
    }

    /// Render the sender's watchlist (shared by /list and the 📃 My wallets button).
    async fn show_list(&self, update: &Update) -> Result<()> {
        let Some(chat_id) = self.chat_id(update) else {
            return Ok(());
        };
        let wallets = self.db.list_for(chat_id).await?;
        // `if not wallets:` — dict truthiness → is_empty().
        if wallets.is_empty() {
            self.reply(
                update,
                "You're not following any wallets yet. Add one with \
                 <code>/add &lt;address&gt; &lt;label&gt;</code>.",
                true,
            )
            .await?;
            return Ok(());
        }
        // PORT NOTE: `sorted(wallets.items())` — the HashMap (db.rs fixed choice: order never
        // observable pre-sort) is materialized and sorted by (address, label) tuple order,
        // exactly Python's tuple comparison; addresses are unique so the label never ties.
        let mut items: Vec<(String, String)> = wallets.into_iter().collect();
        items.sort();
        // Full address, tap-to-copy: <code> copies its exact contents in Telegram, and a
        // truncated 0x1234…abcd form would copy the ellipsis version (not a valid address).
        let lines: Vec<String> = items
            .iter()
            .map(|(addr, label)| format!("• <b>{}</b> — <code>{addr}</code>", h(label)))
            .collect();
        self.reply(
            update,
            &format!("<b>You're following:</b>\n{}", lines.join("\n")),
            false,
        )
        .await
    }
}

/// One rendered row of the `/positions` grid, plus a footer total — cell strings only, so the
/// column widths can be sized to the widest cell before anything is padded.
struct PositionRow {
    coin: String,
    side: String,
    value: String,
    upnl: String,
}

/// Render one wallet's live open positions — the `/positions` view (pure, unit-tested).
///
/// A terminal watchlist: a linked-name header (`📋 name · N open`) over a monospace grid of
/// `COIN · SIDE+leverage · VALUE · uPNL`, largest position (by USD value) first, closed by a
/// `TOT` row summing notional and net unrealized PnL. Column widths are sized to the data so a
/// long ticker or six-figure value can't shear the grid. Notional/PnL are abbreviated (`$128.2k`,
/// `+$3.1k`) to stay inside the mobile monospace budget; the trader name links to the explorer.
fn format_positions(label: &str, address: &str, positions: &[AccountPosition]) -> String {
    let header = format!(
        "📋 {} · {} open",
        linked_name(address, label),
        positions.len()
    );
    if positions.is_empty() {
        return format!("{header}\n\nNo open positions.");
    }
    let mut sorted: Vec<&AccountPosition> = positions.iter().collect();
    sorted.sort_by(|a, b| {
        let value = |p: &AccountPosition| p.position_value.map(|v| v.abs());
        value(b).cmp(&value(a)).then_with(|| a.coin.cmp(&b.coin))
    });

    let mut total_value = Decimal::ZERO;
    let mut total_upnl = Decimal::ZERO;
    let rows: Vec<PositionRow> = sorted
        .iter()
        .map(|p| {
            // account_positions only hands over non-zero sizes; a missing szi renders as Long.
            let szi = p.szi.unwrap_or_default();
            let side_letter = if szi < Decimal::ZERO { "S" } else { "L" };
            let side = match p.leverage_value {
                Some(l) => format!("{side_letter} {l}x"),
                None => side_letter.to_string(),
            };
            let value = p.position_value.map(|v| v.abs()).unwrap_or(Decimal::ZERO);
            total_value += value;
            total_upnl += p.unrealized_pnl.unwrap_or(Decimal::ZERO);
            PositionRow {
                coin: p.coin.clone(),
                side,
                value: format!("${}", notional_short(value)),
                upnl: p
                    .unrealized_pnl
                    .map(money_short)
                    .unwrap_or_else(|| "?".to_string()),
            }
        })
        .collect();

    // The header labels and the TOT row take part in the width fit so every column aligns.
    let tot_side = format!("{} pos", rows.len());
    let tot_value = format!("${}", notional_short(total_value));
    let tot_upnl = money_short(total_upnl);
    let width = |pick: fn(&PositionRow) -> &str, label: &str, tot: &str| {
        rows.iter()
            .map(|r| pick(r).len())
            .chain([label.len(), tot.len()])
            .max()
            .unwrap_or(0)
    };
    let (wc, ws, wv, wu) = (
        width(|r| &r.coin, "COIN", "TOT"),
        width(|r| &r.side, "SIDE", &tot_side),
        width(|r| &r.value, "VALUE", &tot_value),
        width(|r| &r.upnl, "uPNL", &tot_upnl),
    );
    // COIN/SIDE left-aligned, VALUE/uPNL right-aligned so the money columns line up on the dot.
    let line =
        |c: &str, s: &str, v: &str, u: &str| format!("{c:<wc$}  {s:<ws$}  {v:>wv$}  {u:>wu$}");
    let head = line("COIN", "SIDE", "VALUE", "uPNL");
    let body = rows
        .iter()
        .map(|r| line(&r.coin, &r.side, &r.value, &r.upnl))
        .collect::<Vec<_>>()
        .join("\n");
    let sep = "-".repeat(head.chars().count());
    let tot = line("TOT", &tot_side, &tot_value, &tot_upnl);
    format!("{header}\n<pre>{head}\n{body}\n{sep}\n{tot}</pre>")
}

// ──────────────────────────────────────────────────────────────────────────
// tests — Rust-only (bot.py had no test file); pin the pure /positions renderer.
// ──────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn d(s: &str) -> Option<Decimal> {
        Some(Decimal::from_str(s).expect("test literal is a valid decimal"))
    }

    fn addr() -> String {
        format!("0x{}", "ab".repeat(20))
    }

    fn position(
        coin: &str,
        szi: &str,
        entry: &str,
        value: &str,
        upnl: &str,
        leverage: Option<i64>,
        liq: Option<&str>,
    ) -> AccountPosition {
        AccountPosition {
            address: addr(),
            coin: coin.to_string(),
            szi: d(szi),
            entry_px: d(entry),
            position_value: d(value),
            unrealized_pnl: d(upnl),
            liquidation_px: liq.and_then(d),
            leverage_type: None,
            leverage_value: leverage,
            max_leverage: None,
        }
    }

    #[test]
    fn test_format_positions_grid_sorts_by_value_and_totals() {
        let text = format_positions(
            "Whale-1",
            &addr(),
            &[
                // Deliberately out of order: the renderer sorts by |value| descending.
                position(
                    "ETH",
                    "-1",
                    "2500",
                    "37500",
                    "-820",
                    Some(5),
                    Some("3100.5"),
                ),
                position("BTC", "2", "63000", "128200", "1790", Some(10), None),
            ],
        );
        assert_eq!(
            text,
            format!(
                "📋 <b><a href=\"https://app.hyperliquid.xyz/explorer/address/{addr}\">Whale-1</a></b> · 2 open\n\
                 <pre>COIN  SIDE     VALUE    uPNL\n\
                 BTC   L 10x  $128.2k  +$1.8k\n\
                 ETH   S 5x    $37.5k   -$820\n\
                 ----------------------------\n\
                 TOT   2 pos  $165.7k   +$970</pre>",
                addr = addr()
            )
        );
    }

    #[test]
    fn test_format_positions_empty_links_name_and_escapes_label() {
        let text = format_positions("<W&>", &addr(), &[]);
        assert_eq!(
            text,
            format!(
                "📋 <b><a href=\"https://app.hyperliquid.xyz/explorer/address/{addr}\">&lt;W&amp;&gt;</a></b> · 0 open\n\nNo open positions.",
                addr = addr()
            )
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/bot.py (277 lines)
//   confidence: medium
//   todos:      3
//   notes:      python-telegram-bot has no fixed-crate counterpart, so the used PTB surface
//               (Application/Bot/Update/BotCommand, ParseMode.HTML, inline keyboard as raw
//               reply_markup JSON) is hand-rolled over reqwest à la telegram_setup.rs; PTB's
//               handler registry (_register/add_handler) became SettingsBot::handle_update's
//               match, and the polling lifecycle moves to app.rs (Phase B) via
//               Bot::get_updates + Update::update_id. SettingsBot::new is fallible
//               (allowed_chat_ids_set) and holds Arc<Mutex<Registry>> / Arc<Mutex<InMemoryBook>>
//               / Arc<WatchlistDB> / Arc<Enricher> / Arc<Application> per the shared-state
//               fixed decisions — app.rs must construct them that way. Handler errors surface
//               as bot::Error (Telegram=reqwest, Db=tokio_rusqlite, Config) for the poll loop
//               to log-and-continue, replacing PTB's error logger. Crates: reqwest,
//               serde_json, thiserror, async-trait, tokio-rusqlite (error type only), tokio
//               (runtime).
//   divergence: post-port additions with no Python counterpart — /positions (picker keyboard
//               + Enricher::account_positions + format_positions), full addresses in <code>
//               (tap-to-copy), notifications delivered as HTML (escape_html moved to
//               notifier.rs).
// ──────────────────────────────────────────────────────────────────────────
