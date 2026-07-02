//! Telegram delivery + settings UX (the only module that speaks the live Telegram Bot API).
//!
//! A MULTI-TENANT public bot: anyone can message it and manage their own watchlist. Commands
//! operate on the sender's own chat: `/add <address> <label…>`, `/remove <address>`,
//! `/rename <address> <label…>`, `/list`, `/help`. Notifications for a wallet are fanned out
//! to every subscriber of that wallet in their own chat. The core stays Telegram-free (this
//! module is only wired up by `tracker::app` when a bot token is configured).
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
use serde_json::{Value, json};

use crate::book::InMemoryBook;
use crate::config::{self, Settings};
use crate::db::WatchlistDB;
use crate::enrich::Enricher;
use crate::notifier::{MessageSender, SendError};
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

    /// `bot.send_message(...)`.
    // PORT NOTE: PTB's keyword defaults (`parse_mode=None`, `reply_markup=None`) flattened to
    // required Option params (guide option (b): absence is semantic — plain text vs HTML,
    // keyboard vs none). PTB raised TelegramError on an `ok: false` payload; the Bot API sets
    // a non-2xx status on exactly those, so `error_for_status()` is the equivalent raise.
    pub async fn send_message(
        &self,
        chat_id: i64,
        text: &str,
        parse_mode: Option<&str>,
        reply_markup: Option<&Value>,
    ) -> reqwest::Result<()> {
        let mut body = json!({ "chat_id": chat_id, "text": text });
        if let Some(parse_mode) = parse_mode {
            body["parse_mode"] = json!(parse_mode);
        }
        if let Some(reply_markup) = reply_markup {
            body["reply_markup"] = reply_markup.clone();
        }
        self.client
            .post(self.url("sendMessage"))
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

/// Escape user-supplied text before it goes into an HTML-parsed message.
// PORT NOTE: `_h = html.escape` (module alias, quote=True default) → fn reproducing the same
// five replacements in the same order (&, <, >, ", ').
// PERF(port): five allocate-per-pass replaces — profile in Phase B (labels are tiny).
fn h(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

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
    📃 <code>/list</code> — show the wallets you follow\n\n\
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
            { "text": "❓ Help", "callback_data": "help" },
        ]]
    })
});

// The persistent slash-command menu (the blue Menu button + "/" autocomplete). Descriptions are
// plain text — Telegram does not parse HTML here.
static COMMANDS: [BotCommand; 5] = [
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
    // PORT NOTE: notifications go out as PLAIN text — no parse_mode, no keyboard — exactly
    // the Python's `send_message(chat_id=chat_id, text=text)`. The raising `-> None` becomes
    // Result<(), SendError>: the reqwest error is boxed into notifier.rs's type-erased
    // SendError (its contract for Notifier.dispatch's `except Exception`).
    async fn send(&self, chat_id: i64, text: &str) -> std::result::Result<(), SendError> {
        self.app
            .bot()
            .send_message(chat_id, text, None, None)
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
        let short = Registry::short(address);
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
                "💾 Saved <b>{}</b> (<code>{short}</code>) but I couldn't read its \
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
            "✅ Now following <b>{}</b> (<code>{short}</code>).",
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

    /// Reply in the update's chat with HTML formatting (works for commands and button taps).
    // PORT NOTE: keyword-only `keyboard: bool = False` default flattened to a required
    // positional (guide option (a)) — every call site passes it explicitly.
    // PORT NOTE: `message.reply_text(...)` → send_message to the message's own chat id; the
    // `if message is not None` guard folds into the chained Option (a chat/id-less raw message
    // also no-ops, which PTB's typed Message made impossible).
    async fn reply(&self, update: &Update, text: &str, keyboard: bool) -> Result<()> {
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
            .send_message(
                chat_id,
                text,
                Some(PARSE_MODE_HTML),
                // `reply_markup=_MENU_KEYBOARD if keyboard else None`
                if keyboard {
                    Some(&*MENU_KEYBOARD)
                } else {
                    None
                },
            )
            .await?;
        Ok(())
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
        if query.get("data").and_then(Value::as_str) == Some("list") {
            self.show_list(update).await
        } else {
            // "help" (and any unknown payload) falls back to the menu
            self.reply(update, HELP, true).await
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
        let short = Registry::short(&address);
        // PORT NOTE: Python's conditional expression inside the call → a bound local.
        let text = if existed {
            format!("🗑️ Stopped following <code>{short}</code>.")
        } else {
            format!("You weren't following <code>{short}</code>.")
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
        let short = Registry::short(&address);
        if self.db.rename(chat_id, &address, &label).await? {
            // PORT NOTE: the registry rename's bool is discarded, as the Python discarded it
            // (the DB row is the source of truth for "was subscribed").
            self.registry
                .lock()
                .expect("registry mutex poisoned")
                .rename(chat_id, &address, &label);
            self.reply(
                update,
                &format!("✏️ Renamed to <b>{}</b> (<code>{short}</code>).", h(&label)),
                false,
            )
            .await
        } else {
            self.reply(
                update,
                &format!("You aren't following <code>{short}</code>."),
                false,
            )
            .await
        }
    }

    async fn cmd_list(&self, update: &Update, _args: &[String]) -> Result<()> {
        self.show_list(update).await
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
        let lines: Vec<String> = items
            .iter()
            .map(|(addr, label)| {
                format!(
                    "• <b>{}</b> — <code>{}</code>",
                    h(label),
                    Registry::short(addr)
                )
            })
            .collect();
        self.reply(
            update,
            &format!("<b>You're following:</b>\n{}", lines.join("\n")),
            false,
        )
        .await
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
// ──────────────────────────────────────────────────────────────────────────
