//! One-shot helper: validate a Telegram bot token and discover your chat id.
//!
//! A bot token cannot be *generated* programmatically — Telegram issues it when you create a bot
//! via **@BotFather** in the Telegram app (send `/newbot`, choose a name + username, copy the
//! token it replies with). This helper then does the two fiddly parts for you:
//!
//! 1. validates the token via `getMe` (so you know it's live and which bot it is), and
//! 2. prints the `chat_id` of everyone who has recently messaged the bot (via `getUpdates`),
//!
//! so you can paste `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` into your `.env`.
//!
//! Run it with `uv run hl-tracker-telegram-setup` (reads `TELEGRAM_BOT_TOKEN` from env/.env),
//! or pass the token explicitly: `uv run hl-tracker-telegram-setup <token>`.

// PORT NOTE: sync `httpx.Client` main → async reqwest per the fixed port decisions; the
// runtime is tokio (fixed) and Phase B wires the actual bin shim (`hl-tracker-telegram-setup`
// → src/telegram_setup_main.rs with a #[tokio::main] main calling `run`).
// TODO(port): the module docs and BOTFATHER_HELP still say `uv run hl-tracker-telegram-setup`
// — Phase B should settle the Rust invocation wording (cargo bin `hl-tracker-telegram-setup`).

use std::process::ExitCode;
use std::time::Duration;

use serde_json::{Value, json};

use crate::config::{self, Settings, load_env};

// PORT NOTE: leading-underscore module-private names (`_API`, `_TIMEOUT_S`, …) drop the
// underscore — Rust privacy is the absence of `pub`.
const API: &str = "https://api.telegram.org";
const TIMEOUT_S: f64 = 15.0;

const BOTFATHER_HELP: &str = "\
No bot token found. To create one (one-time, in the Telegram app):

  1. Open Telegram and message @BotFather
  2. Send /newbot, then follow the prompts (bot name, then a username ending in 'bot')
  3. BotFather replies with a token like 123456789:AA...  — copy it

Then set it and re-run:

  export TELEGRAM_BOT_TOKEN=123456789:AA...
  uv run hl-tracker-telegram-setup
";

// PORT NOTE: the Python had no error type — httpx transport errors, json.JSONDecodeError, and
// pydantic's ValidationError all propagated to the interpreter (traceback + nonzero exit).
// This per-module enum materializes that propagation so `try_run` can use `?` with the same
// control flow, and `run` plays the interpreter (print + exit 1). It is per-module (like
// `config::Error`) rather than the crate-level `TrackerError` because these are CLI-wiring
// failures, not the Parse/RateLimited/AuthRequired shapes TrackerError covers.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Config(#[from] config::Error),
    #[error(transparent)]
    Http(reqwest::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

// SECURITY NOTE (review follow-up): manual From instead of #[from] so the request URL —
// which embeds the bot token (`/bot<token>/<method>`) — is stripped before `run()`'s
// eprintln can print it. The Python printed only status + description, never the URL.
impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        Self::Http(err.without_url())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// Token from an explicit CLI arg, else `TELEGRAM_BOT_TOKEN` (env or .env).
// PORT NOTE: `str | None` → Option<String>; the return additionally grows a Result because
// `Settings()` (which raised pydantic ValidationError) became the fallible
// `Settings::from_env()` — the `?` propagates exactly where the Python exception did.
fn resolve_token(argv: &[String]) -> config::Result<Option<String>> {
    // PORT NOTE: `if len(argv) > 1 and argv[1].strip():` — the second operand is Python
    // truthiness on the stripped string, so a whitespace-only arg falls through to the env.
    if argv.len() > 1 {
        let arg = argv[1].trim();
        if !arg.is_empty() {
            return Ok(Some(arg.to_string()));
        }
    }
    load_env();
    Ok(Settings::from_env()?.telegram_bot_token)
}

/// Python `bool()` truthiness for a JSON value.
// PORT NOTE: materializes Python's implicit truthiness (the `or` chain in record_chat and
// `not payload.get("ok")` in try_run) — Rust has no truthiness.
fn truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(b) => *b,
        Value::Number(n) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Value::String(s) => !s.is_empty(),
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

/// Render `dict.get(...)` the way a Python f-string would: a missing key or JSON null prints
/// `None`, strings print unquoted.
// PORT NOTE: Value's own Display quotes strings ("name" → "\"name\""); Python's str() does
// not. A bool would render "true"/"false" here vs Python's "True"/"False" — no field
// formatted through this helper (username, id, description, title, …) is ever a bool in the
// Bot API.
fn py_str(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => "None".to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(other) => other.to_string(),
    }
}

/// Record one Telegram `chat` object (from JSON) into `chats` keyed by its id.
// PORT NOTE: `chats: dict[int, str]` → Vec<(i64, String)>. The print order is observable
// (main iterates it) and Python dicts preserve insertion order, but `indexmap` is not in the
// fixed crate set, so insertion-order-with-update-in-place semantics are hand-rolled over a
// Vec. Telegram chat ids are i64 (fixed decision).
// PERF(port): linear scan per insert (dict was O(1)) — getUpdates returns ≤100 updates, so
// n is tiny; profile in Phase B only if this ever leaves the one-shot CLI.
fn record_chat(chat: Option<&Value>, chats: &mut Vec<(i64, String)>) {
    // PORT NOTE: `chat: Any` arrives as Option<&Value> (dict.get miss = None); the
    // `if not isinstance(chat, dict): return` guard covered None too, so both checks fold
    // into one let-else over as_object.
    let Some(chat) = chat.and_then(Value::as_object) else {
        return;
    };
    // PORT NOTE: `isinstance(chat_id, int)` — as_i64 likewise rejects floats and strings.
    // (Python's isinstance would also accept a bool — bool subclasses int — but a bool chat
    // id never occurs in the Bot API; as_i64 rejects it.)
    let Some(chat_id) = chat.get("id").and_then(Value::as_i64) else {
        return;
    };
    // PORT NOTE: `chat.get("type", "?")` — "?" only when the key is missing; a JSON-null
    // `type` renders "None", exactly as the Python f-string did.
    let ctype = chat
        .get("type")
        .map(|v| py_str(Some(v)))
        .unwrap_or_else(|| "?".to_string());
    // PORT NOTE: `a or b or c or ctype` — first *truthy* value wins (empty strings and
    // nulls fall through), rendered via py_str like the f-string would.
    let who = ["title", "username", "first_name"]
        .into_iter()
        .find_map(|key| chat.get(key).filter(|v| truthy(v)))
        .map(|v| py_str(Some(v)))
        .unwrap_or_else(|| ctype.clone());
    // PORT NOTE: `chats[chat_id] = ...` on an existing key updates the value but keeps the
    // original insertion position — mirrored by the in-place find.
    match chats.iter_mut().find(|(id, _)| *id == chat_id) {
        Some(slot) => slot.1 = format!("{who} ({ctype})"),
        None => chats.push((chat_id, format!("{who} ({ctype})"))),
    }
}

/// Map each chat id seen in recent updates (messages / joins) to a human description.
fn discover_chats(updates: &[Value]) -> Vec<(i64, String)> {
    let mut chats: Vec<(i64, String)> = Vec::new();
    for update in updates {
        // PORT NOTE: `if not isinstance(update, dict): continue` — Value::get below would
        // yield None for a non-object anyway, but the explicit guard matches the Python flow.
        if !update.is_object() {
            continue;
        }
        for key in [
            "message",
            "edited_message",
            "channel_post",
            "my_chat_member",
        ] {
            let container = update.get(key);
            if let Some(container) = container.filter(|c| c.is_object()) {
                record_chat(container.get("chat"), &mut chats);
            }
        }
    }
    chats
}

/// Console-script entry point (`hl-tracker-telegram-setup`).
// PORT NOTE: Python's sync `main() -> None` (which exited via `raise SystemExit(1)` or fell
// off the end) becomes `pub async fn run(argv) -> ExitCode` per the fixed port decisions.
// It is split: `try_run` is the faithful body (`?` = Python's implicit exception
// propagation); `run` plays the interpreter — an uncaught exception printed a traceback to
// stderr and exited nonzero, here the error prints and the process exits 1.
// `argv[0]` is the program name, exactly like `sys.argv` (the Phase B bin shim passes
// `std::env::args().collect()`).
pub async fn run(argv: Vec<String>) -> ExitCode {
    match try_run(argv).await {
        Ok(code) => code,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(1)
        }
    }
}

async fn try_run(argv: Vec<String>) -> Result<ExitCode> {
    let token = resolve_token(&argv)?;
    // PORT NOTE: `if not token:` — Python truthiness treats None and "" alike (an empty
    // TELEGRAM_BOT_TOKEN env var yields Some("")).
    let Some(token) = token.filter(|t| !t.is_empty()) else {
        println!("{BOTFATHER_HELP}");
        // PORT NOTE: `raise SystemExit(1)` → exit code 1 (fixed decision).
        return Ok(ExitCode::from(1));
    };

    // PORT NOTE: `with httpx.Client(timeout=_TIMEOUT_S) as client:` — reqwest clients need
    // no explicit close, so the `with` becomes a plain binding (drop() below marks the block
    // end). httpx's timeout=15.0 applied per phase (connect/read/write/pool); reqwest's
    // .timeout() is a whole-request deadline — close enough for two tiny calls.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs_f64(TIMEOUT_S))
        .build()?;

    let me = client.get(format!("{API}/bot{token}/getMe")).send().await?;
    // PORT NOTE: reshaped for reqwest — httpx buffered the body, so .json() and .text were
    // both available after the call; reqwest *consumes* the response on read. Capture
    // status + content-type first, read the text once, and parse the JSON from it.
    let status = me.status();
    let is_json = me
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .starts_with("application/json");
    let text = me.text().await?;
    let payload: Value = if is_json {
        // PORT NOTE: a JSON-labelled body that fails to parse raised json.JSONDecodeError
        // (uncaught) in Python; `?` reproduces that crash-with-message path.
        serde_json::from_str(&text)?
    } else {
        json!({})
    };

    // PORT NOTE: `not payload.get("ok")` — truthiness via the truthy() helper; a missing
    // key is falsy, matching dict.get's None default.
    if status.as_u16() != 200 || !payload.get("ok").is_some_and(truthy) {
        // PORT NOTE: `payload.get("description", me.text[:200])` — the fallback slices the
        // first 200 *code points* (chars), not bytes.
        let desc = match payload.get("description") {
            Some(v) => py_str(Some(v)),
            None => text.chars().take(200).collect::<String>(),
        };
        println!(
            "Token rejected by Telegram (HTTP {}): {desc}",
            status.as_u16()
        );
        return Ok(ExitCode::from(1));
    }
    // PORT NOTE: `payload["result"]` raised KeyError when absent; serde_json's Value index
    // returns Null instead of panicking, so an explicit expect() reproduces the crash —
    // per the fixed decisions this can't-happen (ok was true) is a panic, not a Result.
    let bot = payload
        .get("result")
        .expect("KeyError: 'result' missing from getMe payload");
    // PORT NOTE: `bot.get('username')` would raise AttributeError if `result` were not a
    // dict; Value::get quietly yields None ("None") instead — unreachable with the real
    // Bot API, noted for the record.
    println!(
        "✓ Token valid — bot @{} (id {})",
        py_str(bot.get("username")),
        py_str(bot.get("id"))
    );

    // PORT NOTE: the one-line chain `client.get(...).json().get("result", [])` unrolls into
    // send/json/get; transport and decode errors were uncaught in Python and propagate via
    // `?` here. `.get("result", [])` on a non-dict body would have raised AttributeError in
    // Python; Value::get yields None (→ empty slice) instead — unreachable with the real
    // Bot API. A non-array `result` likewise becomes empty.
    let updates_payload: Value = client
        .get(format!("{API}/bot{token}/getUpdates"))
        .send()
        .await?
        .json()
        .await?;
    let updates: &[Value] = updates_payload
        .get("result")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    // PORT NOTE: end of the `with httpx.Client(...)` block — the explicit drop marks the
    // same lifetime boundary (reqwest has no close()).
    drop(client);

    let chats = discover_chats(updates);
    if chats.is_empty() {
        println!(
            "\nNo chats seen yet. Send any message to your bot (or add it to a group and post \
             there), then re-run this command to reveal your chat id."
        );
        // PORT NOTE: bare `return` from main() → success exit.
        return Ok(ExitCode::SUCCESS);
    }

    println!("\nChats that have messaged this bot — copy the id you want into TELEGRAM_CHAT_ID:");
    for (chat_id, who) in &chats {
        println!("  TELEGRAM_CHAT_ID={chat_id}    # {who}");
    }
    Ok(ExitCode::SUCCESS)
}

// PORT NOTE: was `if __name__ == "__main__": main()` — becomes the Phase B bin shim
// (src/telegram_setup_main.rs per Cargo.toml):
//   #[tokio::main] async fn main() -> ExitCode { tracker::telegram_setup::run(std::env::args().collect()).await }

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/telegram_setup.py (108 lines)
//   confidence: high
//   todos:      1
//   notes:      main() → pub async run(argv) -> ExitCode via private try_run (Result = the
//               Python's uncaught-exception path; run prints + exits 1). Ordered dict →
//               Vec<(i64, String)> because indexmap is not in the fixed crate set. Phase B
//               wires the hl-tracker-telegram-setup bin shim and decides the `uv run` help
//               wording. Crates: reqwest, serde_json, thiserror, tokio (runtime only).
// ──────────────────────────────────────────────────────────────────────────
