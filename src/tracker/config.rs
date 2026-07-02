//! Runtime configuration, sourced from a `.env` file or environment variables.
//!
//! Most knobs use the `TRACKER_` prefix (e.g. `TRACKER_WS_HEARTBEAT_S=30`). The Telegram
//! credentials are read un-prefixed (`TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID`) to match the
//! conventional names, and the SQLite file defaults to `tracker.db` in the current working
//! directory (the Python anchored both at the repo root via `__file__`; a compiled binary
//! has no source path, so the working directory — the repo root under `make run-rs`, and
//! `TRACKER_DB_PATH` in Docker — stands in).

use std::collections::HashSet;
use std::path::{Path, PathBuf};

// PORT NOTE: pydantic's ValidationError (raised by `Settings()` / Field constraints) and the
// ValueError raised by the `allowed_chat_ids_set` property both decompose into this per-module
// error enum. `Parse` = a value that failed type coercion; `Range` = a value that failed a
// Field(ge=..., le=...) constraint.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("invalid value for {var}: {value:?}: {reason}")]
    Parse {
        var: String,
        value: String,
        reason: String,
    },
    #[error("{field}={value} violates constraint {constraint}")]
    Range {
        field: &'static str,
        constraint: &'static str,
        value: String,
    },
    #[error("invalid chat id in allowed_chat_ids: {0:?}")]
    InvalidChatId(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Load `./.env` into the process environment (idempotent).
///
/// Existing environment variables win (`override=False`), so a container-injected value
/// takes precedence over the on-disk file. A no-op when no `.env` exists.
///
/// Must be called before threads that read the environment exist (both binaries call it
/// from `main` before the tokio runtime starts) — see the SAFETY note inside.
// PORT NOTE: Python resolved the repo-root .env from `__file__`; a compiled binary can't,
// so this reads the working directory's .env (see the module doc).
// PORT NOTE: python-dotenv parses line-by-line — a malformed line WARNS and every other
// binding still loads (and unquoted values with spaces are valid there). dotenvy's
// from_path instead aborts at the first unparseable line, which would silently drop every
// later binding (e.g. a TELEGRAM_BOT_TOKEN behind a spaced value) — so iterate per-entry
// and warn, mirroring python-dotenv's tolerance.
pub fn load_env() {
    let Ok(entries) = dotenvy::from_path_iter(Path::new(".env")) else {
        return; // missing/unreadable .env is a no-op, as in Python
    };
    for entry in entries {
        match entry {
            Ok((key, value)) => {
                if std::env::var_os(&key).is_none() {
                    // SAFETY: set_var is only unsound when another thread concurrently
                    // reads the environment. Both binaries call load_env() from main
                    // before the tokio runtime (or any thread) starts; a second call
                    // after startup finds every key already set and never reaches here.
                    unsafe { std::env::set_var(&key, &value) };
                }
            }
            // python-dotenv logs a warning and keeps parsing; eprintln matches its
            // visibility even before tracing is initialized.
            Err(err) => eprintln!("warning: .env: skipping unparseable entry: {err}"),
        }
    }
}

/// Tunable knobs for the firehose listener, enrichment sweep, and Telegram delivery.
// PORT NOTE: pydantic_settings.BaseSettings + SettingsConfigDict becomes a plain struct with
// Default (the Field defaults) + from_env() (env reading, `Settings()` in Python) +
// validate() (the Field ge/le constraints — the __post_init__-style split from the guide).
//   - env_prefix="TRACKER_"      -> hardcoded TRACKER_* variable names in from_env().
//   - env_file=".env"            -> NOT replicated here; pydantic-settings read a CWD-relative
//                                   .env at instantiation, but every real entry point calls
//                                   load_env() first (repo-root .env), which subsumes it.
//   - env_file_encoding="utf-8"  -> dotenvy assumes UTF-8; std::env::var errors on non-UTF-8.
//   - extra="ignore"             -> automatic: from_env() only looks at known variables.
//   - populate_by_name=True      -> only affected telegram_bot_token; both its alias and its
//                                   prefixed name are checked in from_env().
#[derive(Debug, Clone)]
pub struct Settings {
    // --- Hyperliquid public endpoints (sanctioned, unauthenticated) ---
    pub hyperliquid_url: String,
    pub hl_ws_url: String,

    // --- HTTP client (seed + reconcile clearinghouseState calls) ---
    /// Constraint: ge=1.0.
    pub http_timeout_s: f64,
    /// Inter-request politeness. Constraint: ge=0.0.
    pub request_delay_s: f64,
    /// Constraint: ge=0.
    // PORT NOTE: int -> u32; ge=0 plus retry-count usage bounds it, and the unsigned type
    // absorbs the ge=0 constraint.
    pub max_retries: u32,
    /// Constraint: ge=0.0.
    pub backoff_base_s: f64,
    /// Constraint: ge=0.0.
    pub backoff_cap_s: f64,

    // --- WebSocket listener ---
    /// HL closes any connection idle 60s, so ping quiet sockets under that threshold.
    /// Constraint: ge=1.0, le=55.0.
    pub ws_heartbeat_s: f64,
    /// Coins to subscribe to, comma-separated; empty = every perp from Hyperliquid `meta`.
    pub live_coins: String,
    /// Bounded in-memory ring of recently-seen trade `tid` values, so a WS reconnect that
    /// redelivers trades can neither double-ingest (corrupting a position) nor double-notify.
    /// Constraint: ge=100.
    // PORT NOTE: int -> usize; this is a collection capacity (deque maxlen).
    pub tid_dedup_maxlen: usize,

    // --- Cold-start seed + reconcile (clearinghouseState, weight 2 each) ---
    /// Concurrency of the startup seed sweep; bounded so 1k wallets stay within the 1200
    /// weight/min IP budget (clearinghouseState = weight 2 -> <=600/min).
    /// Constraint: ge=1, le=50.
    // PORT NOTE: int -> usize; a task-concurrency bound (semaphore permits).
    pub seed_concurrency: usize,
    /// Periodically refetch clearinghouseState for recently-active wallets to refresh leverage
    /// and correct any size drift from a missed WS message. 0 disables the reconcile loop.
    /// Constraint: ge=0.0.
    pub reconcile_interval_s: f64,
    /// Constraint: ge=1.
    // PORT NOTE: int -> usize; a batch size (slice length).
    pub reconcile_batch: usize,

    // --- Notifications ---
    /// Full lifecycle by default: open/add/reduce/close/flip all push. Set False to mute the
    /// exit side (reduce/close) and notify only new positions + increases.
    pub notify_reduce_close: bool,

    // --- Authoritative close PnL (userFillsByTime lookup on close/flip) ---
    /// On a close, fetch the leg's fills once via REST and report the exchange's own summed
    /// `closedPnl` instead of the local average-cost estimate. The REST fill index can lag
    /// the trades feed, so the lookup retries until the closing fill is visible, then falls
    /// back to the estimate. Weight 20 per call and closes are rare — negligible against the
    /// 1200/min IP budget.
    pub closed_pnl_lookup: bool,
    /// Constraint: ge=1, le=10.
    // PORT NOTE: int -> u32; a retry-attempt count.
    pub closed_pnl_attempts: u32,
    /// Constraint: ge=0.0, le=10.0.
    pub closed_pnl_retry_delay_s: f64,

    // --- Persistence (the ONLY thing stored: per-subscriber watchlists) ---
    pub db_path: PathBuf,

    // --- Telegram delivery ---
    /// This is a MULTI-TENANT public bot: anyone can message it, subscribe to wallets, and
    /// receive notifications in their own chat. The only credential is the bot token from
    /// @BotFather — there is no fixed chat id (each subscriber's chat is captured on /start).
    /// Optionally restrict who may subscribe to a comma-separated allowlist of chat ids.
    // PORT NOTE: validation_alias=AliasChoices("TELEGRAM_BOT_TOKEN", "TRACKER_TELEGRAM_BOT_TOKEN")
    // is honored in from_env(): the un-prefixed name is checked first, exactly in alias order.
    pub telegram_bot_token: Option<String>,
    pub allowed_chat_ids: String,
}

impl Default for Settings {
    /// The pydantic Field defaults, verbatim, in the Python field order.
    fn default() -> Self {
        Self {
            hyperliquid_url: "https://api.hyperliquid.xyz/info".to_string(),
            hl_ws_url: "wss://api.hyperliquid.xyz/ws".to_string(),
            http_timeout_s: 30.0,
            request_delay_s: 0.0,
            max_retries: 4,
            backoff_base_s: 0.5,
            backoff_cap_s: 20.0,
            ws_heartbeat_s: 30.0,
            live_coins: String::new(),
            tid_dedup_maxlen: 100_000,
            seed_concurrency: 8,
            reconcile_interval_s: 45.0,
            reconcile_batch: 20,
            notify_reduce_close: true,
            closed_pnl_lookup: true,
            closed_pnl_attempts: 3,
            closed_pnl_retry_delay_s: 1.0,
            // PORT NOTE: Python defaulted to `<repo root>/tracker.db` via `__file__`;
            // cwd-relative is the compiled-binary equivalent (module doc has the rationale).
            db_path: PathBuf::from("tracker.db"),
            telegram_bot_token: None,
            allowed_chat_ids: String::new(),
        }
    }
}

impl Settings {
    /// Build `Settings` from the process environment, mirroring `Settings()` in Python:
    /// defaults first, then any `TRACKER_*` (or aliased) variable overrides, then validation.
    ///
    /// Callers are expected to run [`load_env`] first (the Python entry points do).
    // PORT NOTE: pydantic-settings matches env vars case-insensitively by default
    // (case_sensitive=False); this port reads the canonical UPPER_CASE names only.
    // TODO(port): confirm no deployment relies on mixed-case TRACKER_ env var names.
    pub fn from_env() -> Result<Self> {
        let mut s = Settings::default();
        if let Some(v) = get_env("TRACKER_HYPERLIQUID_URL") {
            s.hyperliquid_url = v;
        }
        if let Some(v) = get_env("TRACKER_HL_WS_URL") {
            s.hl_ws_url = v;
        }
        if let Some(v) = get_env("TRACKER_HTTP_TIMEOUT_S") {
            s.http_timeout_s = parse_env("TRACKER_HTTP_TIMEOUT_S", &v)?;
        }
        if let Some(v) = get_env("TRACKER_REQUEST_DELAY_S") {
            s.request_delay_s = parse_env("TRACKER_REQUEST_DELAY_S", &v)?;
        }
        if let Some(v) = get_env("TRACKER_MAX_RETRIES") {
            s.max_retries = parse_int_env("TRACKER_MAX_RETRIES", &v)?;
        }
        if let Some(v) = get_env("TRACKER_BACKOFF_BASE_S") {
            s.backoff_base_s = parse_env("TRACKER_BACKOFF_BASE_S", &v)?;
        }
        if let Some(v) = get_env("TRACKER_BACKOFF_CAP_S") {
            s.backoff_cap_s = parse_env("TRACKER_BACKOFF_CAP_S", &v)?;
        }
        if let Some(v) = get_env("TRACKER_WS_HEARTBEAT_S") {
            s.ws_heartbeat_s = parse_env("TRACKER_WS_HEARTBEAT_S", &v)?;
        }
        if let Some(v) = get_env("TRACKER_LIVE_COINS") {
            s.live_coins = v;
        }
        if let Some(v) = get_env("TRACKER_TID_DEDUP_MAXLEN") {
            s.tid_dedup_maxlen = parse_int_env("TRACKER_TID_DEDUP_MAXLEN", &v)?;
        }
        if let Some(v) = get_env("TRACKER_SEED_CONCURRENCY") {
            s.seed_concurrency = parse_int_env("TRACKER_SEED_CONCURRENCY", &v)?;
        }
        if let Some(v) = get_env("TRACKER_RECONCILE_INTERVAL_S") {
            s.reconcile_interval_s = parse_env("TRACKER_RECONCILE_INTERVAL_S", &v)?;
        }
        if let Some(v) = get_env("TRACKER_RECONCILE_BATCH") {
            s.reconcile_batch = parse_int_env("TRACKER_RECONCILE_BATCH", &v)?;
        }
        if let Some(v) = get_env("TRACKER_NOTIFY_REDUCE_CLOSE") {
            s.notify_reduce_close = parse_bool("TRACKER_NOTIFY_REDUCE_CLOSE", &v)?;
        }
        if let Some(v) = get_env("TRACKER_CLOSED_PNL_LOOKUP") {
            s.closed_pnl_lookup = parse_bool("TRACKER_CLOSED_PNL_LOOKUP", &v)?;
        }
        if let Some(v) = get_env("TRACKER_CLOSED_PNL_ATTEMPTS") {
            s.closed_pnl_attempts = parse_int_env("TRACKER_CLOSED_PNL_ATTEMPTS", &v)?;
        }
        if let Some(v) = get_env("TRACKER_CLOSED_PNL_RETRY_DELAY_S") {
            s.closed_pnl_retry_delay_s = parse_env("TRACKER_CLOSED_PNL_RETRY_DELAY_S", &v)?;
        }
        if let Some(v) = get_env("TRACKER_DB_PATH") {
            s.db_path = PathBuf::from(v);
        }
        // AliasChoices order: the un-prefixed conventional name wins over the prefixed one.
        s.telegram_bot_token =
            get_env("TELEGRAM_BOT_TOKEN").or_else(|| get_env("TRACKER_TELEGRAM_BOT_TOKEN"));
        if let Some(v) = get_env("TRACKER_ALLOWED_CHAT_IDS") {
            s.allowed_chat_ids = v;
        }
        s.validate()?;
        Ok(s)
    }

    /// The pydantic `Field(ge=..., le=...)` constraints, checked in field order.
    // PORT NOTE: validate() is the __post_init__-style split — pydantic ran these checks
    // inside `Settings()`; from_env() calls this before returning. ge=0 on max_retries is
    // absorbed by the unsigned type and has no explicit check.
    pub fn validate(&self) -> Result<()> {
        // PORT NOTE: deliberate hardening divergence — pydantic accepted "inf" for these
        // (inf satisfies ge=) and Python then just slept forever; Rust's
        // Duration::from_secs_f64 and rand's random_range PANIC on non-finite values, so
        // reject them up front with a clear error instead.
        for (field, value) in [
            ("http_timeout_s", self.http_timeout_s),
            ("request_delay_s", self.request_delay_s),
            ("backoff_base_s", self.backoff_base_s),
            ("backoff_cap_s", self.backoff_cap_s),
            ("ws_heartbeat_s", self.ws_heartbeat_s),
            ("reconcile_interval_s", self.reconcile_interval_s),
            ("closed_pnl_retry_delay_s", self.closed_pnl_retry_delay_s),
        ] {
            check(value.is_finite(), field, "finite", value)?;
        }
        check(
            self.http_timeout_s >= 1.0,
            "http_timeout_s",
            "ge=1.0",
            self.http_timeout_s,
        )?;
        check(
            self.request_delay_s >= 0.0,
            "request_delay_s",
            "ge=0.0",
            self.request_delay_s,
        )?;
        check(
            self.backoff_base_s >= 0.0,
            "backoff_base_s",
            "ge=0.0",
            self.backoff_base_s,
        )?;
        check(
            self.backoff_cap_s >= 0.0,
            "backoff_cap_s",
            "ge=0.0",
            self.backoff_cap_s,
        )?;
        check(
            self.ws_heartbeat_s >= 1.0 && self.ws_heartbeat_s <= 55.0,
            "ws_heartbeat_s",
            "ge=1.0, le=55.0",
            self.ws_heartbeat_s,
        )?;
        check(
            self.tid_dedup_maxlen >= 100,
            "tid_dedup_maxlen",
            "ge=100",
            self.tid_dedup_maxlen,
        )?;
        check(
            self.seed_concurrency >= 1 && self.seed_concurrency <= 50,
            "seed_concurrency",
            "ge=1, le=50",
            self.seed_concurrency,
        )?;
        check(
            self.reconcile_interval_s >= 0.0,
            "reconcile_interval_s",
            "ge=0.0",
            self.reconcile_interval_s,
        )?;
        check(
            self.reconcile_batch >= 1,
            "reconcile_batch",
            "ge=1",
            self.reconcile_batch,
        )?;
        check(
            self.closed_pnl_attempts >= 1 && self.closed_pnl_attempts <= 10,
            "closed_pnl_attempts",
            "ge=1, le=10",
            self.closed_pnl_attempts,
        )?;
        check(
            self.closed_pnl_retry_delay_s >= 0.0 && self.closed_pnl_retry_delay_s <= 10.0,
            "closed_pnl_retry_delay_s",
            "ge=0.0, le=10.0",
            self.closed_pnl_retry_delay_s,
        )?;
        Ok(())
    }

    /// Parsed allowlist of chat ids; empty = open to anyone (public bot).
    // PORT NOTE: @property returning frozenset[int] -> method returning HashSet<i64> by value;
    // the frozen-ness is expressed by handing the caller its own set. Telegram chat ids are
    // i64. Python's int() tolerates surrounding whitespace ("  123 " parses), so we trim
    // before parsing; the ValueError it raised on garbage becomes Err(InvalidChatId).
    pub fn allowed_chat_ids_set(&self) -> Result<HashSet<i64>> {
        self.allowed_chat_ids
            .split(',')
            .filter(|c| !c.trim().is_empty())
            .map(|c| {
                c.trim()
                    .parse::<i64>()
                    .map_err(|_| Error::InvalidChatId(c.to_string()))
            })
            .collect()
    }

    /// Parsed `live_coins` verbatim (empty list = subscribe to all perps).
    ///
    /// NOT upper-cased: Hyperliquid perp names are exact identifiers and some are lowercase-
    /// prefixed (kPEPE, kSHIB, kBONK, …), so upper-casing would silently subscribe to a feed
    /// that doesn't exist.
    // PORT NOTE: @property -> inherent method; comprehension made eager via .collect().
    pub fn live_coins_list(&self) -> Vec<String> {
        self.live_coins
            .split(',')
            .map(str::trim)
            .filter(|c| !c.is_empty())
            .map(String::from)
            .collect()
    }
}

/// Read one environment variable, treating "unset" and "not valid UTF-8" alike as absent.
// PORT NOTE: os.environ values are always str in Python (surrogate-escaped); std::env::var
// errors on non-UTF-8. All of these values are parsed to strings/numbers anyway, so the
// guide's Vec<u8> rule for env vars is intentionally not applied here.
fn get_env(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

/// Parse a numeric env value, mapping failure to `Error::Parse` (pydantic's coercion error).
// PORT NOTE: pydantic's lax str->number coercion trims whitespace; FromStr does not, so trim
// first. Generic over FromStr to keep from_env() line-per-field like the Python class body.
fn parse_env<T: std::str::FromStr>(var: &str, raw: &str) -> Result<T>
where
    T::Err: std::fmt::Display,
{
    raw.trim().parse().map_err(|e: T::Err| Error::Parse {
        var: var.to_string(),
        value: raw.to_string(),
        reason: e.to_string(),
    })
}

/// Parse an integer env value the way pydantic v2 lax mode does: plain integer strings,
/// plus float-form strings with a zero fraction (`"4.0"` -> 4; `"4.5"` errors).
// PORT NOTE: FromStr alone rejects "4.0", which pydantic accepted for int fields — fall
// back through f64 and re-parse the integral rendering.
fn parse_int_env<T: std::str::FromStr>(var: &str, raw: &str) -> Result<T> {
    let trimmed = raw.trim();
    if let Ok(v) = trimmed.parse::<T>() {
        return Ok(v);
    }
    if let Ok(f) = trimmed.parse::<f64>()
        && f.is_finite()
        && f.fract() == 0.0
        && let Ok(v) = format!("{f:.0}").parse::<T>()
    {
        return Ok(v);
    }
    Err(Error::Parse {
        var: var.to_string(),
        value: raw.to_string(),
        reason: "not a valid integer".to_string(),
    })
}

/// Parse a boolean env value the way pydantic v2 lax mode does.
// PORT NOTE: pydantic v2 accepts (case-insensitive) true/t/yes/y/on/1 and false/f/no/n/off/0;
// Rust's bool::FromStr only accepts "true"/"false", so this helper reproduces the wider set.
// TODO(port): confirm this literal set matches the deployed pydantic version's bool coercion.
fn parse_bool(var: &str, raw: &str) -> Result<bool> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "yes" | "y" | "on" | "1" => Ok(true),
        "false" | "f" | "no" | "n" | "off" | "0" => Ok(false),
        _ => Err(Error::Parse {
            var: var.to_string(),
            value: raw.to_string(),
            reason: "not a valid boolean".to_string(),
        }),
    }
}

/// One `Field(ge=..., le=...)` check -> `Error::Range` on violation.
fn check<V: std::fmt::Display>(
    ok: bool,
    field: &'static str,
    constraint: &'static str,
    value: V,
) -> Result<()> {
    if ok {
        Ok(())
    } else {
        Err(Error::Range {
            field,
            constraint,
            value: value.to_string(),
        })
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/config.py (109 lines)
//   confidence: high
//   todos:      3
//   notes:      Callers use Settings::from_env() where Python did Settings(); it validates
//               and returns Result (pydantic ValidationError -> config::Error). load_env()
//               must run first (from main, pre-runtime) — pydantic's own CWD-relative
//               env_file=".env" read is NOT replicated. .env and the db_path default are
//               cwd-relative (Python used the repo root via __file__).
//               Crates: dotenvy, thiserror.
// ──────────────────────────────────────────────────────────────────────────
