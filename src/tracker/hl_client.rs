//! Client for Hyperliquid's official public `/info` API.
//!
//! Hyperliquid's API is sanctioned for programmatic use — no auth, no bot block — so this is a
//! plain `httpx` client. It is the source for the per-account `clearinghouseState` snapshot
//! used to seed the in-memory book (cold-start correctness) and to refresh leverage. Ported
//! from the sibling `hyperdash-crawl` project.
//!
//! PORT NOTE: the "plain `httpx` client" is now a plain `reqwest` client (fixed dependency
//! set); the retry/backoff contract is unchanged.

use std::time::Duration;

use serde_json::Value;

use crate::config::Settings;
use crate::exceptions::{Result, TrackerError};
// PORT NOTE: `from tracker.exceptions import AuthRequiredError, ParseError, RateLimitedError`
// — the three subclasses flattened into TrackerError variants (fixed decision), so only the
// enum is imported.
use crate::retry::{RETRYABLE_STATUS, backoff_or_raise};

// PORT NOTE: `logger = logging.getLogger(__name__)` disappears — `tracing` macros are
// free-standing and carry the module path automatically (this module never logged directly
// anyway; all logging happens inside retry::backoff_or_raise).

/// The one capability the enrichment layer needs from Hyperliquid.
// PORT NOTE: @runtime_checkable Protocol → trait (structural typing → nominal trait, same
// shape); holders store `Arc<dyn InfoClient>` per the fixed port decisions, hence
// `Send + Sync` bounds and `async_trait` for dyn-compatibility. `@runtime_checkable` existed
// only to allow isinstance checks, which have no Rust analogue — the trait impl below is the
// compile-time proof.
// PORT NOTE: `body: dict[str, Any]` → `serde_json::Value` by value and the `Any` return →
// `Result<Value>` (fixed decision: untyped API payloads stay Value); Python's implicit
// "may raise TrackerError" becomes the explicit Result.
#[async_trait::async_trait]
pub trait InfoClient: Send + Sync {
    async fn info(&self, body: Value) -> Result<Value>;
}

/// POSTs to `/info` with transient-only retry/backoff.
// PORT NOTE: `_client: httpx.AsyncClient | None` (None outside the context manager) becomes a
// plain `reqwest::Client` — the Option and the "used outside its context manager"
// RuntimeError are unrepresentable now that new() builds the client eagerly (fixed decision).
// Leading-underscore privacy → non-pub fields, same order as __init__. Clone is derived:
// reqwest::Client is an Arc'd handle, so clones share the connection pool.
#[derive(Debug, Clone)]
pub struct HyperliquidClient {
    settings: Settings,
    client: reqwest::Client,
}

impl HyperliquidClient {
    // PORT NOTE: `__init__` + `__aenter__` collapse into new() (fixed port decision): reqwest
    // needs no async setup, and `__aexit__`'s aclose() becomes Drop — the pool closes when the
    // last clone of the client is dropped. Call sites replace
    // `async with HyperliquidClient(settings) as client:` with
    // `let client = HyperliquidClient::new(settings);` (Drop runs at scope end, like __aexit__).
    pub fn new(settings: Settings) -> Self {
        // PORT NOTE: httpx.AsyncClient(timeout=...) construction never fails; a reqwest
        // builder failure means system TLS misconfiguration — a programmer/environment error,
        // so it panics via expect (fixed decision: RuntimeError-class failures panic).
        // TODO(port): httpx's `timeout=x` is PER-OPERATION (connect/read/write/pool each get
        // x seconds of quiet time); reqwest's .timeout() is one whole-request deadline. A
        // slow-but-steady large response that httpx tolerated can now time out. Phase B:
        // consider .connect_timeout() + .read_timeout() to mirror httpx more closely.
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs_f64(settings.http_timeout_s))
            // PORT NOTE: httpx.AsyncClient does NOT follow redirects by default; reqwest
            // follows up to 10 (rewriting POST→GET on 301/302/303), which would silently
            // GET /info and mis-parse the result. With Policy::none a 3xx surfaces as its
            // real status and falls into the ParseError arm, exactly like the Python.
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("reqwest client construction failed (TLS backend misconfigured)");
        Self { settings, client }
    }
}

#[async_trait::async_trait]
impl InfoClient for HyperliquidClient {
    /// Run one `/info` request, returning the parsed JSON.
    // PORT NOTE: in Python `info` was an inherent method that satisfied the Protocol
    // structurally; here the body lives directly in the explicit trait impl (nominal
    // conformance) — same single method, no structural divergence.
    async fn info(&self, body: Value) -> Result<Value> {
        // PORT NOTE: the `if self._client is None: raise RuntimeError("HyperliquidClient used
        // outside its context manager")` guard disappears — new() always builds the client,
        // so the error state is unrepresentable (fixed port decision).

        // PORT NOTE: f"hl:{body.get('type', '?')}" — a JSON string must render bare (Python
        // str interpolation adds no quotes), so it is special-cased; any non-string value
        // falls back to its compact JSON text (Value's Display, ≈ Python's str of the
        // object); absent key → "?".
        let label = match body.get("type") {
            Some(Value::String(s)) => format!("hl:{s}"),
            Some(other) => format!("hl:{other}"),
            None => "hl:?".to_string(),
        };
        // PORT NOTE: attempt: int → u32 to match backoff_or_raise (retry.rs narrowed it).
        let mut attempt: u32 = 0;
        loop {
            let resp = match self
                .client
                .post(&self.settings.hyperliquid_url)
                .json(&body)
                .send()
                .await
            {
                Ok(resp) => resp,
                // PORT NOTE: `except httpx.TransportError as err` → any reqwest::Error from
                // send() (connect, timeout, protocol). reqwest's error is slightly broader
                // (e.g. redirect-policy failures), but every case takes the same
                // transient-backoff path, matching the Python contract. f"{label}: {err}"
                // uses Display, like str(err).
                Err(err) => {
                    attempt = backoff_or_raise(
                        attempt,
                        &self.settings,
                        &label,
                        TrackerError::RateLimited {
                            message: format!("{label}: {err}"),
                            retry_after: None,
                        },
                    )
                    .await?;
                    continue;
                }
            };

            // PORT NOTE: resp.status_code (int) → resp.status().as_u16(), the type
            // RETRYABLE_STATUS was narrowed to.
            let status = resp.status().as_u16();
            // PORT NOTE: reshaped — httpx buffers the entire body inside `post()`, so Python's
            // `resp.json()` / `resp.text` were infallible reads of that buffer, and a
            // transport error while reading the body surfaced from `post()` into the except
            // arm above. reqwest's send() resolves at the response headers, so the body is
            // read here, once, up front (also because reqwest's `resp.json()` would consume
            // the response, losing the text the error messages need). An Err from text() is
            // that same mid-body transport failure and takes the same backoff path.
            let text = match resp.text().await {
                Ok(text) => text,
                Err(err) => {
                    attempt = backoff_or_raise(
                        attempt,
                        &self.settings,
                        &label,
                        TrackerError::RateLimited {
                            message: format!("{label}: {err}"),
                            retry_after: None,
                        },
                    )
                    .await?;
                    continue;
                }
            };

            if status == 200 {
                // PORT NOTE: `if self._settings.request_delay_s:` is float truthiness —
                // 0.0 (the default) skips the politeness sleep; the explicit `!= 0.0`
                // mirrors that exactly (validation already enforces ge=0.0).
                if self.settings.request_delay_s != 0.0 {
                    tokio::time::sleep(Duration::from_secs_f64(self.settings.request_delay_s))
                        .await;
                }
                return match serde_json::from_str::<Value>(&text) {
                    Ok(parsed) => Ok(parsed),
                    // A 200 with a non-JSON body (CDN interstitial, proxy error page) —
                    // convert so it stays inside our exception hierarchy.
                    // PORT NOTE: `raise ParseError(...) from err` — TrackerError::Parse
                    // carries only the message (fixed decision, no source field), so the
                    // serde cause is dropped; the message users saw is identical.
                    // resp.text[:200] slices CODE POINTS, hence chars().take(200), not a
                    // byte slice (which could panic mid-code-point).
                    Err(_err) => Err(TrackerError::Parse(format!(
                        "{label}: 200 body was not JSON ({})",
                        text.chars().take(200).collect::<String>()
                    ))),
                };
            }
            if status == 401 || status == 403 {
                // PORT NOTE: `in (401, 403)` tuple membership → boolean or.
                return Err(TrackerError::AuthRequired(format!(
                    "{label}: HTTP {status}"
                )));
            }
            if RETRYABLE_STATUS.contains(&status) {
                attempt = backoff_or_raise(
                    attempt,
                    &self.settings,
                    &label,
                    TrackerError::RateLimited {
                        message: format!("{label}: HTTP {status}"),
                        retry_after: None,
                    },
                )
                .await?;
                continue;
            }
            return Err(TrackerError::Parse(format!(
                "{label}: HTTP {status} ({})",
                text.chars().take(200).collect::<String>()
            )));
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/hl_client.py (87 lines)
//   confidence: high
//   todos:      1
//   notes:      __aenter__/__aexit__ collapsed into new()/Drop per fixed decisions; the
//               context-manager RuntimeError is unrepresentable. Body is read once via
//               text() before branching (reqwest json() consumes the response) and
//               body-read errors join the transport-error backoff path — mirrors httpx's
//               full buffering inside post(). One TODO: httpx per-operation timeout vs
//               reqwest whole-request timeout. Crates: reqwest, serde_json, async-trait,
//               tokio (+ thiserror via exceptions.rs).
// ──────────────────────────────────────────────────────────────────────────
