//! Cold-start seeding + periodic reconcile via Hyperliquid `clearinghouseState`.
//!
//! This is what makes "check whether the wallet ALREADY had a position" work with zero storage:
//! before a wallet is admitted to the live filter, we fetch its current on-chain positions and
//! install them silently, so the next same-direction fill is correctly an ADD, not a false OPEN.
//! `clearinghouseState` is weight 2, so seeding even 1k wallets stays well within the IP budget.
//
// PORT NOTE: asyncio module → tokio (fixed dependency set — the runtime decision is locked, so
// no `TODO(port): runtime` marker despite the guide's default).
// PORT NOTE: `logger = logging.getLogger(__name__)` disappears — tracing macros are
// free-standing and carry the module path automatically (same as hl_client.rs).

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::Utc;
use serde_json::{Value, json};
use tokio::sync::Semaphore;

use crate::book::InMemoryBook;
use crate::config::Settings;
// PORT NOTE: import added relative to the Python (enrich.py named no exception types — its
// bare `except Exception` matched them anonymously); the SeedError reshape below needs the name.
use crate::exceptions::TrackerError;
use crate::hl_client::InfoClient;
use crate::models::{self, AccountPosition, build_position};
use crate::resolve::seed_state_from_row;
use crate::state::PositionState;

/// The closed union of everything a `clearinghouseState` snapshot can fail with.
// PORT NOTE: reshape per the fixed decisions — Python's broad `except Exception` in seed_wallet
// caught (a) a TrackerError out of `client.info` and (b) the pydantic-ValidationError analogue
// (models::Error) out of `build_position`. snapshot_positions() returns this enum so
// seed_wallet's match covers exactly those causes; anything else Python could have
// caught there (a genuine bug) is a panic in Rust and deliberately NOT swallowed.
// pub because `account_positions` (the bot's /positions data source) surfaces it too.
#[derive(thiserror::Error, Debug)]
pub enum SeedError {
    /// Transport/HTTP failure from `InfoClient::info`.
    #[error(transparent)]
    Client(#[from] TrackerError),
    /// A malformed snapshot that won't parse (`build_position`'s decimal coercion).
    #[error(transparent)]
    Model(#[from] models::Error),
}

/// Fetches `clearinghouseState` and folds it into the in-memory book (silently).
// PORT NOTE: field order matches __init__ (settings, book, client); leading-underscore privacy
// → non-pub fields. Shared-state model (fixed decision): the book is held as
// Arc<std::sync::Mutex<InMemoryBook>> because in Rust the enricher runs concurrently with the
// listener's ingest (Python shared the bare object on one event loop); the client is the
// Arc<dyn InfoClient> trait object (fixed decision: holders store the trait object).
// PORT NOTE: no Debug derive — `dyn InfoClient` carries no Debug bound (the Python class had
// no __repr__ either).
pub struct Enricher {
    settings: Settings,
    book: Arc<Mutex<InMemoryBook>>,
    client: Arc<dyn InfoClient>,
}

impl Enricher {
    pub fn new(
        settings: Settings,
        book: Arc<Mutex<InMemoryBook>>,
        client: Arc<dyn InfoClient>,
    ) -> Self {
        Self {
            settings,
            book,
            client,
        }
    }

    /// Snapshot `address`'s positions and reseed the book. Returns `false` on failure.
    ///
    /// A failure leaves the wallet UNSEEDED so the caller can quarantine it (not admit it to
    /// the filter) rather than risk mis-labelling a later add as a new open. Any failure — a
    /// transport/HTTP error OR a malformed snapshot that won't parse — returns `false` rather
    /// than propagating, so one bad wallet can never abort a whole seed sweep.
    pub async fn seed_wallet(&self, address: &str) -> bool {
        // Capture the fill epoch BEFORE the await so a live fill that lands during the snapshot
        // window makes the reconcile reseed skip (rather than clobber the fresher live state).
        // PORT NOTE: GIL-free — lock scope: the guard lives only for this statement and is
        // dropped before the await below. A poisoned mutex means another task panicked
        // mid-mutation — a programmer error, so it panics via expect (fixed decision:
        // RuntimeError-class failures panic).
        let epoch = self
            .book
            .lock()
            .expect("book mutex poisoned")
            .fill_epoch(address);
        // PORT NOTE: reshaped per the fixed decisions — the whole snapshot+parse `try:` body is
        // the private snapshot_positions() below; `except Exception: log + return False`
        // becomes this match on its Result.
        match self.snapshot_positions(address).await {
            Ok((states, leverage)) => {
                // PORT NOTE: GIL-free — lock scope (re-acquired after the await, never across).
                // Keyword `expected_epoch=epoch` → Some(epoch): this path always has a captured
                // epoch — None is the seed-before-admit case where no snapshot race exists.
                // The applied/skipped bool is discarded exactly as in Python: a skipped reseed
                // means fresher live state won, which still counts as a successful seed.
                self.book
                    .lock()
                    .expect("book mutex poisoned")
                    .reseed_wallet(address, states, leverage, Some(epoch));
                true
            }
            Err(err) => {
                // PORT NOTE: logger.exception(...) → tracing::error! carrying the error's Debug
                // form (fixed decision — the Python traceback has no Rust analogue).
                tracing::error!("seed: clearinghouseState failed for {address}: {err:?}");
                false
            }
        }
    }

    /// Fetch and parse `address`'s live open positions (non-zero size, malformed rows skipped).
    ///
    /// The fetch+parse half of the seed path, shared with the bot's `/positions` view — which
    /// is why it returns the full [`AccountPosition`] rows (entry px, value, unrealized PnL,
    /// liquidation px, leverage) rather than just seed states.
    pub async fn account_positions(
        &self,
        address: &str,
    ) -> Result<Vec<AccountPosition>, SeedError> {
        let resp = self
            .client
            .info(json!({"type": "clearinghouseState", "user": address}))
            .await?;
        // `(resp or {}).get("assetPositions") or []` — Python's `or {}` only rescued FALSY
        // responses (null/false/0/""/[]/{} → a deliberate empty seed); a TRUTHY non-dict hit
        // AttributeError, which seed_wallet's `except Exception` turned into a FAILED seed so
        // the wallet stayed quarantined. Mirroring both matters: treating a truthy garbage
        // body as "no positions" would wipe the wallet's live book state on reconcile.
        // PORT NOTE: a *truthy non-list* assetPositions survived Python's `or []` and got
        // iterated anyway (str → chars, dict → keys), each junk item then skipped inside
        // build_position; here it short-circuits to no entries — same net result, minus the
        // accidental iteration.
        let entries: &[Value] = match &resp {
            Value::Object(obj) => obj
                .get("assetPositions")
                .and_then(Value::as_array)
                .map_or(&[], Vec::as_slice),
            other if value_is_falsy(other) => &[],
            other => {
                return Err(TrackerError::Parse(format!(
                    "clearinghouseState: expected an object, got {}",
                    json_type_name(other)
                ))
                .into());
            }
        };
        let mut positions: Vec<AccountPosition> = Vec::new();
        for entry in entries {
            // PORT NOTE: build_position's two failure exits (fixed decision): Ok(None) = the
            // Python `pos is None` arm, Err = a numeric field failed decimal coercion (the
            // pydantic ValidationError that aborted the whole seed) — propagated by `?`.
            let Some(pos) = build_position(address, entry)? else {
                continue; // skip malformed
            };
            // `not pos.szi` — Money truthiness: None and zero are both falsy → skip.
            if pos.szi.filter(|s| !s.is_zero()).is_none() {
                continue; // skip zero-size
            }
            positions.push(pos);
        }
        Ok(positions)
    }

    /// Fetch `clearinghouseState` for `address` and parse it into seed states + leverage.
    // PORT NOTE: structural addition (fixed decision for enrich.py) — this is seed_wallet's
    // Python `try:` block verbatim; extracting it lets `?` replace exception propagation while
    // seed_wallet keeps the Python's log-and-return-false shape. The fetch+parse half has
    // since been factored into `account_positions` above (shared with the bot).
    async fn snapshot_positions(
        &self,
        address: &str,
    ) -> Result<(Vec<PositionState>, HashMap<String, i64>), SeedError> {
        let positions = self.account_positions(address).await?;
        let now = Utc::now();
        let mut states: Vec<PositionState> = Vec::new();
        let mut leverage: HashMap<String, i64> = HashMap::new();
        for pos in positions {
            // account_positions only returns non-zero sizes; stay defensive anyway. The
            // unwrapped non-zero Decimal is exactly what seed_state_from_row's `szi` takes.
            let Some(szi) = pos.szi.filter(|s| !s.is_zero()) else {
                continue;
            };
            // PORT NOTE: keyword-only `fallback_ts=now` flattened to positional (resolve.rs);
            // `entry_px: Decimal | None` is models.rs's Money, passed through unchanged.
            states.push(seed_state_from_row(
                address,
                &pos.coin,
                szi,
                pos.entry_px,
                now,
            ));
            if let Some(value) = pos.leverage_value {
                // PORT NOTE: pos.coin moves into the map here — its last use (Python shared
                // the one str between the seeded state and the leverage key).
                leverage.insert(pos.coin, value);
            }
        }
        Ok((states, leverage))
    }

    /// Seed every address with bounded concurrency. Returns `(seeded, failed)`.
    // PORT NOTE: `addresses: list[str]` → owned Vec<String>: every address is moved through
    // its per-address future into one of the returned partitions.
    pub async fn seed_many(&self, addresses: Vec<String>) -> (Vec<String>, Vec<String>) {
        // PORT NOTE: asyncio.Semaphore → tokio::sync::Semaphore (fixed decision). No Arc
        // needed: the per-address futures only borrow it, and join_all resolves before drop.
        let sem = Semaphore::new(self.settings.seed_concurrency);

        // PORT NOTE: nested `async def _one(addr)` → per-address async blocks;
        // `asyncio.gather(*(_one(a) for a in addresses))` → futures_util::future::join_all
        // (order-preserving, fixed decision). `async with sem:` → acquire().await with the
        // permit guard dropping at block end — the slot is released exactly where __aexit__
        // ran. acquire() only errs if the semaphore is closed, which never happens here →
        // can't-happen expect.
        let results: Vec<(String, bool)> =
            futures_util::future::join_all(addresses.into_iter().map(|addr| {
                let sem = &sem;
                async move {
                    let _permit = sem.acquire().await.expect("semaphore is never closed");
                    let ok = self.seed_wallet(&addr).await;
                    (addr, ok)
                }
            }))
            .await;
        // PERF(port): the two list comprehensions stay two passes with clones (both partitions
        // borrow the same results); a single into_iter().partition() would avoid the clones —
        // profile in Phase B (n = watchlist size, negligible).
        let seeded: Vec<String> = results
            .iter()
            .filter(|(_, ok)| *ok)
            .map(|(a, _)| a.clone())
            .collect();
        let failed: Vec<String> = results
            .iter()
            .filter(|(_, ok)| !*ok)
            .map(|(a, _)| a.clone())
            .collect();
        if !failed.is_empty() {
            tracing::warn!(
                "seed sweep: {} seeded, {} failed",
                seeded.len(),
                failed.len()
            );
        }
        (seeded, failed)
    }
}

// PORT NOTE: structural additions for the `(resp or {})` translation in snapshot_positions.

/// Python truthiness over a JSON value: null, false, 0, "", [], and {} are falsy.
fn value_is_falsy(v: &Value) -> bool {
    match v {
        Value::Null => true,
        Value::Bool(b) => !b,
        Value::Number(n) => n.as_f64() == Some(0.0),
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(o) => o.is_empty(),
    }
}

/// The JSON type name, for the seed-failure message (a whole body could be huge).
fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests — ported from tests/test_enrich.py
// ──────────────────────────────────────────────────────────────────────────

/// Cold-start enrichment: clearinghouseState → silent book seed → correct open-vs-add.
// PORT NOTE: the Python tests are async (pytest asyncio_mode=auto) → #[tokio::test].
#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::str::FromStr;

    use rust_decimal::Decimal;

    // PORT NOTE: `from tracker.state import EVENT_ADD` — the str constant became the EventKind
    // enum in the Rust draft (fixed shared decision).
    use crate::state::EventKind;

    // PORT NOTE: `D = Decimal` alias → tiny parse helper (matching the other modules' tests);
    // the Python int-form literals D(5) become d("5").
    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).expect("test literal is a valid decimal")
    }

    /// Stands in for HyperliquidClient — returns canned clearinghouseState per user.
    // PORT NOTE: `_FakeInfoClient` → underscore dropped (privacy is the missing `pub`); the
    // `fail: frozenset[str] = frozenset()` default arg is spelled out at each struct-literal
    // call site (no default args in Rust). frozenset → HashSet, never mutated.
    struct FakeInfoClient {
        responses: HashMap<String, Value>,
        fail: HashSet<String>,
    }

    // PORT NOTE: the Python fake satisfied the InfoClient Protocol structurally; here it is an
    // explicit trait impl (fixed decision: test fakes are small local structs impl'ing the
    // trait). Same async_trait plumbing as the real client.
    #[async_trait::async_trait]
    impl InfoClient for FakeInfoClient {
        async fn info(&self, body: Value) -> crate::exceptions::Result<Value> {
            // `user = body.get("user")` — every test body carries a JSON string; a missing or
            // non-string user stays None and (as in Python) matches neither the fail set nor
            // a canned response.
            let user = body.get("user").and_then(Value::as_str);
            if user.is_some_and(|u| self.fail.contains(u)) {
                // PORT NOTE: `raise RateLimitedError("simulated transient failure")` →
                // TrackerError::RateLimited (flattened hierarchy); retry_after: None is the
                // Python constructor's default arg.
                return Err(TrackerError::RateLimited {
                    message: "simulated transient failure".to_string(),
                    retry_after: None,
                });
            }
            // `self._responses.get(user, {"assetPositions": []})` — dict-get default made
            // explicit; the canned Value is cloned out (Python handed back the shared object).
            Ok(user
                .and_then(|u| self.responses.get(u))
                .cloned()
                .unwrap_or_else(|| json!({"assetPositions": []})))
        }
    }

    // PORT NOTE: `_chs` → `chs` (underscore dropped); `leverage: int` → i64 (models.rs narrows
    // the JSON leverage value via Value::as_i64).
    fn chs(coin: &str, szi: &str, entry: &str, leverage: i64) -> Value {
        json!({
            "assetPositions": [
                {
                    "position": {
                        "coin": coin,
                        "szi": szi,
                        "entryPx": entry,
                        "leverage": {"type": "cross", "value": leverage},
                    },
                    "type": "oneWay",
                }
            ]
        })
    }

    // PORT NOTE: `Settings()` (pydantic: defaults + env + .env overrides) → Settings::default()
    // — the tests mean "the defaults", and from_env() would make them environment-dependent.
    // The book arrives as Arc<Mutex<..>> (shared-state fixed decision) where Python passed the
    // bare object; the fake client is boxed into the Arc<dyn InfoClient> the Enricher holds.
    fn enricher(client: FakeInfoClient, book: Arc<Mutex<InMemoryBook>>) -> Enricher {
        Enricher::new(Settings::default(), book, Arc::new(client))
    }

    #[tokio::test]
    async fn test_seed_wallet_installs_position_and_leverage() {
        let book = Arc::new(Mutex::new(InMemoryBook::new()));
        let client = FakeInfoClient {
            responses: HashMap::from([("0xa".to_string(), chs("BTC", "5", "90", 10))]),
            fail: HashSet::new(),
        };
        // `assert await ... is True` → the returned bool itself.
        assert!(enricher(client, Arc::clone(&book)).seed_wallet("0xa").await);
        // PORT NOTE: reads go through the lock guard (Python read the shared object directly).
        let book = book.lock().expect("book mutex poisoned");
        let pos = book.position("0xa", "BTC");
        // PORT NOTE: `assert pos is not None and pos.szi == D(5)` — Option<&_> is Copy, so
        // is_some() + unwrap() keeps the Python's single combined assertion shape (book.rs).
        assert!(pos.is_some() && pos.unwrap().szi == d("5"));
        // `== 10` on the Python int|None → Some(10) on the Option<i64>.
        assert_eq!(book.leverage("0xa", "BTC"), Some(10));
    }

    #[tokio::test]
    async fn test_seeded_then_same_direction_fill_is_add_not_open() {
        let book = Arc::new(Mutex::new(InMemoryBook::new()));
        let client = FakeInfoClient {
            responses: HashMap::from([("0xa".to_string(), chs("BTC", "5", "90", 10))]),
            fail: HashSet::new(),
        };
        enricher(client, Arc::clone(&book)).seed_wallet("0xa").await;
        // PORT NOTE: the Python's inline `from datetime import UTC, datetime` is just
        // Utc::now() here; ingest's keyword args flattened to positional (book.rs).
        let events = book
            .lock()
            .expect("book mutex poisoned")
            .ingest("0xa", "BTC", d("1"), d("100"), Utc::now())
            .events;
        assert_eq!(
            events.iter().map(|e| e.kind).collect::<Vec<_>>(),
            vec![EventKind::Add]
        );
    }

    #[tokio::test]
    async fn test_account_positions_returns_parsed_rows() {
        let book = Arc::new(Mutex::new(InMemoryBook::new()));
        let client = FakeInfoClient {
            responses: HashMap::from([("0xa".to_string(), chs("BTC", "5", "90", 10))]),
            fail: HashSet::new(),
        };
        let positions = enricher(client, book)
            .account_positions("0xa")
            .await
            .expect("canned snapshot parses");
        assert_eq!(positions.len(), 1);
        assert_eq!(positions[0].coin, "BTC");
        assert_eq!(positions[0].szi, Some(d("5")));
        assert_eq!(positions[0].entry_px, Some(d("90")));
        assert_eq!(positions[0].leverage_value, Some(10));
    }

    #[tokio::test]
    async fn test_seed_wallet_returns_false_on_client_error() {
        let book = Arc::new(Mutex::new(InMemoryBook::new()));
        let client = FakeInfoClient {
            responses: HashMap::new(),
            fail: HashSet::from(["0xa".to_string()]),
        };
        // `assert await ... is False` → negated bool.
        assert!(!enricher(client, Arc::clone(&book)).seed_wallet("0xa").await);
        assert!(
            book.lock()
                .expect("book mutex poisoned")
                .position("0xa", "BTC")
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_seed_many_partitions_seeded_and_failed() {
        let book = Arc::new(Mutex::new(InMemoryBook::new()));
        let client = FakeInfoClient {
            responses: HashMap::from([("0xa".to_string(), chs("BTC", "1", "90", 5))]),
            fail: HashSet::from(["0xb".to_string()]),
        };
        let (seeded, failed) = enricher(client, Arc::clone(&book))
            .seed_many(vec!["0xa".to_string(), "0xb".to_string()])
            .await;
        assert_eq!(seeded, vec!["0xa"]);
        assert_eq!(failed, vec!["0xb"]);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/enrich.py (75 lines) + tests/test_enrich.py (82 lines)
//   confidence: high
//   todos:      0
//   notes:      async module (tokio, per fixed decisions). Enricher holds
//               Arc<std::sync::Mutex<InMemoryBook>> + Arc<dyn InfoClient> per the
//               shared-state fixed decisions — Phase B's app/bot wiring must construct it
//               that way; no lock is ever held across an await. seed_wallet's try/except
//               is reshaped into private snapshot_positions() returning
//               Result<_, SeedError> where SeedError = TrackerError | models::Error
//               (Python's bare `except Exception`; genuine bugs now panic instead of
//               returning false). seed_wallet discards reseed_wallet's applied/skipped
//               bool, as Python did. Tests use Settings::default() where Python's
//               Settings() would also read env. Crates: tokio, futures-util, serde_json,
//               chrono, thiserror, tracing, async-trait + rust_decimal (tests).
// ──────────────────────────────────────────────────────────────────────────
