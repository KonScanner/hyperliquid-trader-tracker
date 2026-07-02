//! Authoritative close PnL: one REST `userFillsByTime` sweep of the just-closed leg.
//!
//! The realized figure the state machine attaches to a close is an average-cost estimate over
//! the public `trades` feed — that feed carries no `closedPnl`, a seeded leg's basis is
//! whatever `entryPx` the snapshot reported, and a restart collapses multi-add legs. The
//! exchange, however, publishes its own per-fill `closedPnl`; summed over the leg's fills it
//! IS the leg's realized price PnL (opening fills contribute zero). So on a close/flip the
//! listener asks this module for that sum and swaps it into the event before dispatch.
//!
//! Best-effort by design: the REST fill index can lag the trades feed by a moment, so the
//! lookup retries briefly until the closing fill is visible, and ANY failure falls back to the
//! local estimate rather than dropping or stalling the notification. Fees and funding stay out
//! of the number (Hyperliquid reports them separately) — the message remains price PnL, now
//! exchange-accurate. `userFillsByTime` is weight 20 and closes are rare, so this never
//! threatens the 1200/min IP budget.
//
// PORT NOTE: asyncio module → tokio per the fixed port decisions (runtime: tokio, fixed —
// no `TODO(port): runtime` needed).
// PORT NOTE: `import dataclasses` (for dataclasses.replace) → struct-update syntax, no import.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};

use crate::config::Settings;
// PORT NOTE: `from tracker.exceptions import ParseError, TrackerError` — the subclasses are
// flattened into TrackerError variants (fixed decision): ParseError → TrackerError::Parse,
// "except TrackerError" → any variant of the enum.
use crate::exceptions::{Result, TrackerError};
use crate::hl_client::InfoClient;
use crate::models::CompletedTrade;
// PORT NOTE: `from tracker.state import EVENT_CLOSE, LiveEvent` — the str constant became
// EventKind::Close in state.rs (fixed shared decision: str constants → enum).
use crate::state::{EventKind, LiveEvent};

// PORT NOTE: `logger = logging.getLogger(__name__)` disappears — `tracing` macros are
// free-standing and carry the module path automatically.

// PORT NOTE: `_ZERO = Decimal(0)` — underscore dropped (in Rust it means "unused");
// privacy is expressed by omitting `pub`.
const ZERO: Decimal = Decimal::ZERO;
// Hyperliquid caps one userFillsByTime response at 2000 fills; a longer leg is time-walked.
// PORT NOTE: int → usize (compared against a page length).
const PAGE_LIMIT: usize = 2000;
// A leg spanning more pages than this costs more than one notification is worth — give up.
const MAX_PAGES: usize = 5;
// Ask past the close so an inclusive/exclusive endTime quibble can't hide the closing fill.
// PORT NOTE: int → i64 (added to epoch-millisecond timestamps).
const END_SKEW_MS: i64 = 2_000;

// PORT NOTE: `_to_ms` → `to_ms` (underscore dropped, module-private via no `pub`); the
// Python `int` return is epoch milliseconds → i64.
fn to_ms(ts: DateTime<Utc>) -> i64 {
    // PORT NOTE: Python needed round(), not int(): `timestamp()` is a float and truncation
    // could land 1ms early — which would make the closing-fill visibility check below miss
    // forever. chrono's `timestamp_millis()` is exact integer math, so the float hazard
    // (and the round() workaround) disappears.
    ts.timestamp_millis()
}

/// Fetches the exchange's realized PnL for a leg that just closed.
pub struct ClosedPnlResolver {
    // PORT NOTE: leading-underscore privacy (`_client`, `_attempts`, `_retry_delay_s`) →
    // non-pub fields, same order as __init__. `client: InfoClient` (Protocol) is held as
    // Arc<dyn InfoClient> per the fixed port decisions.
    client: Arc<dyn InfoClient>,
    attempts: u32,
    retry_delay_s: f64,
    // PORT NOTE: structural addition — the tests monkeypatch the module globals
    // `_PAGE_LIMIT` / `_MAX_PAGES`; Rust consts are unpatchable, so the resolver carries
    // them as private fields defaulted from the consts, overridden only by the
    // #[cfg(test)] builders below. Production behaviour is identical to the Python.
    page_limit: usize,
    max_pages: usize,
}

impl ClosedPnlResolver {
    // PORT NOTE: `settings` passed by reference — Python's __init__ copied out the two
    // knobs and dropped its reference; only Copy scalars are read here.
    pub fn new(settings: &Settings, client: Arc<dyn InfoClient>) -> Self {
        Self {
            client,
            attempts: settings.closed_pnl_attempts,
            retry_delay_s: settings.closed_pnl_retry_delay_s,
            page_limit: PAGE_LIMIT,
            max_pages: MAX_PAGES,
        }
    }

    // PORT NOTE: test-only stand-ins for `monkeypatch.setattr(pnl_mod, "_PAGE_LIMIT", …)`.
    #[cfg(test)]
    fn with_page_limit(mut self, page_limit: usize) -> Self {
        self.page_limit = page_limit;
        self
    }

    #[cfg(test)]
    fn with_max_pages(mut self, max_pages: usize) -> Self {
        self.max_pages = max_pages;
        self
    }

    /// Sum of `closedPnl` over the leg's fills, or `None` (caller keeps its estimate).
    ///
    /// The window is `(opened, closed]` — strictly after the opening fill, because on a
    /// flip that fill's `closedPnl` belongs to the PREVIOUS leg (for a from-flat open it
    /// is just zero). `closed_at` must be the close event's exchange timestamp: a fill
    /// with exactly that time proves the REST index has caught up with the trades feed;
    /// until one appears the lookup waits and retries, then gives up.
    // PORT NOTE: keyword-only param (`*, closed_at`) flattened to positional — Rust has no
    // keyword arguments. `Decimal | None` return → Option<Decimal>; every failure path is
    // swallowed into None exactly as in the Python (this function never errors outward).
    pub async fn close_pnl(
        &self,
        trade: &CompletedTrade,
        closed_at: DateTime<Utc>,
    ) -> Option<Decimal> {
        let opened_ms = to_ms(trade.start_time);
        let closed_ms = to_ms(closed_at);
        for attempt in 0..self.attempts {
            // PORT NOTE: `if attempt:` int truthiness → explicit `!= 0` (skip the sleep
            // before the first attempt only).
            if attempt != 0 {
                tokio::time::sleep(Duration::from_secs_f64(self.retry_delay_s)).await;
            }
            // PORT NOTE: `try: ... except TrackerError as err:` → match on the Result;
            // any TrackerError variant takes the warn-and-give-up arm.
            let fills = match self
                .window_fills(&trade.address, opened_ms, closed_ms)
                .await
            {
                Ok(fills) => fills,
                Err(err) => {
                    tracing::warn!(
                        "close-pnl: lookup failed for {} {}: {}",
                        trade.address,
                        trade.coin,
                        err
                    );
                    return None;
                }
            };
            let fills = match fills {
                Some(fills) => fills,
                // window too long to page through — don't trust a partial sum
                None => return None,
            };
            // PORT NOTE: `f.get("coin") == trade.coin` compares a JSON value against a str —
            // only an equal string matches, hence as_str(). `isinstance(f.get("time"), int)`
            // → Value::as_i64 (None for floats, like the isinstance check; divergence: JSON
            // `true` is `isinstance(True, int)` in Python but as_i64 → None — unobservable
            // on real payloads). Chained `opened_ms < t <= closed_ms` unrolled into &&.
            let leg: Vec<&Map<String, Value>> = fills
                .iter()
                .filter(|f| {
                    f.get("coin").and_then(Value::as_str) == Some(trade.coin.as_str())
                        && f.get("time")
                            .and_then(Value::as_i64)
                            .is_some_and(|t| opened_ms < t && t <= closed_ms)
                })
                .collect();
            if !leg
                .iter()
                .any(|f| f.get("time").and_then(Value::as_i64) == Some(closed_ms))
            {
                continue; // the closing fill isn't indexed yet — retry after a beat
            }
            // `sum((Decimal(str(f.get("closedPnl") or 0)) for f in leg), _ZERO)` under a
            // `try/except InvalidOperation` — try_fold short-circuits on the first
            // unparseable term (None = InvalidOperation).
            let total = leg.iter().try_fold(ZERO, |acc, f| {
                Some(acc + closed_pnl_term(f.get("closedPnl"))?)
            });
            match total {
                Some(total) => return Some(total),
                None => {
                    tracing::warn!(
                        "close-pnl: unparseable closedPnl for {} {}",
                        trade.address,
                        trade.coin
                    );
                    return None;
                }
            }
        }
        tracing::info!(
            "close-pnl: closing fill for {} {} not visible after {} attempts; using estimate",
            trade.address,
            trade.coin,
            self.attempts
        );
        None
    }

    /// Every fill for `address` in the window, time-walking full pages; None on overflow.
    // PORT NOTE: `-> list[dict[str, Any]] | None` plus implicit "may raise TrackerError" →
    // Result<Option<Vec<Map<String, Value>>>>: Err = raised, Ok(None) = page-cap overflow.
    // `dict[str, Any]` → serde_json::Map<String, Value> (fixed decision: untyped API
    // payloads stay Value). start_ms/end_ms are epoch-ms ints → i64.
    async fn window_fills(
        &self,
        address: &str,
        start_ms: i64,
        end_ms: i64,
    ) -> Result<Option<Vec<Map<String, Value>>>> {
        let mut fills: Vec<Map<String, Value>> = Vec::new();
        let mut cursor = start_ms;
        for _ in 0..self.max_pages {
            // PORT NOTE: dict literal → json! body. Python dicts are insertion-ordered;
            // serde_json's default Map is sorted — key order in a JSON request body is
            // semantically irrelevant, so no IndexMap needed.
            let page = self
                .client
                .info(json!({
                    "type": "userFillsByTime",
                    "user": address,
                    "startTime": cursor,
                    "endTime": end_ms + END_SKEW_MS,
                }))
                .await?;
            // PORT NOTE: `if not isinstance(page, list): raise ParseError(...)` — the
            // message printed `type(page).__name__` (a Python type name, e.g. "dict");
            // json_type_name prints the JSON name ("object") — log-message-only divergence.
            let page = match page {
                Value::Array(page) => page,
                other => {
                    return Err(TrackerError::Parse(format!(
                        "userFillsByTime: expected a list, got {}",
                        json_type_name(&other)
                    )));
                }
            };
            // `fills.extend(f for f in page if isinstance(f, dict))`
            // PERF(port): Python extended by reference; this clones each fill object out of
            // the page (the page is re-read for len/times below) — profile in Phase B.
            fills.extend(page.iter().filter_map(|f| f.as_object().cloned()));
            if page.len() < self.page_limit {
                return Ok(Some(fills));
            }
            let times: Vec<i64> = page
                .iter()
                .filter_map(|f| f.as_object())
                .filter_map(|f| f.get("time"))
                .filter_map(Value::as_i64)
                .collect();
            if times.is_empty() {
                return Err(TrackerError::Parse(
                    "userFillsByTime: full page with no usable timestamps".to_string(),
                ));
            }
            // the doc-sanctioned walk: advance past the last row's time
            cursor = *times.iter().max().expect("times is non-empty") + 1;
        }
        tracing::warn!(
            "close-pnl: leg for {} spans more than {} pages of fills; skipping",
            address,
            self.max_pages
        );
        Ok(None)
    }
}

/// `Decimal(str(f.get("closedPnl") or 0))` for one fill; `None` = Python's `InvalidOperation`.
// PORT NOTE: structural addition — the Python spelled this inline in the sum() generator;
// extracted so the truthiness fallback and the InvalidOperation mapping stay readable.
// `value or 0` is Python truthiness: absent key, null, False, 0/0.0, "", [] and {} all fall
// back to 0; everything else goes through str() then Decimal().
fn closed_pnl_term(value: Option<&Value>) -> Option<Decimal> {
    let s = match value {
        None | Some(Value::Null) | Some(Value::Bool(false)) => return Some(ZERO),
        Some(Value::String(s)) if s.is_empty() => return Some(ZERO),
        Some(Value::Number(n)) if n.as_f64() == Some(0.0) => return Some(ZERO),
        Some(Value::Array(a)) if a.is_empty() => return Some(ZERO),
        Some(Value::Object(o)) if o.is_empty() => return Some(ZERO),
        Some(Value::String(s)) => s.clone(),
        // PORT NOTE: serde_json renders numbers via their shortest repr, like Python's str().
        Some(Value::Number(n)) => n.to_string(),
        // str(True) / str([...]) / str({...}) are not decimal syntax → Python raised
        // InvalidOperation; rejected directly here.
        Some(_) => return None,
    };
    // PORT NOTE: from_str then from_scientific fallback — rust_decimal's FromStr does not
    // accept exponent notation ("1e5"), which Python's Decimal does (models.rs precedent).
    // TODO(port): rust_decimal cannot represent Decimal("NaN")/"Infinity" or magnitudes
    // beyond ~7.9e28 — such a closedPnl string parses in Python (and would sum to a
    // non-finite/huge total attached to the notification) but fails here, so the whole
    // lookup falls back to the local estimate. Arguably better; confirm in Phase B.
    Decimal::from_str(&s)
        .or_else(|_| Decimal::from_scientific(&s))
        .ok()
}

/// JSON type name for error messages (`type(x).__name__` stand-in).
// PORT NOTE: structural addition — see the ParseError note in window_fills.
fn json_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// `event`, with the exchange's realized PnL swapped in when it applies and resolves.
///
/// A no-op for non-close events, when no resolver is wired, or when the lookup comes back
/// empty-handed — the event's local average-cost estimate is kept in those cases.
// PORT NOTE: `trade: CompletedTrade | None` / `resolver: ClosedPnlResolver | None` →
// Option<&…> (borrowed — the caller keeps ownership, as in Python). `event` is taken by
// value and returned (Python returned the same object or a replaced copy; the by-value
// flow makes both one move).
pub async fn with_authoritative_pnl(
    event: LiveEvent,
    trade: Option<&CompletedTrade>,
    resolver: Option<&ClosedPnlResolver>,
) -> LiveEvent {
    if resolver.is_none() || trade.is_none() || event.kind != EventKind::Close {
        return event;
    }
    // PORT NOTE: unwraps are guarded by the early return above (Python's `or` chain
    // proved the same non-Noneness before the attribute uses below).
    let resolver = resolver.unwrap();
    let trade = trade.unwrap();
    let pnl = resolver.close_pnl(trade, event.ts).await;
    if pnl.is_none() {
        return event;
    }
    // PORT NOTE: dataclasses.replace(event, realized_pnl=pnl) → struct-update syntax;
    // `pnl` is already the Some(..) the Option field wants.
    LiveEvent {
        realized_pnl: pnl,
        ..event
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests — ported from tests/test_pnl.py
// ──────────────────────────────────────────────────────────────────────────

/// Authoritative close PnL: leg-window filtering, retry-until-visible, pagination, fallbacks.
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::sync::Mutex;

    use crate::models::TRADE_SOURCE_LIVE;
    use crate::state::Direction;

    // PORT NOTE: `D = Decimal` alias → tiny parse helper (tests feed decimal strings).
    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).expect("test literal is a valid decimal")
    }

    // PORT NOTE: module constants OPENED/CLOSED/OPENED_MS/CLOSED_MS → helper fns (chrono
    // constructors aren't const). `round(OPENED.timestamp() * 1000)` → timestamp_millis(),
    // exactly as in to_ms.
    fn opened() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 15, 12, 0, 0).unwrap()
    }

    fn closed() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 15, 15, 0, 0).unwrap()
    }

    fn opened_ms() -> i64 {
        opened().timestamp_millis()
    }

    fn closed_ms() -> i64 {
        closed().timestamp_millis()
    }

    /// Returns one canned response per call (the last repeats); records request bodies.
    ///
    /// An `Exception` instance in `responses` is raised instead of returned.
    // PORT NOTE: `responses: list[Any]` mixing plain values and Exception instances →
    // Vec<Result<Value, TrackerError>>; Err = "raised instead of returned". The fields
    // mutate under `&self` (the trait takes &self), so both live behind std Mutexes —
    // never held across an await.
    struct FakeInfoClient {
        responses: Mutex<Vec<Result<Value>>>,
        calls: Mutex<Vec<Value>>,
    }

    impl FakeInfoClient {
        fn new(responses: Vec<Result<Value>>) -> Self {
            Self {
                responses: Mutex::new(responses),
                calls: Mutex::new(Vec::new()),
            }
        }
    }

    // PORT NOTE: structural addition — TrackerError does not derive Clone, but the fake's
    // "last response repeats" contract needs to hand out the same canned response many
    // times, so it is re-built field-by-field.
    fn clone_response(resp: &Result<Value>) -> Result<Value> {
        match resp {
            Ok(v) => Ok(v.clone()),
            Err(TrackerError::Parse(m)) => Err(TrackerError::Parse(m.clone())),
            Err(TrackerError::RateLimited {
                message,
                retry_after,
            }) => Err(TrackerError::RateLimited {
                message: message.clone(),
                retry_after: *retry_after,
            }),
            Err(TrackerError::AuthRequired(m)) => Err(TrackerError::AuthRequired(m.clone())),
        }
    }

    #[async_trait::async_trait]
    impl InfoClient for FakeInfoClient {
        async fn info(&self, body: Value) -> Result<Value> {
            self.calls.lock().unwrap().push(body);
            let mut responses = self.responses.lock().unwrap();
            // `self._responses.pop(0) if len(self._responses) > 1 else self._responses[0]`
            if responses.len() > 1 {
                responses.remove(0)
            } else {
                clone_response(&responses[0])
            }
        }
    }

    fn fill(coin: &str, time_ms: i64, closed_pnl: &str) -> Value {
        json!({"coin": coin, "time": time_ms, "closedPnl": closed_pnl, "px": "1", "sz": "1"})
    }

    // PORT NOTE: the `coin: str = "BTC"` default arg is never overridden by any test, so
    // the parameter is dropped entirely (state.rs test-port precedent).
    fn trade() -> CompletedTrade {
        CompletedTrade {
            address: "0xa".to_string(),
            coin: "BTC".to_string(),
            direction: "Long".to_string(),
            start_time: opened(),
            end_time: closed(),
            duration_mins: 180,
            // PORT NOTE: the pydantic field defaults (None money fields, source =
            // TRADE_SOURCE_LIVE) spelled out — models.rs dropped the defaults.
            size: None,
            avg_entry_px: None,
            avg_exit_px: None,
            gross_pnl: None,
            funding_pnl: None,
            total_fees: None,
            net_pnl: None,
            source: TRADE_SOURCE_LIVE.to_string(),
        }
    }

    // PORT NOTE: was default arg `realized: str = "20"` — flattened; the one default-using
    // call site passes "20" explicitly. `direction="Long"` → Direction::Long (state.rs enum).
    fn close_event(realized: &str) -> LiveEvent {
        LiveEvent {
            kind: EventKind::Close,
            address: "0xa".to_string(),
            coin: "BTC".to_string(),
            direction: Direction::Long,
            delta: d("-2"),
            px: d("110"),
            szi_after: d("0"),
            realized_pnl: Some(d(realized)),
            ts: closed(),
        }
    }

    // PORT NOTE: was default arg `attempts: int = 3` — flattened; call sites pass 3.
    // `Settings(closed_pnl_attempts=…, closed_pnl_retry_delay_s=0.0)` → struct-update from
    // Default (pydantic-settings would also consult the process env; Default deliberately
    // does not, keeping the test hermetic).
    fn resolver(client: Arc<dyn InfoClient>, attempts: u32) -> ClosedPnlResolver {
        let settings = Settings {
            closed_pnl_attempts: attempts,
            closed_pnl_retry_delay_s: 0.0,
            ..Settings::default()
        };
        ClosedPnlResolver::new(&settings, client)
    }

    #[tokio::test]
    async fn test_close_pnl_sums_closed_pnl_over_the_leg_window() {
        let client = Arc::new(FakeInfoClient::new(vec![Ok(json!([
            fill("BTC", opened_ms(), "7.0"), // opening fill: a flip's residue — excluded
            fill("BTC", opened_ms() + 1000, "5.5"), // reduce inside the leg
            fill("ETH", opened_ms() + 2000, "99"), // other coin — excluded
            fill("BTC", closed_ms(), "4.5"), // the closing fill
            fill("BTC", closed_ms() + 1, "99"), // after the close (endTime skew) — excluded
        ]))]));
        assert_eq!(
            resolver(client, 3).close_pnl(&trade(), closed()).await,
            Some(d("10.0"))
        );
    }

    #[tokio::test]
    async fn test_close_pnl_requests_the_leg_window() {
        let client = Arc::new(FakeInfoClient::new(vec![Ok(json!([fill(
            "BTC",
            closed_ms(),
            "1"
        )]))]));
        resolver(client.clone(), 3)
            .close_pnl(&trade(), closed())
            .await;
        let calls = client.calls.lock().unwrap();
        let body = &calls[0];
        assert_eq!(
            body.get("type").and_then(Value::as_str),
            Some("userFillsByTime")
        );
        assert_eq!(body.get("user").and_then(Value::as_str), Some("0xa"));
        assert_eq!(
            body.get("startTime").and_then(Value::as_i64),
            Some(opened_ms())
        );
        // skewed past the close so the closing fill can't hide
        assert!(body.get("endTime").and_then(Value::as_i64).unwrap() > closed_ms());
    }

    #[tokio::test]
    async fn test_close_pnl_retries_until_the_closing_fill_is_visible() {
        // REST index hasn't caught up yet
        let lagging = json!([fill("BTC", opened_ms() + 1000, "5.5")]);
        let caught_up = json!([
            fill("BTC", opened_ms() + 1000, "5.5"),
            fill("BTC", closed_ms(), "4.5")
        ]);
        let client = Arc::new(FakeInfoClient::new(vec![Ok(lagging), Ok(caught_up)]));
        assert_eq!(
            resolver(client.clone(), 3)
                .close_pnl(&trade(), closed())
                .await,
            Some(d("10.0"))
        );
        assert_eq!(client.calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_close_pnl_gives_up_when_the_closing_fill_never_appears() {
        let client = Arc::new(FakeInfoClient::new(vec![Ok(json!([fill(
            "BTC",
            opened_ms() + 1000,
            "5.5"
        )]))]));
        assert!(
            resolver(client.clone(), 2)
                .close_pnl(&trade(), closed())
                .await
                .is_none()
        );
        assert_eq!(client.calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_close_pnl_returns_none_on_client_error() {
        let client = Arc::new(FakeInfoClient::new(vec![Err(TrackerError::RateLimited {
            message: "simulated 429".to_string(),
            retry_after: None,
        })]));
        assert!(
            resolver(client, 3)
                .close_pnl(&trade(), closed())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_close_pnl_returns_none_on_unparseable_closed_pnl() {
        let client = Arc::new(FakeInfoClient::new(vec![Ok(json!([fill(
            "BTC",
            closed_ms(),
            "not-a-number"
        )]))]));
        assert!(
            resolver(client, 3)
                .close_pnl(&trade(), closed())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_close_pnl_returns_none_on_non_list_response() {
        let client = Arc::new(FakeInfoClient::new(vec![Ok(json!({"error": "nope"}))]));
        assert!(
            resolver(client, 3)
                .close_pnl(&trade(), closed())
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_close_pnl_paginates_full_pages() {
        // PORT NOTE: `monkeypatch.setattr(pnl_mod, "_PAGE_LIMIT", 2)` → test-only builder.
        let page1 = json!([
            fill("BTC", opened_ms() + 1, "1"),
            fill("BTC", opened_ms() + 2, "2")
        ]);
        let page2 = json!([fill("BTC", closed_ms(), "3")]);
        let client = Arc::new(FakeInfoClient::new(vec![Ok(page1), Ok(page2)]));
        let resolver = resolver(client.clone(), 3).with_page_limit(2);
        assert_eq!(resolver.close_pnl(&trade(), closed()).await, Some(d("6")));
        // The walk resumes strictly past the last row of the full page.
        let calls = client.calls.lock().unwrap();
        assert_eq!(
            calls[1].get("startTime").and_then(Value::as_i64),
            Some(opened_ms() + 3)
        );
    }

    #[tokio::test]
    async fn test_close_pnl_gives_up_when_the_leg_overflows_the_page_cap() {
        // PORT NOTE: `monkeypatch.setattr(pnl_mod, "_PAGE_LIMIT"/"_MAX_PAGES", …)` →
        // test-only builders.
        // every page comes back full
        let client = Arc::new(FakeInfoClient::new(vec![Ok(json!([fill(
            "BTC",
            opened_ms() + 1,
            "1"
        )]))]));
        let resolver = resolver(client.clone(), 3)
            .with_page_limit(1)
            .with_max_pages(2);
        assert!(resolver.close_pnl(&trade(), closed()).await.is_none());
        // stopped at the page cap, not the retry budget
        assert_eq!(client.calls.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn test_with_authoritative_pnl_swaps_in_the_exchange_number() {
        let client = Arc::new(FakeInfoClient::new(vec![Ok(json!([fill(
            "BTC",
            closed_ms(),
            "19.25"
        )]))]));
        let resolver = resolver(client, 3);
        let event =
            with_authoritative_pnl(close_event("20"), Some(&trade()), Some(&resolver)).await;
        assert_eq!(event.realized_pnl, Some(d("19.25")));
    }

    #[tokio::test]
    async fn test_with_authoritative_pnl_keeps_the_estimate_when_lookup_fails() {
        let client = Arc::new(FakeInfoClient::new(vec![Err(TrackerError::RateLimited {
            message: "simulated 429".to_string(),
            retry_after: None,
        })]));
        let resolver = resolver(client, 3);
        let event =
            with_authoritative_pnl(close_event("20"), Some(&trade()), Some(&resolver)).await;
        assert_eq!(event.realized_pnl, Some(d("20")));
    }

    #[tokio::test]
    async fn test_with_authoritative_pnl_ignores_non_close_events() {
        let open_event = LiveEvent {
            kind: EventKind::Open,
            address: "0xa".to_string(),
            coin: "BTC".to_string(),
            direction: Direction::Long,
            delta: d("2"),
            px: d("100"),
            szi_after: d("2"),
            realized_pnl: None,
            ts: opened(),
        };
        let client = Arc::new(FakeInfoClient::new(vec![Ok(json!([]))]));
        let resolver = resolver(client.clone(), 3);
        // PORT NOTE: Python asserted identity (`is open_event`); the Rust fn takes and
        // returns the event by value, so structural equality is the strongest available
        // assertion (state.rs test-port precedent).
        let result =
            with_authoritative_pnl(open_event.clone(), Some(&trade()), Some(&resolver)).await;
        assert_eq!(result, open_event);
        // no lookup spent on an open
        assert!(client.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_with_authoritative_pnl_is_a_passthrough_without_a_resolver() {
        // PORT NOTE: was `_close_event()` — the realized="20" default arg, passed explicitly.
        let event = close_event("20");
        // PORT NOTE: identity assertion (`is event`) → structural equality, as above.
        let result = with_authoritative_pnl(event.clone(), Some(&trade()), None).await;
        assert_eq!(result, event);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/pnl.py (150 lines) + tests/test_pnl.py (176 lines)
//   confidence: high
//   todos:      1
//   notes:      close_pnl swallows all failures into Option<Decimal> like the Python;
//               window_fills returns Result<Option<Vec<Map>>> (Err = raised TrackerError,
//               Ok(None) = page-cap overflow). _PAGE_LIMIT/_MAX_PAGES consts are mirrored
//               as private resolver fields solely so tests can override them (monkeypatch
//               replacement); production values come from the consts. Client held as
//               Arc<dyn InfoClient> per fixed decisions. Crates: tokio, chrono,
//               rust_decimal, serde_json, tracing, async-trait (tests).
// ──────────────────────────────────────────────────────────────────────────
