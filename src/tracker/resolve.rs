//! Pure trade-resolution helpers lifted from the sibling `hyperdash-crawl` listener.
//!
//! These are the I/O-free parts: resolving a public `WsTrade` to per-wallet signed deltas,
//! listing perp coins from a `meta` response, and seeding a [`PositionState`] from a
//! `clearinghouseState` snapshot (the cold-start primitive).
//
// PORT NOTE: pure sync module — no asyncio in the Python, so no tokio here (guide rule).
// PORT NOTE: trade / meta payloads are untyped API dicts (`dict[str, Any]` / `Any`) —
// per the fixed decisions they stay `serde_json::Value`. `Value::get` returns None for
// non-objects, which absorbs the Python `isinstance(x, dict)` guards.

use std::collections::HashSet;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;

// PORT NOTE: `from tracker.state import DIRECTION_LONG, DIRECTION_SHORT, PositionState` —
// the str constants became the `Direction` enum in state.rs (fixed shared decision).
use crate::state::{Direction, PositionState};

// PORT NOTE: `_ZERO = Decimal(0)` — underscore dropped (in Rust it means "unused");
// privacy is expressed by omitting `pub`. Same pattern as state.rs.
const ZERO: Decimal = Decimal::ZERO;

// PORT NOTE: `_now` → `now` (underscore dropped, module-private via no `pub`).
// `datetime.now(UTC)` → `Utc::now()` (fixed decision).
fn now() -> DateTime<Utc> {
    Utc::now()
}

/// One watched wallet's signed participation in a public trade.
// PORT NOTE: `@dataclass(slots=True)` → plain struct (Rust structs are already "slots").
// Derives per the fixed shared decision (Debug, Clone, PartialEq — no serde: never
// (de)serialized). Field order matches the Python.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedFill {
    pub address: String,
    pub coin: String,
    /// +sz if the wallet bought, -sz if it sold
    pub delta: Decimal,
    pub px: Decimal,
    pub ts: DateTime<Utc>,
}

/// Extract the perp coin names from a Hyperliquid `meta` response's `universe`.
// PORT NOTE: `meta: Any` → `&Value`. `meta.get("universe", []) if isinstance(meta, dict)
// else []` — Value::get covers the isinstance guard; a present-but-non-list `universe`
// yields `[]` here, where Python's `for u in universe` would raise TypeError on a
// non-iterable (a crash on malformed data, not behaviour worth carrying forward).
pub fn perp_coins_from_meta(meta: &Value) -> Vec<String> {
    let Some(universe) = meta.get("universe").and_then(Value::as_array) else {
        return Vec::new();
    };
    // `[u["name"] for u in universe if isinstance(u, dict) and u.get("name")]`
    // PORT NOTE: `isinstance(u, dict)` is absorbed by Value::get (None on non-objects);
    // the `u.get("name")` truthiness check narrows to non-empty *strings* — a truthy
    // non-str name would have slipped into Python's `list[str]` as a non-str (type-hint
    // violation), and is dropped here instead.
    universe
        .iter()
        .filter_map(|u| match u.get("name") {
            Some(Value::String(name)) if !name.is_empty() => Some(name.clone()),
            _ => None,
        })
        .collect()
}

// PORT NOTE: structural addition — `str(value)` over a `serde_json::Value`. A parsed-JSON
// string is used bare (serde's `Display` would re-quote it: `"0xabc"`); everything else
// falls back to its JSON text. Diverges from Python's `str()` only for bool/None
// ("true"/"null" vs "True"/"None") — unobservable: both forms fail decimal parsing and
// neither is a watched address.
fn py_str(value: &Value) -> String {
    match value.as_str() {
        Some(s) => s.to_owned(),
        None => value.to_string(),
    }
}

// PORT NOTE: structural addition — the `Decimal(str(value))` expression from the
// try-block in `resolve_deltas`, returning None where Python raised InvalidOperation.
// `from_scientific` fallback: Python's Decimal grammar accepts exponent notation
// ("1e5"), rust_decimal's FromStr does not.
// PORT NOTE: Python's Decimal also parses "NaN"/"Infinity" into *valid* non-finite
// Decimals that would have flowed through (`NaN == 0` is False, so a NaN sz survived the
// zero-size guard); rust_decimal has no non-finite values, so they fail the parse and the
// trade yields no fills — divergence unobservable on the real feed. (Python's grammar
// also tolerates surrounding whitespace; rust_decimal's doesn't — same caveat.)
fn decimal_from_value(value: &Value) -> Option<Decimal> {
    let s = py_str(value);
    Decimal::from_str(&s)
        .or_else(|_| Decimal::from_scientific(&s))
        .ok()
}

/// Resolve one `WsTrade` to the signed deltas for any watched wallets it touched.
///
/// Convention: `users == [buyer, seller]` — the buyer's signed size grows by `+sz`, the
/// seller's by `-sz`, regardless of which side was the aggressor. Documented by Hyperliquid
/// and VERIFIED on the live socket by the sibling project (2026-06-16). If this ever needs
/// revisiting, it is the only function that changes. Malformed trades (missing coin/users,
/// unparseable numbers, zero size) yield no fills.
///
/// `watchlist` addresses MUST be lowercase; the trade's addresses are lowered here to match.
// PORT NOTE: `watchlist: frozenset[str]` → `&HashSet<String>` — immutability by shared
// reference; the Registry's cached `Arc<HashSet<String>>` (fixed decision) derefs into
// this parameter at the listener call site.
pub fn resolve_deltas(trade: &Value, watchlist: &HashSet<String>) -> Vec<ResolvedFill> {
    let coin = trade.get("coin");
    let users = trade.get("users");
    // `if not coin or not isinstance(users, list) or len(users) != 2: return []`
    // PORT NOTE: split into two let-else guards (same bail target). `not coin` truthiness:
    // a missing key, JSON null, and "" all bail here as in Python; a *truthy non-string*
    // coin (e.g. a number) passed Python's check and flowed into ResolvedFill.coin as a
    // non-str — narrowed away here since the field is `String` (the `coin: str` shape).
    let Some(coin) = coin.and_then(Value::as_str).filter(|c| !c.is_empty()) else {
        return Vec::new();
    };
    let Some(users) = users.and_then(Value::as_array).filter(|u| u.len() == 2) else {
        return Vec::new();
    };
    // PORT NOTE: `except KeyError, InvalidOperation:` — PEP 758 (3.14) unparenthesized
    // multi-except, i.e. it catches BOTH: a missing px/sz key (`trade["px"]`) and an
    // unparseable value. Both collapse into the Option chain returning None → bail.
    let Some(px) = trade.get("px").and_then(decimal_from_value) else {
        return Vec::new();
    };
    let Some(sz) = trade.get("sz").and_then(decimal_from_value) else {
        return Vec::new();
    };
    if sz == ZERO {
        return Vec::new();
    }
    let raw_time = trade.get("time");
    // `is not None` (not a falsy check): a legitimate epoch 0 must use the real ts; only a
    // missing `time` key falls back to the receive clock.
    // PORT NOTE: JSON null parses to Python None, so `Some(Value::Null)` joins the
    // missing-key fallback arm — same dict.get semantics.
    let ts = match raw_time {
        None | Some(Value::Null) => now(),
        Some(raw_time) => {
            // PORT NOTE: `datetime.fromtimestamp(raw_time / 1000, tz=UTC)` — float true
            // division: an integer `time` maps exactly via from_timestamp_millis, and a
            // float (fractional-ms) `time` parsed fine in Python too, mapped here via µs.
            // Python raised on a non-numeric / out-of-range `time` and the listener's
            // reconnect loop absorbed it; a panic here would instead kill the spawned
            // listener task for good (see app.rs) — so treat it as a malformed trade per
            // this function's contract and yield no fills. (The f64→i64 `as` cast
            // saturates, so an absurd magnitude lands in from_timestamp_micros's None.)
            let Some(ts) = raw_time
                .as_i64()
                .and_then(DateTime::from_timestamp_millis)
                .or_else(|| {
                    raw_time
                        .as_f64()
                        .and_then(|ms| DateTime::from_timestamp_micros((ms * 1000.0) as i64))
                })
            else {
                return Vec::new();
            };
            ts
        }
    };
    // `str(users[0]).lower()` — see py_str; .lower() → to_lowercase (both Unicode-aware).
    let buyer = py_str(&users[0]).to_lowercase();
    let seller = py_str(&users[1]).to_lowercase();
    let mut fills: Vec<ResolvedFill> = Vec::new();
    if watchlist.contains(buyer.as_str()) {
        fills.push(ResolvedFill {
            address: buyer,
            coin: coin.to_string(),
            delta: sz,
            px,
            ts,
        });
    }
    if watchlist.contains(seller.as_str()) {
        fills.push(ResolvedFill {
            address: seller,
            coin: coin.to_string(),
            delta: -sz,
            px,
            ts,
        });
    }
    fills
}

/// Build a resume-state from a `clearinghouseState` position for cold-start seeding.
///
/// Treats the existing position as a single open leg at its entry price (we don't have its
/// constituent fills — those are never stored), so subsequent stream fills extend/close it
/// correctly rather than mistaking the next fill for a brand-new open.
// PORT NOTE: keyword-only marker (`*, fallback_ts`) flattened to positional — Rust has no
// keyword arguments (same convention as state.rs). `entry_px: Decimal | None` →
// Option<Decimal> — this is models.rs's `Money`, matching the enrich.py call site
// (`pos.entry_px`).
pub fn seed_state_from_row(
    address: &str,
    coin: &str,
    szi: Decimal,
    entry_px: Option<Decimal>,
    fallback_ts: DateTime<Utc>,
) -> PositionState {
    // `entry = entry_px if entry_px is not None else _ZERO`
    let entry = entry_px.unwrap_or(ZERO);
    PositionState {
        address: address.to_string(),
        coin: coin.to_string(),
        szi,
        // PORT NOTE: DIRECTION_LONG/DIRECTION_SHORT str constants → Direction enum
        // (fixed decision; see state.rs). `szi > 0` compared Decimal to int — ZERO here.
        direction: if szi > ZERO {
            Direction::Long
        } else {
            Direction::Short
        },
        opened_at: fallback_ts,
        last_added_at: fallback_ts,
        avg_entry: entry,
        entry_qty_total: szi.abs(),
        exit_qty: ZERO,
        exit_notional: ZERO,
        realized_pnl: ZERO,
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests — ported from tests/test_resolve.py
// ──────────────────────────────────────────────────────────────────────────

/// Trade→signed-delta resolution, perp-coin extraction, and the seed helper.
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::json;
    use std::collections::HashMap;

    // PORT NOTE: `D = Decimal` alias → tiny parse helper (same as state.rs's tests).
    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).expect("test literal is a valid decimal")
    }

    // PORT NOTE: module constant `WATCH = frozenset({...})` → helper fn (HashSet of
    // owned Strings isn't const-constructible).
    fn watch() -> HashSet<String> {
        ["0xbuyer", "0xseller"]
            .into_iter()
            .map(str::to_string)
            .collect()
    }

    // PORT NOTE: module constant `TS = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)` →
    // helper fn (chrono constructors aren't const).
    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 15, 12, 0, 0).unwrap()
    }

    // PORT NOTE: `_trade(**kw)` (base dict + kwargs overrides) → a no-override `trade()`
    // plus `trade_with(key, value)` — every Python call site overrides at most one key.
    // Underscore prefix dropped as elsewhere.
    fn trade() -> Value {
        json!({
            "coin": "BTC",
            "side": "B",
            "px": "100",
            "sz": "2",
            "time": 1_700_000_000_000_i64,
            "tid": 42,
            "users": ["0xBUYER", "0xSELLER"],
        })
    }

    fn trade_with(key: &str, value: Value) -> Value {
        let mut t = trade();
        t.as_object_mut()
            .expect("trade() is a JSON object")
            .insert(key.to_string(), value);
        t
    }

    #[test]
    fn test_buyer_gets_positive_delta_seller_negative() {
        // `{f.address: f for f in resolve_deltas(_trade(), WATCH)}`
        let by_addr: HashMap<String, ResolvedFill> = resolve_deltas(&trade(), &watch())
            .into_iter()
            .map(|f| (f.address.clone(), f))
            .collect();
        // PORT NOTE: `by_addr["0xbuyer"]` — HashMap indexing panics on a missing key,
        // matching Python's KeyError.
        assert_eq!(by_addr["0xbuyer"].delta, d("2"));
        assert_eq!(by_addr["0xseller"].delta, d("-2"));
        assert_eq!(by_addr["0xbuyer"].coin, "BTC");
        assert_eq!(by_addr["0xbuyer"].px, d("100"));
    }

    #[test]
    fn test_addresses_are_lowercased_to_match_watchlist() {
        let fills = resolve_deltas(
            &trade_with("users", json!(["0xBUYER", "0xUNTRACKED"])),
            &watch(),
        );
        assert_eq!(
            fills.iter().map(|f| f.address.as_str()).collect::<Vec<_>>(),
            vec!["0xbuyer"]
        );
    }

    #[test]
    fn test_no_watched_wallet_yields_nothing() {
        assert_eq!(
            resolve_deltas(&trade_with("users", json!(["0xx", "0xy"])), &watch()),
            vec![]
        );
    }

    #[test]
    fn test_zero_size_and_malformed_yield_nothing() {
        assert_eq!(
            resolve_deltas(&trade_with("sz", json!("0")), &watch()),
            vec![]
        );
        assert_eq!(
            resolve_deltas(&trade_with("users", json!(["only-one"])), &watch()),
            vec![]
        );
        assert_eq!(resolve_deltas(&json!({"coin": "BTC"}), &watch()), vec![]);
        assert_eq!(
            resolve_deltas(&trade_with("px", json!("not-a-number")), &watch()),
            vec![]
        );
    }

    #[test]
    fn test_trade_time_is_parsed_to_utc() {
        let fills = resolve_deltas(&trade_with("time", json!(1_700_000_000_000_i64)), &watch());
        let fill = &fills[0];
        // `datetime.fromtimestamp(1_700_000_000, tz=UTC)`
        assert_eq!(fill.ts, DateTime::from_timestamp(1_700_000_000, 0).unwrap());
    }

    // Rust-only regressions for the review finding (no Python twin): resolve.py accepted a
    // float `time` (fromtimestamp float division) and RAISED into the reconnect loop on a
    // non-numeric one — the port must neither panic nor kill the listener for either.
    #[test]
    fn test_float_time_is_accepted() {
        let fills = resolve_deltas(&trade_with("time", json!(1_700_000_000_000.5)), &watch());
        assert_eq!(fills.len(), 2);
        assert_eq!(
            fills[0].ts,
            DateTime::from_timestamp_micros(1_700_000_000_000_500).unwrap()
        );
    }

    #[test]
    fn test_non_numeric_time_yields_nothing() {
        assert!(resolve_deltas(&trade_with("time", json!("oops")), &watch()).is_empty());
    }

    #[test]
    fn test_perp_coins_from_meta_extracts_names() {
        let meta =
            json!({"universe": [{"name": "BTC"}, {"name": "ETH"}, {"notname": "x"}, "junk"]});
        assert_eq!(perp_coins_from_meta(&meta), vec!["BTC", "ETH"]);
    }

    #[test]
    fn test_seed_state_from_row_sets_direction_from_sign() {
        let long_state = seed_state_from_row("0xa", "BTC", d("3"), Some(d("100")), ts());
        assert_eq!(long_state.direction, Direction::Long);
        assert_eq!(long_state.entry_qty_total, d("3"));
        let short_state = seed_state_from_row("0xa", "BTC", d("-3"), Some(d("100")), ts());
        assert_eq!(short_state.direction, Direction::Short);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/resolve.py (102 lines) + tests/test_resolve.py (67 lines)
//   confidence: high
//   todos:      1
//   notes:      pure sync module (no asyncio → no tokio; all tests are #[test]).
//               External crates: chrono, rust_decimal, serde_json. Trade/meta payloads
//               stay serde_json::Value; frozenset watchlist → &HashSet<String>
//               (Registry's Arc<HashSet<String>> derefs in). Direction enum from
//               state.rs replaces DIRECTION_LONG/SHORT. entry_px is Option<Decimal>
//               (models.rs Money) for the enrich.py call site. Single TODO: policy for
//               a non-i64 `time` value (Python accepted floats; here it panics).
// ──────────────────────────────────────────────────────────────────────────
