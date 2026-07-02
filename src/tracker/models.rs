//! Pydantic models + decimal coercion for Hyperliquid `clearinghouseState` positions.
//!
//! A focused subset of the sibling `hyperdash-crawl` models: just what the tracker needs to
//! (a) parse an `assetPositions` entry into a flat [`AccountPosition`] (for the cold-start
//! seed + leverage), and (b) carry a completed round-trip out of the state machine (for the
//! realized-PnL in close/reduce notifications). Everything monetary is
//! [`rust_decimal::Decimal`].
//
// PORT NOTE: the Python models are pydantic `BaseModel`s whose validation runs at keyword
// construction (`BeforeValidator(_to_decimal)` on every `Money` field). Rust has no
// construction-time hook, so the coercion moves to the single parse boundary —
// `build_position` calls `to_decimal` explicitly on each raw JSON field. `CompletedTrade`
// is only ever built field-by-field from already-`Decimal` values (state.py::_close_trade),
// so it needs no validator at all. No serde derives in Phase A: nothing in the package
// `model_dump`s / `model_validate`s these models.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::Value;
use std::str::FromStr;

/// Errors raised by this module.
// PORT NOTE: models.py raises a stdlib `ValueError` — deliberately NOT part of the
// `TrackerError` family (see exceptions.py docstring: stdlib errors must stay
// distinguishable from transport errors). Hence a local error enum here instead of
// `use crate::exceptions::...`.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    /// `ValueError(f"not a decimal: {value!r}")` from `_to_decimal`.
    // PORT NOTE: `raise ... from err` chain → `#[source]`. The source is `None` for
    // bool/array/object inputs, which Python rejected via `Decimal(str(value))` raising
    // `InvalidOperation` — there is no meaningful parse error to carry for those.
    // PORT NOTE: `{value!r}` repr → serde_json's `Display` (JSON text) — close enough.
    #[error("not a decimal: {value}")]
    NotADecimal {
        value: Value,
        #[source]
        source: Option<rust_decimal::Error>,
    },
}

/// Coerce ints, floats, and numeric strings to `Decimal` via `str`.
///
/// Going through `str` avoids binary-float artefacts (`Decimal(0.1)`); empty strings and
/// `None` become `None` so missing money fields stay null. NaN/Infinity are rejected to
/// `None` at this single source of truth.
// PORT NOTE: was `_to_decimal(value: object)` (module-private — kept private here; the
// leading underscore is dropped because in Rust it means "unused"). The parameter is
// `Option<&Value>` so a missing JSON key (`dict.get` → `None`) and an explicit JSON
// `null` coerce identically to `Ok(None)`, exactly as in Python.
// PORT NOTE: the Python `isinstance(value, Decimal)` fast path (keep if finite, else
// None) is absorbed by the type system: the input here is always raw JSON, and
// `rust_decimal::Decimal` cannot represent NaN/Infinity, so an already-`Decimal` value
// is finite by construction.
fn to_decimal(value: Option<&Value>) -> Result<Money, Error> {
    // `if value is None or value == "":` — only a str compares equal to "" in Python.
    let value = match value {
        None | Some(Value::Null) => return Ok(None),
        Some(v) => v,
    };
    let s: String = match value {
        Value::String(s) if s.is_empty() => return Ok(None),
        // `Decimal(str(value))` — go through the string form in both branches so binary
        // floats round-trip through their shortest decimal repr, as in Python.
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        // PORT NOTE: bool/array/object → Python did `Decimal(str(value))` which raises
        // `InvalidOperation` → re-raised as ValueError. Rejected directly here.
        other => {
            return Err(Error::NotADecimal {
                value: other.clone(),
                source: None,
            });
        }
    };
    // PORT NOTE: Python parses "NaN"/"Infinity" into a *valid* non-finite Decimal and
    // then `d.is_finite()` maps it to None. rust_decimal has no non-finite values (the
    // parse would fail), so the special forms are detected up front and mapped to None
    // to preserve the "reject to None, not to error" contract. Grammar mirrors Python's
    // Decimal special-value syntax: optional sign, inf|infinity|nan|snan (+ optional
    // NaN diagnostic digits), case-insensitive, surrounding whitespace ignored.
    if is_non_finite(&s) {
        return Ok(None);
    }
    // PORT NOTE: rust_decimal's `FromStr` accepts exponent notation ("2.5e3") directly, so
    // no separate from_scientific fallback is needed (pinned by test below). Python's
    // Decimal constructor also ignores surrounding whitespace — trim to match. (Its
    // underscore-tolerance, `Decimal("1_000")`, is deliberately NOT replicated: no
    // Hyperliquid payload carries underscore numerics.)
    match Decimal::from_str(s.trim()) {
        Ok(d) => Ok(Some(d)),
        Err(err) => Err(Error::NotADecimal {
            value: value.clone(),
            source: Some(err),
        }),
    }
    // TODO(port): rust_decimal is 96-bit fixed-point (max ~7.9e28), Python Decimal is
    // arbitrary precision — a magnitude beyond that range parses in Python but returns
    // Err here. Fine for Hyperliquid sizes/prices; revisit in Phase B if it ever trips.
}

/// True for Python-`Decimal` special values: `[+-]?(inf|infinity|nan\d*|snan\d*)`,
/// case-insensitive, ignoring surrounding whitespace.
// PORT NOTE: structural addition — helper extracted so `to_decimal` reads like the
// Python (parse, then finiteness check); see the non-finite PORT NOTE above.
fn is_non_finite(s: &str) -> bool {
    let t = s.trim();
    let t = t.strip_prefix(['+', '-']).unwrap_or(t);
    let lower = t.to_ascii_lowercase();
    lower == "inf"
        || lower == "infinity"
        || lower
            .strip_prefix("snan")
            .or_else(|| lower.strip_prefix("nan"))
            .is_some_and(|rest| rest.chars().all(|c| c.is_ascii_digit()))
}

/// `Money = Annotated[Decimal | None, BeforeValidator(_to_decimal)]`.
// PORT NOTE: the `BeforeValidator` half of the annotation lives in `to_decimal`, applied
// explicitly at the JSON parse boundary (`build_position`); the alias keeps the
// `Decimal | None` half so struct fields read the same as the Python.
pub type Money = Option<Decimal>;

/// Provenance tag on a completed round-trip. The tracker only ever produces `live` trades
/// (derived from the public feed, so PnL is approximate — no per-fill fee/funding).
pub const TRADE_SOURCE_LIVE: &str = "live";

/// A normalized open perp position — one row per (wallet, coin).
///
/// Hyperliquid's `clearinghouseState.assetPositions[].position` nests `leverage` and
/// `cumFunding` objects; the fields the tracker uses are flattened here. `szi` is the
/// signed size (`> 0` long, `< 0` short).
// PORT NOTE: `model_config = ConfigDict(extra="forbid")` — construction-time extras are
// impossible in Rust (struct literals are exhaustive). If Phase B adds a serde
// `Deserialize` derive, mirror it with `#[serde(deny_unknown_fields)]`.
// PORT NOTE: pydantic field defaults (`= None`) are dropped: the only constructor site
// (`build_position`) sets every field explicitly.
// PORT NOTE: pydantic BaseModel defines structural `__eq__` → derive PartialEq/Eq.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountPosition {
    pub address: String,
    pub coin: String,
    pub szi: Money,
    pub entry_px: Money,
    pub position_value: Money,
    pub unrealized_pnl: Money,
    pub liquidation_px: Money,
    pub leverage_type: Option<String>,
    pub leverage_value: Option<i64>,
    pub max_leverage: Option<i64>,
}

/// Normalize one Hyperliquid `assetPositions` entry into a flat [`AccountPosition`].
///
/// Returns `None` for a malformed entry (no nested `position` block / missing coin) so a
/// bad row is skipped rather than fatal.
// PORT NOTE: Python signature was `-> AccountPosition | None`, but the pydantic
// constructor could ALSO raise (ValidationError wrapping `_to_decimal`'s ValueError for a
// non-numeric money string) — the sole caller (enrich.py::seed_wallet) catches it under a
// broad `except Exception` and fails the seed. `Result<Option<...>, Error>` makes both
// exits explicit: `Ok(None)` = skip row, `Err` = seed failure.
pub fn build_position(address: &str, raw: &Value) -> Result<Option<AccountPosition>, Error> {
    // `raw.get("position") if isinstance(raw, dict) else None` — serde_json's
    // `Value::get` already returns None for non-objects.
    let pos = match raw.get("position") {
        Some(Value::Object(pos)) => pos,
        _ => return Ok(None),
    };
    let coin = match pos.get("coin") {
        // `if not isinstance(coin, str) or not coin: return None`
        Some(Value::String(coin)) if !coin.is_empty() => coin.clone(),
        _ => return Ok(None),
    };
    // `leverage = pos.get("leverage") if isinstance(pos.get("leverage"), dict) else {}`
    // PORT NOTE: Python's `{}` fallback becomes `None` — `.get` on an empty dict and
    // `.and_then` on `None` yield the same absent fields.
    let leverage = match pos.get("leverage") {
        Some(Value::Object(leverage)) => Some(leverage),
        _ => None,
    };
    Ok(Some(AccountPosition {
        address: address.to_string(),
        coin,
        szi: to_decimal(pos.get("szi"))?,
        entry_px: to_decimal(pos.get("entryPx"))?,
        position_value: to_decimal(pos.get("positionValue"))?,
        unrealized_pnl: to_decimal(pos.get("unrealizedPnl"))?,
        liquidation_px: to_decimal(pos.get("liquidationPx"))?,
        // PORT NOTE: pydantic validated these as `str | None` / `int | None` with lax
        // coercion (numeric strings / integral floats → int) and RAISED on other types;
        // `as_str`/`as_i64` narrow to exact JSON types and map mismatches to None
        // instead. Hyperliquid sends `{"type": <str>, "value": <int>}`, so the
        // difference is unobservable on real payloads — flagged for Phase B anyway.
        leverage_type: leverage
            .and_then(|l| l.get("type"))
            .and_then(Value::as_str)
            .map(str::to_string),
        leverage_value: leverage
            .and_then(|l| l.get("value"))
            .and_then(Value::as_i64),
        max_leverage: pos.get("maxLeverage").and_then(Value::as_i64),
    }))
}

/// A round-trip trade emitted by the state machine on a close/flip.
///
/// Transient (never persisted) — carried only so a close notification can report realized PnL,
/// direction, and duration. PnL is approximate (the public feed carries no fee/funding), hence
/// the `source='live'` tag.
// PORT NOTE: `model_config = ConfigDict(extra="forbid")` — see AccountPosition.
// PORT NOTE: pydantic field defaults (`= None` on the Money fields, `source =
// TRADE_SOURCE_LIVE`) are dropped: the only constructor site (state.py::_close_trade)
// passes every field, `source` included. If Phase B grows a caller that wants the
// defaults, add a `new(...)` helper — don't impl `Default` (address/coin/direction/
// timestamps have no meaningful default).
// PORT NOTE: `direction: str` stays `String` to match the Python (state.py compares it
// against DIRECTION_LONG/DIRECTION_SHORT string constants); a shared enum is a Phase B
// refactor across modules, not a models.rs decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompletedTrade {
    pub address: String,
    pub coin: String,
    pub direction: String,
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
    pub duration_mins: i64,
    pub size: Money,
    pub avg_entry_px: Money,
    pub avg_exit_px: Money,
    pub gross_pnl: Money,
    pub funding_pnl: Money,
    pub total_fees: Money,
    pub net_pnl: Money,
    pub source: String,
}

// Rust-only regressions (models.py has no test file): pin the to_decimal contract the
// review relied on — exponent notation parses via FromStr alone, whitespace is tolerated
// like Python's Decimal constructor, and special values still reject to None.
#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn dec(s: &str) -> Decimal {
        Decimal::from_str(s).expect("test literal")
    }

    #[test]
    fn exponent_notation_parses_without_a_fallback() {
        assert_eq!(
            to_decimal(Some(&json!("2.5e3"))).unwrap(),
            Some(dec("2500"))
        );
        assert_eq!(
            to_decimal(Some(&json!("1e-7"))).unwrap(),
            Some(dec("0.0000001"))
        );
    }

    #[test]
    fn surrounding_whitespace_is_tolerated_like_python_decimal() {
        assert_eq!(
            to_decimal(Some(&json!("  1.5  "))).unwrap(),
            Some(dec("1.5"))
        );
    }

    #[test]
    fn non_finite_specials_reject_to_none() {
        for s in ["NaN", "-Infinity", " inf ", "sNaN123"] {
            assert_eq!(to_decimal(Some(&json!(s))).unwrap(), None, "{s}");
        }
    }

    #[test]
    fn garbage_string_is_an_error_not_none() {
        assert!(to_decimal(Some(&json!("not-a-number"))).is_err());
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/models.py (113 lines)
//   confidence: high
//   todos:      0
//   notes:      local Error enum replaces the stdlib ValueError (models.py never imports
//               exceptions.py). to_decimal: FromStr handles exponents (pinned by tests);
//               whitespace trimmed like Python's Decimal; non-finite specials -> None.
// ──────────────────────────────────────────────────────────────────────────
