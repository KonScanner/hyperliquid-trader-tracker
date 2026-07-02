//! The position state machine — pure, I/O-free, fully unit-testable.
//!
//! Given a wallet's current open position and one observed fill (a *signed* size delta at a
//! price), [`apply_fill`] returns the next position, the lifecycle events to publish, and —
//! on a full close — one completed round-trip trade. It is decoupled from *how* the signed delta
//! was derived: the listener resolves "which side of this trade was the watched wallet on" (the
//! `users:[buyer,seller]` convention) and hands this module a clean `+sz` / `-sz`.
//!
//! Accounting is **average-cost**: `avg_entry` is the volume-weighted average of every opening
//! fill in the current leg; reduces realize PnL against that average. PnL is approximate (the
//! public feed carries no per-fill fee or funding). A *flip* (a delta that crosses zero) closes
//! the current leg and opens a new one in the opposite direction with the residual size.
//!
//! Everything is [`rust_decimal::Decimal`] end-to-end (the feed sends decimal strings), so there
//! is no float drift. Ported verbatim (behaviour-wise) from the sibling `hyperdash-crawl` project.
//
// PORT NOTE: pure sync module — no asyncio in the Python, so no tokio here (guide rule).
// PORT NOTE: rust_decimal is 96-bit fixed-point; divisions (`new_avg`, `avg_exit_px`) round
// to at most 28 fractional digits where Python's Decimal rounds to the 28-significant-digit
// default context. Identical on realistic Hyperliquid sizes/prices; far-tail digits can
// differ. Fixed decision (rust_decimal) — not re-litigated here.

use chrono::{DateTime, Timelike, Utc};
use rust_decimal::Decimal;

use crate::models::{CompletedTrade, TRADE_SOURCE_LIVE};

/// Leg direction labels.
// PORT NOTE: str constants → enum (fixed shared decision). DIRECTION_LONG → Direction::Long,
// DIRECTION_SHORT → Direction::Short; `Display` renders exactly "Long" / "Short" so the
// notifier's text and models.rs's `CompletedTrade.direction: String` stay byte-identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Long,
    Short,
}

impl std::fmt::Display for Direction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Direction::Long => "Long",
            Direction::Short => "Short",
        })
    }
}

// PORT NOTE: `_ZERO = Decimal(0)` — underscore dropped (in Rust it means "unused");
// privacy is expressed by omitting `pub`.
const ZERO: Decimal = Decimal::ZERO;

/// Lifecycle event kinds, in order of "how much they matter".
// PORT NOTE: str constants → enum (fixed shared decision). EVENT_OPEN → EventKind::Open,
// EVENT_ADD → Add, EVENT_REDUCE → Reduce, EVENT_CLOSE → Close; `Display` renders exactly
// "open" / "add" / "reduce" / "close".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKind {
    Open,
    Add,
    Reduce,
    Close,
}

impl std::fmt::Display for EventKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            EventKind::Open => "open",
            EventKind::Add => "add",
            EventKind::Reduce => "reduce",
            EventKind::Close => "close",
        })
    }
}

/// A wallet's current open position in one coin, as a running average-cost leg.
///
/// `szi` is the signed size (`> 0` long, `< 0` short); it is never exactly zero for a
/// live state (a position that reaches zero is closed and dropped). `avg_entry` is the
/// average-cost basis of the *currently open* size — recomputed on each add, left UNCHANGED by
/// a reduce. `entry_qty_total` is the running sum of every opening fill; `exit_qty` /
/// `exit_notional` accumulate closing fills; `realized_pnl` is the PnL booked by reduces so
/// far this leg.
// PORT NOTE: `@dataclass(slots=True)` → plain struct (Rust structs are already "slots";
// no PERF note needed — there is no un-slotted form to fall back to).
// PORT NOTE: derives per fixed shared decision (Debug, Clone, PartialEq — no serde:
// never (de)serialized). Not Eq: kept to the decision's exact derive list.
#[derive(Debug, Clone, PartialEq)]
pub struct PositionState {
    pub address: String,
    pub coin: String,
    pub szi: Decimal,
    pub direction: Direction,
    pub opened_at: DateTime<Utc>,
    pub last_added_at: DateTime<Utc>,
    pub avg_entry: Decimal,
    pub entry_qty_total: Decimal,
    pub exit_qty: Decimal,
    pub exit_notional: Decimal,
    pub realized_pnl: Decimal,
}

impl PositionState {
    // PORT NOTE: @property → inherent getter method.
    pub fn avg_entry_px(&self) -> Decimal {
        self.avg_entry
    }

    // PORT NOTE: @property → inherent getter; Python truthiness `if self.exit_qty`
    // (non-zero) made explicit with is_zero(). Division-by-zero would panic in
    // rust_decimal — guarded exactly as the Python guarded DivisionByZero.
    pub fn avg_exit_px(&self) -> Option<Decimal> {
        if !self.exit_qty.is_zero() {
            Some(self.exit_notional / self.exit_qty)
        } else {
            None
        }
    }
}

/// One position-lifecycle event, for the notifier (transient, never persisted).
#[derive(Debug, Clone, PartialEq)]
pub struct LiveEvent {
    pub kind: EventKind,
    pub address: String,
    pub coin: String,
    pub direction: Direction,
    /// signed size change that caused the event
    pub delta: Decimal,
    pub px: Decimal,
    /// resulting position size (0 when the leg closed)
    pub szi_after: Decimal,
    /// set on reduce / close
    pub realized_pnl: Option<Decimal>,
    pub ts: DateTime<Utc>,
}

/// The outcome of folding one fill into a position.
///
/// `state` is the next position, or `None` when the leg is now flat (the caller drops the
/// key). `events` are the lifecycle events to publish (a flip yields close + open).
/// `closed_trade` is the completed round-trip, set on a close or flip.
#[derive(Debug, Clone, PartialEq)]
pub struct ApplyResult {
    pub state: Option<PositionState>,
    pub events: Vec<LiveEvent>,
    pub closed_trade: Option<CompletedTrade>,
}

// PORT NOTE: `_sign` → `sign` (underscore dropped, module-private via no `pub`).
// Return narrowed `int` → `i8`: the value is bounded to {-1, 0, 1} by construction and
// only ever compared for equality.
fn sign(value: Decimal) -> i8 {
    if value > ZERO {
        return 1;
    }
    if value < ZERO {
        return -1;
    }
    0
}

/// Start a fresh leg from flat with the (signed, non-zero) `delta` at `px`.
// PORT NOTE: keyword-only params (`*, delta, px, ts`) flattened to positional — Rust has
// no keyword arguments. Tuple return kept as-is.
fn open_leg(
    address: &str,
    coin: &str,
    delta: Decimal,
    px: Decimal,
    ts: DateTime<Utc>,
) -> (PositionState, LiveEvent) {
    let qty = delta.abs();
    let direction = if delta > ZERO {
        Direction::Long
    } else {
        Direction::Short
    };
    let state = PositionState {
        address: address.to_string(),
        coin: coin.to_string(),
        szi: delta,
        direction,
        opened_at: ts,
        last_added_at: ts,
        avg_entry: px,
        entry_qty_total: qty,
        exit_qty: ZERO,
        exit_notional: ZERO,
        realized_pnl: ZERO,
    };
    let event = LiveEvent {
        kind: EventKind::Open,
        address: address.to_string(),
        coin: coin.to_string(),
        direction,
        delta,
        px,
        szi_after: delta,
        realized_pnl: None,
        ts,
    };
    (state, event)
}

/// Average-cost realized PnL for closing `qty` of a `direction` leg at `px`.
// PORT NOTE: `direction: str` compared against DIRECTION_LONG → takes the Direction enum;
// the early-return if/fallthrough shape of the Python is preserved.
fn realized_chunk(direction: Direction, avg_entry: Decimal, px: Decimal, qty: Decimal) -> Decimal {
    if direction == Direction::Long {
        return (px - avg_entry) * qty;
    }
    (avg_entry - px) * qty
}

/// Build the round-trip [`CompletedTrade`] for a leg that just reached flat at `px`.
// PORT NOTE: takes `&PositionState` (read-only math over the old leg); Python passed the
// object it was about to discard.
fn close_trade(state: &PositionState, px: Decimal, ts: DateTime<Utc>) -> CompletedTrade {
    let avg_entry = state.avg_entry;
    let closing_qty = state.szi.abs(); // the still-open size being closed by this final fill
    let exit_notional = state.exit_notional + closing_qty * px;
    let exit_qty = state.exit_qty + closing_qty;
    let realized = state.realized_pnl + realized_chunk(state.direction, avg_entry, px, closing_qty);
    // PORT NOTE: `max(0, int((ts - opened_at).total_seconds() // 60))` →
    // `max(0, num_minutes())`. For non-negative durations chrono's whole-minute count
    // equals Python's float floor-div; for negative ones floor vs trunc differ by 1,
    // but the max(0, ..) clamp makes that unobservable.
    let duration_mins = std::cmp::max(0, (ts - state.opened_at).num_minutes());
    CompletedTrade {
        address: state.address.clone(),
        coin: state.coin.clone(),
        // PORT NOTE: models.rs (fixed dependency) keeps `direction: String`; the enum
        // renders "Long"/"Short" via Display — byte-identical to the Python constants.
        direction: state.direction.to_string(),
        start_time: state.opened_at,
        // `ts.replace(microsecond=0)` — zeroing nanoseconds subsumes zeroing microseconds.
        end_time: ts.with_nanosecond(0).expect("0 is a valid nanosecond"),
        duration_mins,
        // PORT NOTE: CompletedTrade's monetary fields are `Money = Option<Decimal>` —
        // the always-set Python Decimals arrive wrapped in Some(..).
        size: Some(state.entry_qty_total),
        avg_entry_px: Some(avg_entry),
        avg_exit_px: Some(if !exit_qty.is_zero() {
            exit_notional / exit_qty
        } else {
            px
        }),
        gross_pnl: Some(realized),
        // The public feed carries no per-fill fee/funding, so net == gross (approximate).
        funding_pnl: None,
        total_fees: None,
        net_pnl: Some(realized),
        source: TRADE_SOURCE_LIVE.to_string(),
    }
}

/// Fold one signed fill (`delta` units at `px`) into `state`.
///
/// `state` is `None` when the wallet has no open position in `coin`. `delta` is the
/// *signed* size change for this wallet (`+` if it bought, `-` if it sold). Returns the
/// next state (`None` if now flat), the lifecycle events, and any completed round-trip.
// PORT NOTE: keyword-only params flattened to positional. `state` comes in by reference
// (the book hands us `HashMap::get`'s Option<&PositionState>) and the next state goes out
// OWNED in ApplyResult — mirrors Python's read-old/return-new flow.
pub fn apply_fill(
    state: Option<&PositionState>,
    address: &str,
    coin: &str,
    delta: Decimal,
    px: Decimal,
    ts: DateTime<Utc>,
) -> ApplyResult {
    if delta == ZERO {
        // defensive: a zero-size fill changes nothing
        // PORT NOTE: Python returned the *same object* (`state is opened` in the test);
        // returning an owned ApplyResult forces a clone — structural equality preserved,
        // identity not (unobservable to callers).
        return ApplyResult {
            state: state.cloned(),
            events: vec![],
            closed_trade: None,
        };
    }

    // --- no open position: this fill opens one ---
    // PORT NOTE: `if state is None or state.szi == _ZERO:` folded into one match — the
    // non-guard arm covers both None and a zero-size state; the happy arm rebinds the
    // unwrapped &PositionState for the rest of the function.
    let state = match state {
        Some(s) if s.szi != ZERO => s,
        _ => {
            let (new_state, event) = open_leg(address, coin, delta, px, ts);
            return ApplyResult {
                state: Some(new_state),
                events: vec![event],
                closed_trade: None,
            };
        }
    };

    let same_direction = sign(delta) == sign(state.szi);

    // --- ADD: same-direction fill grows the leg ---
    if same_direction {
        let qty = delta.abs();
        let open_now = state.szi.abs();
        let new_avg = (state.avg_entry * open_now + px * qty) / (open_now + qty);
        // PORT NOTE: dataclasses.replace(state, ...) → struct-update from old.clone().
        let new_state = PositionState {
            szi: state.szi + delta,
            last_added_at: ts,
            avg_entry: new_avg,
            entry_qty_total: state.entry_qty_total + qty,
            ..state.clone()
        };
        let event = LiveEvent {
            kind: EventKind::Add,
            address: address.to_string(),
            coin: coin.to_string(),
            direction: state.direction,
            delta,
            px,
            szi_after: new_state.szi,
            realized_pnl: None,
            ts,
        };
        return ApplyResult {
            state: Some(new_state),
            events: vec![event],
            closed_trade: None,
        };
    }

    // --- opposite-direction fill: reduce, close, or flip ---
    let open_qty = state.szi.abs();
    let close_qty = delta.abs();

    // REDUCE: closes part of the leg without reaching flat.
    if close_qty < open_qty {
        // PORT NOTE: the local deliberately shadows fn realized_chunk (as the Python
        // local shadowed nothing but reused the name); the initializer resolves the fn
        // before the binding exists, so this is legal and keeps the Python's names.
        let realized_chunk = realized_chunk(state.direction, state.avg_entry_px(), px, close_qty);
        let new_state = PositionState {
            szi: state.szi + delta,
            exit_qty: state.exit_qty + close_qty,
            exit_notional: state.exit_notional + close_qty * px,
            realized_pnl: state.realized_pnl + realized_chunk,
            ..state.clone()
        };
        let event = LiveEvent {
            kind: EventKind::Reduce,
            address: address.to_string(),
            coin: coin.to_string(),
            direction: state.direction,
            delta,
            px,
            szi_after: new_state.szi,
            realized_pnl: Some(realized_chunk),
            ts,
        };
        return ApplyResult {
            state: Some(new_state),
            events: vec![event],
            closed_trade: None,
        };
    }

    // CLOSE (exact) or FLIP (crosses zero): the current leg reaches flat either way.
    let trade = close_trade(state, px, ts);
    let close_event = LiveEvent {
        kind: EventKind::Close,
        address: address.to_string(),
        coin: coin.to_string(),
        direction: state.direction,
        delta,
        px,
        szi_after: ZERO,
        // PORT NOTE: Python assigned the (always-set) Decimal `trade.net_pnl`; here
        // net_pnl is Money = Option<Decimal> (always Some from close_trade) and the
        // Option carries through unchanged into LiveEvent.realized_pnl.
        realized_pnl: trade.net_pnl,
        ts,
    };

    if close_qty == open_qty {
        // exact close → flat
        return ApplyResult {
            state: None,
            events: vec![close_event],
            closed_trade: Some(trade),
        };
    }

    // FLIP: open a new opposite-direction leg with the residual size.
    let residual = delta + state.szi; // signed; same sign as delta (it dominated)
    let (new_state, open_event) = open_leg(address, coin, residual, px, ts);
    ApplyResult {
        state: Some(new_state),
        events: vec![close_event, open_event],
        closed_trade: Some(trade),
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests — ported from tests/test_state.py
// ──────────────────────────────────────────────────────────────────────────

/// The pure position state machine: open / add / reduce / close / flip, PnL sign-correctness.
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::str::FromStr;

    // PORT NOTE: `D = Decimal` alias → tiny parse helper (tests feed decimal strings).
    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).expect("test literal is a valid decimal")
    }

    // PORT NOTE: module constant `TS = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)` →
    // helper fn (chrono constructors aren't const).
    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 15, 12, 0, 0).unwrap()
    }

    // PORT NOTE: `_apply` → `apply` (underscore dropped). The `ts=TS` default arg is
    // never overridden by any test, so the parameter is dropped entirely.
    fn apply(state: Option<&PositionState>, delta: &str, px: &str) -> ApplyResult {
        apply_fill(state, "0xa", "BTC", d(delta), d(px), ts())
    }

    #[test]
    fn test_fill_from_flat_opens_a_long() {
        let result = apply(None, "2", "100");
        assert!(result.state.is_some());
        // PORT NOTE: Python's attribute access on the Optional relied on non-None
        // (AttributeError otherwise) — unwrap() panics the same way.
        let state = result.state.as_ref().unwrap();
        assert_eq!(state.szi, d("2"));
        assert_eq!(state.direction, Direction::Long);
        assert_eq!(
            result.events.iter().map(|e| e.kind).collect::<Vec<_>>(),
            vec![EventKind::Open]
        );
        assert!(result.closed_trade.is_none());
    }

    #[test]
    fn test_negative_fill_from_flat_opens_a_short() {
        let result = apply(None, "-3", "100");
        let state = result.state.as_ref().unwrap();
        assert_eq!(state.szi, d("-3"));
        assert_eq!(state.direction, Direction::Short);
        assert_eq!(result.events[0].kind, EventKind::Open);
    }

    #[test]
    fn test_same_direction_fill_adds_and_blends_avg_entry() {
        let opened = apply(None, "2", "100").state;
        let result = apply(opened.as_ref(), "2", "110");
        let state = result.state.as_ref().unwrap();
        assert_eq!(state.szi, d("4"));
        assert_eq!(state.avg_entry, d("105")); // (2*100 + 2*110) / 4
        assert_eq!(result.events[0].kind, EventKind::Add);
        assert!(result.closed_trade.is_none());
    }

    #[test]
    fn test_partial_reduce_books_pnl_and_keeps_avg_entry() {
        let opened = apply(None, "4", "100").state;
        let result = apply(opened.as_ref(), "-1", "120"); // sell 1 of a long at 120
        let state = result.state.as_ref().unwrap();
        assert_eq!(state.szi, d("3"));
        assert_eq!(state.avg_entry, d("100")); // reduce does not move the basis
        assert_eq!(result.events[0].kind, EventKind::Reduce);
        assert_eq!(result.events[0].realized_pnl, Some(d("20"))); // (120-100)*1
        assert!(result.closed_trade.is_none());
    }

    #[test]
    fn test_exact_close_flattens_and_emits_completed_trade() {
        let opened = apply(None, "2", "100").state;
        let result = apply(opened.as_ref(), "-2", "150");
        assert!(result.state.is_none());
        assert_eq!(result.events[0].kind, EventKind::Close);
        assert_eq!(result.events[0].realized_pnl, Some(d("100"))); // (150-100)*2
        assert!(result.closed_trade.is_some());
        // PORT NOTE: net_pnl is Money = Option<Decimal> → compare against Some(..).
        assert_eq!(
            result.closed_trade.as_ref().unwrap().net_pnl,
            Some(d("100"))
        );
    }

    #[test]
    fn test_short_close_pnl_sign_is_correct() {
        let opened = apply(None, "-2", "100").state; // short at 100
        let result = apply(opened.as_ref(), "2", "90"); // buy back at 90 → profit
        assert!(result.state.is_none());
        assert_eq!(result.events[0].realized_pnl, Some(d("20"))); // (100-90)*2
    }

    #[test]
    fn test_flip_closes_then_opens_residual() {
        let opened = apply(None, "2", "100").state; // long 2
        let result = apply(opened.as_ref(), "-5", "120"); // sell 5 → close 2, open short 3
        assert_eq!(
            result.events.iter().map(|e| e.kind).collect::<Vec<_>>(),
            vec![EventKind::Close, EventKind::Open]
        );
        assert_eq!(result.closed_trade.as_ref().unwrap().net_pnl, Some(d("40"))); // (120-100)*2
        let state = result.state.as_ref().unwrap();
        assert_eq!(state.szi, d("-3"));
        assert_eq!(state.direction, Direction::Short);
        assert_eq!(state.avg_entry, d("120"));
    }

    #[test]
    fn test_zero_delta_is_a_noop() {
        let opened = apply(None, "2", "100").state;
        let result = apply(opened.as_ref(), "0", "100");
        // PORT NOTE: Python asserted identity (`result.state is opened`); the Rust no-op
        // path clones, so structural equality is the strongest available assertion.
        assert_eq!(result.state, opened);
        assert_eq!(result.events, vec![]);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/state.py (272 lines) + tests/test_state.py (90 lines)
//   confidence: high
//   todos:      0
//   notes:      sync pure-math module, no async. External crates: chrono, rust_decimal
//               (+ models.rs dependency). Direction/EventKind enums replace the str
//               constants per the fixed decisions — downstream modules (book, notifier,
//               listener) must match on them and use Display for the exact "Long"/"open"
//               strings; CompletedTrade.direction stays String (models.rs), converted
//               via to_string() in close_trade. apply_fill takes Option<&PositionState>
//               (book passes HashMap::get) and returns an owned next state.
// ──────────────────────────────────────────────────────────────────────────
