//! In-memory position book + trade-id de-dupe — the tracker's whole runtime state.
//!
//! Replaces the sibling project's Redis+Postgres layer. Nothing here is persisted: on restart the
//! book is rebuilt from the chain via the `clearinghouseState` seed (see [`crate::enrich`]).
//! The module itself is lock-free (methods take `&self`/`&mut self`); in the Python, one event
//! loop serialized every caller, so no lock existed anywhere. The Rust wiring instead shares
//! the book across the listener, bot, and reconcile tasks behind `Arc<Mutex<InMemoryBook>>`
//! (see [`crate::app`]) — exclusivity moves from "one event loop" to the mutex.
//
// PORT NOTE: pure sync module — no asyncio in the Python, so no tokio here (guide rule).
// The Python docstring's "single-writer by construction — no lock is needed" claim does not
// survive the port (three tokio tasks write); the fixed shared-state decision keeps this
// module lock-free and puts the Mutex at the call sites.

use std::collections::{HashMap, HashSet, VecDeque};

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;

use crate::state::{ApplyResult, PositionState, apply_fill};

// PORT NOTE: `_Key = tuple[str, str]` → private type alias; underscore dropped (privacy is
// expressed by omitting `pub`, matching state.rs's `_ZERO` → `ZERO`).
/// (address, coin), both lowercase / as-fed
type Key = (String, String);

/// A bounded FIFO set of recently-seen trade `tid` values (reconnect idempotency).
///
/// A WS reconnect can redeliver trades; re-ingesting the same trade would both corrupt the
/// position (double-counted delta) and double-notify. [`SeenTids::check_and_add`] records a
/// `tid` and reports whether it had already been seen, evicting the oldest once `maxlen` is hit.
// PORT NOTE: tid `int` → i64 — tids arrive as JSON integers and the other drafts narrow JSON
// ints via `Value::as_i64` (models.rs, registry.rs chat_id); Hyperliquid tids (~50-bit hashes)
// fit comfortably. The listener's `isinstance(tid, int)` gate becomes `Value::as_i64` there.
// PORT NOTE: maxlen `int` → usize (compared against a collection length, so bounded by
// API contract per the guide).
#[derive(Debug)]
pub struct SeenTids {
    // PORT NOTE: dropped Python's `_` privacy prefix — Rust fields are private by default.
    maxlen: usize,
    // PORT NOTE: `collections.deque` → VecDeque per the guide (unbounded deque; eviction is
    // manual, exactly as the Python did it — deque(maxlen=..) was deliberately NOT used there
    // because the set must be pruned in lockstep).
    order: VecDeque<i64>,
    set: HashSet<i64>,
}

impl SeenTids {
    pub fn new(maxlen: usize) -> Self {
        Self {
            maxlen,
            order: VecDeque::new(),
            set: HashSet::new(),
        }
    }

    /// Return `true` if `tid` was already seen; otherwise record it and return `false`.
    pub fn check_and_add(&mut self, tid: i64) -> bool {
        if self.set.contains(&tid) {
            return true;
        }
        self.set.insert(tid);
        self.order.push_back(tid);
        if self.order.len() > self.maxlen {
            // PORT NOTE: `self._set.discard(self._order.popleft())` — popleft() would raise
            // IndexError on empty, but this branch guarantees non-empty (len > maxlen >= 0),
            // so a can't-happen expect() per the fixed decisions; `discard` never errors, so
            // `remove`'s bool is ignored.
            let oldest = self
                .order
                .pop_front()
                .expect("len > maxlen implies non-empty");
            self.set.remove(&oldest);
        }
        false
    }
}

/// Per-`(address, coin)` position state + a per-`(address, coin)` leverage cache.
// PORT NOTE: derived Default mirrors the argless __init__ (all fields start empty),
// matching registry.rs. Plain HashMaps (not IndexMap): no code path iterates these where
// order is observable (lookups, insert/remove, len; drop_wallet's key scan is
// order-insensitive).
#[derive(Debug, Default)]
pub struct InMemoryBook {
    positions: HashMap<Key, PositionState>,
    // PORT NOTE: leverage `int` → i64 (models.rs narrows the JSON leverage via Value::as_i64
    // into Option<i64>; this cache stores those values).
    // PORT NOTE: field `leverage` and method `leverage()` share a name after dropping the
    // Python `_` prefix — legal in Rust (fields and methods live in separate namespaces).
    leverage: HashMap<Key, i64>,
    // Per-address fill counter, bumped on every ingest. A reconcile reseed captures it BEFORE
    // its clearinghouseState await and passes it back, so a live fill that lands during that
    // await isn't clobbered by the now-stale snapshot. See reseed_wallet(expected_epoch=...).
    // PORT NOTE: epoch `int` → u64 — a monotone non-negative counter, only bumped by 1 and
    // compared for equality.
    epoch: HashMap<String, u64>,
}

impl InMemoryBook {
    pub fn new() -> Self {
        Self::default()
    }

    // --- live ingestion ----------------------------------------------------------------

    /// Fold one resolved fill into the wallet's position and return the [`ApplyResult`].
    ///
    /// Its `events` are what the notifier pushes; `closed_trade` (set on a close/flip)
    /// carries the leg's time window so the close notification can fetch the exchange's own
    /// realized PnL. The book updates in place.
    // PORT NOTE: keyword-only params (`*, address, coin, delta, px, ts`) flattened to
    // positional — Rust has no keyword arguments (same call shape as state.rs::apply_fill).
    pub fn ingest(
        &mut self,
        address: &str,
        coin: &str,
        delta: Decimal,
        px: Decimal,
        ts: DateTime<Utc>,
    ) -> ApplyResult {
        let key: Key = (address.to_string(), coin.to_string());
        let state = self.positions.get(&key);
        let result = apply_fill(state, address, coin, delta, px, ts);
        // PORT NOTE: Python stored the SAME object in the dict and in the returned
        // ApplyResult; ApplyResult owns its state here, so the next state is CLONED into the
        // map and the caller keeps the owned result (structural equality preserved, identity
        // not — unobservable to callers). The &self.positions borrow ends at apply_fill's
        // return, so no borrowck reshaping was needed for the writes below.
        match &result.state {
            None => {
                // `self._positions.pop(key, None)` — absent is fine, return value dropped.
                self.positions.remove(&key);
            }
            Some(new_state) => {
                self.positions.insert(key, new_state.clone());
            }
        }
        // PORT NOTE: `self._epoch[address] = self._epoch.get(address, 0) + 1` → entry API
        // (guide's defaultdict/setdefault mapping).
        *self.epoch.entry(address.to_string()).or_insert(0) += 1;
        result
    }

    /// The per-address fill counter — capture it before a reconcile snapshot's await.
    pub fn fill_epoch(&self, address: &str) -> u64 {
        self.epoch.get(address).copied().unwrap_or(0)
    }

    // --- seeding / reconcile (SILENT — never emits) ------------------------------------

    /// Replace all of `address`'s positions with `states` and refresh its leverage.
    ///
    /// Used both for the cold-start seed and the periodic reconcile. Replacing (not merging)
    /// drops any coin the chain no longer reports — i.e. a position closed while we were
    /// disconnected self-heals. Emits NOTHING: these are corrections, not new activity.
    ///
    /// `expected_epoch` guards the reconcile-of-an-admitted-wallet case: if a live fill was
    /// folded in since the snapshot was requested (the fill epoch moved), the reseed is SKIPPED
    /// (returns `false`) so a stale snapshot can't clobber the fresher live state — the next
    /// reconcile cycle corrects it. `None` (the seed-before-admit path, where no fills can land
    /// because the wallet isn't in the filter yet) always applies. Returns `true` when applied.
    // PORT NOTE: `Iterable[PositionState]` → impl IntoIterator<Item = PositionState>, OWNED
    // items because the book stores them. `leverage: dict[str, int]` → owned
    // HashMap<String, i64> so each coin key moves into the (address, coin) map without a
    // clone. Keyword-only `expected_epoch: int | None = None` → required Option<u64> param
    // (guide option (b): "absent" has semantic meaning — None IS the seed-before-admit path,
    // so no default-arg helper).
    pub fn reseed_wallet(
        &mut self,
        address: &str,
        states: impl IntoIterator<Item = PositionState>,
        leverage: HashMap<String, i64>,
        expected_epoch: Option<u64>,
    ) -> bool {
        // PORT NOTE: `if expected_epoch is not None and self._epoch.get(address, 0) !=
        // expected_epoch:` → if-let over the Option (short-circuit shape preserved).
        if let Some(expected) = expected_epoch
            && self.epoch.get(address).copied().unwrap_or(0) != expected
        {
            return false;
        }
        self.drop_wallet(address);
        for state in states {
            // PORT NOTE: the coin key is cloned out of the state before the state moves into
            // the map (Python shared the str between key and object).
            self.positions
                .insert((address.to_string(), state.coin.clone()), state);
        }
        for (coin, value) in leverage {
            self.leverage.insert((address.to_string(), coin), value);
        }
        true
    }

    /// Forget every position + leverage entry for `address` (on watchlist removal).
    pub fn drop_wallet(&mut self, address: &str) {
        // PORT NOTE: Python's `for key in [k for k in dict if k[0] == address]: del dict[key]`
        // (collect-then-delete, a CPython iteration-invalidation workaround) → `retain` with
        // the predicate negated — the direct single-pass Rust form of the same filter.
        self.positions.retain(|key, _| key.0 != address);
        self.leverage.retain(|key, _| key.0 != address);
        // `self._epoch.pop(address, None)` — absent is fine.
        self.epoch.remove(address);
    }

    // --- reads -------------------------------------------------------------------------

    // PORT NOTE: returns a borrow into the book — Python returned the shared object; callers
    // (listener/notifier) only read fields. `Option<&PositionState>` is also exactly what
    // apply_fill takes, so ingest-through-position round-trips cleanly.
    // PERF(port): tuple-key lookup allocates two Strings per call — profile in Phase B
    // (a Borrow<Q> pair-key wrapper or nested maps would make it allocation-free).
    pub fn position(&self, address: &str, coin: &str) -> Option<&PositionState> {
        self.positions.get(&(address.to_string(), coin.to_string()))
    }

    // PERF(port): same two-String tuple-key lookup as position() — profile in Phase B.
    pub fn leverage(&self, address: &str, coin: &str) -> Option<i64> {
        self.leverage
            .get(&(address.to_string(), coin.to_string()))
            .copied()
    }

    // PORT NOTE: @property → inherent getter; `len(...)` → usize per the guide.
    pub fn open_position_count(&self) -> usize {
        self.positions.len()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests — ported from tests/test_book.py
// ──────────────────────────────────────────────────────────────────────────

/// In-memory book: ingest lifecycle, cold-start seeding, leverage cache, tid de-dupe.
// PORT NOTE: the Python tests are sync (no asyncio) → plain #[test], no #[tokio::test].
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::str::FromStr;

    // PORT NOTE: `from tracker.state import EVENT_ADD, EVENT_CLOSE, EVENT_OPEN` — the str
    // constants are the EventKind enum in the Rust draft (fixed shared decision).
    use crate::state::{EventKind, LiveEvent};
    // TODO(port): resolve.rs is not drafted yet — `seed_state_from_row` is assumed to land
    // with the keyword-only `fallback_ts` flattened to positional and `entry_px: Decimal |
    // None` as Option<Decimal>:
    //   pub fn seed_state_from_row(address: &str, coin: &str, szi: Decimal,
    //       entry_px: Option<Decimal>, fallback_ts: DateTime<Utc>) -> PositionState
    // Phase B must reconcile this import with the actual resolve.rs draft.
    use crate::resolve::seed_state_from_row;

    // PORT NOTE: `D = Decimal` alias → tiny parse helper (tests feed decimal strings),
    // matching state.rs's tests.
    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).expect("test literal is a valid decimal")
    }

    // PORT NOTE: module constant `TS = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)` →
    // helper fn (chrono constructors aren't const).
    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 15, 12, 0, 0).unwrap()
    }

    // PORT NOTE: `_ingest` → `ingest` (underscore dropped); returns `.events` like the
    // Python helper. Takes &mut for the book — the Python mutated it through the reference.
    fn ingest(
        book: &mut InMemoryBook,
        address: &str,
        coin: &str,
        delta: &str,
        px: &str,
    ) -> Vec<LiveEvent> {
        book.ingest(address, coin, d(delta), d(px), ts()).events
    }

    #[test]
    fn test_first_fill_on_empty_book_opens() {
        let mut book = InMemoryBook::new();
        let events = ingest(&mut book, "0xa", "BTC", "2", "100");
        assert_eq!(
            events.iter().map(|e| e.kind).collect::<Vec<_>>(),
            vec![EventKind::Open]
        );
        let pos = book.position("0xa", "BTC");
        // PORT NOTE: `assert pos is not None and pos.szi == D(2)` — the Option<&_> is Copy,
        // so is_some() + unwrap() keeps the Python's single combined assertion shape.
        assert!(pos.is_some() && pos.unwrap().szi == d("2"));
    }

    #[test]
    fn test_cold_start_seeded_position_makes_next_fill_an_add_not_open() {
        // The crux: a wallet opened a long "yesterday"; with no storage we seed from the chain.
        let mut book = InMemoryBook::new();
        // PORT NOTE: Python passed the always-set `D(90)` for `entry_px: Decimal | None` →
        // Some(d("90")).
        let seeded = seed_state_from_row("0xa", "BTC", d("5"), Some(d("90")), ts());
        // PORT NOTE: dict literal `{"BTC": 10}` → HashMap::from; the omitted
        // `expected_epoch=None` default is spelled out (no default args in Rust).
        book.reseed_wallet(
            "0xa",
            vec![seeded],
            HashMap::from([("BTC".to_string(), 10)]),
            None,
        );

        let events = ingest(&mut book, "0xa", "BTC", "1", "100"); // buys 1 more today
        assert_eq!(
            events.iter().map(|e| e.kind).collect::<Vec<_>>(),
            vec![EventKind::Add]
        ); // NOT a false "Started trade"
        let pos = book.position("0xa", "BTC");
        assert!(pos.is_some() && pos.unwrap().szi == d("6"));
        assert_eq!(book.leverage("0xa", "BTC"), Some(10));
    }

    #[test]
    fn test_close_drops_the_position_from_the_book() {
        let mut book = InMemoryBook::new();
        ingest(&mut book, "0xa", "BTC", "2", "100");
        ingest(&mut book, "0xa", "BTC", "-2", "110");
        assert!(book.position("0xa", "BTC").is_none());
        assert_eq!(book.open_position_count(), 0);
    }

    #[test]
    fn test_close_surfaces_the_completed_trade_with_the_leg_window() {
        // The close notification's authoritative-PnL lookup needs the round-trip out of ingest:
        // start_time bounds the userFillsByTime window.
        let mut book = InMemoryBook::new();
        ingest(&mut book, "0xa", "BTC", "2", "100");
        let result = book.ingest("0xa", "BTC", d("-2"), d("110"), ts());
        assert_eq!(
            result.events.iter().map(|e| e.kind).collect::<Vec<_>>(),
            vec![EventKind::Close]
        );
        assert!(result.closed_trade.is_some());
        let trade = result.closed_trade.as_ref().unwrap();
        assert_eq!(trade.start_time, ts());
        // PORT NOTE: net_pnl is Money = Option<Decimal> (models.rs) → compare against Some(..),
        // matching state.rs's ported tests.
        assert_eq!(trade.net_pnl, Some(d("20")));
    }

    #[test]
    fn test_reseed_replaces_and_drops_stale_coins() {
        let mut book = InMemoryBook::new();
        ingest(&mut book, "0xa", "BTC", "2", "100");
        ingest(&mut book, "0xa", "ETH", "3", "50");
        // Chain now only reports ETH (BTC closed while we were disconnected).
        book.reseed_wallet(
            "0xa",
            vec![seed_state_from_row(
                "0xa",
                "ETH",
                d("3"),
                Some(d("50")),
                ts(),
            )],
            HashMap::new(),
            None,
        );
        assert!(book.position("0xa", "BTC").is_none());
        let eth = book.position("0xa", "ETH");
        assert!(eth.is_some() && eth.unwrap().szi == d("3"));
    }

    #[test]
    fn test_drop_wallet_forgets_positions_and_leverage() {
        let mut book = InMemoryBook::new();
        ingest(&mut book, "0xa", "BTC", "2", "100");
        book.reseed_wallet(
            "0xa",
            vec![seed_state_from_row(
                "0xa",
                "BTC",
                d("2"),
                Some(d("100")),
                ts(),
            )],
            HashMap::from([("BTC".to_string(), 5)]),
            None,
        );
        book.drop_wallet("0xa");
        assert!(book.position("0xa", "BTC").is_none());
        // PORT NOTE: `book.leverage(...) is None` → the method returns Option<i64>, so None.
        assert_eq!(book.leverage("0xa", "BTC"), None);
    }

    #[test]
    fn test_reconcile_reseed_skips_when_a_fill_landed_since_snapshot() {
        // Models the reconcile lost-update guard: capture the epoch (as the reconcile does BEFORE
        // its clearinghouseState await), let a live fill land during the "snapshot window", then a
        // reseed with the now-stale snapshot must be skipped rather than clobber the fresher live
        // state.
        let mut book = InMemoryBook::new();
        book.reseed_wallet(
            "0xa",
            vec![seed_state_from_row(
                "0xa",
                "BTC",
                d("10"),
                Some(d("90")),
                ts(),
            )],
            HashMap::new(),
            None,
        );
        let epoch = book.fill_epoch("0xa"); // captured before the (simulated) await
        ingest(&mut book, "0xa", "BTC", "5", "100"); // live +5 lands during the window → now long 15

        let stale = vec![seed_state_from_row(
            "0xa",
            "BTC",
            d("10"),
            Some(d("90")),
            ts(),
        )];
        let applied = book.reseed_wallet("0xa", stale, HashMap::new(), Some(epoch));

        assert!(!applied); // skipped — Python `assert applied is False`
        let pos = book.position("0xa", "BTC");
        // live +5 preserved, not reverted to 10
        assert!(pos.is_some() && pos.unwrap().szi == d("15"));
    }

    #[test]
    fn test_reconcile_reseed_applies_when_no_fill_since_snapshot() {
        let mut book = InMemoryBook::new();
        ingest(&mut book, "0xa", "BTC", "2", "100");
        let epoch = book.fill_epoch("0xa");
        let applied = book.reseed_wallet(
            "0xa",
            vec![seed_state_from_row(
                "0xa",
                "BTC",
                d("3"),
                Some(d("120")),
                ts(),
            )],
            HashMap::new(),
            Some(epoch),
        );
        assert!(applied); // Python `assert applied is True`
        let pos = book.position("0xa", "BTC");
        assert!(pos.is_some() && pos.unwrap().szi == d("3")); // snapshot applied
    }

    #[test]
    fn test_seen_tids_dedupes_and_evicts() {
        // PORT NOTE: `SeenTids(maxlen=2)` keyword arg → positional new(2).
        let mut seen = SeenTids::new(2);
        assert!(!seen.check_and_add(1));
        assert!(seen.check_and_add(1)); // duplicate
        assert!(!seen.check_and_add(2));
        assert!(!seen.check_and_add(3)); // evicts tid 1 (oldest)
        assert!(!seen.check_and_add(1)); // 1 was evicted, so it's "new" again
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/book.py (129 lines) + tests/test_book.py (120 lines)
//   confidence: high
//   todos:      1
//   notes:      sync module, no async. External crates: chrono, rust_decimal (+ state.rs
//               dependency). Tests import crate::resolve::seed_state_from_row — resolve.rs
//               is NOT drafted yet; the assumed signature is documented at the use site and
//               must be reconciled in Phase B. ingest clones the next state into the map
//               (Python shared one object between book and ApplyResult). Key = (String,
//               String) tuple keys make reads allocate — PERF(port) flags at position()/
//               leverage(). Epoch counter is u64, leverage values i64, tids i64.
// ──────────────────────────────────────────────────────────────────────────
