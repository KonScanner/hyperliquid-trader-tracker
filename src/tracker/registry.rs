//! The in-memory subscriber registry + address normalization.
//!
//! Multi-tenant: each watched `address` maps to the set of subscribers tracking it, each with
//! their own label — `{address: {chat_id: label}}`. The listener filters trades against
//! [`Registry::addresses`] (the union of all tracked wallets on one firehose connection), and on
//! a lifecycle event fans the notification out to every subscriber of that address.
//!
//! An address is added to the filter ([`Registry::subscribe`]) only AFTER its cold-start seed —
//! the caller checks [`Registry::is_tracked`] first and seeds new addresses — which is what keeps
//! an add on a pre-existing position from being mis-reported as a new open.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// PORT NOTE: `_ADDR_RE = re.compile(r"^0x[0-9a-f]{40}$")` → hand-rolled check (fixed
// decision: no regex crate for one anchored literal pattern): "0x" + exactly 40
// lowercase-hex chars.
fn is_valid_address(addr: &str) -> bool {
    addr.len() == 42
        && addr.starts_with("0x")
        && addr.as_bytes()[2..]
            .iter()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

/// The error raised by this module.
///
/// PORT NOTE: `normalize_address` raised `ValueError` — decomposed into a per-module
/// error enum per the porting guide (Phase B may fold this into a crate-level enum;
/// note `exceptions.py`'s `TrackerError` hierarchy deliberately does NOT cover stdlib
/// `ValueError`, so this stays a distinct type).
#[derive(thiserror::Error, Debug)]
pub enum Error {
    // PORT NOTE: Python message was f"not a valid 0x-address: {raw!r}" — `!r` (repr)
    // becomes `{0:?}` (Debug), which quotes/escapes the string similarly.
    #[error("not a valid 0x-address: {0:?}")]
    InvalidAddress(String),
}

/// Lowercase + validate an EVM address. Raises `ValueError` on a malformed input.
// PORT NOTE: raise ValueError → Result<String, Error>. The error carries the ORIGINAL
// `raw` (pre-strip/lower), exactly as the Python f-string did.
// PORT NOTE: Python `re.match` with a `$`-anchored pattern also matches just before a
// single trailing "\n"; Rust's `$` (non-multiline) does not. Unreachable difference here
// because `.strip()`/`.trim()` runs first and removes any trailing newline.
pub fn normalize_address(raw: &str) -> Result<String, Error> {
    let addr = raw.trim().to_lowercase();
    if !is_valid_address(&addr) {
        return Err(Error::InvalidAddress(raw.to_string()));
    }
    Ok(addr)
}

/// `{address: {chat_id: label}}` with a cached `frozenset` of tracked addresses.
// PORT NOTE: derived Default mirrors the argless __init__ (all fields start empty).
#[derive(Debug, Default)]
pub struct Registry {
    // PORT NOTE: dropped Python's `_` privacy prefix — Rust fields are private by default.
    // Plain HashMaps (not IndexMap): no code path iterates these where order is observable
    // (lookups, membership, and set/dict-equality only).
    // PORT NOTE: chat_id `int` → i64 (Telegram chat ids exceed i32; Python ints are unbounded).
    subs: HashMap<String, HashMap<i64, String>>,
    // PORT NOTE: frozenset[str] → Arc<HashSet<String>> per guide — immutability via API
    // surface, and Arc preserves Python's semantics where a caller holding the old
    // frozenset keeps a stable snapshot while the registry swaps in a rebuilt one.
    addresses: Arc<HashSet<String>>,
}

impl Registry {
    pub fn new() -> Self {
        Self {
            subs: HashMap::new(),
            addresses: Arc::new(HashSet::new()),
        }
    }

    /// Whether any subscriber currently tracks `address` (i.e. it is already seeded/admitted).
    pub fn is_tracked(&self, address: &str) -> bool {
        self.subs.contains_key(address)
    }

    /// Record `chat_id` as a subscriber of `address` (admitting it to the filter if new).
    pub fn subscribe(&mut self, chat_id: i64, address: &str, label: &str) {
        let subscribers = self.subs.get_mut(address);
        match subscribers {
            None => {
                self.subs.insert(
                    address.to_string(),
                    HashMap::from([(chat_id, label.to_string())]),
                );
                // PORT NOTE: `frozenset(self._subs)` iterates the dict's KEYS — rebuild the
                // cached set from `keys()`.
                // PERF(port): rebuild clones every tracked address on each first-subscribe
                // (Python's frozenset shares the interned str objects) — profile in Phase B.
                self.addresses = Arc::new(self.subs.keys().cloned().collect());
            }
            Some(subscribers) => {
                subscribers.insert(chat_id, label.to_string());
            }
        }
    }

    /// Remove one subscriber. Returns `(existed, is_now_orphan)`.
    ///
    /// `is_now_orphan` is `true` when the last subscriber left, so the caller can drop the
    /// address from the position book and it leaves the filter.
    pub fn unsubscribe(&mut self, chat_id: i64, address: &str) -> (bool, bool) {
        // PORT NOTE: Python's single `if subscribers is None or chat_id not in subscribers`
        // is split into two early returns (let-else has no `or` with a follow-on check).
        let Some(subscribers) = self.subs.get_mut(address) else {
            return (false, false);
        };
        if !subscribers.contains_key(&chat_id) {
            return (false, false);
        }
        subscribers.remove(&chat_id);
        // PORT NOTE: `if subscribers:` is dict truthiness → explicit !is_empty().
        if !subscribers.is_empty() {
            return (true, false);
        }
        // PORT NOTE: &mut borrow of the inner map is dead by here, so re-borrowing
        // self.subs for the removal is fine under NLL — no reshape needed.
        self.subs.remove(address);
        self.addresses = Arc::new(self.subs.keys().cloned().collect());
        (true, true)
    }

    /// Relabel one subscriber's view of `address`. Returns `true` if subscribed.
    pub fn rename(&mut self, chat_id: i64, address: &str, label: &str) -> bool {
        let Some(subscribers) = self.subs.get_mut(address) else {
            return false;
        };
        if !subscribers.contains_key(&chat_id) {
            return false;
        }
        subscribers.insert(chat_id, label.to_string());
        true
    }

    /// A snapshot `{chat_id: label}` of who tracks `address` (empty if none).
    // PORT NOTE: `dict(self._subs.get(address, {}))` is a shallow copy → clone the inner map.
    // PERF(port): clones every label String (Python's dict() copy shares the str objects) —
    // profile in Phase B; a borrowed `&HashMap` return may suffice if callers don't mutate.
    pub fn subscribers(&self, address: &str) -> HashMap<i64, String> {
        self.subs.get(address).cloned().unwrap_or_default()
    }

    /// The union of tracked addresses (what `resolve_deltas` filters against).
    // PORT NOTE: @property returning the cached frozenset → inherent getter returning an
    // Arc clone: cheap, and the caller's snapshot stays valid across later mutations,
    // matching Python's shared-immutable-frozenset semantics.
    pub fn addresses(&self) -> Arc<HashSet<String>> {
        Arc::clone(&self.addresses)
    }

    /// A compact `0x1234…abcd` rendering for display/fallback.
    // PORT NOTE: @staticmethod → associated fn (no self).
    // PORT NOTE: Python slices/len by code point; used chars() so a non-ASCII fallback
    // input can't panic on a byte boundary (normalized addresses are ASCII in practice).
    // PERF(port): chars().count() walks the string twice on the long branch — profile in
    // Phase B (byte ops are equivalent for the ASCII-address hot path).
    pub fn short(address: &str) -> String {
        let n = address.chars().count();
        if n >= 10 {
            let head: String = address.chars().take(6).collect();
            let tail: String = address.chars().skip(n - 4).collect();
            format!("{head}…{tail}")
        } else {
            address.to_string()
        }
    }

    // PORT NOTE: __len__ → inherent len() (no stable Len trait). Counts tracked
    // ADDRESSES, not subscribers.
    pub fn len(&self) -> usize {
        self.subs.len()
    }

    // PORT NOTE: structural addition — clippy's len_without_is_empty pairing; the Python
    // spelled this `not registry` via __len__ truthiness.
    pub fn is_empty(&self) -> bool {
        self.subs.is_empty()
    }
}

// ──────────────────────────────────────────────────────────────────────────
// Tests — ported from tests/test_registry.py
// "Address normalization + the multi-tenant subscriber registry."
// ──────────────────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use super::*;

    // PORT NOTE: module constants ADDR = "0x" + "ab" * 20 need runtime repeat() → LazyLock.
    static ADDR: LazyLock<String> = LazyLock::new(|| format!("0x{}", "ab".repeat(20)));
    static ADDR2: LazyLock<String> = LazyLock::new(|| format!("0x{}", "cd".repeat(20)));

    #[test]
    fn test_normalize_lowercases_valid_address() {
        assert_eq!(
            normalize_address(&format!("  0x{} ", "AB".repeat(20))).unwrap(),
            *ADDR
        );
    }

    // PORT NOTE: @pytest.mark.parametrize → loop over the same cases in one test.
    #[test]
    fn test_normalize_rejects_bad_address() {
        for bad in ["0x123", "not-hex", &format!("0x{}", "zz".repeat(20)), ""] {
            // PORT NOTE: pytest.raises(ValueError) → assert the Err variant.
            assert!(
                matches!(normalize_address(bad), Err(Error::InvalidAddress(_))),
                "expected rejection of {bad:?}"
            );
        }
    }

    #[test]
    fn test_first_subscribe_tracks_address_others_join() {
        let mut reg = Registry::new();
        assert!(!reg.is_tracked(&ADDR));
        reg.subscribe(1, &ADDR, "Alice-W");
        assert!(reg.is_tracked(&ADDR));
        assert_eq!(*reg.addresses(), HashSet::from([ADDR.clone()]));

        reg.subscribe(2, &ADDR, "Bob-W"); // same wallet, second subscriber, own label
        assert_eq!(
            reg.subscribers(&ADDR),
            HashMap::from([(1, "Alice-W".to_string()), (2, "Bob-W".to_string())])
        );
        assert_eq!(reg.len(), 1); // still one tracked address
    }

    #[test]
    fn test_unsubscribe_orphan_semantics_drive_book_cleanup() {
        let mut reg = Registry::new();
        reg.subscribe(1, &ADDR, "A");
        reg.subscribe(2, &ADDR, "B");

        assert_eq!(reg.unsubscribe(1, &ADDR), (true, false)); // existed, not orphan (2 still follows)
        assert!(reg.is_tracked(&ADDR));
        assert_eq!(reg.unsubscribe(2, &ADDR), (true, true)); // existed, now orphan → caller drops book
        assert!(!reg.is_tracked(&ADDR));
        assert_eq!(*reg.addresses(), HashSet::new());
        assert_eq!(reg.unsubscribe(1, &ADDR), (false, false)); // already gone
    }

    #[test]
    fn test_rename_only_affects_subscribed_chat() {
        let mut reg = Registry::new();
        assert!(!reg.rename(1, &ADDR, "x"));
        reg.subscribe(1, &ADDR, "old");
        reg.subscribe(2, &ADDR, "other");
        assert!(reg.rename(1, &ADDR, "new"));
        assert_eq!(
            reg.subscribers(&ADDR),
            HashMap::from([(1, "new".to_string()), (2, "other".to_string())])
        );
    }

    #[test]
    fn test_addresses_is_union_across_subscribers() {
        let mut reg = Registry::new();
        reg.subscribe(1, &ADDR, "A");
        reg.subscribe(2, &ADDR2, "B");
        assert_eq!(
            *reg.addresses(),
            HashSet::from([ADDR.clone(), ADDR2.clone()])
        );
    }

    #[test]
    fn test_short_renders_compact_address() {
        // ADDR is ASCII, so byte slicing here mirrors the Python test's code-point slice.
        assert_eq!(
            Registry::short(&ADDR),
            format!("{}…{}", &ADDR[..6], &ADDR[ADDR.len() - 4..])
        );
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/registry.py (85 lines) + tests/test_registry.py (63 lines)
//   confidence: high
//   todos:      0
//   notes:      sync module, no async. External crates: regex, thiserror. Uses
//               std::sync::LazyLock (needs Rust >= 1.80). Local Error enum stands in for
//               ValueError — Phase B decides whether to fold into a crate-level enum
//               (keep it distinct from the TrackerError family in exceptions.py).
// ──────────────────────────────────────────────────────────────────────────
