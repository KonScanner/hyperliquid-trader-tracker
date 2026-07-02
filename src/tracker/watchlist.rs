//! The in-memory *admitted* watchlist and address normalization.
//!
//! Distinct from `tracker::db::WatchlistDB` (persistence): this is the set the listener
//! filters trades against. A wallet is *admitted* here only AFTER its cold-start seed completes,
//! which is what guarantees an add on a pre-existing position is never mis-reported as a new open.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

// PORT NOTE: `_ADDR_RE = re.compile(r"^0x[0-9a-f]{40}$")` → hand-rolled check (fixed
// decision: no regex crate for one anchored literal pattern): "0x" + exactly 40
// lowercase-hex chars. The Python pattern is fully anchored (^...$), so this is equivalent.
fn is_valid_address(addr: &str) -> bool {
    addr.len() == 42
        && addr.starts_with("0x")
        && addr.as_bytes()[2..]
            .iter()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

// PORT NOTE: `raise ValueError(...)` → per-module error enum (thiserror). One variant: the
// single ValueError raised by `normalize_address`.
#[derive(thiserror::Error, Debug)]
pub enum Error {
    // PORT NOTE: Python message used {raw!r} (repr). `{0:?}` (Debug) gives double-quoted
    // output where Python repr uses single quotes — content otherwise identical.
    #[error("not a valid 0x-address: {0:?}")]
    InvalidAddress(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Lowercase + validate an EVM address. Raises ``ValueError`` on a malformed input.
// PORT NOTE: raises → Result; `ValueError` → `Error::InvalidAddress`.
pub fn normalize_address(raw: &str) -> Result<String> {
    let addr = raw.trim().to_lowercase();
    if !is_valid_address(&addr) {
        return Err(Error::InvalidAddress(raw.to_string()));
    }
    Ok(addr)
}

/// Address→label of admitted wallets, with a cached ``frozenset`` for O(1) trade filtering.
#[derive(Debug, Clone)]
pub struct Watchlist {
    // PORT NOTE: Python `_labels`/`_addresses` (underscore-private) → non-pub fields, same order.
    // `dict[str, str]` → HashMap: the only key iteration feeds a set, so insertion order is
    // never observable — plain HashMap, not IndexMap.
    labels: HashMap<String, String>,
    // PORT NOTE: `frozenset[str]` → Arc<HashSet<String>>. The Python caches an *immutable
    // snapshot* object that holders keep across later admit/forget calls; replacing the Arc on
    // each mutation preserves exactly that snapshot semantics for anyone holding a clone.
    addresses: Arc<HashSet<String>>,
}

impl Watchlist {
    pub fn new() -> Self {
        Self {
            labels: HashMap::new(),
            addresses: Arc::new(HashSet::new()),
        }
    }

    /// Add (or relabel) an admitted wallet and refresh the filter set.
    pub fn admit(&mut self, address: &str, label: &str) {
        self.labels.insert(address.to_string(), label.to_string());
        // PERF(port): Python rebuilt the frozenset from all keys on every admit/forget (O(n));
        // mirrored here — profile in Phase B if admits are hot.
        self.addresses = Arc::new(self.labels.keys().cloned().collect());
    }

    /// Remove a wallet from the filter. Returns ``true`` if it was present.
    pub fn forget(&mut self, address: &str) -> bool {
        // PORT NOTE: `self._labels.pop(address, None) is None` → `remove(...).is_none()`.
        // (The Python truthiness pitfall of a None *value* can't occur: labels are non-optional.)
        if self.labels.remove(address).is_none() {
            return false;
        }
        self.addresses = Arc::new(self.labels.keys().cloned().collect());
        true
    }

    /// Relabel an admitted wallet. Returns ``true`` if it was present.
    pub fn rename(&mut self, address: &str, label: &str) -> bool {
        if !self.labels.contains_key(address) {
            return false;
        }
        self.labels.insert(address.to_string(), label.to_string());
        true
    }

    /// The current admitted address set (what ``resolve_deltas`` filters against).
    // PORT NOTE: @property → inherent method. Returns a cheap Arc clone of the cached
    // frozenset snapshot rather than a &-borrow, so callers (the listener) can hold it
    // across subsequent &mut mutations — matching Python object semantics.
    pub fn addresses(&self) -> Arc<HashSet<String>> {
        Arc::clone(&self.addresses)
    }

    /// The wallet's label, or a shortened address if somehow unlabeled.
    pub fn label(&self, address: &str) -> String {
        // PORT NOTE: Python `get(...) or self.short(...)` uses truthiness — an *empty* stored
        // label also falls back to the short form. Preserved explicitly (Rust has no truthiness).
        match self.labels.get(address) {
            Some(label) if !label.is_empty() => label.clone(),
            _ => Self::short(address),
        }
    }

    /// A compact ``0x1234…abcd`` rendering for display/fallback.
    // PORT NOTE: @staticmethod → associated fn (no self).
    pub fn short(address: &str) -> String {
        // PORT NOTE: Python len()/slicing count code points, not bytes; ported with chars()
        // so arbitrary (non-ASCII) input can't panic on a UTF-8 boundary. For the expected
        // ASCII hex addresses this is identical to byte slicing.
        let n = address.chars().count();
        if n >= 10 {
            let head: String = address.chars().take(6).collect();
            let tail: String = address.chars().skip(n - 4).collect();
            format!("{head}…{tail}")
        } else {
            address.to_string()
        }
    }

    // PORT NOTE: __contains__ → inherent contains() per guide (dict-like membership,
    // not indexed access).
    pub fn contains(&self, address: &str) -> bool {
        self.labels.contains_key(address)
    }

    // PORT NOTE: __len__ → inherent len().
    pub fn len(&self) -> usize {
        self.labels.len()
    }

    // PORT NOTE: no Python counterpart — added because Rust convention (clippy::len_without_is_empty)
    // expects is_empty() next to len().
    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }
}

// PORT NOTE: no Python counterpart — Default delegating to new() is the Rust convention for
// zero-arg constructors.
impl Default for Watchlist {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    //! Address normalization + the in-memory admitted watchlist.

    use super::*;

    // PORT NOTE: was module-level `ADDR = "0x" + "ab" * 20` (42-char lowercase hex);
    // a const can't call repeat(), so a helper fn.
    fn addr() -> String {
        format!("0x{}", "ab".repeat(20))
    }

    #[test]
    fn test_normalize_lowercases_valid_address() {
        let input = format!("  0x{} ", "AB".repeat(20));
        assert_eq!(normalize_address(&input).unwrap(), addr());
    }

    #[test]
    fn test_normalize_rejects_bad_address() {
        // PORT NOTE: @pytest.mark.parametrize → loop over the same cases in one test.
        let zz = format!("0x{}", "zz".repeat(20));
        for bad in ["0x123", "not-hex", zz.as_str(), ""] {
            assert!(
                matches!(normalize_address(bad), Err(Error::InvalidAddress(_))),
                "expected ValueError for {bad:?}"
            );
        }
    }

    #[test]
    fn test_admit_forget_and_addresses_snapshot() {
        let mut wl = Watchlist::new();
        let a = addr();
        wl.admit(&a, "Whale-1");
        assert!(wl.contains(&a));
        assert_eq!(*wl.addresses(), HashSet::from([a.clone()]));
        assert_eq!(wl.label(&a), "Whale-1");
        assert!(wl.forget(&a));
        assert!(!wl.forget(&a));
        assert_eq!(*wl.addresses(), HashSet::new());
    }

    #[test]
    fn test_rename_only_affects_admitted() {
        let mut wl = Watchlist::new();
        let a = addr();
        assert!(!wl.rename(&a, "X"));
        wl.admit(&a, "old");
        assert!(wl.rename(&a, "new"));
        assert_eq!(wl.label(&a), "new");
    }

    #[test]
    fn test_label_falls_back_to_short_address() {
        let wl = Watchlist::new();
        let a = addr();
        assert_eq!(wl.label(&a), format!("{}…{}", &a[..6], &a[a.len() - 4..]));
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/watchlist.py (65 lines)
//   confidence: high
//   todos:      0
//   notes:      addresses() returns Arc<HashSet<String>> (frozenset snapshot semantics) —
//               downstream modules (listener/resolve) must hold the Arc, not a &-borrow.
//               Crates: regex, thiserror. LazyLock needs Rust >= 1.80.
// ──────────────────────────────────────────────────────────────────────────
