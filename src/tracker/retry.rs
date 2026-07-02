//! Shared exponential-backoff-with-jitter helper for the HTTP client.
//!
//! Ported from the sibling `hyperdash-crawl` project (its `retry.py`), unchanged in
//! behaviour: retry only transient statuses, honour a server `Retry-After` hint, escalate
//! hint-less throttles via exponential growth.

use std::collections::HashSet;
use std::sync::LazyLock;
use std::time::Duration;

// PORT NOTE: rand 0.10 moved `random_range` onto the `RngExt` extension trait.
use rand::RngExt;

use crate::config::Settings;
use crate::exceptions::{Result, TrackerError};

// PORT NOTE: `logger = logging.getLogger(__name__)` disappears — `tracing` macros are
// free-standing and carry the module path automatically.

/// Single source of truth for which HTTP statuses warrant a retry. 501 (Not Implemented) is
/// intentionally excluded — it is a permanent error, not transient.
// PORT NOTE: frozenset[int] → LazyLock<HashSet<u16>>; immutability is expressed by the
// non-mut static (guide's &'static-HashSet frozenset rule). int → u16 because HTTP status
// codes are bounded (and reqwest's `StatusCode::as_u16()` is what callers will compare).
// PERF(port): frozenset membership test on 5 small ints — a linear scan over a const
// [u16; 5] may beat hashing; profile in Phase B.
pub static RETRYABLE_STATUS: LazyLock<HashSet<u16>> =
    LazyLock::new(|| HashSet::from([429, 500, 502, 503, 504]));

/// Sleep with exponential backoff + jitter and return the next attempt number.
///
/// Raises `err` once `max_retries` is exhausted, so callers can write a simple
/// `attempt = backoff_or_raise(...).await?` retry loop.
// PORT NOTE: `attempt: int` → u32 to match `Settings::max_retries` (config.rs narrowed it);
// the return narrows identically. `raise err` → `return Err(err)`, so the function takes
// `err` by value (`TrackerError`, not Python's untyped `Exception` — per the fixed port
// decisions, the only errors flowing through this path are TrackerError values).
pub async fn backoff_or_raise(
    attempt: u32,
    settings: &Settings,
    label: &str,
    err: TrackerError,
) -> Result<u32> {
    if attempt >= settings.max_retries {
        tracing::error!("{label}: giving up after {attempt} retries");
        return Err(err);
    }
    // Honour a server-supplied Retry-After (rate-limit hint); otherwise exponential backoff.
    // A hint-less rate limit arrives as retry_after == 0.0, so `if retry_after:` lets it fall
    // through to exponential growth rather than backing off by jitter alone.
    // PORT NOTE: `getattr(err, "retry_after", None)` → match on the RateLimited variant;
    // every other variant has no retry_after field, i.e. the getattr default of None.
    let retry_after = match &err {
        TrackerError::RateLimited { retry_after, .. } => *retry_after,
        _ => None,
    };
    // PORT NOTE: Python's `if retry_after:` is truthiness — BOTH None and 0.0 fall through
    // to the exponential branch; the explicit `ra != 0.0` guard mirrors that 0.0 nuance
    // exactly (fixed port decision).
    // PORT NOTE: random.uniform(0, b) samples the CLOSED interval [0, b] — use the inclusive
    // range form of rand 0.9's random_range; the half-open `0.0..b` would also panic on an
    // empty range when backoff_base_s == 0.0 (legal: its constraint is ge=0.0), whereas
    // Python returns 0.0 there.
    let delay = match retry_after {
        Some(ra) if ra != 0.0 => {
            // PORT NOTE: `float(retry_after)` is a no-op — retry_after is already f64.
            ra + rand::rng().random_range(0.0..=settings.backoff_base_s)
        }
        _ => {
            // PORT NOTE: Python's `2**attempt` is an unbounded int; `2f64.powi` saturates to
            // +inf for huge exponents, which `min(backoff_cap_s, ...)` clamps identically.
            let mut delay = settings
                .backoff_cap_s
                .min(settings.backoff_base_s * 2f64.powi(attempt as i32));
            delay += rand::rng().random_range(0.0..=settings.backoff_base_s);
            delay
        }
    };
    // PORT NOTE: %s on the exception is str(err) → Display ({err}); %.1f → {delay:.1}.
    tracing::warn!(
        "{label}: transient failure ({err}); retry {} in {delay:.1}s",
        attempt + 1
    );
    tokio::time::sleep(Duration::from_secs_f64(delay)).await;
    Ok(attempt + 1)
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/retry.py (41 lines)
//   confidence: high
//   todos:      0
//   notes:      err param narrowed Exception → TrackerError (fixed decision); jitter uses
//               rand 0.9 inclusive range to match random.uniform's closed interval and to
//               survive backoff_base_s == 0.0. Crates: tokio, rand, tracing, thiserror
//               (via exceptions.rs).
// ──────────────────────────────────────────────────────────────────────────
