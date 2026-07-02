//! The tracker's exception hierarchy.
//!
//! Everything raised by the transport/enrichment layers descends from [`TrackerError`]
//! so callers can catch the whole family with one `except` without swallowing unrelated
//! `ValueError`/`RuntimeError` from the stdlib.
//!
//! PORT NOTE: the Python class hierarchy (base `TrackerError` with `ParseError`,
//! `RateLimitedError`, `AuthRequiredError` subclasses) collapses into a single
//! `thiserror` enum. "Catch the whole family" becomes matching on `TrackerError`
//! itself; "catch one subclass" becomes matching a single variant
//! (e.g. `TrackerError::RateLimited { .. }`).

/// Base class for every error this package raises.
///
/// PORT NOTE: each Python subclass is one variant. Every Python exception
/// carried a positional `message` (via `Exception.__init__`), so every variant
/// carries a `String` message; `Display` (the `#[error]` string) reproduces
/// Python's `str(exc)` == message behavior.
#[derive(thiserror::Error, Debug)]
pub enum TrackerError {
    /// A response could not be parsed into the shape we expected (bad/absent JSON, wrong type).
    #[error("{0}")]
    Parse(String),

    /// A transient failure worth retrying: HTTP 429/5xx or a transport error.
    ///
    /// Carries an optional `retry_after` (seconds) parsed from a server hint; `None` means
    /// "no hint, use exponential backoff".
    #[error("{message}")]
    RateLimited {
        message: String,
        // PORT NOTE: was keyword-only default arg `retry_after: float | None = None`
        // in `RateLimitedError.__init__`; construct with `retry_after: None` when
        // the server gave no hint.
        retry_after: Option<f64>,
    },

    /// A permanent 401/403 — never retried. Should not happen on the public read API.
    #[error("{0}")]
    AuthRequired(String),
}

// PORT NOTE: convenience alias per porting guide — modules that raised
// TrackerError subclasses return this from fallible functions.
pub type Result<T> = std::result::Result<T, TrackerError>;

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/exceptions.py (31 lines)
//   confidence: high
//   todos:      0
//   notes:      hierarchy flattened to one thiserror enum; RateLimited keeps
//               retry_after as Option<f64>; needs `thiserror` in Cargo.toml.
// ──────────────────────────────────────────────────────────────────────────
