//! Notification formatting + multi-tenant dispatch.
//!
//! [`format_event`] is a pure function (fully unit-tested); [`Notifier`] applies the lifecycle
//! filter and fans one lifecycle event out to every subscriber of that wallet, each in their own
//! chat and with their own label. The sender is a Protocol (a trait in this port) so the Telegram
//! binding lives entirely in `tracker::bot` and the core stays dependency-free and testable with
//! a fake.
//
// PORT NOTE: async module (`async def send` / `async def dispatch`) → tokio + async-trait per
// the fixed port decisions (runtime: tokio, fixed — no `TODO(port): runtime` needed).
// PORT NOTE: `logger = logging.getLogger(__name__)` disappears — `tracing` macros are
// free-standing and carry the module path automatically (same as retry.rs).

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use rust_decimal::Decimal;

// PORT NOTE: the EVENT_* str constants arrive as the EventKind enum (state.rs fixed decision);
// equality/membership tests become `==` / `matches!` on variants, and the defensive fallback
// in format_event renders the kind via Display ("open"/"add"/"reduce"/"close").
use crate::state::{EventKind, LiveEvent};

/// What a sender may fail with — the `except Exception` contract, spelled as a type.
// PORT NOTE: Notifier.dispatch catches bare `Exception`, i.e. the sender may raise *anything*
// and the only handling is log-and-drop. The type-erased Box<dyn Error> is the faithful shape:
// the guide's "no Box<dyn Error> in library code" rule targets errors callers pattern-match
// on, and no caller ever matches this one. bot.rs's TelegramSender boxes its client error.
pub type SendError = Box<dyn std::error::Error + Send + Sync>;

/// Anything that can deliver one rendered notification to one chat.
// PORT NOTE: `@runtime_checkable class MessageSender(Protocol)` → trait (guide rule:
// Protocol → trait). runtime_checkable only enabled isinstance() checks, which Rust replaces
// with compile-time bounds — nothing else to carry. `Send + Sync` because the one Notifier is
// shared across tokio tasks (listener + bot) and #[async_trait] futures must be Send.
// PORT NOTE: chat_id `int` → i64 (Telegram chat ids are i64 — fixed decision, cf.
// telegram_setup.rs and registry.rs); `text: str` param → &str; `-> None` that may raise →
// Result<(), SendError>.
#[async_trait]
pub trait MessageSender: Send + Sync {
    async fn send(&self, chat_id: i64, text: &str) -> Result<(), SendError>;
}

/// Render a Decimal without trailing zeros or scientific notation (``2.50`` -> ``2.5``).
// PORT NOTE: `_trim` → `trim` (underscore dropped, module-private via no `pub`).
// `format(d.normalize(), "f")` — Python's normalize() can go exponential (Decimal("100")
// → 1E+2) and the "f" format re-expands it; rust_decimal's normalize() only strips trailing
// fractional zeros and its Display never uses scientific notation, so `.to_string()` alone
// lands on the same text.
pub(crate) fn trim(d: Decimal) -> String {
    d.normalize().to_string()
}

/// Thousands-grouped fixed-point, i.e. Python's ``f"{value:,.{dp}f}"``.
// PORT NOTE: helper extracted (structural divergence — the Python inlined `:,.2f` in two
// f-strings) because std::fmt has no thousands separator. round_dp defaults to
// MidpointNearestEven — the same banker's rounding Decimal.__format__ applies
// (ROUND_HALF_EVEN) — so the `{:.*}` afterwards only zero-pads, never re-rounds.
pub(crate) fn comma(d: Decimal, dp: u32) -> String {
    let rounded = d.round_dp(dp);
    let unsigned = format!("{:.*}", dp as usize, rounded.abs());
    // `{:.0}` renders no dot, so the fractional part is optional.
    let (int_part, frac_part) = match unsigned.split_once('.') {
        Some((int_part, frac_part)) => (int_part, Some(frac_part)),
        None => (unsigned.as_str(), None),
    };
    let mut grouped = String::with_capacity(int_part.len() + int_part.len() / 3);
    for (i, ch) in int_part.chars().enumerate() {
        if i > 0 && (int_part.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    let sign = if rounded.is_sign_negative() { "-" } else { "" };
    match frac_part {
        Some(frac_part) => format!("{sign}{grouped}.{frac_part}"),
        None => format!("{sign}{grouped}"),
    }
}

/// Signed USD PnL, e.g. ``+$2,760.00`` / ``-$440.00``; ``?`` when unknown.
// PORT NOTE: `_pnl` → `pnl` (underscore dropped). `Decimal | None` → Option<Decimal>
// (Decimal is Copy, so by value). `d >= 0` holds for Decimal("-0") in both languages
// (compares equal to zero) → "+".
pub(crate) fn pnl(d: Option<Decimal>) -> String {
    let Some(d) = d else {
        return "?".to_string();
    };
    let sign = if d >= Decimal::ZERO { "+" } else { "-" };
    format!("{sign}${}", comma(d.abs(), 2))
}

/// Escape user-supplied text before it goes into an HTML-parsed message.
// Same five replacements as Python's `html.escape(s, quote=True)`, in the same order.
// Lives here (not bot.rs) because notifications are now HTML too and the label is
// user-supplied; bot.rs imports it.
pub(crate) fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

/// Render one lifecycle event as a compact two-line HTML push message.
///
/// Line 1 says who did what (bold label, bold coin, direction, leverage when known —
/// unknown leverage is omitted rather than rendered as "?x"); line 2 carries the numbers.
/// The subscriber's label is user-supplied and therefore HTML-escaped; the sender must
/// deliver with `parse_mode=HTML` (bot.rs's TelegramSender does).
// PORT NOTE: keyword-only params (`*, label, leverage, mark`) flattened to positional — Rust
// has no keyword arguments (same convention as state.rs).
// PORT NOTE: `leverage: int | None` → Option<i64>, matching the fixed dependencies that feed
// it (models.rs `leverage_value: Option<i64>`, book.leverage). `event` by reference —
// dispatch renders the same event once per recipient.
pub fn format_event(
    event: &LiveEvent,
    label: &str,
    leverage: Option<i64>,
    mark: Option<Decimal>,
) -> String {
    let lev = leverage.map(|l| format!(" {l}x")).unwrap_or_default();
    let label = escape_html(label);
    let coin = escape_html(&event.coin);
    let direction = event.direction;
    let size = trim(event.delta.abs());
    let px = trim(event.px);
    // Approximate traded notional from the live mark (absent until the first allMids tick).
    let ntl = match mark {
        Some(mark) => format!(" (~${})", comma(event.delta.abs() * mark, 0)),
        None => String::new(),
    };

    match event.kind {
        EventKind::Open => {
            format!("🟢 <b>{label}</b> opened <b>{coin}</b> {direction}{lev}\n{size} @ {px}{ntl}")
        }
        EventKind::Add => {
            let total = trim(event.szi_after.abs());
            format!(
                "➕ <b>{label}</b> added <b>{coin}</b> {direction}{lev}\n\
                 +{size} @ {px}{ntl} → {total} total"
            )
        }
        EventKind::Reduce => {
            let left = trim(event.szi_after.abs());
            format!(
                "➖ <b>{label}</b> reduced <b>{coin}</b> {direction}{lev}\n\
                 -{size} @ {px} · PnL {} · {left} left",
                pnl(event.realized_pnl)
            )
        }
        EventKind::Close => {
            format!(
                "🔴 <b>{label}</b> closed <b>{coin}</b> {direction}{lev}\n\
                 {size} @ {px} · PnL {}",
                pnl(event.realized_pnl)
            )
        }
    }
}

/// A [`MessageSender`] that logs instead of pushing — the no-token fallback.
// PORT NOTE: Python's LoggingSender satisfied the Protocol structurally (duck typing); Rust
// traits are nominal, so the impl is explicit.
#[derive(Debug, Default)]
pub struct LoggingSender;

#[async_trait]
impl MessageSender for LoggingSender {
    async fn send(&self, chat_id: i64, text: &str) -> Result<(), SendError> {
        tracing::info!("NOTIFY chat={chat_id}: {text}");
        Ok(())
    }
}

/// Applies the reduce/close mute toggle, then renders + fans out to each subscriber.
// PORT NOTE: `_sender` / `_notify_reduce_close` → underscore dropped; privacy is the absence
// of `pub` on the fields.
pub struct Notifier {
    // PORT NOTE: `sender: MessageSender` (a Protocol-typed reference) → Arc<dyn MessageSender>:
    // app.py picks TelegramSender vs LoggingSender at *runtime* (so dyn, not a generic), and
    // Arc reproduces Python's shared object reference (the tests keep aliasing the fake after
    // handing it over).
    sender: Arc<dyn MessageSender>,
    notify_reduce_close: bool,
}

impl Notifier {
    // PORT NOTE: keyword-only `*, notify_reduce_close` flattened to positional.
    pub fn new(sender: Arc<dyn MessageSender>, notify_reduce_close: bool) -> Self {
        Self {
            sender,
            notify_reduce_close,
        }
    }

    /// Send ``event`` to every ``{chat_id: label}`` recipient (their own label per chat).
    // PORT NOTE: `recipients: dict[int, str]` → &HashMap<i64, String> — registry.rs's
    // subscribers() returns exactly this (plain HashMap per its fixed decision: no code path
    // observes iteration order — the Python dict iterated in insertion order, but delivery
    // order across chats is not observable behaviour).
    pub async fn dispatch(
        &self,
        event: &LiveEvent,
        recipients: &HashMap<i64, String>,
        leverage: Option<i64>,
        mark: Option<Decimal>,
    ) {
        // PORT NOTE: `event.kind in (EVENT_REDUCE, EVENT_CLOSE)` → matches! on the enum.
        if matches!(event.kind, EventKind::Reduce | EventKind::Close) && !self.notify_reduce_close {
            return;
        }
        for (chat_id, label) in recipients {
            let text = format_event(event, label, leverage, mark);
            // delivery is best-effort — a failed push must not kill the listener
            // PORT NOTE: `try/except Exception` → if let Err over the type-erased SendError;
            // `logger.exception` (error line + traceback) → tracing::error! with the error's
            // Display appended (Rust has no traceback to attach).
            if let Err(err) = self.sender.send(*chat_id, &text).await {
                tracing::error!(
                    "notification send failed; dropping (chat={chat_id}): {text}: {err}"
                );
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests — ported from tests/test_notifier.py
// ──────────────────────────────────────────────────────────────────────────

/// Notification formatting + multi-tenant fan-out + the reduce/close mute toggle.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::Direction;
    use chrono::{DateTime, TimeZone, Utc};
    use std::collections::HashSet;
    use std::str::FromStr;
    use std::sync::Mutex;

    // PORT NOTE: `D = Decimal` alias → tiny parse helper (tests feed decimal strings).
    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).expect("test literal is a valid decimal")
    }

    // PORT NOTE: module constant `TS = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)` →
    // helper fn (chrono constructors aren't const).
    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 15, 12, 0, 0).unwrap()
    }

    // PORT NOTE: `_event` → `event` (underscore dropped). Keyword-only defaults flattened to
    // positional: `szi_after="0"` and `realized=None` ARE overridden by some tests, so the
    // params stay and every call site passes them explicitly; `direction="Long"` is never
    // overridden, so the parameter is dropped entirely (same convention as state.rs's ts).
    fn event(kind: EventKind, delta: &str, szi_after: &str, realized: Option<&str>) -> LiveEvent {
        LiveEvent {
            kind,
            address: "0xa".to_string(),
            coin: "BTC".to_string(),
            direction: Direction::Long,
            delta: d(delta),
            px: d("63120"),
            szi_after: d(szi_after),
            realized_pnl: realized.map(d),
            ts: ts(),
        }
    }

    // PORT NOTE: `_FakeSender` → `FakeSender`. `self.sent: list[tuple[int, str]]` grows
    // through `&self` in async send → std Mutex interior mutability (never held across an
    // await). `__init__`'s empty list → derive(Default).
    #[derive(Default)]
    struct FakeSender {
        sent: Mutex<Vec<(i64, String)>>,
    }

    #[async_trait]
    impl MessageSender for FakeSender {
        async fn send(&self, chat_id: i64, text: &str) -> Result<(), SendError> {
            self.sent.lock().unwrap().push((chat_id, text.to_string()));
            Ok(())
        }
    }

    // --- format_event (pure) ----------------------------------------------------------------

    #[test]
    fn test_open_format_leads_with_label_and_shows_leverage() {
        let text = format_event(
            &event(EventKind::Open, "2.5", "2.5", None),
            "Whale-1",
            Some(10),
            None,
        );
        assert_eq!(
            text,
            "🟢 <b>Whale-1</b> opened <b>BTC</b> Long 10x\n2.5 @ 63120"
        );
    }

    #[test]
    fn test_add_format_includes_notional_from_mark_and_running_total() {
        let text = format_event(
            &event(EventKind::Add, "1", "3", None),
            "Whale-1",
            Some(10),
            Some(d("63120")),
        );
        assert_eq!(
            text,
            "➕ <b>Whale-1</b> added <b>BTC</b> Long 10x\n+1 @ 63120 (~$63,120) → 3 total"
        );
    }

    #[test]
    fn test_add_format_without_mark_or_leverage_omits_both() {
        let text = format_event(&event(EventKind::Add, "1", "3", None), "W", None, None);
        assert_eq!(
            text,
            "➕ <b>W</b> added <b>BTC</b> Long\n+1 @ 63120 → 3 total"
        );
    }

    #[test]
    fn test_reduce_format_shows_realized_and_remaining() {
        let text = format_event(
            &event(EventKind::Reduce, "-0.5", "2", Some("440")),
            "Whale-1",
            Some(10),
            None,
        );
        assert_eq!(
            text,
            "➖ <b>Whale-1</b> reduced <b>BTC</b> Long 10x\n-0.5 @ 63120 · PnL +$440.00 · 2 left"
        );
    }

    #[test]
    fn test_close_format_shows_negative_pnl() {
        let text = format_event(
            &event(EventKind::Close, "-2", "0", Some("-1250.5")),
            "W",
            Some(10),
            None,
        );
        assert_eq!(
            text,
            "🔴 <b>W</b> closed <b>BTC</b> Long 10x\n2 @ 63120 · PnL -$1,250.50"
        );
    }

    #[test]
    fn test_label_is_html_escaped() {
        let text = format_event(
            &event(EventKind::Open, "1", "1", None),
            "<Whale & 'co'>",
            None,
            None,
        );
        assert!(text.starts_with("🟢 <b>&lt;Whale &amp; &#x27;co&#x27;&gt;</b> opened"));
    }

    // --- Notifier dispatch (fan-out) --------------------------------------------------------

    #[tokio::test]
    async fn test_dispatch_fans_out_to_each_subscriber_with_their_own_label() {
        let sender = Arc::new(FakeSender::default());
        // PORT NOTE: Python passed the same object it kept a name for; the Arc clone is that
        // shared reference.
        let notifier = Notifier::new(sender.clone(), true);
        let recipients: HashMap<i64, String> =
            HashMap::from([(1, "Alice-W".to_string()), (2, "Bob-W".to_string())]);
        notifier
            .dispatch(
                &event(EventKind::Open, "2", "0", None),
                &recipients,
                Some(10),
                None,
            )
            .await;
        // PORT NOTE: `dict(sender.sent)` — later entries win on duplicate chat ids, exactly
        // what collect() into a HashMap does.
        let by_chat: HashMap<i64, String> = sender.sent.lock().unwrap().iter().cloned().collect();
        assert_eq!(
            by_chat.keys().copied().collect::<HashSet<_>>(),
            HashSet::from([1, 2])
        );
        assert!(by_chat[&1].contains("Alice-W"));
        assert!(by_chat[&2].contains("Bob-W"));
    }

    #[tokio::test]
    async fn test_dispatch_mutes_reduce_close_when_disabled() {
        let sender = Arc::new(FakeSender::default());
        let notifier = Notifier::new(sender.clone(), false);
        let recipients: HashMap<i64, String> = HashMap::from([(1, "W".to_string())]);
        notifier
            .dispatch(
                &event(EventKind::Close, "-2", "0", Some("5")),
                &recipients,
                Some(1),
                None,
            )
            .await;
        notifier
            .dispatch(
                &event(EventKind::Open, "2", "0", None),
                &recipients,
                Some(1),
                None,
            )
            .await;
        let sent = sender.sent.lock().unwrap();
        assert_eq!(sent.len(), 1);
        assert!(sent[0].1.starts_with("🟢"));
    }

    #[tokio::test]
    async fn test_dispatch_swallows_sender_failure_and_continues_to_next_recipient() {
        // PORT NOTE: `_FlakySender` stays local to the test, like the Python class did.
        // `raise RuntimeError("telegram down")` → a boxed string error — SendError erases the
        // type exactly as `except Exception` erased the class.
        struct FlakySender {
            ok: Mutex<Vec<i64>>,
        }

        #[async_trait]
        impl MessageSender for FlakySender {
            async fn send(&self, chat_id: i64, _text: &str) -> Result<(), SendError> {
                if chat_id == 1 {
                    return Err("telegram down".into());
                }
                self.ok.lock().unwrap().push(chat_id);
                Ok(())
            }
        }

        let sender = Arc::new(FlakySender {
            ok: Mutex::new(Vec::new()),
        });
        let notifier = Notifier::new(sender.clone(), true);
        // Chat 1 fails, chat 2 must still receive it — best-effort delivery.
        let recipients: HashMap<i64, String> =
            HashMap::from([(1, "A".to_string()), (2, "B".to_string())]);
        notifier
            .dispatch(
                &event(EventKind::Open, "2", "0", None),
                &recipients,
                Some(1),
                None,
            )
            .await;
        assert_eq!(*sender.ok.lock().unwrap(), vec![2]);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/notifier.py (106 lines) + tests/test_notifier.py (125 lines)
//   confidence: high
//   todos:      0
//   notes:      MessageSender Protocol → #[async_trait] trait, send returns
//               Result<(), SendError> where SendError = Box<dyn Error + Send + Sync>
//               (the `except Exception` contract — bot.rs's TelegramSender must box its
//               client error into it). Notifier holds Arc<dyn MessageSender> (runtime
//               sender choice in app.py; tests alias the fake). Python's f"{x:,.2f}"
//               hand-rolled in comma() (std fmt lacks thousands grouping); rounding via
//               round_dp = banker's, matching Decimal.__format__. leverage int|None →
//               Option<i64> (models.rs leverage_value). Crates: async-trait, rust_decimal,
//               tracing (+ tokio, chrono in tests).
//   divergence: format_event has since moved past the Python original — it now renders a
//               two-line HTML message (escaped label, bold label/coin, leverage omitted
//               when unknown, running total on adds) delivered with parse_mode=HTML.
// ──────────────────────────────────────────────────────────────────────────
