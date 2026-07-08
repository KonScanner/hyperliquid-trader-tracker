//! Notification formatting + multi-tenant, edit-in-place dispatch.
//!
//! Rather than pushing one message per fill, the tracker keeps ONE Telegram message per
//! `(chat, wallet, coin)` position lifecycle and **edits it in place** as fills arrive
//! (open → add → reduce → close). [`format_card`] is a pure function (fully unit-tested) that
//! renders the current state of that card; [`Notifier`] owns the per-subscriber card map and
//! decides, per event, whether to send a fresh message or edit the live one, fanning out to
//! every subscriber of the wallet in their own chat with their own label. The sender is a trait
//! so the Telegram binding lives entirely in `tracker::bot` and the core stays Telegram-free and
//! testable with a fake.
//
// PORT NOTE: async module (`async def send` / `async def dispatch`) → tokio + async-trait per
// the fixed port decisions (runtime: tokio, fixed — no `TODO(port): runtime` needed).
// PORT NOTE: `logger = logging.getLogger(__name__)` disappears — `tracing` macros are
// free-standing and carry the module path automatically (same as retry.rs).

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use chrono::Timelike;
use rust_decimal::Decimal;

use crate::models::CompletedTrade;
// PORT NOTE: the EVENT_* str constants arrive as the EventKind enum (state.rs fixed decision);
// equality/membership tests become `==` / `matches!` on variants.
use crate::state::{EventKind, LiveEvent};

/// What a sender may fail with — the `except Exception` contract, spelled as a type.
// PORT NOTE: Notifier.dispatch catches bare `Exception`, i.e. the sender may raise *anything*
// and the only handling is log-and-drop. The type-erased Box<dyn Error> is the faithful shape:
// the guide's "no Box<dyn Error> in library code" rule targets errors callers pattern-match
// on, and no caller ever matches this one. bot.rs's TelegramSender boxes its client error.
pub type SendError = Box<dyn std::error::Error + Send + Sync>;

/// Anything that can deliver — and later edit — one rendered notification in one chat.
///
/// Because the tracker keeps a single message per position lifecycle and edits it in place as
/// fills arrive, a sender must both create a message (returning its id, so a later fill can
/// target it) and edit an existing one.
// PORT NOTE: `@runtime_checkable class MessageSender(Protocol)` → trait (guide rule:
// Protocol → trait). `Send + Sync` because the one Notifier is shared across tokio tasks
// (listener + bot) and #[async_trait] futures must be Send. chat_id `int` → i64 (Telegram
// chat ids are i64 — fixed decision); `text: str` → &str.
// DIVERGENCE (this feature): the Python `send(chat_id, text) -> None` grew a `-> message_id`
// return and a sibling `edit(chat_id, message_id, text)` so the notifier can update one card in
// place instead of sending a new message per fill.
#[async_trait]
pub trait MessageSender: Send + Sync {
    /// Deliver a new message; returns its id so a later fill can edit it in place.
    async fn send(&self, chat_id: i64, text: &str) -> Result<i64, SendError>;
    /// Edit a previously-sent message in place (a silent update — it never pings the user).
    async fn edit(&self, chat_id: i64, message_id: i64, text: &str) -> Result<(), SendError>;
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
// Lives here (not bot.rs) because notifications are HTML too and the label is
// user-supplied; bot.rs imports it.
pub(crate) fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#x27;")
}

/// Base URL of the Hyperliquid (HyperCore) explorer — public trades land here, not on HyperEVM.
const EXPLORER_TX: &str = "https://app.hyperliquid.xyz/explorer/tx/";

/// Everything needed to render + route one lifecycle card, minus the per-recipient label.
///
/// Built once per event by the listener and rendered per subscriber (each with their own label).
pub struct EventContext<'a> {
    pub event: &'a LiveEvent,
    /// Position leverage when known — omitted from the card rather than rendered as `?x`.
    pub leverage: Option<i64>,
    /// Live mark for the coin (drives the notional; falls back to the fill price when absent).
    pub mark: Option<Decimal>,
    /// The fill's on-chain tx hash, for a "View TX" explorer link.
    pub tx_hash: Option<&'a str>,
    /// The completed round-trip on a close/flip — entry/exit/size/duration for the close card.
    pub trade: Option<&'a CompletedTrade>,
}

/// Render one lifecycle event as a compact, position-aware HTML card.
///
/// The listener keeps ONE message per `(chat, wallet, coin)` lifecycle and edits it in place as
/// fills arrive, so this same renderer produces the open card, each add/reduce update, and the
/// final closed card. Line 1 is the header (label · coin · direction · leverage); line 2 is the
/// action + resulting position; the footer carries the copyable full address, a View TX link,
/// and the fill time. The subscriber's label is user-supplied and HTML-escaped; the sender must
/// deliver with `parse_mode=HTML` (bot.rs's TelegramSender does).
// PORT NOTE: this has moved well past the Python `format_event` two-liner — see the module doc.
pub fn format_card(ctx: &EventContext<'_>, label: &str) -> String {
    let event = ctx.event;
    let lev = ctx.leverage.map(|l| format!(" {l}x")).unwrap_or_default();
    let label = escape_html(label);
    let coin = escape_html(&event.coin);
    let direction = event.direction;
    let px = trim(event.px);
    // Notional uses the live mark when we have it, else the fill price (a close approximation
    // until the first allMids tick lands).
    let mark = ctx.mark.unwrap_or(event.px);

    let (emoji, body) = match event.kind {
        EventKind::Open => {
            let size = trim(event.szi_after.abs());
            let notional = comma(event.szi_after.abs() * mark, 0);
            ("🟢", format!("Opened {size} @ {px} (≈${notional})"))
        }
        EventKind::Add => {
            let added = trim(event.delta.abs());
            let total = trim(event.szi_after.abs());
            let notional = comma(event.szi_after.abs() * mark, 0);
            (
                "➕",
                format!("Added +{added} @ {px} → {total} total (≈${notional})"),
            )
        }
        EventKind::Reduce => {
            let reduced = trim(event.delta.abs());
            let left = trim(event.szi_after.abs());
            (
                "➖",
                format!(
                    "Reduced -{reduced} @ {px} → {left} left · realized {}",
                    pnl(event.realized_pnl)
                ),
            )
        }
        EventKind::Close => ("🔴", format_close_body(ctx, &px)),
    };

    let header = format!("{emoji} <b>{label}</b> · <b>{coin}</b> {direction}{lev}");
    // The full address in <code> is tap-to-copy (a truncated 0x…abcd form would copy the
    // ellipsis, not a usable address — the same reasoning as the /list and /positions views).
    let mut footer = format!("👤 <code>{}</code>", escape_html(&event.address));
    let time = format!("{:02}:{:02} UTC", event.ts.hour(), event.ts.minute());
    match ctx.tx_hash {
        Some(hash) if !hash.is_empty() => footer.push_str(&format!(
            "\n🔗 <a href=\"{EXPLORER_TX}{}\">View TX</a> · 🕒 {time}",
            escape_html(hash)
        )),
        _ => footer.push_str(&format!("\n🕒 {time}")),
    }
    format!("{header}\n{body}\n{footer}")
}

/// The two body lines of a closed-position card: `Closed {size} · {entry} → {exit}` and a
/// PnL / ROI / holding-time line. Entry/exit/size/duration come from the completed round-trip;
/// the money figure is the event's realized PnL (which the listener may have replaced with the
/// exchange's authoritative `closedPnl`).
fn format_close_body(ctx: &EventContext<'_>, fill_px: &str) -> String {
    let event = ctx.event;
    let (entry, exit, size, held) = match ctx.trade {
        Some(trade) => (
            trade
                .avg_entry_px
                .map(trim)
                .unwrap_or_else(|| fill_px.to_string()),
            trade
                .avg_exit_px
                .map(trim)
                .unwrap_or_else(|| fill_px.to_string()),
            trade
                .size
                .map(|s| trim(s.abs()))
                .unwrap_or_else(|| trim(event.delta.abs())),
            fmt_duration(trade.duration_mins),
        ),
        None => (
            fill_px.to_string(),
            fill_px.to_string(),
            trim(event.delta.abs()),
            String::new(),
        ),
    };
    let money = event.realized_pnl;
    let up = money.map(|m| m >= Decimal::ZERO).unwrap_or(true);
    let money_emoji = if up { "💰" } else { "🔻" };
    let roi = close_roi(money, ctx.trade, ctx.leverage);
    let held = if held.is_empty() {
        String::new()
    } else {
        format!(" · held {held}")
    };
    format!(
        "Closed {size} · {entry} → {exit}\n{money_emoji} {}{roi}{held}",
        pnl(money)
    )
}

/// Leveraged return on margin as a signed percentage suffix, e.g. `" (+21.9%)"`; `""` when it
/// can't be computed (unknown leverage, missing size/entry, or a zero-notional leg).
fn close_roi(
    pnl: Option<Decimal>,
    trade: Option<&CompletedTrade>,
    leverage: Option<i64>,
) -> String {
    let (Some(pnl), Some(trade), Some(lev)) = (pnl, trade, leverage) else {
        return String::new();
    };
    let (Some(size), Some(entry)) = (trade.size, trade.avg_entry_px) else {
        return String::new();
    };
    let entry_notional = size.abs() * entry;
    if entry_notional.is_zero() || lev <= 0 {
        return String::new();
    }
    // ROI on margin = price move × leverage = (pnl / entry_notional) × leverage.
    let roi = (pnl / entry_notional * Decimal::from(lev) * Decimal::from(100)).round_dp(1);
    let roi = roi.normalize();
    // A negative value already prints its own '-', so only the non-negative case needs a sign.
    let sign = if roi.is_sign_negative() { "" } else { "+" };
    format!(" ({sign}{roi}%)")
}

/// Compact holding time: `45m`, `3h12m`, `2d3h`.
fn fmt_duration(mins: i64) -> String {
    let mins = mins.max(0);
    let (days, hours, minutes) = (mins / 1440, (mins % 1440) / 60, mins % 60);
    if days > 0 {
        format!("{days}d{hours}h")
    } else if hours > 0 {
        format!("{hours}h{minutes}m")
    } else {
        format!("{minutes}m")
    }
}

/// A [`MessageSender`] that logs instead of pushing — the no-token fallback.
// PORT NOTE: Python's LoggingSender satisfied the Protocol structurally (duck typing); Rust
// traits are nominal, so the impl is explicit.
#[derive(Debug, Default)]
pub struct LoggingSender {
    // Hands back monotonically increasing ids so the notifier's edit-tracking still works in
    // log-only mode (edits are just logged).
    next_id: AtomicI64,
}

#[async_trait]
impl MessageSender for LoggingSender {
    async fn send(&self, chat_id: i64, text: &str) -> Result<i64, SendError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::info!("NOTIFY chat={chat_id} (new msg {id}): {text}");
        Ok(id)
    }

    async fn edit(&self, chat_id: i64, message_id: i64, text: &str) -> Result<(), SendError> {
        tracing::info!("NOTIFY chat={chat_id} (edit msg {message_id}): {text}");
        Ok(())
    }
}

/// Renders + routes each lifecycle event, keeping one editable card per position lifecycle.
// PORT NOTE: `_sender` / `_notify_reduce_close` → underscore dropped; privacy is the absence
// of `pub` on the fields.
pub struct Notifier {
    // PORT NOTE: `sender: MessageSender` (a Protocol-typed reference) → Arc<dyn MessageSender>:
    // app.py picks TelegramSender vs LoggingSender at *runtime* (so dyn, not a generic), and
    // Arc reproduces Python's shared object reference (the tests keep aliasing the fake after
    // handing it over).
    sender: Arc<dyn MessageSender>,
    notify_reduce_close: bool,
    // (chat_id, address, coin) → message id of that subscriber's live position card. In memory
    // only: message ids can't be recovered after a restart, and Telegram won't edit messages
    // past its edit window anyway — a new lifecycle simply opens a fresh card. A close removes
    // its key, so the map holds at most one entry per currently-open (subscriber, position).
    cards: Mutex<HashMap<(i64, String, String), i64>>,
}

impl Notifier {
    // PORT NOTE: keyword-only `*, notify_reduce_close` flattened to positional.
    pub fn new(sender: Arc<dyn MessageSender>, notify_reduce_close: bool) -> Self {
        Self {
            sender,
            notify_reduce_close,
            cards: Mutex::new(HashMap::new()),
        }
    }

    /// Route `event` to every `{chat_id: label}` recipient, editing each one's live card in
    /// place (or opening a fresh one).
    // PORT NOTE: `recipients: dict[int, str]` → &HashMap<i64, String> — registry.rs's
    // subscribers() returns exactly this. The Python early-returned to mute reduce/close;
    // that mute now lives per-recipient in `deliver` (see `can_create_new`) so a muted exit
    // still *edits* an existing card silently — it only suppresses a *new* ping.
    pub async fn dispatch(&self, ctx: &EventContext<'_>, recipients: &HashMap<i64, String>) {
        let event = ctx.event;
        // Opens and adds may always create a fresh message; reduces/closes only when exits
        // aren't muted. Editing an existing card is always allowed — a Telegram edit is silent.
        let can_create_new = match event.kind {
            EventKind::Open | EventKind::Add => true,
            EventKind::Reduce | EventKind::Close => self.notify_reduce_close,
        };
        for (chat_id, label) in recipients {
            let text = format_card(ctx, label);
            let key = (*chat_id, event.address.clone(), event.coin.clone());
            self.deliver(*chat_id, &key, event.kind, &text, can_create_new)
                .await;
        }
    }

    /// Deliver one card to one chat: send-fresh on an open, otherwise edit the live card in
    /// place (falling back to a fresh send when the message is gone and a new one is allowed).
    // PORT NOTE: GIL-free — every `cards` lock is scoped to a single statement and dropped
    // before the send/edit await (a std Mutex must never be held across an await; the
    // listener/bot lock-scope rule).
    async fn deliver(
        &self,
        chat_id: i64,
        key: &(i64, String, String),
        kind: EventKind,
        text: &str,
        can_create_new: bool,
    ) {
        // An open always starts a fresh card — any stale id for this (wallet, coin) is replaced.
        if kind == EventKind::Open {
            self.send_and_track(chat_id, key, kind, text).await;
            return;
        }

        let existing = self
            .cards
            .lock()
            .expect("cards mutex poisoned")
            .get(key)
            .copied();

        if let Some(message_id) = existing {
            match self.sender.edit(chat_id, message_id, text).await {
                // The lifecycle ended — forget the card so the next open starts clean.
                Ok(()) if kind == EventKind::Close => {
                    self.cards.lock().expect("cards mutex poisoned").remove(key);
                }
                Ok(()) => {}
                Err(err) => {
                    // The message is gone (deleted by the user, or past Telegram's edit window)
                    // — drop the dead id and, if a new message is allowed, resend it fresh.
                    self.cards.lock().expect("cards mutex poisoned").remove(key);
                    tracing::warn!("card edit failed (chat={chat_id} msg={message_id}): {err}");
                    if can_create_new {
                        self.send_and_track(chat_id, key, kind, text).await;
                    }
                }
            }
            return;
        }

        // No live card to edit. Create one only when allowed — muting exits suppresses a *new*
        // reduce/close message (which would ping), never the silent edit above.
        if can_create_new {
            self.send_and_track(chat_id, key, kind, text).await;
        }
    }

    /// Send `text` as a new message and remember its id for later edits. A close is delivered
    /// but not tracked — its lifecycle is already over.
    async fn send_and_track(
        &self,
        chat_id: i64,
        key: &(i64, String, String),
        kind: EventKind,
        text: &str,
    ) {
        match self.sender.send(chat_id, text).await {
            Ok(message_id) => {
                if kind != EventKind::Close {
                    self.cards
                        .lock()
                        .expect("cards mutex poisoned")
                        .insert(key.clone(), message_id);
                }
            }
            // delivery is best-effort — a failed push must not kill the listener
            Err(err) => {
                tracing::error!("card send failed; dropping (chat={chat_id}): {text}: {err}");
            }
        }
    }
}

// ──────────────────────────────────────────────────────────────────────────
// tests — Rust-only for the edit-mode cards (diverged from tests/test_notifier.py, which
// still pins the Python two-liner + early-return mute).
// ──────────────────────────────────────────────────────────────────────────

/// Card rendering + multi-tenant fan-out + the edit-in-place lifecycle.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::TRADE_SOURCE_LIVE;
    use crate::state::Direction;
    use chrono::{DateTime, TimeZone, Utc};
    use std::collections::HashSet;
    use std::str::FromStr;

    fn d(s: &str) -> Decimal {
        Decimal::from_str(s).expect("test literal is a valid decimal")
    }

    fn ts() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 6, 15, 12, 0, 0).unwrap()
    }

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

    /// A completed round-trip for the close-card tests.
    fn trade(size: &str, entry: &str, exit: &str, duration_mins: i64, net: &str) -> CompletedTrade {
        CompletedTrade {
            address: "0xa".to_string(),
            coin: "BTC".to_string(),
            direction: Direction::Long.to_string(),
            start_time: ts(),
            end_time: ts(),
            duration_mins,
            size: Some(d(size)),
            avg_entry_px: Some(d(entry)),
            avg_exit_px: Some(d(exit)),
            gross_pnl: Some(d(net)),
            funding_pnl: None,
            total_fees: None,
            net_pnl: Some(d(net)),
            source: TRADE_SOURCE_LIVE.to_string(),
        }
    }

    fn ctx<'a>(
        event: &'a LiveEvent,
        leverage: Option<i64>,
        mark: Option<Decimal>,
        tx_hash: Option<&'a str>,
        trade: Option<&'a CompletedTrade>,
    ) -> EventContext<'a> {
        EventContext {
            event,
            leverage,
            mark,
            tx_hash,
            trade,
        }
    }

    fn recipients(pairs: &[(i64, &str)]) -> HashMap<i64, String> {
        pairs.iter().map(|(c, l)| (*c, l.to_string())).collect()
    }

    // --- format_card (pure) -----------------------------------------------------------------

    #[test]
    fn test_open_card_leads_with_label_shows_leverage_and_notional() {
        let e = event(EventKind::Open, "2.5", "2.5", None);
        let text = format_card(&ctx(&e, Some(10), None, None, None), "Whale-1");
        assert_eq!(
            text,
            "🟢 <b>Whale-1</b> · <b>BTC</b> Long 10x\n\
             Opened 2.5 @ 63120 (≈$157,800)\n\
             👤 <code>0xa</code>\n\
             🕒 12:00 UTC"
        );
    }

    #[test]
    fn test_add_card_shows_running_total_and_tx_link() {
        let e = event(EventKind::Add, "1", "3", None);
        let text = format_card(
            &ctx(&e, Some(10), Some(d("63120")), Some("0xdead"), None),
            "W",
        );
        assert_eq!(
            text,
            "➕ <b>W</b> · <b>BTC</b> Long 10x\n\
             Added +1 @ 63120 → 3 total (≈$189,360)\n\
             👤 <code>0xa</code>\n\
             🔗 <a href=\"https://app.hyperliquid.xyz/explorer/tx/0xdead\">View TX</a> · 🕒 12:00 UTC"
        );
    }

    #[test]
    fn test_add_card_without_mark_or_leverage_uses_fill_price_and_omits_leverage() {
        let e = event(EventKind::Add, "1", "3", None);
        let text = format_card(&ctx(&e, None, None, None, None), "W");
        assert_eq!(
            text,
            "➕ <b>W</b> · <b>BTC</b> Long\n\
             Added +1 @ 63120 → 3 total (≈$189,360)\n\
             👤 <code>0xa</code>\n\
             🕒 12:00 UTC"
        );
    }

    #[test]
    fn test_reduce_card_shows_realized_and_remaining() {
        let e = event(EventKind::Reduce, "-0.5", "2", Some("440"));
        let text = format_card(&ctx(&e, Some(10), None, None, None), "Whale-1");
        assert_eq!(
            text,
            "➖ <b>Whale-1</b> · <b>BTC</b> Long 10x\n\
             Reduced -0.5 @ 63120 → 2 left · realized +$440.00\n\
             👤 <code>0xa</code>\n\
             🕒 12:00 UTC"
        );
    }

    #[test]
    fn test_close_card_shows_entry_exit_pnl_roi_and_duration() {
        let e = event(EventKind::Close, "-2", "0", Some("2760"));
        let t = trade("2", "63120", "64500", 192, "2760");
        let text = format_card(&ctx(&e, Some(10), None, Some("0xbeef"), Some(&t)), "W");
        assert_eq!(
            text,
            "🔴 <b>W</b> · <b>BTC</b> Long 10x\n\
             Closed 2 · 63120 → 64500\n\
             💰 +$2,760.00 (+21.9%) · held 3h12m\n\
             👤 <code>0xa</code>\n\
             🔗 <a href=\"https://app.hyperliquid.xyz/explorer/tx/0xbeef\">View TX</a> · 🕒 12:00 UTC"
        );
    }

    #[test]
    fn test_close_card_negative_pnl_uses_loss_emoji_and_signed_roi() {
        let e = event(EventKind::Close, "-2", "0", Some("-1250.5"));
        let t = trade("2", "63120", "62000", 45, "-1250.5");
        let text = format_card(&ctx(&e, Some(10), None, None, Some(&t)), "W");
        assert!(text.contains("🔻 -$1,250.50 (-9.9%) · held 45m"), "{text}");
    }

    #[test]
    fn test_close_card_without_leverage_omits_roi() {
        let e = event(EventKind::Close, "-2", "0", Some("2760"));
        let t = trade("2", "63120", "64500", 90, "2760");
        let text = format_card(&ctx(&e, None, None, None, Some(&t)), "W");
        assert!(text.contains("💰 +$2,760.00 · held 1h30m"), "{text}");
        assert!(!text.contains('%'), "{text}");
    }

    #[test]
    fn test_label_is_html_escaped() {
        let e = event(EventKind::Open, "1", "1", None);
        let text = format_card(&ctx(&e, None, None, None, None), "<Whale & 'co'>");
        assert!(text.starts_with("🟢 <b>&lt;Whale &amp; &#x27;co&#x27;&gt;</b> · <b>BTC</b> Long"));
    }

    // --- edit-in-place dispatch -------------------------------------------------------------

    #[derive(Default)]
    struct FakeSender {
        sent: Mutex<Vec<(i64, String)>>,
        edited: Mutex<Vec<(i64, i64, String)>>,
        next_id: AtomicI64,
    }

    #[async_trait]
    impl MessageSender for FakeSender {
        async fn send(&self, chat_id: i64, text: &str) -> Result<i64, SendError> {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
            self.sent.lock().unwrap().push((chat_id, text.to_string()));
            Ok(id)
        }
        async fn edit(&self, chat_id: i64, message_id: i64, text: &str) -> Result<(), SendError> {
            self.edited
                .lock()
                .unwrap()
                .push((chat_id, message_id, text.to_string()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_open_sends_then_add_edits_the_same_message() {
        let sender = Arc::new(FakeSender::default());
        let notifier = Notifier::new(sender.clone(), true);
        let rcpt = recipients(&[(1, "W")]);

        let open = event(EventKind::Open, "2", "2", None);
        notifier
            .dispatch(&ctx(&open, Some(10), None, None, None), &rcpt)
            .await;
        let add = event(EventKind::Add, "1", "3", None);
        notifier
            .dispatch(&ctx(&add, Some(10), None, None, None), &rcpt)
            .await;

        assert_eq!(sender.sent.lock().unwrap().len(), 1); // only the open created a message
        let edited = sender.edited.lock().unwrap();
        assert_eq!(edited.len(), 1);
        assert_eq!(edited[0].0, 1); // chat
        assert_eq!(edited[0].1, 1); // the id the open's send returned
        assert!(edited[0].2.contains("Added +1"));
    }

    #[tokio::test]
    async fn test_dispatch_fans_out_with_each_label_then_edits_each() {
        let sender = Arc::new(FakeSender::default());
        let notifier = Notifier::new(sender.clone(), true);
        let rcpt = recipients(&[(1, "Alice-W"), (2, "Bob-W")]);

        let open = event(EventKind::Open, "2", "2", None);
        notifier
            .dispatch(&ctx(&open, Some(10), None, None, None), &rcpt)
            .await;
        let reduce = event(EventKind::Reduce, "-1", "1", Some("5"));
        notifier
            .dispatch(&ctx(&reduce, Some(10), None, None, None), &rcpt)
            .await;

        let sent = sender.sent.lock().unwrap();
        let by_chat: HashMap<i64, String> = sent.iter().cloned().collect();
        assert_eq!(
            by_chat.keys().copied().collect::<HashSet<_>>(),
            HashSet::from([1, 2])
        );
        assert!(by_chat[&1].contains("Alice-W"));
        assert!(by_chat[&2].contains("Bob-W"));
        assert_eq!(sender.edited.lock().unwrap().len(), 2); // both cards edited on the reduce
    }

    #[tokio::test]
    async fn test_muted_exits_still_edit_but_never_create_a_new_message() {
        let sender = Arc::new(FakeSender::default());
        let notifier = Notifier::new(sender.clone(), false); // exits muted
        let rcpt = recipients(&[(1, "W")]);

        let open = event(EventKind::Open, "2", "2", None);
        notifier
            .dispatch(&ctx(&open, Some(1), None, None, None), &rcpt)
            .await;
        let t = trade("2", "100", "150", 5, "100");
        let close = event(EventKind::Close, "-2", "0", Some("100"));
        notifier
            .dispatch(&ctx(&close, Some(1), None, None, Some(&t)), &rcpt)
            .await;

        assert_eq!(sender.sent.lock().unwrap().len(), 1); // no new message on the muted close
        let edited = sender.edited.lock().unwrap();
        assert_eq!(edited.len(), 1);
        assert!(edited[0].2.starts_with("🔴")); // the card was still finalized silently
    }

    #[tokio::test]
    async fn test_muted_exit_with_no_live_card_is_skipped_entirely() {
        let sender = Arc::new(FakeSender::default());
        let notifier = Notifier::new(sender.clone(), false);
        let rcpt = recipients(&[(1, "W")]);
        let reduce = event(EventKind::Reduce, "-1", "1", Some("5"));
        notifier
            .dispatch(&ctx(&reduce, Some(1), None, None, None), &rcpt)
            .await;
        assert!(sender.sent.lock().unwrap().is_empty());
        assert!(sender.edited.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_close_forgets_the_card_so_a_new_open_starts_fresh() {
        let sender = Arc::new(FakeSender::default());
        let notifier = Notifier::new(sender.clone(), true);
        let rcpt = recipients(&[(1, "W")]);

        let open = event(EventKind::Open, "2", "2", None);
        notifier
            .dispatch(&ctx(&open, Some(1), None, None, None), &rcpt)
            .await;
        let t = trade("2", "100", "150", 5, "100");
        let close = event(EventKind::Close, "-2", "0", Some("100"));
        notifier
            .dispatch(&ctx(&close, Some(1), None, None, Some(&t)), &rcpt)
            .await;
        // a brand-new open on the same (wallet, coin) must create a *new* message, not edit
        let open2 = event(EventKind::Open, "3", "3", None);
        notifier
            .dispatch(&ctx(&open2, Some(1), None, None, None), &rcpt)
            .await;

        assert_eq!(sender.sent.lock().unwrap().len(), 2); // two opens → two messages
        assert_eq!(sender.edited.lock().unwrap().len(), 1); // only the close edited
    }

    #[tokio::test]
    async fn test_dispatch_swallows_send_failure_and_continues_to_next_recipient() {
        struct FlakySender {
            ok: Mutex<Vec<i64>>,
            next_id: AtomicI64,
        }
        #[async_trait]
        impl MessageSender for FlakySender {
            async fn send(&self, chat_id: i64, _text: &str) -> Result<i64, SendError> {
                if chat_id == 1 {
                    return Err("telegram down".into());
                }
                self.ok.lock().unwrap().push(chat_id);
                Ok(self.next_id.fetch_add(1, Ordering::Relaxed) + 1)
            }
            async fn edit(&self, _c: i64, _m: i64, _t: &str) -> Result<(), SendError> {
                Ok(())
            }
        }
        let sender = Arc::new(FlakySender {
            ok: Mutex::new(Vec::new()),
            next_id: AtomicI64::new(0),
        });
        let notifier = Notifier::new(sender.clone(), true);
        let rcpt = recipients(&[(1, "A"), (2, "B")]);
        let open = event(EventKind::Open, "2", "2", None);
        notifier
            .dispatch(&ctx(&open, Some(1), None, None, None), &rcpt)
            .await;
        assert_eq!(*sender.ok.lock().unwrap(), vec![2]);
    }

    #[tokio::test]
    async fn test_edit_failure_resends_as_a_new_card() {
        // A sender whose edit always fails (e.g. the user deleted the message).
        struct EditFails {
            sent: Mutex<u32>,
            next_id: AtomicI64,
        }
        #[async_trait]
        impl MessageSender for EditFails {
            async fn send(&self, _c: i64, _t: &str) -> Result<i64, SendError> {
                *self.sent.lock().unwrap() += 1;
                Ok(self.next_id.fetch_add(1, Ordering::Relaxed) + 1)
            }
            async fn edit(&self, _c: i64, _m: i64, _t: &str) -> Result<(), SendError> {
                Err("message to edit not found".into())
            }
        }
        let sender = Arc::new(EditFails {
            sent: Mutex::new(0),
            next_id: AtomicI64::new(0),
        });
        let notifier = Notifier::new(sender.clone(), true);
        let rcpt = recipients(&[(1, "W")]);

        let open = event(EventKind::Open, "2", "2", None);
        notifier
            .dispatch(&ctx(&open, Some(1), None, None, None), &rcpt)
            .await; // send #1
        let add = event(EventKind::Add, "1", "3", None);
        notifier
            .dispatch(&ctx(&add, Some(1), None, None, None), &rcpt)
            .await; // edit fails → send #2
        assert_eq!(*sender.sent.lock().unwrap(), 2);
    }
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/notifier.py (106 lines) + tests/test_notifier.py (125 lines)
//   confidence: high
//   todos:      0
//   notes:      MessageSender Protocol → #[async_trait] trait. Python's f"{x:,.2f}" is
//               hand-rolled in comma() (std fmt lacks thousands grouping); pnl/trim/escape_html
//               unchanged. Notifier holds Arc<dyn MessageSender> (runtime sender choice in
//               app.py). Crates: async-trait, chrono (Timelike), rust_decimal, tracing (+ tokio
//               in tests).
//   divergence: this module now implements EDIT-IN-PLACE position cards (this feature request).
//               `send` returns the new message id and `edit` was added to MessageSender; the
//               Notifier keeps an in-memory (chat, addr, coin) → message-id map and, per event,
//               either opens a fresh card (open, or add/reduce/close with no live card) or edits
//               the live one (silent — so a muted reduce/close still keeps the card accurate,
//               it just never creates a new pinging message). format_event → format_card renders
//               a compact position-aware card (header + action/position line + copyable address
//               + View TX explorer link + time; the close card adds entry→exit, realized PnL,
//               leveraged ROI, and holding time). Tests are Rust-only and diverge from
//               tests/test_notifier.py (the Python still pins the old two-liner + early-return
//               mute).
// ──────────────────────────────────────────────────────────────────────────
