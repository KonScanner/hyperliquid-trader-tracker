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
use chrono::{DateTime, Timelike, Utc};
use rust_decimal::Decimal;
use serde_json::{Value, json};

use crate::models::{AccountPosition, CompletedTrade};
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
    /// Deliver a new message; returns its id so a later fill can edit it in place. `reply_markup`
    /// carries the optional inline keyboard (the 🔄 Update P&L button on live-position cards).
    async fn send(
        &self,
        chat_id: i64,
        text: &str,
        reply_markup: Option<&Value>,
    ) -> Result<i64, SendError>;
    /// Edit a previously-sent message in place (a silent update — it never pings the user).
    async fn edit(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        reply_markup: Option<&Value>,
    ) -> Result<(), SendError>;
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

/// Group an unsigned integer-part string into thousands: `"63120"` -> `"63,120"`.
fn group_int(int_part: &str) -> String {
    let mut grouped = String::with_capacity(int_part.len() + int_part.len() / 3);
    for (i, ch) in int_part.chars().enumerate() {
        if i > 0 && (int_part.len() - i).is_multiple_of(3) {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    grouped
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
    let sign = if rounded.is_sign_negative() { "-" } else { "" };
    match frac_part {
        Some(frac_part) => format!("{sign}{}.{frac_part}", group_int(int_part)),
        None => format!("{sign}{}", group_int(int_part)),
    }
}

/// A price with its integer part thousands-grouped but full precision kept: `1.6444` stays
/// `1.6444`, `63120` becomes `63,120`. Unlike [`comma`] it never rounds — a price is a number a
/// user may want exact, so only the grouping is cosmetic.
pub(crate) fn money_px(px: Decimal) -> String {
    let s = trim(px.abs());
    let (int_part, frac) = match s.split_once('.') {
        Some((int_part, frac)) => (int_part, Some(frac)),
        None => (s.as_str(), None),
    };
    let sign = if px.is_sign_negative() { "-" } else { "" };
    match frac {
        Some(frac) => format!("{sign}{}.{frac}", group_int(int_part)),
        None => format!("{sign}{}", group_int(int_part)),
    }
}

/// Abbreviate a magnitude for the *scanned* notional: `834`, `157.8k`, `1.2M`. The exact figure
/// is never the copyable one (sizes and prices stay full-precision), so 1-dp is safe here.
pub(crate) fn notional_short(v: Decimal) -> String {
    let v = v.abs();
    let k = Decimal::from(1_000);
    let m = Decimal::from(1_000_000);
    if v < k {
        comma(v, 0)
    } else if v < m {
        format!("{}k", (v / k).round_dp(1).normalize())
    } else {
        format!("{}M", (v / m).round_dp(1).normalize())
    }
}

/// Signed, abbreviated dollar figure for aggregates, e.g. `+$3.1k`, `-$320`, `+$1.2M`.
pub(crate) fn money_short(d: Decimal) -> String {
    let sign = if d.is_sign_negative() { "-" } else { "+" };
    format!("{sign}${}", notional_short(d.abs()))
}

/// Signed whole-dollar PnL, thousands-grouped: `+$1,329`, `-$320`; `?` when unknown. The card
/// grids favour whole dollars over cents — the leader's realized figure reads as a headline, and
/// the exact value is one tap away on the TX.
// PORT NOTE: `d >= 0` holds for Decimal("-0") (compares equal to zero) → "+".
pub(crate) fn pnl_whole(d: Option<Decimal>) -> String {
    let Some(d) = d else {
        return "?".to_string();
    };
    let sign = if d >= Decimal::ZERO { "+" } else { "-" };
    format!("{sign}${}", comma(d.abs(), 0))
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

/// Explorer address page — the trader name links here, so a card needs no raw-address line.
pub(crate) const EXPLORER_ADDR: &str = "https://app.hyperliquid.xyz/explorer/address/";

/// The bold, tap-through trader name used in every card header:
/// `<b><a href="…/address/{addr}">{label}</a></b>`. Both arguments are HTML-escaped (the label is
/// user-supplied; the address is hex but escaped for uniformity). Shared by `format_card`,
/// `format_live_position`, and the bot's `/positions` view so the header markup lives in one place.
pub(crate) fn linked_name(address: &str, label: &str) -> String {
    format!(
        "<b><a href=\"{EXPLORER_ADDR}{}\">{}</a></b>",
        escape_html(address),
        escape_html(label)
    )
}

/// A card's `HH:MM UTC` meta stamp.
fn hhmm_utc(dt: DateTime<Utc>) -> String {
    format!("{:02}:{:02} UTC", dt.hour(), dt.minute())
}

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
    /// Blended average entry of the currently-open leg, when known — drives the ENTRY row on an
    /// add and the ROI on a reduce (a reduce books PnL against this basis, not the fill price).
    pub avg_entry: Option<Decimal>,
}

/// A key/value row in a card's monospace grid: an 8-wide label column, then the value. Eight is
/// the widest label used (`NOTIONAL`, `REALIZED`), so every value lands on the same edge.
fn row(label: &str, value: &str) -> String {
    format!("{label:<8} {value}")
}

/// Render one lifecycle event as a compact, terminal-style HTML card.
///
/// The listener keeps ONE message per `(chat, wallet, coin)` lifecycle and edits it in place as
/// fills arrive, so this same renderer produces the open card, each add/reduce update, and the
/// final closed card. Line 1 is the header (linked trader name · coin · direction · leverage); a
/// monospace `<pre>` grid aligns the numbers a copy-trader acts on; the meta line carries a
/// View TX link and the fill time. The trader name links to the wallet's explorer page, so the
/// raw address needs no line of its own. The subscriber's label is user-supplied and
/// HTML-escaped; the sender must deliver with `parse_mode=HTML` (bot.rs's TelegramSender does).
// PORT NOTE: this has moved well past the Python `format_event` two-liner — see the module doc.
pub fn format_card(ctx: &EventContext<'_>, label: &str) -> String {
    let event = ctx.event;
    let lev = ctx.leverage.map(|l| format!(" {l}x")).unwrap_or_default();
    let coin = escape_html(&event.coin);
    // Header side is upper-cased ("LONG"/"SHORT") for the terminal look; Display gives "Long".
    let dir = event.direction.to_string().to_uppercase();
    // Notional uses the live mark when we have it, else the fill price (a close approximation
    // until the first allMids tick lands).
    let mark = ctx.mark.unwrap_or(event.px);
    let size = trim(event.szi_after.abs());
    let notional = notional_short(event.szi_after.abs() * mark);

    let (emoji, mut rows) = match event.kind {
        EventKind::Open => (
            "🟢",
            vec![
                row("ENTRY", &format!("${}", money_px(event.px))),
                row("SIZE", &format!("{size} {coin}")),
                row("NOTIONAL", &format!("${notional}")),
            ],
        ),
        EventKind::Add => {
            let added = trim(event.delta.abs());
            let pct = pct_of_position(event.delta, event.szi_after, "+")
                .map(|p| format!("  {p}"))
                .unwrap_or_default();
            let mut rows = vec![
                row("ADDED", &format!("+{added} {coin}{pct}")),
                row("FILL", &format!("${}", money_px(event.px))),
                row("SIZE", &format!("{size} {coin}")),
            ];
            // Blended basis after scaling in — only when the listener plumbed it through.
            if let Some(entry) = ctx.avg_entry {
                rows.push(row("ENTRY", &format!("${}", money_px(entry))));
            }
            rows.push(row("NOTIONAL", &format!("${notional}")));
            ("➕", rows)
        }
        EventKind::Reduce => {
            let reduced = trim(event.delta.abs());
            let pct = pct_of_position(event.delta, event.szi_after, "-")
                .map(|p| format!("  {p}"))
                .unwrap_or_default();
            // ROI on a reduce is booked against the reduced slice's entry notional.
            let entry_notional = ctx.avg_entry.map(|e| event.delta.abs() * e);
            let mut rows = vec![
                row("REDUCED", &format!("-{reduced} {coin}{pct}")),
                row("FILL", &format!("${}", money_px(event.px))),
                row("REALIZED", &pnl_whole(event.realized_pnl)),
            ];
            if let Some(roi) = roi_on_margin(event.realized_pnl, entry_notional, ctx.leverage) {
                rows.push(row("ROI", &roi));
            }
            rows.push(row("SIZE", &format!("{size} left")));
            rows.push(row("NOTIONAL", &format!("${notional}")));
            ("➖", rows)
        }
        EventKind::Close => ("🔴", close_rows(ctx, &coin)),
    };

    // Live uPnL on the still-open size, from the current mark — frozen at send time, refreshed
    // live by the 🔄 Update button. A close shows realized PnL instead, so it gets no uPnL row.
    if event.kind != EventKind::Close
        && let Some(upnl) = unrealized(ctx.mark, ctx.avg_entry, event.szi_after)
    {
        rows.push(row("uPNL", &pnl_whole(Some(upnl))));
    }

    let header = format!(
        "{emoji} {} · {coin} {dir}{lev}",
        linked_name(&event.address, label)
    );
    // Emojis stay OUT of the <pre> — inside a monospace block many clients render them ~2 cells
    // wide, which would shear the column the whole design depends on.
    let grid = format!("<pre>{}</pre>", rows.join("\n"));
    let time = hhmm_utc(event.ts);
    let meta = match ctx.tx_hash {
        Some(hash) if !hash.is_empty() => format!(
            "🔗 <a href=\"{EXPLORER_TX}{}\">TX</a> · 🕒 {time}",
            escape_html(hash)
        ),
        _ => format!("🕒 {time}"),
    };
    format!("{header}\n{grid}\n{meta}")
}

/// The rows of a closed-position card: SIZE / ENTRY / EXIT / NET P/L / ROI / HELD. Entry, exit,
/// size and duration come from the completed round-trip; the money figure is the event's
/// realized PnL (which the listener may have replaced with the exchange's authoritative
/// `closedPnl`). Keeping SIZE here means a scroll-back reader still sees how big the leg was.
fn close_rows(ctx: &EventContext<'_>, coin: &str) -> Vec<String> {
    let event = ctx.event;
    let fill = money_px(event.px);
    let (size, entry, exit, held, entry_notional) = match ctx.trade {
        Some(trade) => (
            trade
                .size
                .map(|s| trim(s.abs()))
                .unwrap_or_else(|| trim(event.delta.abs())),
            trade
                .avg_entry_px
                .map(money_px)
                .unwrap_or_else(|| fill.clone()),
            trade
                .avg_exit_px
                .map(money_px)
                .unwrap_or_else(|| fill.clone()),
            Some(fmt_duration(trade.duration_mins)),
            match (trade.size, trade.avg_entry_px) {
                (Some(s), Some(e)) => Some(s.abs() * e),
                _ => None,
            },
        ),
        None => (
            trim(event.delta.abs()),
            fill.clone(),
            fill.clone(),
            None,
            None,
        ),
    };
    let mut rows = vec![
        row("SIZE", &format!("{size} {coin}")),
        row("ENTRY", &format!("${entry}")),
        row("EXIT", &format!("${exit}")),
        row("NET P/L", &pnl_whole(event.realized_pnl)),
    ];
    if let Some(roi) = roi_on_margin(event.realized_pnl, entry_notional, ctx.leverage) {
        rows.push(row("ROI", &roi));
    }
    if let Some(held) = held {
        rows.push(row("HELD", &held));
    }
    rows
}

/// The signed fraction of the prior position this fill added or removed, e.g. `"+40%"` /
/// `"-43%"`; `None` when the prior size is unknown/zero (so a bare or NaN `%` never prints). The
/// caller passes the sign (`"+"` on an add, `"-"` on a reduce) because a short scales the same
/// way a long does — the delta's own sign tracks side, not grow-vs-shrink.
fn pct_of_position(delta: Decimal, szi_after: Decimal, sign: &str) -> Option<String> {
    let prior = szi_after - delta;
    if prior.is_zero() {
        return None;
    }
    let pct = (delta.abs() / prior.abs() * Decimal::from(100)).round_dp(0);
    Some(format!("{sign}{}%", pct.normalize()))
}

/// Leveraged return on margin as a signed percentage, e.g. `"+21.9%"`; `None` when it can't be
/// computed (unknown leverage/entry, or a zero-notional leg). Shared by reduces (booked against
/// the reduced slice's entry notional) and closes (against the whole leg's).
fn roi_on_margin(
    pnl: Option<Decimal>,
    entry_notional: Option<Decimal>,
    leverage: Option<i64>,
) -> Option<String> {
    let (pnl, entry_notional, lev) = (pnl?, entry_notional?, leverage?);
    if entry_notional.is_zero() || lev <= 0 {
        return None;
    }
    // ROI on margin = price move × leverage = (pnl / entry_notional) × leverage.
    let roi = (pnl / entry_notional * Decimal::from(lev) * Decimal::from(100)).round_dp(1);
    let roi = roi.normalize();
    // A negative value already prints its own '-', so only the non-negative case needs a sign.
    let sign = if roi.is_sign_negative() { "" } else { "+" };
    Some(format!("{sign}{roi}%"))
}

/// Unrealized PnL on the still-open size = (mark − entry) × signed size. `None` unless both the
/// live mark and the blended entry are known, so an un-marked coin never shows a fake $0.
fn unrealized(
    mark: Option<Decimal>,
    avg_entry: Option<Decimal>,
    szi_after: Decimal,
) -> Option<Decimal> {
    Some((mark? - avg_entry?) * szi_after)
}

/// The 🔄 Update P&L inline keyboard for a live-position card, or `None` when the callback payload
/// `upnl:{address}:{coin}` would exceed Telegram's 64-byte limit (a very long HIP-3 coin symbol).
/// The bot re-fetches that wallet's clearinghouse snapshot on tap and edits the card in place with
/// fresh uPnL/ROI + the liquidation price. Kept here so the dispatch path and the bot's callback
/// handler build an identical button. Returning `None` lets the caller send a button-less card
/// rather than have the Bot API reject the whole message.
pub(crate) fn update_pnl_keyboard(address: &str, coin: &str) -> Option<Value> {
    let data = format!("upnl:{address}:{coin}");
    (data.len() <= 64).then(|| {
        json!({
            "inline_keyboard": [[{
                "text": "🔄 Update P&L",
                "callback_data": data,
            }]]
        })
    })
}

/// Round a derived price to a magnitude-appropriate precision (a mark implied by value/size can
/// carry a long fractional tail): whole dollars ≥ $1k, cents ≥ $1, six places below.
fn round_price(px: Decimal) -> Decimal {
    let a = px.abs();
    if a >= Decimal::from(1000) {
        px.round_dp(0)
    } else if a >= Decimal::ONE {
        px.round_dp(2)
    } else {
        px.round_dp(6)
    }
}

/// Signed distance from the mark to the liquidation price: `"-10.7%"` (liq below, a long) /
/// `"+8.2%"` (liq above, a short). `None` when the mark is zero.
fn liq_distance(mark: Decimal, liq: Decimal) -> Option<String> {
    if mark.is_zero() {
        return None;
    }
    let pct = ((liq - mark) / mark * Decimal::from(100))
        .round_dp(1)
        .normalize();
    let sign = if pct.is_sign_negative() { "" } else { "+" };
    Some(format!("{sign}{pct}%"))
}

/// Render a wallet's CURRENT state in one coin as a live TAPE snapshot — the 🔄 Update button's
/// target. Unlike [`format_card`] (which narrates a fill), this is a point-in-time clearinghouse
/// read: size, entry, mark, notional, uPnL (+ margin ROI), and the liquidation price + distance.
/// The caller re-attaches [`update_pnl_keyboard`] so the card can be refreshed again.
pub(crate) fn format_live_position(
    label: &str,
    address: &str,
    p: &AccountPosition,
    now: DateTime<Utc>,
) -> String {
    let coin = escape_html(&p.coin);
    let szi = p.szi.unwrap_or_default();
    let (emoji, dir) = if szi < Decimal::ZERO {
        ("🔴", "SHORT")
    } else {
        ("🟢", "LONG")
    };
    let lev = p
        .leverage_value
        .map(|l| format!(" {l}x"))
        .unwrap_or_default();

    // Mark is implied by notional / size (positionValue = |size| × mark).
    let mark = match (p.position_value, p.szi) {
        (Some(v), Some(s)) if !s.is_zero() => Some(round_price(v.abs() / s.abs())),
        _ => None,
    };
    let entry_notional = match (p.szi, p.entry_px) {
        (Some(s), Some(e)) => Some(s.abs() * e),
        _ => None,
    };

    let mut rows = vec![row("SIZE", &format!("{} {coin}", trim(szi.abs())))];
    if let Some(entry) = p.entry_px {
        rows.push(row("ENTRY", &format!("${}", money_px(entry))));
    }
    if let Some(m) = mark {
        rows.push(row("MARK", &format!("${}", money_px(m))));
    }
    if let Some(v) = p.position_value {
        rows.push(row("NOTIONAL", &format!("${}", notional_short(v.abs()))));
    }
    if let Some(upnl) = p.unrealized_pnl {
        let roi = roi_on_margin(Some(upnl), entry_notional, p.leverage_value)
            .map(|r| format!("  ({r})"))
            .unwrap_or_default();
        rows.push(row("uPNL", &format!("{}{roi}", pnl_whole(Some(upnl)))));
    }
    if let Some(liq) = p.liquidation_px {
        let dist = mark
            .and_then(|m| liq_distance(m, liq))
            .map(|d| format!("  {d}"))
            .unwrap_or_default();
        rows.push(row("LIQ", &format!("${}{dist}", money_px(liq))));
    }

    let header = format!(
        "{emoji} {} · {coin} {dir}{lev}",
        linked_name(address, label)
    );
    let time = hhmm_utc(now);
    format!(
        "{header}\n<pre>{}</pre>\n🕒 {time} · updated",
        rows.join("\n")
    )
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
    async fn send(
        &self,
        chat_id: i64,
        text: &str,
        _reply_markup: Option<&Value>,
    ) -> Result<i64, SendError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        tracing::info!("NOTIFY chat={chat_id} (new msg {id}): {text}");
        Ok(id)
    }

    async fn edit(
        &self,
        chat_id: i64,
        message_id: i64,
        text: &str,
        _reply_markup: Option<&Value>,
    ) -> Result<(), SendError> {
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
        // Every still-open card carries the 🔄 Update P&L button (same for all recipients — the
        // callback keys on wallet+coin, not the chat); a close finalizes the card with no button.
        // `update_pnl_keyboard` returns None if the payload would overflow 64 bytes → button-less.
        let keyboard = match event.kind {
            EventKind::Close => None,
            _ => update_pnl_keyboard(&event.address, &event.coin),
        };
        for (chat_id, label) in recipients {
            let text = format_card(ctx, label);
            let key = (*chat_id, event.address.clone(), event.coin.clone());
            self.deliver(
                *chat_id,
                &key,
                event.kind,
                &text,
                keyboard.as_ref(),
                can_create_new,
            )
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
        reply_markup: Option<&Value>,
        can_create_new: bool,
    ) {
        // An open always starts a fresh card — any stale id for this (wallet, coin) is replaced.
        if kind == EventKind::Open {
            self.send_and_track(chat_id, key, kind, text, reply_markup)
                .await;
            return;
        }

        let existing = self
            .cards
            .lock()
            .expect("cards mutex poisoned")
            .get(key)
            .copied();

        if let Some(message_id) = existing {
            match self
                .sender
                .edit(chat_id, message_id, text, reply_markup)
                .await
            {
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
                        self.send_and_track(chat_id, key, kind, text, reply_markup)
                            .await;
                    }
                }
            }
            return;
        }

        // No live card to edit. Create one only when allowed — muting exits suppresses a *new*
        // reduce/close message (which would ping), never the silent edit above.
        if can_create_new {
            self.send_and_track(chat_id, key, kind, text, reply_markup)
                .await;
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
        reply_markup: Option<&Value>,
    ) {
        match self.sender.send(chat_id, text, reply_markup).await {
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
            // Tests that exercise the ENTRY row / reduce-ROI set this field explicitly after.
            avg_entry: None,
        }
    }

    fn recipients(pairs: &[(i64, &str)]) -> HashMap<i64, String> {
        pairs.iter().map(|(c, l)| (*c, l.to_string())).collect()
    }

    // --- format_card (pure) -----------------------------------------------------------------

    #[test]
    fn test_open_card_links_name_and_renders_grid() {
        let e = event(EventKind::Open, "2.5", "2.5", None);
        let text = format_card(&ctx(&e, Some(10), None, None, None), "Whale-1");
        assert_eq!(
            text,
            "🟢 <b><a href=\"https://app.hyperliquid.xyz/explorer/address/0xa\">Whale-1</a></b> · BTC LONG 10x\n\
             <pre>ENTRY    $63,120\n\
             SIZE     2.5 BTC\n\
             NOTIONAL $157.8k</pre>\n\
             🕒 12:00 UTC"
        );
    }

    #[test]
    fn test_add_card_shows_pct_blended_entry_and_tx_link() {
        let e = event(EventKind::Add, "1", "3", None);
        let mut c = ctx(&e, Some(10), Some(d("63120")), Some("0xdead"), None);
        c.avg_entry = Some(d("62800"));
        let text = format_card(&c, "W");
        assert_eq!(
            text,
            "➕ <b><a href=\"https://app.hyperliquid.xyz/explorer/address/0xa\">W</a></b> · BTC LONG 10x\n\
             <pre>ADDED    +1 BTC  +50%\n\
             FILL     $63,120\n\
             SIZE     3 BTC\n\
             ENTRY    $62,800\n\
             NOTIONAL $189.4k\n\
             uPNL     +$960</pre>\n\
             🔗 <a href=\"https://app.hyperliquid.xyz/explorer/tx/0xdead\">TX</a> · 🕒 12:00 UTC"
        );
    }

    #[test]
    fn test_add_card_without_mark_leverage_or_entry_degrades_gracefully() {
        let e = event(EventKind::Add, "1", "3", None);
        let text = format_card(&ctx(&e, None, None, None, None), "W");
        assert_eq!(
            text,
            "➕ <b><a href=\"https://app.hyperliquid.xyz/explorer/address/0xa\">W</a></b> · BTC LONG\n\
             <pre>ADDED    +1 BTC  +50%\n\
             FILL     $63,120\n\
             SIZE     3 BTC\n\
             NOTIONAL $189.4k</pre>\n\
             🕒 12:00 UTC"
        );
    }

    #[test]
    fn test_reduce_card_shows_realized_roi_and_remaining_notional() {
        let e = event(EventKind::Reduce, "-0.5", "2", Some("440"));
        let mut c = ctx(&e, Some(10), None, None, None);
        c.avg_entry = Some(d("60000"));
        let text = format_card(&c, "Whale-1");
        assert_eq!(
            text,
            "➖ <b><a href=\"https://app.hyperliquid.xyz/explorer/address/0xa\">Whale-1</a></b> · BTC LONG 10x\n\
             <pre>REDUCED  -0.5 BTC  -20%\n\
             FILL     $63,120\n\
             REALIZED +$440\n\
             ROI      +14.7%\n\
             SIZE     2 left\n\
             NOTIONAL $126.2k</pre>\n\
             🕒 12:00 UTC"
        );
    }

    #[test]
    fn test_reduce_card_omits_roi_without_leverage() {
        let e = event(EventKind::Reduce, "-0.5", "2", Some("440"));
        let mut c = ctx(&e, None, None, None, None);
        c.avg_entry = Some(d("60000"));
        let text = format_card(&c, "W");
        assert!(text.contains("REALIZED +$440"), "{text}");
        assert!(!text.contains("ROI"), "{text}");
    }

    #[test]
    fn test_close_card_shows_size_entry_exit_pnl_roi_and_duration() {
        let e = event(EventKind::Close, "-2", "0", Some("2760"));
        let t = trade("2", "63120", "64500", 192, "2760");
        let text = format_card(&ctx(&e, Some(10), None, Some("0xbeef"), Some(&t)), "W");
        assert_eq!(
            text,
            "🔴 <b><a href=\"https://app.hyperliquid.xyz/explorer/address/0xa\">W</a></b> · BTC LONG 10x\n\
             <pre>SIZE     2 BTC\n\
             ENTRY    $63,120\n\
             EXIT     $64,500\n\
             NET P/L  +$2,760\n\
             ROI      +21.9%\n\
             HELD     3h12m</pre>\n\
             🔗 <a href=\"https://app.hyperliquid.xyz/explorer/tx/0xbeef\">TX</a> · 🕒 12:00 UTC"
        );
    }

    #[test]
    fn test_close_card_negative_pnl_shows_signed_pnl_and_roi() {
        let e = event(EventKind::Close, "-2", "0", Some("-1250"));
        let t = trade("2", "63120", "62000", 45, "-1250");
        let text = format_card(&ctx(&e, Some(10), None, None, Some(&t)), "W");
        assert!(text.contains("NET P/L  -$1,250"), "{text}");
        assert!(text.contains("ROI      -9.9%"), "{text}");
        assert!(text.contains("HELD     45m"), "{text}");
        assert!(text.starts_with("🔴"), "{text}");
    }

    #[test]
    fn test_close_card_without_leverage_omits_roi() {
        let e = event(EventKind::Close, "-2", "0", Some("2760"));
        let t = trade("2", "63120", "64500", 90, "2760");
        let text = format_card(&ctx(&e, None, None, None, Some(&t)), "W");
        assert!(text.contains("NET P/L  +$2,760"), "{text}");
        assert!(text.contains("HELD     1h30m"), "{text}");
        assert!(!text.contains("ROI"), "{text}");
        assert!(!text.contains('%'), "{text}");
    }

    #[test]
    fn test_label_is_html_escaped_inside_the_anchor() {
        let e = event(EventKind::Open, "1", "1", None);
        let text = format_card(&ctx(&e, None, None, None, None), "<Whale & 'co'>");
        assert!(text.starts_with(
            "🟢 <b><a href=\"https://app.hyperliquid.xyz/explorer/address/0xa\">\
             &lt;Whale &amp; &#x27;co&#x27;&gt;</a></b> · BTC LONG"
        ));
    }

    #[test]
    fn test_open_card_shows_upnl_when_marked() {
        // A mark above the fill price yields a positive uPnL row on the open card.
        let e = event(EventKind::Open, "2", "2", None);
        let mut c = ctx(&e, Some(10), Some(d("63500")), None, None);
        c.avg_entry = Some(d("63120"));
        let text = format_card(&c, "W");
        // (63500 - 63120) * 2 = 760
        assert!(text.contains("uPNL     +$760"), "{text}");
    }

    // --- live-position snapshot (the 🔄 Update button target) -------------------------------

    fn position(
        coin: &str,
        szi: &str,
        entry: &str,
        value: &str,
        upnl: &str,
        leverage: Option<i64>,
        liq: Option<&str>,
    ) -> AccountPosition {
        AccountPosition {
            address: "0xa".to_string(),
            coin: coin.to_string(),
            szi: Some(d(szi)),
            entry_px: Some(d(entry)),
            position_value: Some(d(value)),
            unrealized_pnl: Some(d(upnl)),
            liquidation_px: liq.map(d),
            leverage_type: None,
            leverage_value: leverage,
            max_leverage: None,
        }
    }

    #[test]
    fn test_format_live_position_snapshot_with_roi_and_liq_distance() {
        // value 160125 / size 2.5 => mark 64050.
        let p = position(
            "BTC",
            "2.5",
            "63120",
            "160125",
            "2325",
            Some(10),
            Some("57180"),
        );
        let now = Utc.with_ymd_and_hms(2026, 6, 15, 12, 41, 0).unwrap();
        let text = format_live_position("Whale-1", "0xa", &p, now);
        assert_eq!(
            text,
            "🟢 <b><a href=\"https://app.hyperliquid.xyz/explorer/address/0xa\">Whale-1</a></b> · BTC LONG 10x\n\
             <pre>SIZE     2.5 BTC\n\
             ENTRY    $63,120\n\
             MARK     $64,050\n\
             NOTIONAL $160.1k\n\
             uPNL     +$2,325  (+14.7%)\n\
             LIQ      $57,180  -10.7%</pre>\n\
             🕒 12:41 UTC · updated"
        );
    }

    #[test]
    fn test_format_live_position_short_and_missing_liq() {
        // Short (szi < 0): SHORT header, red dot, and no LIQ row when the exchange omits it.
        let p = position("ETH", "-4", "2500", "10000", "-120", Some(5), None);
        let now = Utc.with_ymd_and_hms(2026, 6, 15, 9, 5, 0).unwrap();
        let text = format_live_position("W", "0xa", &p, now);
        assert!(text.starts_with("🔴 "), "{text}");
        assert!(text.contains("· ETH SHORT 5x"), "{text}");
        assert!(text.contains("SIZE     4 ETH"), "{text}");
        assert!(!text.contains("LIQ"), "{text}");
    }

    #[test]
    fn test_update_pnl_keyboard_encodes_address_and_coin() {
        let kb = update_pnl_keyboard("0xabc", "BTC").expect("well under 64 bytes");
        let btn = &kb["inline_keyboard"][0][0];
        assert_eq!(btn["callback_data"], "upnl:0xabc:BTC");
        assert_eq!(btn["text"], "🔄 Update P&L");
    }

    #[test]
    fn test_update_pnl_keyboard_drops_button_when_payload_too_long() {
        // A real 42-char address: "upnl:" (5) + 42 + ":" (1) = 48, leaving 16 bytes for the coin.
        let addr = format!("0x{}", "a".repeat(40));
        assert!(update_pnl_keyboard(&addr, &"X".repeat(16)).is_some()); // 48 + 16 = 64
        assert!(update_pnl_keyboard(&addr, &"X".repeat(17)).is_none()); // 65 > 64 → no button
    }

    #[test]
    fn test_format_live_position_omits_roi_without_leverage() {
        // No leverage → the uPnL row carries the dollar figure but no margin-ROI parenthetical.
        let p = position("BTC", "2", "63000", "128000", "1000", None, Some("57000"));
        let now = Utc.with_ymd_and_hms(2026, 6, 15, 12, 0, 0).unwrap();
        let text = format_live_position("W", "0xa", &p, now);
        assert!(text.contains("uPNL     +$1,000"), "{text}");
        assert!(!text.contains('('), "{text}"); // ROI parenthetical omitted
    }

    #[test]
    fn test_format_live_position_without_value_omits_mark_notional_and_liq_distance() {
        let mut p = position("BTC", "2", "63000", "0", "500", Some(10), Some("57000"));
        p.position_value = None; // no notional → no derived mark, no liq distance
        let now = Utc.with_ymd_and_hms(2026, 6, 15, 12, 0, 0).unwrap();
        let text = format_live_position("W", "0xa", &p, now);
        assert!(!text.contains("MARK"), "{text}");
        assert!(!text.contains("NOTIONAL"), "{text}");
        assert!(text.contains("LIQ      $57,000"), "{text}"); // liq shown, bare (no distance)
        assert!(!text.contains("LIQ      $57,000  "), "{text}"); // no "  ±x%" suffix
    }

    #[test]
    fn test_format_live_position_sub_dollar_price_keeps_precision() {
        // mark = value/size = 560/1000 = 0.56 → the sub-$1 round_price branch keeps decimals.
        let p = position("HPOS", "1000", "0.5", "560", "60", Some(3), None);
        let now = Utc.with_ymd_and_hms(2026, 6, 15, 12, 0, 0).unwrap();
        let text = format_live_position("W", "0xa", &p, now);
        assert!(text.contains("ENTRY    $0.5"), "{text}");
        assert!(text.contains("MARK     $0.56"), "{text}");
    }

    // --- edit-in-place dispatch -------------------------------------------------------------

    #[derive(Default)]
    struct FakeSender {
        sent: Mutex<Vec<(i64, String)>>,
        edited: Mutex<Vec<(i64, i64, String)>>,
        // markup presence per send/edit, in call order — asserts the Update button is attached to
        // open cards and dropped on close.
        sent_has_kb: Mutex<Vec<bool>>,
        edited_has_kb: Mutex<Vec<bool>>,
        next_id: AtomicI64,
    }

    #[async_trait]
    impl MessageSender for FakeSender {
        async fn send(
            &self,
            chat_id: i64,
            text: &str,
            reply_markup: Option<&Value>,
        ) -> Result<i64, SendError> {
            let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
            self.sent.lock().unwrap().push((chat_id, text.to_string()));
            self.sent_has_kb
                .lock()
                .unwrap()
                .push(reply_markup.is_some());
            Ok(id)
        }
        async fn edit(
            &self,
            chat_id: i64,
            message_id: i64,
            text: &str,
            reply_markup: Option<&Value>,
        ) -> Result<(), SendError> {
            self.edited
                .lock()
                .unwrap()
                .push((chat_id, message_id, text.to_string()));
            self.edited_has_kb
                .lock()
                .unwrap()
                .push(reply_markup.is_some());
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
        assert!(edited[0].2.contains("ADDED    +1 BTC"));
    }

    #[tokio::test]
    async fn test_open_carries_update_button_and_close_drops_it() {
        let sender = Arc::new(FakeSender::default());
        let notifier = Notifier::new(sender.clone(), true);
        let rcpt = recipients(&[(1, "W")]);

        let open = event(EventKind::Open, "2", "2", None);
        notifier
            .dispatch(&ctx(&open, Some(10), None, None, None), &rcpt)
            .await;
        let t = trade("2", "100", "150", 5, "100");
        let close = event(EventKind::Close, "-2", "0", Some("100"));
        notifier
            .dispatch(&ctx(&close, Some(10), None, None, Some(&t)), &rcpt)
            .await;

        // The open card is sent WITH the 🔄 button; the close edits the card WITHOUT one.
        assert_eq!(*sender.sent_has_kb.lock().unwrap(), vec![true]);
        assert_eq!(*sender.edited_has_kb.lock().unwrap(), vec![false]);
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
            async fn send(
                &self,
                chat_id: i64,
                _text: &str,
                _reply_markup: Option<&Value>,
            ) -> Result<i64, SendError> {
                if chat_id == 1 {
                    return Err("telegram down".into());
                }
                self.ok.lock().unwrap().push(chat_id);
                Ok(self.next_id.fetch_add(1, Ordering::Relaxed) + 1)
            }
            async fn edit(
                &self,
                _c: i64,
                _m: i64,
                _t: &str,
                _reply_markup: Option<&Value>,
            ) -> Result<(), SendError> {
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
            async fn send(
                &self,
                _c: i64,
                _t: &str,
                _reply_markup: Option<&Value>,
            ) -> Result<i64, SendError> {
                *self.sent.lock().unwrap() += 1;
                Ok(self.next_id.fetch_add(1, Ordering::Relaxed) + 1)
            }
            async fn edit(
                &self,
                _c: i64,
                _m: i64,
                _t: &str,
                _reply_markup: Option<&Value>,
            ) -> Result<(), SendError> {
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
