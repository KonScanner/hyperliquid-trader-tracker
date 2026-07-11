//! The firehose listener: Hyperliquid public `trades` feed → watched-wallet notifications.
//!
//! Subscribes to every perp coin's public `trades` feed (each trade carries both counterparty
//! addresses) plus `allMids` (for notional in messages), filters against the admitted watchlist,
//! folds the resulting signed fills through the in-memory book's state machine, and dispatches each
//! lifecycle event to the notifier. One reconnecting connection loop owns the socket; an app-level
//! ping keeps quiet sockets alive (HL closes any connection idle 60s). Trimmed from the sibling
//! `hyperdash-crawl` listener — no Redis, no Postgres, no flush loop.
//
// PORT NOTE: asyncio module → tokio per the fixed port decisions (runtime: tokio, fixed —
// no `TODO(port): runtime` needed). `import websockets` → tokio-tungstenite + futures-util
// (fixed dependency set); `import json` → serde_json; `contextlib.suppress(TimeoutError)`
// around wait_for → tokio::select! in sleep_or_stop (fixed decision).
// PORT NOTE: `logger = logging.getLogger(__name__)` disappears — `tracing` macros are
// free-standing and carry the module path automatically (same as notifier.rs / pnl.rs).

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde_json::{Map, Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async_with_config};
use tokio_util::sync::CancellationToken;

use crate::book::{InMemoryBook, SeenTids};
use crate::config::Settings;
use crate::hl_client::InfoClient;
use crate::notifier::{EventContext, Notifier};
use crate::pnl::{ClosedPnlResolver, with_authoritative_pnl};
use crate::registry::Registry;
use crate::resolve::{perp_coins_from_meta, resolve_deltas};

// PORT NOTE: the Python `ws: Any` parameters become the concrete split halves of the one
// socket type this module ever opens (fixed decision: `_run_connection` splits the socket —
// recv loop takes the stream, subscribe + heartbeat take the sink).
type WsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;
type WsSink = SplitSink<WsStream, Message>;

// PORT NOTE: `except* Exception` in `_connection_loop` catches ANYTHING the connect /
// subscribe / recv / heartbeat path raises (tungstenite transport errors, TrackerError out of
// the meta fetch, the no-perp-coins RuntimeError, the socket-closed ConnectionError) and the
// only handling is log-and-backoff — no caller ever pattern-matches. The type-erased box is
// the faithful shape (same argument as notifier::SendError; the guide's "no Box<dyn Error>
// in library code" rule targets errors callers match on).
type ConnError = Box<dyn std::error::Error + Send + Sync>;
type ConnResult<T> = Result<T, ConnError>;

/// Owns the WebSocket connection, live marks, and per-trade dispatch.
pub struct Listener {
    // PORT NOTE: leading-underscore privacy (`_settings`, …) → non-pub fields, same order as
    // __init__.
    // PORT NOTE: GIL-free — `registry` and `book` are mutated from other tokio tasks too
    // (bot subscribes/unsubscribes, enrich seeds/reconciles), so per the fixed shared-state
    // decision they arrive as Arc<std::sync::Mutex<..>>; every lock below is scoped to a
    // single statement and NEVER held across an await. Python shared the bare objects on
    // one event loop.
    settings: Settings,
    registry: Arc<Mutex<Registry>>,
    book: Arc<Mutex<InMemoryBook>>,
    // PORT NOTE: app.py hands the Notifier to the Listener and keeps no other reference —
    // owned by value (the Notifier itself only needs &self).
    notifier: Notifier,
    // PORT NOTE: `client: InfoClient` (Protocol) → Arc<dyn InfoClient> (fixed decision).
    client: Arc<dyn InfoClient>,
    pnl: Option<ClosedPnlResolver>,
    /// empty = all perps
    coins: Vec<String>,
    // PORT NOTE: `dict[str, Decimal]` → plain HashMap (fixed decision: marks map owned by
    // the Listener); no code path iterates it — .get()/.insert() only — so no IndexMap.
    marks: HashMap<String, Decimal>,
    seen: SeenTids,
    // PORT NOTE: `asyncio.Event` used as a stop flag → CancellationToken (fixed decision).
    // Field `stop` and method `stop()` share a name — legal in Rust (separate namespaces;
    // book.rs `leverage` precedent).
    stop: CancellationToken,
}

impl Listener {
    // PORT NOTE: keyword-only `*, pnl_resolver: ClosedPnlResolver | None = None` → required
    // Option param (guide option (b): "absent" has semantic meaning — None disables the
    // authoritative close-PnL lookup). PORT NOTE: was default arg pnl_resolver=None — call
    // sites spell out None.
    pub fn new(
        settings: Settings,
        registry: Arc<Mutex<Registry>>,
        book: Arc<Mutex<InMemoryBook>>,
        notifier: Notifier,
        client: Arc<dyn InfoClient>,
        pnl_resolver: Option<ClosedPnlResolver>,
    ) -> Self {
        // PORT NOTE: `settings.live_coins_list` @property → method call (config.rs); both
        // reads happen before `settings` moves into Self (Python read through the shared ref).
        let coins = settings.live_coins_list(); // empty = all perps
        let seen = SeenTids::new(settings.tid_dedup_maxlen);
        Self {
            settings,
            registry,
            book,
            notifier,
            client,
            pnl: pnl_resolver,
            coins,
            marks: HashMap::new(),
            seen,
            stop: CancellationToken::new(),
        }
    }

    /// Signal a graceful shutdown (the reconnect loop exits).
    pub fn stop(&self) {
        // PORT NOTE: `self._stop.set()` → CancellationToken::cancel (fixed decision).
        self.stop.cancel();
    }

    /// A clonable handle onto the stop flag (cancel it to stop the listener).
    // PORT NOTE: structural addition — app.py's signal handler calls `listener.stop()` while
    // `listener.run()` is executing as a task; in Rust `run(&mut self)` borrows the listener
    // exclusively, so the app must grab this token clone BEFORE spawning run and cancel
    // through it (cancelling a clone cancels the shared token).
    // TODO(port): confirm app.rs wires shutdown via this accessor (or constructs the token
    // outside and passes it into new()) in Phase B.
    pub fn stop_token(&self) -> CancellationToken {
        self.stop.clone()
    }

    // PORT NOTE: &mut self — the recv path mutates `marks`/`seen` directly (Python mutated
    // through the shared self on one event loop).
    pub async fn run(&mut self) {
        self.connection_loop().await;
    }

    // --- connection (reconnecting) -----------------------------------------------------

    async fn connection_loop(&mut self) {
        let mut backoff: f64 = 1.0;
        while !self.stop.is_cancelled() {
            // PORT NOTE: `try: ... except* Exception as eg:` → match on the Result (fixed
            // decision: `except* Exception` reconnect loop → match Err => log + backoff).
            match self.run_connection().await {
                Ok(()) => backoff = 1.0,
                Err(err) => {
                    // PORT NOTE: Python joined `eg.exceptions` with "; " — a TaskGroup can
                    // carry BOTH loops' failures; tokio::try_join! yields only the FIRST
                    // error (the sibling is cancelled by drop), so a single Display stands
                    // in for the joined list. `%.1fs` → `{:.1}`.
                    tracing::warn!("listener WS dropped ({err}); reconnecting in {backoff:.1}s");
                    Self::sleep_or_stop(&self.stop, backoff).await;
                    backoff = (backoff * 2.0).min(30.0);
                }
            }
        }
    }

    async fn run_connection(&mut self) -> ConnResult<()> {
        // PORT NOTE: `self._coins or await self._fetch_perp_coins()` — list truthiness →
        // is_empty(); the configured list is cloned so it doesn't hold a &self borrow across
        // the &mut self recv loop below (per-connection cost only).
        let coins: Vec<String> = if self.coins.is_empty() {
            self.fetch_perp_coins().await?
        } else {
            self.coins.clone()
        };
        // PORT NOTE: `websockets.connect(url, ping_interval=None, max_size=None)`:
        //   - ping_interval=None (no client auto-ping): tokio-tungstenite never pings on its
        //     own, so there is nothing to disable; server pings are still answered with pongs
        //     automatically during stream polling, matching the websockets library.
        //   - max_size=None (unbounded messages): tungstenite defaults to a bounded
        //     max_message_size/max_frame_size, so both are lifted explicitly.
        // TODO(port): WebSocketConfig's construction API varies across tungstenite versions
        // (pub fields vs builder methods, #[non_exhaustive]) — verify this form against the
        // pinned version in Phase B.
        let mut config = WebSocketConfig::default();
        config.max_message_size = None;
        config.max_frame_size = None;
        // PORT NOTE: websockets.connect's default open_timeout=10 bounded the whole
        // TCP+TLS+upgrade handshake; tokio-tungstenite has no built-in equivalent, so bound
        // it explicitly — otherwise one stalled handshake wedges the reconnect loop forever.
        let (ws, _response) = tokio::time::timeout(
            Duration::from_secs(10),
            connect_async_with_config(self.settings.hl_ws_url.as_str(), Some(config), false),
        )
        .await
        .map_err(|_| "WebSocket open timed out (10s)")??;
        // PORT NOTE: `async with ... as ws:` → the split halves drop at scope end (guide:
        // Drop runs at scope end). Divergence: websockets' __aexit__ performed the closing
        // handshake; dropping just tears down the TCP stream.
        // TODO(port): decide in Phase B whether a graceful close (send Message::Close before
        // returning on stop) is worth mirroring — the server treats both as a disconnect.
        let (mut sink, stream) = ws.split();
        self.subscribe(&mut sink, &coins).await?;
        tracing::info!(
            "listener subscribed to {} coin trade feeds + allMids",
            coins.len()
        );
        // PORT NOTE: `asyncio.TaskGroup` (create_task × 2, __aexit__ waits, first failure
        // cancels the sibling) → tokio::try_join! on the two loop futures (fixed decision):
        // both are polled concurrently, the first Err returns immediately and drops
        // (cancels) the other; on double-Ok it waits for both — same semantics.
        // PORT NOTE: reshaped for borrowck — the recv loop takes &mut self, so the
        // heartbeat's inputs (the sink half, a Copy interval, a token clone) are hoisted
        // into locals first instead of borrowing self inside the join.
        let heartbeat_s = self.settings.ws_heartbeat_s;
        let stop = self.stop.clone();
        tokio::try_join!(
            self.recv_loop(stream),
            Self::heartbeat_loop(sink, heartbeat_s, stop)
        )?;
        Ok(())
    }

    // PORT NOTE: `raise RuntimeError("Hyperliquid meta returned no perp coins")` does NOT
    // panic despite the fixed RuntimeError→panic rule: that rule targets programmer errors,
    // while this one is a bad-API-response error that `_connection_loop`'s `except*
    // Exception` deliberately catches and retries with backoff — so it is an Err here.
    async fn fetch_perp_coins(&self) -> ConnResult<Vec<String>> {
        // PORT NOTE: dict literal body → json! (Value per fixed decision); a TrackerError
        // from info() boxes into ConnError via `?`, landing in the same reconnect arm the
        // Python exception did.
        let meta = self.client.info(json!({"type": "meta"})).await?;
        let coins = perp_coins_from_meta(&meta);
        if coins.is_empty() {
            return Err("Hyperliquid meta returned no perp coins".into());
        }
        Ok(coins)
    }

    // PORT NOTE: `ws: Any` → &mut WsSink (subscribe runs between split() and the two loops,
    // so it writes through the sink half). `json.dumps` → json!(..).to_string(); serde_json's
    // map is key-sorted where Python's dict was insertion-ordered — key order in a JSON
    // request body is semantically irrelevant (pnl.rs precedent).
    async fn subscribe(&self, ws: &mut WsSink, coins: &[String]) -> ConnResult<()> {
        ws.send(Message::text(
            json!({"method": "subscribe", "subscription": {"type": "allMids"}}).to_string(),
        ))
        .await?;
        for coin in coins {
            ws.send(Message::text(
                json!({"method": "subscribe", "subscription": {"type": "trades", "coin": coin}})
                    .to_string(),
            ))
            .await?;
        }
        Ok(())
    }

    async fn recv_loop(&mut self, mut ws: SplitStream<WsStream>) -> ConnResult<()> {
        // PORT NOTE: `async for raw in ws` yields data frames (str | bytes) and raises on a
        // transport error; here each item is a Result — `?` re-creates the raise. tungstenite
        // also surfaces control frames (Ping/Pong/Close) as items, which the websockets
        // library handled out of band — they are skipped below (pongs for server pings are
        // queued/flushed automatically during polling).
        while let Some(item) = ws.next().await {
            let msg = item?;
            if self.stop.is_cancelled() {
                return Ok(());
            }
            match msg {
                // PORT NOTE: `raw: str | bytes` → both arms feed handle_message as bytes
                // (json.loads accepted either; serde_json::from_slice covers both).
                Message::Text(raw) => self.handle_message(raw.as_bytes()).await,
                Message::Binary(raw) => self.handle_message(&raw).await,
                _ => {} // Ping / Pong / Close / raw Frame — control, not data
            }
        }
        if !self.stop.is_cancelled() {
            // socket closed on us → trigger a reconnect
            // PORT NOTE: `raise ConnectionError(...)` → a boxed message error; the only
            // consumer is the reconnect loop's log line.
            return Err("Hyperliquid WebSocket closed".into());
        }
        Ok(())
    }

    // PORT NOTE: was a `self._…` method; reshaped for borrowck — it runs under try_join!
    // beside recv_loop's &mut self, so it takes exactly what it needs (the sink half, the
    // Copy heartbeat interval, a token clone) instead of borrowing self.
    async fn heartbeat_loop(
        mut ws: WsSink,
        heartbeat_s: f64,
        stop: CancellationToken,
    ) -> ConnResult<()> {
        while !stop.is_cancelled() {
            Self::sleep_or_stop(&stop, heartbeat_s).await;
            if stop.is_cancelled() {
                return Ok(());
            }
            ws.send(Message::text(json!({"method": "ping"}).to_string()))
                .await?;
        }
        Ok(())
    }

    // PORT NOTE: `raw: str | bytes` → &[u8] (the union collapses; serde_json::from_slice
    // parses either arm, exactly like json.loads).
    async fn handle_message(&mut self, raw: &[u8]) {
        // PORT NOTE: `except ValueError, TypeError: return` — PEP 758 (3.14) unparenthesized
        // multi-except (resolve.rs precedent); both cases (undecodable/bad JSON, wrong raw
        // type) collapse into from_slice's Err → return (fixed decision: failure-tolerant
        // parse, return on Err).
        let Ok(msg) = serde_json::from_slice::<Value>(raw) else {
            return;
        };
        // PORT NOTE: on a non-object message Python's `msg.get` raised AttributeError, which
        // escaped to the reconnect loop (a crash on malformed data); Value::get returns None
        // on non-objects, so such a message is silently ignored instead — the resolve.rs
        // stance: not behaviour worth carrying forward. `channel == "trades"` compared Any
        // to str (only an equal string matched) → as_str().
        let channel = msg.get("channel").and_then(Value::as_str);
        if channel == Some("trades") {
            // PORT NOTE: `msg.get("data") or []` — falsy (missing/null/empty) → []; a truthy
            // NON-list `data` crashed Python inside _handle_trades (AttributeError →
            // reconnect) and is ignored here (as_array → None), per the same stance.
            let trades = msg
                .get("data")
                .and_then(Value::as_array)
                .map_or(&[][..], Vec::as_slice);
            self.handle_trades(trades).await;
        } else if channel == Some("allMids") {
            // PORT NOTE: `(msg.get("data") or {}).get("mids") or {}` — every falsy/absent/
            // non-object rung lands on the empty dict, making update_marks a no-op; the
            // None arm here skips the call outright (same effect, no empty Map to build).
            let mids = msg
                .get("data")
                .and_then(|data| data.get("mids"))
                .and_then(Value::as_object);
            if let Some(mids) = mids {
                self.update_marks(mids);
            }
        }
    }

    /// For each public trade that touches a watched wallet, fold it in and notify.
    // PORT NOTE: `trades: list[dict[str, Any]]` → &[Value] (untyped API payloads stay Value
    // per the fixed decisions; resolve_deltas absorbs the per-trade shape checks).
    async fn handle_trades(&mut self, trades: &[Value]) {
        for trade in trades {
            // PORT NOTE: GIL-free — lock scope: `self._registry.addresses` (@property) →
            // lock, clone the cached Arc<HashSet> snapshot, guard drops at the end of the
            // statement (never held across an await; fixed decision). resolve_deltas takes
            // &HashSet, which the Arc derefs into.
            let watchlist = self
                .registry
                .lock()
                .expect("registry mutex poisoned")
                .addresses();
            let fills = resolve_deltas(trade, &watchlist);
            if fills.is_empty() {
                continue;
            }
            // De-dupe on the trade id only for watched trades (keeps the ring window meaningful):
            // a WS reconnect that redelivers this trade must not double-count or double-notify.
            // PORT NOTE: `isinstance(tid, int)` gate → Value::as_i64 (tid is i64 — fixed
            // decision, book.rs narrowing; JSON `true` was isinstance-int in Python but is
            // rejected here — unobservable on real payloads, pnl.rs precedent).
            let tid = trade.get("tid").and_then(Value::as_i64);
            if let Some(tid) = tid
                && self.seen.check_and_add(tid)
            {
                continue;
            }
            for fill in fills {
                // PORT NOTE: GIL-free — lock scope: the book guard is a temporary that drops
                // at the end of this statement, before the awaits below. Keyword args to
                // ingest flattened to positional (book.rs shape).
                let result = self.book.lock().expect("book mutex poisoned").ingest(
                    &fill.address,
                    &fill.coin,
                    fill.delta,
                    fill.px,
                    fill.ts,
                );
                if result.events.is_empty() {
                    continue;
                }
                // Fan out to every subscriber of this wallet, each with their own label.
                let recipients = self
                    .registry
                    .lock()
                    .expect("registry mutex poisoned")
                    .subscribers(&fill.address);
                if recipients.is_empty() {
                    continue;
                }
                // PORT NOTE: `for event in result.events` moves the Vec out of result
                // (partial move) — result.closed_trade stays borrowable for the PnL swap.
                for event in result.events {
                    // A close swaps in the exchange's own realized PnL when it resolves in
                    // time; the local estimate stays as the fallback. This awaits on the recv
                    // path — bounded by attempts x delay, and closes are rare.
                    // PORT NOTE: `event = await with_authoritative_pnl(...)` rebinding → let
                    // shadowing; trade/resolver pass as Option<&_> (pnl.rs shape).
                    let event = with_authoritative_pnl(
                        event,
                        result.closed_trade.as_ref(),
                        self.pnl.as_ref(),
                    )
                    .await;
                    // PORT NOTE: Python evaluated the leverage lookup as a dispatch argument,
                    // i.e. AFTER the pnl await — the book lock is taken here (its own
                    // statement, so the guard cannot ride into dispatch's await) to preserve
                    // both that ordering and the lock-scope rule.
                    let leverage = self
                        .book
                        .lock()
                        .expect("book mutex poisoned")
                        .leverage(&event.address, &event.coin);
                    // PORT NOTE: `self._marks.get(event.coin)` returned Decimal | None →
                    // copied() Option<Decimal> (Decimal is Copy).
                    let mark = self.marks.get(&event.coin).copied();
                    // The card carries the fill's tx hash (for a View TX link) and, on a
                    // close, the completed round-trip (entry/exit/PnL/duration). The notifier
                    // edits this subscriber-set's live card in place instead of sending anew.
                    // The next state carries the blended avg entry (unchanged by a reduce, so it
                    // is the basis a reduce's ROI is booked against). None on an exact close —
                    // the close card reads entry from the completed trade instead.
                    let avg_entry = result.state.as_ref().map(|s| s.avg_entry);
                    let ctx = EventContext {
                        event: &event,
                        leverage,
                        mark,
                        tx_hash: fill.hash.as_deref(),
                        trade: result.closed_trade.as_ref(),
                        avg_entry,
                    };
                    self.notifier.dispatch(&ctx, &recipients).await;
                }
            }
        }
    }

    /// Refresh the live mark map from an `allMids` payload (drives the notional in adds).
    // PORT NOTE: `mids: dict[str, Any]` → &serde_json::Map<String, Value> (the object arm
    // was already narrowed in handle_message; untyped payloads stay Value per the fixed
    // decisions).
    fn update_marks(&mut self, mids: &Map<String, Value>) {
        for (coin, px) in mids {
            // PORT NOTE: `try: self._marks[coin] = Decimal(str(px)) except InvalidOperation:
            // continue` → the helper returns None where Python raised InvalidOperation.
            match decimal_from_value(px) {
                Some(d) => {
                    self.marks.insert(coin.clone(), d);
                }
                None => continue,
            }
        }
    }

    /// Sleep `seconds` unless a stop is signalled first (so shutdown is prompt).
    // PORT NOTE: was a `&self` method; hoisted to an associated fn taking the token so
    // heartbeat_loop (which cannot borrow self alongside the recv loop) can share it.
    // `contextlib.suppress(TimeoutError)` around `asyncio.wait_for(stop.wait(), timeout)` →
    // tokio::select! racing the cancellation against the sleep (fixed decision) — both
    // outcomes fall through, exactly like the suppressed TimeoutError.
    async fn sleep_or_stop(stop: &CancellationToken, seconds: f64) {
        tokio::select! {
            _ = stop.cancelled() => {},
            _ = tokio::time::sleep(Duration::from_secs_f64(seconds)) => {},
        }
    }
}

/// `Decimal(str(value))` over a JSON value; `None` = Python's `InvalidOperation`.
// PORT NOTE: structural addition — the Python inlined `Decimal(str(px))` in _update_marks;
// this duplicates resolve.rs's private `decimal_from_value` (same semantics: a parsed-JSON
// string is used bare — Display would re-quote it — everything else falls back to its JSON
// text; from_scientific fallback because Python's Decimal grammar accepts exponent notation
// and rust_decimal's FromStr does not; "NaN"/"Infinity" parsed in Python but fail here —
// unobservable on the real mids feed).
// TODO(port): dedupe with resolve.rs in Phase B (e.g. make resolve's helper pub(crate)).
fn decimal_from_value(value: &Value) -> Option<Decimal> {
    let s = match value.as_str() {
        Some(s) => s.to_owned(),
        None => value.to_string(),
    };
    Decimal::from_str(&s)
        .or_else(|_| Decimal::from_scientific(&s))
        .ok()
}

// ──────────────────────────────────────────────────────────────────────────
// PORT STATUS
//   source:     src/tracker/listener.py (176 lines)
//   confidence: medium
//   todos:      4
//   notes:      TaskGroup → try_join! over split socket halves (recv loop keeps &mut self;
//               heartbeat/sleep_or_stop reshaped to associated fns taking a token clone +
//               sink). except* Exception → type-erased ConnError (Box<dyn Error>) since the
//               only handling is log-and-backoff; the no-perp-coins RuntimeError is an Err,
//               NOT a panic (it is caught by the reconnect loop). registry/book held as
//               Arc<std::sync::Mutex<..>> per fixed decisions, locks statement-scoped, never
//               across an await; leverage lookup deliberately stays AFTER the pnl await
//               (Python argument-evaluation order). run(&mut self) excludes a concurrent
//               stop() — stop_token() accessor added for app.rs (TODO). Confidence medium
//               only for the tokio-tungstenite API surface (WebSocketConfig construction,
//               Message::text/Utf8Bytes across versions); the logic mirrors the Python 1:1.
//               Crates: tokio, tokio-util, tokio-tungstenite, futures-util, serde_json,
//               rust_decimal, tracing.
// ──────────────────────────────────────────────────────────────────────────
