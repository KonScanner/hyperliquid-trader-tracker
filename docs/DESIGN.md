# hyperliquid-trader-tracker — Design (decisions locked 2026-07-01)

Full API reference: [hyperliquid-api-map.md](./hyperliquid-api-map.md). This doc is the build spec.

## Mission
Watch a list of Hyperliquid wallets (up to **1k+**) and **push a Telegram notification** whenever a watched wallet's perp position changes — distinguishing *"Started trade"* (new position) from *"Added to position"* (increase), plus reduce/close/flip with realized PnL.

## Locked decisions
| Decision | Choice |
|---|---|
| Delivery | **Telegram bot** (push) + bot-command settings UX (`/add`, `/remove`, `/rename`, `/list`) |
| Watchlist persistence | **SQLite** file at repo root (`tracker.db`) — table `wallets(address TEXT PK, label TEXT)`. The **only** thing persisted. |
| Position/trade storage | **None** — in-memory only; the chain (`clearinghouseState`) is the durable store, queried on demand |
| Events notified | **Full lifecycle**: open, add, reduce, close, flip (close+open) |
| Watchlist scale | **1k+ wallets** → **pure firehose** architecture; per-user grafts NOT built (they cap at 10 users/IP) |
| Market scope | **Perps only** (the `trades` universe + `clearinghouseState` are perps; spot `@index` fills filtered out) |
| Leverage | From `clearinghouseState.leverage.value` (weight 2), cached; refreshed by the reconcile sweep — best-effort/last-confirmed |

## Architecture — firehose-first (judge-scored 88/100)

```
 wss://api.hyperliquid.xyz/ws
   ├─ subscribe allMids                          ── marks ──▶ _update_marks (notional in messages)
   └─ subscribe trades {coin} for every perp     ── WsTrade{users:[buyer,seller]} ─┐
                                                                                    ▼
   resolve_deltas(trade, watchlist_frozenset)  ── signed ResolvedFill (+sz buyer / −sz seller, watched only)
                                                                                    ▼
   InMemoryBook.ingest(addr, coin, delta, px, ts) = dict.get → apply_fill(state,…) → dict.set
                                                       │  LiveEvent(s): open/add/reduce/close(/flip=close+open)
                                                       ▼
   Notifier.dispatch(event) ──▶ Telegram push  (open→"Started trade …", add→"Added to position …", …)

 POST /info clearinghouseState (weight 2) ──▶ build_position ──▶ Book.seed(…)  [SILENT, no push]
     · startup: throttled seed sweep over all watched wallets   · on every /add   · periodic reconcile (leverage/drift)
```

### Cold-start correctness (no stored positions)
1. On startup and on every `/add`, POST `clearinghouseState` → `build_position` → `seed_state_from_row` installs each open coin **silently** (no push).
2. **Seed before admit**: a wallet is added to the `resolve_deltas` filter set only after its seed completes. → a day-old long is already in the book, so its next same-direction fill is an `ADD`, never a false `OPEN`.
3. **Unseeded guard**: if a wallet's seed fails after retries, quarantine it (no emits) until a seed succeeds.
4. **Startup seed sweep for 1k wallets**: throttle to the 1200 weight/min IP budget (`clearinghouseState` = weight 2 → ≤600/min; ~1k wallets ≈ 2 min ramp). Admit wallets progressively as seeded; document the bounded ramp-window in which a brand-new open on a not-yet-seeded wallet is missed.

### Reconnect idempotency
In-memory recently-seen `tid` ring buffer (bounded, no persistence) so a WS reconnect redelivering trades can't double-notify.

## Notification formats (lead with the human label)
| Event | Format |
|---|---|
| open | `🟢 Started trade {LABEL}: {COIN} {Long\|Short} {size} @ {px} ({lev}x)` |
| add | `➕ Added to position {LABEL}: {COIN} {Long\|Short} +{size} (~${notional}) @ {px} ({lev}x)` |
| reduce | `➖ Reduced {LABEL}: {COIN} {Long\|Short} -{size} @ {px} \| realized {±}${closedPnl} \| {remaining} left` |
| close | `🔴 Closed {LABEL}: {COIN} {Long\|Short} {size} @ {px} \| realized {±}${closedPnl}` |
| flip | rendered as close(old) + Started-trade(new) — `apply_fill` already emits close+open |

`notional = |delta| × mark` (mark from `allMids`). All money via `Decimal`.

## Module layout (`src/tracker/`)
| Module | Origin | Role |
|---|---|---|
| `state.py` | **vendored verbatim** from hyperdash `live/state.py` | `apply_fill` + `PositionState` + `LiveEvent` + event kinds (the open/add/reduce/close/flip brain) |
| `resolve.py` | lifted pure parts of hyperdash `live/listener.py` | `resolve_deltas`, `perp_coins_from_meta`, `seed_state_from_row` |
| `hl_client.py` | **vendored verbatim** | async `/info` client, transient retry |
| `retry.py` | **vendored verbatim** | backoff/jitter |
| `models.py` | subset of hyperdash `models.py` | `build_position` (+ leverage), `AccountPosition` |
| `config.py` | new (pydantic-settings) | HL urls, heartbeat, retry, reconcile interval, seed budget, Telegram token/chat, db path |
| `db.py` | new | SQLite watchlist store: `wallets(address PK, label)`; `list/add/delete/rename` (aiosqlite or stdlib+thread) |
| `book.py` | new (replaces hyperdash Redis `store.py`) | `InMemoryBook`: `dict[(addr,coin)→PositionState]` + leverage cache; `ingest`=get/apply/set→events; `seed`=silent install; `tid` ring buffer |
| `enrich.py` | new | `clearinghouseState`→`build_position`→seed + leverage cache; startup sweep + reconcile loop (throttled) |
| `listener.py` | trimmed hyperdash `live/listener.py` | connection/heartbeat/allMids/`_handle_trades`→book.ingest→notifier; **no** flush/Redis/Postgres |
| `notifier.py` | new | format LiveEvent → Telegram push |
| `bot.py` | new | Telegram settings UX; `/add` triggers `enrich.seed` **before** admitting to the filter |
| `app.py` | new | asyncio `TaskGroup`(connection loop, reconcile loop, bot); SIGINT/SIGTERM |

## Tooling (match hyperdash conventions)
Python ≥3.14, `uv`, `ruff` (line 100; select E,F,I,UP,B,SIM,RUF,TID,PTH,C4,PIE), `ty`, `pytest` (`asyncio_mode=auto`, `filterwarnings=["error"]`), pydantic + pydantic-settings, httpx, websockets, python-telegram-bot. Tests: port `test_live_state.py` + `resolve_deltas` tests; add cold-start test (seeded day-old long → next fill = ADD not OPEN), book/dedup tests, db tests.

## Residual risks (see api-map §Limitations)
- `resolve_deltas` depends on live-verified `[buyer,seller]` ordering — re-verify on a live socket before shipping.
- Firehose book drift from a missed `trades` msg → corrected only by the eventually-consistent reconcile (no per-user cross-check at 1k scale).
- Startup seed ramp window (~2 min at 1k wallets) can miss a brand-new open on a not-yet-seeded wallet.
- Notification spam from an active wallet → per-(wallet,coin) debounce (in-memory) optional.
