# Mapping the Public, Open Hyperliquid API — and a Design for a Wallet-Tracking Push Notifier

**Prepared:** 2026-07-01 · **Mode:** deep research · **Scope owner:** hyperliquid-trader-tracker

---

## Executive Summary

Hyperliquid exposes a **fully public, unauthenticated, read-only data surface** that is explicitly sanctioned for programmatic use — no API key, no signature, no user-agent/CAPTCHA gating (in sharp contrast to third-party analytics sites like Hyperdash, whose GraphQL actively blocks bots). That surface is two endpoints:

- **`POST https://api.hyperliquid.xyz/info`** — a single JSON-over-HTTP endpoint whose `type` field selects one of ~50 request kinds spanning market data, the perps universe, the spot universe, and per-account state. This is the source of truth for **current positions, leverage, margin, fills, and PnL** of any address.
- **`wss://api.hyperliquid.xyz/ws`** — a WebSocket carrying market-wide feeds (per-coin `trades`, `allMids`, `l2Book`, `candle`, …) and per-user feeds (`userFills`, `webData2`, `clearinghouseState`, `activeAssetData`, …).

For the wallet-tracker use case, three facts are load-bearing and were verified against the official docs:

1. **The public `trades` feed carries both counterparties as `users: [buyer, seller]`.** This lets a single market-data subscription track an *unlimited* number of wallets by filtering each trade against a watchlist — no per-wallet subscription needed. (Independently verified live in the sibling `hyperdash-crawl` project.)
2. **`userFills.startPosition` is the signed position size immediately *before* a fill.** This is the native, authoritative primitive for the user's core requirement: `startPosition == 0` ⇒ *"Started trade"*; a same-direction fill on a nonzero `startPosition` ⇒ *"Added to position"*; opposite ⇒ reduce/close/flip. No inference needed.
3. **Read-only tracking is never rate-limited by trading volume.** The per-address limit (1 request per 1 USDC traded) applies **only to exchange actions**, not to info reads. `clearinghouseState` — which returns per-position **leverage** — costs weight 2, so leverage enrichment is effectively free at watchlist scale.

The binding constraint is the **WebSocket cap of 10 unique users per IP** for *per-user* subscriptions. This produces a clean architectural fork: a watchlist of ≤10 wallets can subscribe per-user (`userFills` gives `dir`/`startPosition`/`closedPnl`/leverage natively); a larger watchlist must use the global `trades` firehose plus a position state machine and REST enrichment. The sibling `hyperdash-crawl` codebase already implements the firehose path (verified `trades` listener + pure `apply_fill` state machine emitting `open`/`add`/`reduce`/`close`), so the tracker is largely an *assembly-and-strip* job (remove Redis/Postgres → in-memory dict; add a push sink).

---

## Introduction

### Scope

This report (a) maps the public, open Hyperliquid API surface relevant to observing wallet activity, and (b) grounds the design of `hyperliquid-trader-tracker` — a service that watches a curated list of wallets and pushes a notification when any of them opens or adds to a position. Exchange (order-placement) endpoints are catalogued only for completeness; the tracker is strictly read-only.

### Methodology

Primary sources are the official Hyperliquid developer docs (`hyperliquid.gitbook.io/hyperliquid-docs`), fetched 2026-07-01, cross-referenced with mirror providers (Chainstack, QuickNode) where the GitBook omits field-level schemas. Three independent research agents mapped the Info HTTP API, the WebSocket API, and the rate-limit/auth posture; a fourth pass adversarially re-verified the five load-bearing claims against the primary docs. Reusable implementation facts were drawn from the sibling `hyperdash-crawl` project, whose `trades`-feed listener has the `[buyer, seller]` convention **verified against the live socket** (2026-06-16).

### Assumptions (surfaced, not silently defaulted)

- **Perps-first.** The user's notification semantics ("Long/Short", "leverage") are perpetuals concepts; spot is mapped but treated as an extension.
- **"Store nothing" = no database/persistence of trades or positions.** The watchlist itself must live somewhere (in-memory seeded from config/env, or a small config file) — this is flagged as an open question, not assumed.
- **Push delivery ≈ Telegram bot**, inferred from the provided screenshots (a Telegram "Drops Bot" tracking UI). Confirmed as an open question.

---

## 1. Access Posture & Authentication

| Property | Finding | Source |
|---|---|---|
| Auth for `/info` | **None.** Fully public read-only; `user` param is just the target address (the real account address, not an agent/API wallet). | [Info endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint) |
| Auth for public WS | **None.** Market feeds and per-user *read* subscriptions need only the target address; subscribing is unsigned. | [WebSocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket) |
| Bot blocking | **None** on the official surface — no user-agent gating, CAPTCHA, or IP ban for polite polling. Enforcement is purely weight-based 429 throttling. | [Rate limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits) |
| Official SDK | [`hyperliquid-python-sdk`](https://github.com/hyperliquid-dex/hyperliquid-python-sdk) (also community Rust/TS, CCXT). | API overview |
| Base URLs | Mainnet REST `https://api.hyperliquid.xyz` (`/info`), WS `wss://api.hyperliquid.xyz/ws`. Testnet: `…-testnet.xyz`. | API overview |

**Contrast with third-party analytics.** Hyperdash (`api.hyperdash.com/graphql`) returns `403 BLOCKED_USER_AGENT` to non-browser clients and its ToS forbids scraping; its *proprietary* metrics (copyScore, computed sharpe/winrate) are unavailable elsewhere. But everything the tracker needs — positions, leverage, fills, PnL — is Hyperliquid's own public on-chain data, so **the tracker never needs Hyperdash and never needs a browser.**

---

## 2. Transport Map

```
                    ┌──────────────────────────── Hyperliquid public API ───────────────────────────┐
                    │                                                                                │
  REST  ── POST ───▶│  https://api.hyperliquid.xyz/info    {"type": <one of ~50>, ...}   (no auth)   │
                    │      ├─ general/market:  allMids, l2Book, candleSnapshot, ...                  │
                    │      ├─ perps:           meta, metaAndAssetCtxs, clearinghouseState, ...        │
                    │      ├─ spot:            spotMeta(AndAssetCtxs), spotClearinghouseState, ...     │
                    │      └─ per-user:        userFills(ByTime), portfolio, webData2, ...             │
                    │                                                                                │
  WS  ── connect ──▶│  wss://api.hyperliquid.xyz/ws     {"method":"subscribe","subscription":{...}}   │
                    │      ├─ market-wide (uncapped):   trades, allMids, l2Book, bbo, candle, ...      │
                    │      └─ per-user (≤10 uniq/IP):   userFills, webData2, clearinghouseState, ...    │
                    │      + post requests: {"method":"post","id":N,"request":{"type":"info",...}}     │
                    └────────────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Info Endpoint — `type` Catalog

### (a) General / market

| `type` | Params | Returns |
|---|---|---|
| `allMids` | opt `dex` | `{coin: "price"}` object of all mid prices |
| `l2Book` | `coin`, opt `nSigFigs`/`mantissa` | `{coin, time, levels:[bids,asks]}`, level `{px,sz,n}` |
| `candleSnapshot` | `req:{coin,interval,startTime,endTime}` | OHLCV array; only most recent 5000 candles |
| `openOrders` / `frontendOpenOrders` | `user`, opt `dex` | Open orders (frontend variant adds trigger/TP-SL/reduceOnly) |
| `orderStatus` | `user`, `oid` | `{status, order?}` |
| `historicalOrders` | `user` | ≤2000 recent `{order,status,statusTimestamp}` |
| `userRateLimit` | `user` | `{cumVlm, nRequestsUsed, nRequestsCap, nRequestsSurplus}` — self-monitor budget |
| `maxBuilderFee`, `perpDeployAuctionStatus` | … | builder/deploy metadata |

Candle intervals: `1m,3m,5m,15m,30m,1h,2h,4h,8h,12h,1d,3d,1w,1M`.

### (b) Perpetuals

| `type` | Params | Returns |
|---|---|---|
| `meta` | opt `dex` | `{universe[], marginTables[], collateralToken}`; universe entry `{name, szDecimals, maxLeverage, onlyIsolated}` |
| `metaAndAssetCtxs` | opt `dex` | `[meta, [ctx…]]` positionally parallel to universe; ctx has `markPx, midPx, oraclePx, funding, openInterest, premium, dayNtlVlm, prevDayPx` |
| **`clearinghouseState`** | `user`, opt `dex` | **User perps state + open positions + leverage — §4** |
| `userFunding` | `user`, `startTime` | Funding payments `{delta,hash,time}` |
| `fundingHistory` | `coin`, `startTime` | `{coin,fundingRate,premium,time}` |
| `predictedFundings`, `perpsAtOpenInterestCap` | … | funding/OI-cap views |
| `activeAssetData` | `user`, `coin` | `{leverage, maxTradeSzs, availableToTrade, markPx}` per user+coin |
| `perpDexs`, `perpDexLimits` | … | HIP-3 builder-dex metadata |

### (c) Spot

| `type` | Params | Returns |
|---|---|---|
| `spotMeta` | — | `{tokens[], universe[]}`; pair name e.g. `"PURR/USDC"` or index `"@107"` |
| `spotMetaAndAssetCtxs` | — | `[spotMeta, [ctx…]]` |
| `spotClearinghouseState` | `user` | `{balances:[{coin,token,total,hold,entryNtl}]}` |
| `tokenDetails`, `spotDeployState`, … | … | token/deploy metadata |

### (d) Per-user / account

| `type` | Params | Returns |
|---|---|---|
| **`userFills`** | `user`, opt `aggregateByTime` | **≤2000 most recent fills — §5** |
| **`userFillsByTime`** | `user`, `startTime`, opt `endTime` | Fills in a time range, paginated; only 10,000 most recent retained |
| `portfolio` | `user` | `[[period,{accountValueHistory,pnlHistory,vlm}]]`; periods `day/week/month/allTime` + `perp*` |
| `userFees`, `referral`, `subAccounts`, `userVaultEquities`, `vaultDetails` | `user`/… | fees, referrals, subaccounts, vault equity |
| `userRole`, `delegations`, `delegatorSummary/History/Rewards` | `user` | account role & staking |
| **`webData2`** | `user` | **One-shot aggregate: clearinghouseState + openOrders + meta/assetCtxs + spotState + twapStates + flags.** Whole-account snapshot in a single call |

---

## 4. `clearinghouseState` — the position + leverage source (deep dive)

Request: `{"type":"clearinghouseState","user":"0x…","dex":""}` · **weight 2** · no auth.

```jsonc
{
  "assetPositions": [{
    "position": {
      "coin": "ETH",
      "szi": "0.0335",                 // SIGNED size: >0 long, <0 short; zero-size omitted
      "entryPx": "2986.3",
      "positionValue": "100.02765",
      "unrealizedPnl": "-0.0134",
      "returnOnEquity": "-0.0026789",
      "leverage": { "type": "isolated", "value": 20, "rawUsd": "-95.059824" },  // ← LEVERAGE
      "liquidationPx": "2866.26936529",
      "marginUsed": "4.967826",
      "maxLeverage": 50,
      "cumFunding": { "allTime": "514.08", "sinceOpen": "0.0", "sinceChange": "0.0" }
    },
    "type": "oneWay"
  }],
  "marginSummary":      { "accountValue":"13109.48", "totalMarginUsed":"4.97", "totalNtlPos":"100.03", "totalRawUsd":"13009.45" },
  "crossMarginSummary": { "accountValue":"13104.51", "totalMarginUsed":"0.0",  "totalNtlPos":"0.0",    "totalRawUsd":"13104.51" },
  "crossMaintenanceMarginUsed": "0.0",
  "withdrawable": "13104.51",
  "time": 1708622398623
}
```

**All numeric fields are strings** → parse to `Decimal` (no float drift). `leverage.value` is the `{leverage}x` in the notification; `leverage.type` distinguishes cross/isolated. This is the object the sibling project already normalizes in [`build_position`](../../hyperdash-crawl/src/hyperdash_cohorts/models.py) (`leverage.get("value")`).

---

## 5. `userFills` — native open-vs-add detection (deep dive)

Request: `{"type":"userFills","user":"0x…"}` (REST) or WS subscription `{"type":"userFills","user":"0x…"}`.

```jsonc
{
  "coin":"AVAX", "px":"18.435", "sz":"93.53", "side":"B",   // B=buy/bid, A=sell/ask
  "time":1681222254710,
  "startPosition":"26.86",        // ← SIGNED position size IMMEDIATELY BEFORE this fill
  "dir":"Open Long",              // advisory display text ONLY — do not branch on it
  "closedPnl":"0.0",              // nonzero only when the fill reduces/closes
  "hash":"0xa166…", "oid":90542681, "crossed":false,        // crossed=true ⇒ this side was taker
  "fee":"0.01", "feeToken":"USDC", "builderFee":"0.01", "tid":118906512037719
}
```

**The classification primitive (authoritative):**

| Condition | Meaning | Notification |
|---|---|---|
| `startPosition == 0` | opened a brand-new position (dir from `side`: B→long, A→short) | **"Started trade"** |
| `startPosition ≠ 0`, fill **same** direction | added to an existing position (`closedPnl == 0`) | **"Added to position"** |
| `startPosition ≠ 0`, fill **opposite**, `|sz| < |startPosition|` | reduced | (extension) |
| opposite, `|sz| == |startPosition|` | closed to flat (`closedPnl ≠ 0`) | (extension) |
| opposite, `|sz| > |startPosition|` | flipped (`dir` shows `"Long > Short"` etc.) | close + open |

**Snapshot semantics (critical for a *notifier*).** The first `userFills`/`webData2`/`userFundings` WS message has `isSnapshot: true` — a backfill of *recent history*. A notifier **must skip the snapshot batch** (and re-dedupe by `tid` after any reconnect) or it will re-notify old fills on every (re)connect. `dir` is advisory: the docs publish only examples, not an exhaustive enumeration, so branch on `startPosition`/`side`, not on `dir` text.

**Pagination (REST `userFillsByTime`):** ≤2000 rows/response; time-walk by advancing `startTime` past the last row's `time`; only the 10,000 most-recent fills are retained.

---

## 6. WebSocket Subscription Catalog

### (a) Market-wide (do NOT count against the 10-unique-user cap)

| `type` | Params | Data |
|---|---|---|
| **`trades`** | `coin` | `{coin, side, px, sz, hash, time, tid, users:[buyer,seller]}` — **§7** |
| `allMids` | opt `dex` | `{mids:{coin:"px"}}` |
| `l2Book` | `coin`, opt `nSigFigs`/`mantissa`/`fast` | `{coin, time, levels:[bids,asks]}` |
| `bbo` | `coin` | `{coin, time, bbo:[bestBid,bestAsk]}` — pushed only on BBO change |
| `candle` | `coin`, `interval` | `{t,T,s,i,o,h,l,c,v,n}` |
| `activeAssetCtx` | `coin` | mark/oracle/funding/OI (perp) or supply (spot) |

### (b) Per-user (each distinct address counts against the **10 unique users / IP** cap)

| `type` | Params | Data |
|---|---|---|
| **`userFills`** | `user`, opt `aggregateByTime` | `{isSnapshot?, user, fills:[WsFill]}` — §5 |
| **`webData2`/`webData3`** | `user` | Aggregate account state incl. clearinghouseState/leverage/positions (`webData3` nests it under `perpDexStates`) |
| `clearinghouseState` | `user`, opt `dex` | Full margin/positions state (unambiguous positions+leverage feed) |
| `activeAssetData` | `user`, `coin` | `{leverage, maxTradeSzs, availableToTrade}` — perps only |
| `userEvents` | `user` | Tagged union: `{fills}`/`{funding}`/`{liquidation}`/`{nonUserCancel}` (arrives on `channel:"user"`) |
| `orderUpdates` | `user` | `{order, status, statusTimestamp}` |
| `notification`, `userFundings`, `userNonFundingLedgerUpdates`, `spotState`, `twapStates`, `userTwapSliceFills` | `user` | as named |

**Lifecycle:** subscribe → `subscriptionResponse` ack → data pushes on `channel:<type>`. Server closes any connection idle 60s → send `{"method":"ping"}` every ~30s, expect `{"channel":"pong"}`. **Post requests** over WS: `{"method":"post","id":N,"request":{"type":"info","payload":{…}}}` → reply on `channel:"post"` (≤100 in-flight; `explorer` not supported over WS).

---

## 7. The `trades` feed and the `[buyer, seller]` convention

```typescript
interface WsTrade {
  coin: string; side: string;   // "B"=buy-aggressor, "A"=sell-aggressor
  px: string; sz: string; hash: string; time: number; tid: number;
  users: [string, string];      // [buyer, seller] — NOT [maker, taker]
}
```

- **`users[0]` bought (+sz), `users[1]` sold (−sz).** Confirmed by the official docs **and** verified on the live socket by `hyperdash-crawl` (2026-06-16): across buy-aggressor BTC trades the resting maker consistently appeared at index 1.
- A tracker matches its watchlist against **both** entries (a wallet may be on either side).
- The feed gives **executions only** — no leverage, no open/close semantics, no PnL. Those come from the state machine (open/add/reduce/close) plus REST `clearinghouseState` enrichment.
- `tid` is a 50-bit hash of `(buyer_oid, seller_oid)`; for a globally unique key use `(time, coin, tid)`.

This is why the firehose path scales to unlimited wallets on **one** market-data connection: one `trades` subscription per coin covers every wallet trading that coin.

---

## 8. Rate-Limit Budget

| Scope | Limit | Relevance to tracker |
|---|---|---|
| REST weight / IP | **1200 / min** (info + exchange aggregated) | Ample for enrichment |
| `clearinghouseState`, `allMids`, `l2Book` weight | **2** | ~600 clearinghouseState/min possible |
| Default info weight | 20 (+1 per 20 items on paginated; +1 per 60 for candles) | — |
| **Address limit (1 req / 1 USDC traded, +10k buffer)** | **exchange ACTIONS only — NOT info reads** | **Irrelevant** — read-only tracking is never throttled by it |
| WS connections / IP | **10** | ≥1; shard per-user subs across them |
| **WS unique users (per-user subs) / IP** | **10** | **The architectural fork** |
| WS total subscriptions / IP | 1000 | trades-per-coin fits easily |
| WS inbound msgs / IP | 2000 / min | ping + subs well under |
| Breach behavior | HTTP 429 soft-throttle, no ban | back off exponentially |

**Implication:** a read-only tracker of *tens* of wallets never approaches any limit. The only hard wall is 10 unique users for *per-user* subscriptions — which is exactly what dictates the tiered architecture in the next section.

---

## 9. Synthesis — Recommended Architecture for `hyperliquid-trader-tracker`

A judge panel scored three candidate architectures. The winner (**88/100**) is **firehose-first**, beating tiered-adaptive (79) and per-user-subscription (68). Rationale: it is the tightest fit to the *stated* requirements (small watchlist, store nothing, correct open-vs-add, check pre-existing position) while spending the fewest lines of net-new, unverified code — and its ingestion cost is **independent of watchlist size** because `trades`/`allMids` are market subscriptions that never touch the 10-unique-user cap.

### 9.1 Data flow

```
 wss://…/ws  ── trades (every perp coin) ──▶ resolve_deltas(trade, watchlist)   [users:[buyer,seller] filter]
             ── allMids ──▶ marks                     │  signed ResolvedFill (+sz buyer / -sz seller)
                                                       ▼
                                   InMemoryBook.ingest(addr, coin, delta, px, ts)
                                       = dict get → apply_fill(state,…) → dict set
                                                       │  LiveEvent(s): open / add / reduce / close
                                                       ▼
                                   notifier.dispatch(event)  ──▶  PUSH (Telegram)
                                       open → "Started trade …"   add → "Added to position …"

  POST /info clearinghouseState (weight 2) ──▶ build_position ──▶ book.seed(…)   [SILENT, no push]
      · at startup for the initial watchlist   · on every /add   · optional periodic reconcile (leverage/drift)
```

**The single default path is the firehose.** Per-event dispatch (not hyperdash's windowed batching) is correct here because a personal watchlist is low-volume.

### 9.2 The cold-start guarantee (the user's "check pre-existing position" requirement)

With no storage, the in-memory book is empty at boot and for every newly-added wallet — so a naive first fill on a day-old position would mis-fire as *"Started trade"*. Solved by a **mandatory synchronous pre-seed**, reusing the verified silent primitive `store.seed_positions` (installs state **without emitting**):

1. On startup (initial list) **and on every `/add`**, POST `clearinghouseState` (weight 2) → `build_position` → `seed_state_from_row` installs each open coin as one leg at its entry price.
2. **Order matters:** seed **first**, *then* admit the address to the `resolve_deltas` filter set. Any trade before admission is simply not resolved.
3. Result: yesterday's BTC long is already in the book, so today's same-direction fill resolves to `EVENT_ADD` → *"Added to position"*, never a false *"Started trade"*.
4. **Unseeded guard:** if the seed REST call fails after retries, **quarantine** that wallet (no emits) until a seed succeeds — an add can never be mislabeled during a failed seed.

The chain *is* the durable store, queried on demand — "store nothing" holds while cold-start correctness is preserved.

### 9.3 Two optional grafts (only while watchlist ≤ 10)

Strictly additive, flag-gated, and switch off cleanly past 10 wallets with the firehose still fully correct underneath:

- **Graft 1 — `userFills` cross-check:** open a per-user `userFills` sub (inside the 10-user cap), drop the `isSnapshot:true` backfill, and use each live fill's `startPosition` as an **authoritative corrector** for the firehose-derived book. This closes the one real firehose weakness — a single missed `trades` message silently drifting the book — without waiting for the eventually-consistent reconcile.
- **Graft 2 — `webData2` push-leverage:** a per-user `webData2` sub eliminates leverage staleness between reconciles on the small tier. (Note: `webData2` is a full-state push, **not** an `isSnapshot` history backfill — handle by replacing local state, not deduping fills.)

### 9.4 Notification formats

| Event | Format |
|---|---|
| **open** | `🟢 Started trade {LABEL}: {COIN} {Long\|Short} {size} @ {px} ({lev}x)` |
| **add** | `➕ Added to position {LABEL}: {COIN} {Long\|Short} +{size} (~${notional}) @ {px} ({lev}x)` |
| **reduce** *(ext.)* | `➖ Reduced {LABEL}: {COIN} {Long\|Short} -{size} @ {px} \| realized {±}${closedPnl} \| {remaining} left` |
| **close** *(ext.)* | `🔴 Closed {LABEL}: {COIN} {Long\|Short} {size} @ {px} \| realized {±}${closedPnl}` |

`notional = |delta| × mark` (mark from `allMids`). Lead with the human **label** so the user knows which wallet without decoding the address. A **flip** naturally renders as close + Started-trade (the state machine already emits close+open).

### 9.5 Module layout (in the new repo)

```
src/tracker/
  state.py     ← vendored VERBATIM from hyperdash live/state.py (apply_fill + PositionState + LiveEvent)
  resolve.py   ← resolve_deltas + perp_coins_from_meta + seed_state_from_row (pure parts of listener.py)
  book.py      ← InMemoryBook: dict[(addr,coin)→PositionState] + leverage cache; ingest()=get/apply/set; seed()=silent install
  enrich.py    ← clearinghouseState → build_position → seed + leverage cache (watchlist-add + reconcile)
  hl_client.py ← vendored VERBATIM (async /info client, transient retry)
  retry.py     ← vendored VERBATIM (backoff/jitter)
  listener.py  ← trimmed Listener: connection/heartbeat/allMids/_handle_trades → book.ingest → notifier; NO Redis/Postgres
  notifier.py  ← push sink + message formatting
  watchlist.py ← in-memory {address: label}, add/delete/rename
  bot.py       ← settings UX; on /add triggers enrich.seed BEFORE admitting to the filter
  app.py       ← asyncio TaskGroup(connection loop, optional reconcile, bot)
tests/         ← port test_live_state.py + resolve_deltas tests; add cold-start test (seeded day-old long → next fill = ADD not OPEN)
```

**Reuse verified against the sibling repo:** `state.py` verbatim; `resolve_deltas`/`perp_coins_from_meta`/`seed_state_from_row`/connection loop/heartbeat/`_update_marks` lifted; `hl_client.py`+`retry.py` verbatim; `build_position` for leverage. Only the clearly-separable Redis/Postgres layer is stripped — `store.py`'s orchestration collapses to ~10 lines of dict get/apply/set.

---

## Limitations & Caveats

Corrections from adversarial re-verification of the load-bearing claims (folded in above):

1. **`trades.users = [buyer, seller]`** is documented, but the docs do *not* state the position-delta interpretation ("+sz/−sz") — that is a sound inference, not a doc guarantee. A buy can *reduce* an existing short rather than "grow" a position. **Maker/taker is a separate axis** exposed via `crossed` on `WsFill`, not by array position.
2. **The 10-unique-user cap** is verbatim, but "market subs don't count" is an *inference* from the "user-specific" scoping (those streams take no `user` param). Market subs still count toward the separate **1000-subscription** per-IP cap.
3. **`startPosition` semantics** are confirmed but **inferred from JSON examples** (the GitBook gives no prose definition; only `dir` carries an official "for frontend display" comment). Parse as a **signed Decimal** (never float — values like `10659.65434798` lose precision). Edge case: a reducing fill executed exactly at average entry realizes `closedPnl == 0`, so don't treat `closedPnl != 0` as the sole reduce test.
4. **Info reads are unauthenticated and exempt from the address (USDC-traded) limit**, but still subject to the **IP 1200 weight/min** budget (≤600 `clearinghouseState`/min on one IP).
5. **`isSnapshot`** backfill behavior applies to **`userFills`/`userFundings`** (time-series). **`webData2` is NOT** an `isSnapshot` history feed — it's a full account-state push; handle by state-replacement. The snapshot is "can be ignored," i.e. recommended dedupe, not a hard normative MUST.

**Design-level residual risks:** (a) `resolve_deltas` depends on the empirically-verified `[buyer, seller]` ordering — a single point of semantic failure; re-verify on a live socket before shipping. (b) Firehose book drift from a missed `trades` message (fixed by Graft 1 on a small list, else eventually-consistent reconcile). (c) Boot/seed race window (bounded; narrow with buffer-then-apply). (d) Restart collapses a multi-add leg to a single leg at current `entryPx` (fine for notifications, not for avg-cost analytics). (e) No dedup by default → a WS reconnect can double-notify; add an in-memory recently-seen `tid` ring buffer. (f) Notification spam from an active wallet → consider a per-(wallet,coin) debounce.

## Recommendations

1. **Build the firehose-only path first** — it is correct at any watchlist size and reuses the most proven code. Ship open+add; leave the one-line reduce/close dispatch hooks.
2. **Make the pre-seed mandatory and ordered** (seed → admit). This is the entire correctness of the pre-existing-position requirement.
3. **Add the `tid` ring buffer** for reconnect idempotency from day one (cheap, in-memory, high-value).
4. **Re-verify `[buyer, seller]` on a live socket** before shipping.
5. **Defer the two grafts** behind a flag; only worth it if the list stays ≤10 and you want authoritative per-fill cross-check / push leverage.
6. **Hand to `/python-expert`** with the module layout above once the open questions (§ below) are resolved.

## Open Decisions (for `/grill-me`)

1. **Delivery channel** — Telegram bot (matches screenshots) vs an abstracted sink (Telegram/Discord/ntfy/webhook)?
2. **Watchlist persistence vs "store nothing"** — a tiny config file/TOML for `{address: label}` (configuration, survives restart) vs purely in-memory re-entered each boot?
3. **Which events notify** — open+add only (your two examples), or also reduce/close/flip with realized PnL?
4. **Watchlist size & growth** — firmly ≤10 (enable the per-user grafts) or could grow to dozens/hundreds (pure firehose, skip grafts)?
5. **Spot vs perps** — perps-only (all confirmed API facts are perp) or also spot fills?
6. **Leverage freshness tolerance** — best-effort/last-confirmed acceptable, or exact-per-notification (forces `webData2` graft or per-event REST)?

## Methodology Appendix

Retrieval: three parallel research agents mapped the Info HTTP API, WebSocket API, and rate-limit/auth posture against the official Hyperliquid GitBook (fetched 2026-07-01), cross-referenced with Chainstack/QuickNode mirrors for field schemas. A synthesis workflow then (a) ran five independent adversarial skeptics that each attempted to *refute* a load-bearing claim against the primary docs, and (b) ran a three-proposal design panel scored by a high-effort judge that verified reuse claims against the `hyperdash-crawl` source. Reusable implementation facts (the live-verified `[buyer, seller]` convention, the `apply_fill` state machine, the silent `seed_positions` primitive, `build_position`'s leverage extraction) were confirmed by reading the sibling repo directly. All numeric limits, the `[buyer, seller]` ordering, `startPosition` semantics, and `isSnapshot` behavior are quoted from primary docs in the Bibliography.

## Bibliography

Primary (official Hyperliquid docs, fetched 2026-07-01):
1. [Info endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint)
2. [Perpetuals > Info endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals)
3. [Spot > Info endpoint](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot)
4. [WebSocket](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket)
5. [WebSocket > Subscriptions](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions)
6. [WebSocket > Post requests](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/post-requests)
7. [WebSocket > Timeouts and heartbeats](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats)
8. [Rate limits and user limits](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits)
9. [API overview](https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api)
10. [hyperliquid-python-sdk (official)](https://github.com/hyperliquid-dex/hyperliquid-python-sdk)

Secondary (mirror providers, for field schemas):
11. [Chainstack — userFills reference](https://docs.chainstack.com/reference/hyperliquid-info-user-fills)
12. [QuickNode — webData2](https://www.quicknode.com/docs/hyperliquid/info-endpoints/webData2)
13. [QuickNode — userRateLimit](https://www.quicknode.com/docs/hyperliquid/info-endpoints/userRateLimit)
14. [OneKey — rate-limit best practices](https://onekey.so/blog/ecosystem/hyperliquid-rate-limits-best-practices/)

Internal:
15. `hyperdash-crawl` — live-verified `trades` listener + `apply_fill` state machine (`[buyer, seller]` verified on live socket 2026-06-16).

