# Data sources — onchain-crime-watcher

Reference for builders and reviewers. As of 2026-10-25. Contract: [`../SCOPE.md`](../SCOPE.md)
(revision 6, Q1–Q6 / R1–R12). Siblings: `feed.md`, `bundle.md`, `ranking.md`, `operations.md`,
`backtest.md`, `failover.md`.

Evidence tags:

| Tag | Meaning |
|---|---|
| **D** | Documented by the vendor, read 2026-10-25 at the URL named |
| **O** | Observed: working client code, a live fetch made for this document, or the captured payload in `REVIEW.md` |
| **C** | Community-reported or third-party; not contract, not reproduced here |
| **U** | Unknown. The test that resolves it is named |
| **V** | Verify at runtime; recited rather than read today |

## 0. What was actually checked

| Item | Method | Result |
|---|---|---|
| PumpPortal subscription page | Read `pumpportal.fun/data-api/real-time` | Methods, metering, key rules, one-socket rule confirmed (**D**) |
| Working client | Read `~/Projects/volume-scanner/src/spine/*.mjs`, commit `7d6613b` | Field usage and connection behaviour (**O**) |
| DexScreener | Live CTO fetch ~17:25Z; docs at `docs.dexscreener.com/api/reference` | 200, 6 entries, oldest `claimDate` 2026-10-24T22:47Z (**O**); CTO 60 req/min documented, pair endpoints no stated limit (**D**) |
| SOL/USD | Binance `data-api…/ticker/price?symbol=SOLUSDT`; Pyth Hermes | Binance 200, ~0.97 s (**O**); Pyth feed list 200, `latest` price 401 (**O**) |
| Helius RPC docs | `helius.dev/docs`, `getTransfersByAddress` reference | Method is Helius-only; credit cost not stated on those pages (**D/C**) |

Not checked here: any live PumpPortal **trade** subscription, any paid RPC call, Discord limits, any
Geyser provider, or any secondary-provider client license. Those claims are **U**/**V**. No trade
frame was captured for this document; `REVIEW.md` B4 is the only captured trade payload in evidence.

## 1. Coverage matrix by venue

"Coverage" means the individual trades needed for volume, percent-sold and ranking. A price seen
through an index is not coverage.

| Venue | Known | Expected | Test | Tag |
|---|---|---|---|---|
| pump.fun bonding curve | `subscribeTokenTrade` delivers trades; payload carries reserves; no timestamp or slot | Every trade for a watched mint, 100–500 ms behind chain | C1, C6, C7 | **O** |
| PumpSwap | Current post-graduation destination; migration events are free; whether trades continue there unverified | Covered, same shape, reserve fields possibly frozen | C4 | **C/U** |
| Raydium | Legacy destination; no subscription path found in any source read | **Not covered** until C5 proves otherwise | C5 | **U** |
| Meteora, Orca, Moonshot | Display names only in the client's `DEX_NAMES`; no feed path | Not covered | C5, one mint per venue | **O/U** |
| Bonk.fun | Reported to surface through the same PumpPortal stream | Unknown | C1: attribute captured mints by program and pair `dexId` | **C/U** |
| DexScreener index | Pairs and prices across the venues above | A price, not a feed | Not a venue test; index per scope §7 | **D** |

### 1.1 The Raydium gap

- **Blast radius.** Tokens migrating to Raydium stop producing events: the volume ring freezes at
  the last curve trade, percent-sold stalls at 60% or 80% and never shows *cleared*, and a ranked
  wallet's Raydium PnL and breadth are understated. `ranking.md` must exclude them explicitly.
- **Undetectable from the migration frame.** The venue field is **U**; the client reads only `mint`
  (`ingest.mjs:107-111`, **O**), so it cannot even name the destination.
- **Required counters and wording.** `trades_by_venue`, `mints_migrated_by_venue`,
  `mints_with_zero_events_after_migration`. Every ranking card quotes mints watched and per-venue
  coverage (§2C); a card without Raydium never says "top gainers on pump.fun"
  (`operations.md`, `ranking.md`).

## 2. Sources

### 2.1 PumpPortal websocket

Endpoint (**D**): `wss://pumpportal.fun/api/data?api-key=…`. The client connects to
`wss://pumpportal.fun/api/data` with **no key** (`ingest.mjs:11`, **O**); unkeyed trade subscriptions
are **U** and must not be assumed.

| Method | Metering (**D**) | Yields | Use | Tag |
|---|---|---|---|---|
| `subscribeNewToken` | Free | `create` | Admission, watchdog liveness, launch-window subs | **D/O** |
| `subscribeMigration` | Free | migration | Regime boundary, PumpSwap test | **D/O** |
| `subscribeTokenTrade` | 0.01 SOL / 10,000 messages | `buy`/`sell` per mint | Volume, bundle, percent sold, shadow | **D/O** |
| `subscribeAccountTrade` | 0.01 SOL / 10,000 messages | listed wallets' trades, any mint | Watch-set matching (§2C) | **D/O** |
| `unsubscribe*` | Not on the page read | ends a subscription | Prune and cap enforcement | **U**; client sends `unsubscribeTokenTrade` (**O**) |

Constraints (**D**): one websocket at a time; repeated connection attempts may be timed out and bans
expire hourly; trade and account subscriptions require a key linked to a wallet holding **≥ 0.02 SOL**;
metering is per message received, `SOL/day = messages_per_s × 86,400 / 10,000 × 0.01`. At a
hypothetical 300 msg/s that is ~26 SOL/day; real rate unmeasured until M1. Balance-exhaustion
behaviour is **U** — monitor with `getBalance` and alert below a floor. The one-socket limit is per
provider: a second PumpPortal socket or a second key is a ban risk and is not a failover path. A
second feed means a second provider (`failover.md` §0–1).

Latency: 100–500 ms new-token (`SCOPE.md` §4), 200–1,000 ms trade (`README.md`). Estimates (**U**);
M1 measures per-stage histograms at a stated rate.

Failure modes:

- Close/DNS/TLS: visible; reconnect; one outage card per process lifetime (§6.6).
- **Silent loss while connected**: no sequence numbers, no replay, no gap marker (**O**). Silence is
  not a quiet market; state stays `single-source, unverified` until a shadow exists (§6.1, §6.4).
- Malformed frames: count, sample, drop. Unknown `txType`: count, never crash-loop.
- Duplicate after reconnect: the client dedups on `signature` alone (**O**), losing the second trade
  in a multi-trade transaction. Dedup must be
  `(signature, mint, trader, txType, tokenAmount)` (§5, `feed.md`).
- Watchdog: the client force-closes after 90 s of silence (**O**) — fixed threshold, reconnect can
  manufacture a gap. §6.3's adaptive, probe-first rule replaces it.

Cannot prove: completeness for any window; ordering; event time; that a subscription was accepted.
A silent accept and a rejected subscription look identical until events arrive, so the cap probe
(C3) must judge by delivery, not by absence of an error.

### 2.2 Solana RPC

Truth rung: on-chain state read by us. Credit prices were not on the pages read (**U**); measure in
the provider dashboard during M1 and record the date.

| Method | Use | Documented shape | Failure mode | Cannot prove |
|---|---|---|---|---|
| `getSignaturesForAddress` | Per-mint heal; funding entry | `before`/`until`, `limit` ≤ 1,000, commitment; returns signature, slot, `err`, `memo`, `blockTime` | Cursor mishandling; provider prunes history | That a page boundary lost nothing; instructions need `getTransaction` |
| `getTransaction` | `signature → slot`, `blockTime`, fee, pre/post balances | `maxSupportedTransactionVersion`, `jsonParsed`; `meta`, `blockTime`, `slot`, `fee` | Old txs unavailable; `blockTime` nullable | Trades the feed never delivered |
| `getMultipleAccounts` | Opening bags, reconciliation | ≤ 100 accounts; `value: null` for missing | Closed ATA reads null, not zero | Wallet holdings without enumerating every token account |
| `getBlocks` / `getBlock` | Global block-level heal fallback | Slot range; `getBlock` gives `blockTime` and full transactions | Skipped slots; retention; large responses | Per-mint attribution without decoding every transaction |
| `getTokenLargestAccounts` | Top-20 token accounts | 20 largest accounts with `address`, `amount`, `uiAmount` | 20 is not "holders"; owners need a second lookup | Holder count, unique owners, anything below the top 20 |
| `getTransfersByAddress` | Funding in one call: inbound native SOL | **Helius-only**, not standard JSON-RPC. `direction=in`, `mint=So111…1111`, `filters` on `blockTime`, `limit` 1–100 | **C:** 10 credits, paid plans, not batchable; excludes failed txs and balance-delta SOL | A funder routed through an intermediary program, or outside the window |

- **Heal.** `getSignaturesForAddress` on the pump.fun program is global and unattributed; heal must
  be per-mint via the bonding-curve key (§6.3). If `create` lacks `bondingCurveKey`, the design has
  no key — C2 is a blocker. `getTransfersByAddress` trades credits for latency in the funding stage
  and is a Helius dependency; every other provider falls back to signatures + transactions
  (`bundle.md`, C11).
- **Creator identity.** `create.traderPublicKey` is the deploying wallet (**O**, stored as `dev`).
  `getMint` authority is a capability fact, not identity. Call it "deployer wallet" on cards.

### 2.3 DexScreener

| Endpoint | Documented (**D**, 2026-10-25) | Observed (**O**) | Use |
|---|---|---|---|
| `GET /community-takeovers/latest/v1` | 60 req/min, no parameters | 6 entries; fields `chainId`, `tokenAddress`, `claimDate`, `description`, `header`, `icon`, `links`, `openGraph`, `url` | CTO only |
| `GET /latest/dex/tokens/{mint}` | No limit stated | `pairs[]`: `chainId`, `baseToken.{address,name,symbol}`, `info.imageUrl`, `dexId`, `pairAddress`, `marketCap`, `fdv`, `liquidity.usd`, `volume.m5`, `txns.m5.{buys,sells}`, `priceChange.m5`, `pairCreatedAt` | Pool price, card context |
| `token-pairs/v1/{chain}/{mint}`, `tokens/v1/{chain}/{mints}` | Paths **V** | Unused | Possible pair-poll replacements |

- `liquidity.usd` is nullable: null is unknown, not zero. No gate may compare it numerically.
- There is **no maker or trader-count field**. "N wallets traded" cannot come from DexScreener.
- Pair choice is unstable: max-liquidity selection can switch pairs between polls, switching
  `priceUsd`, `marketCap` and `pairCreatedAt`. Pin the pair once known; label a switch.
- CTO retention is a sample of one: 6 entries spanning ~18.6 h today (oldest 2026-10-24T22:47Z,
  fetched 2026-10-25T17:25Z). A longer restart can miss claims; persist both the high-water
  `claimDate` and a bounded posted-token set in `data/cto_state.json`, and prime on boot without
  alerting (§2B, Q5). Claims first seen below the high-water are counted `suppressed_by_high_water`
  and are not recovered.
- A listing certifies that someone claimed the token — nothing about holdings or the claimant.

Failure modes: 429 on burst (serialise, back off); vendor cache; empty `pairs` means no pool, not no
data. Pair data older than its max age is `stale`, not current.

### 2.4 SOL/USD tickers

| Source | Shape | Cost | Observed | Use |
|---|---|---|---|---|
| Binance `data-api.binance.vision/api/v3/ticker/price?symbol=SOLUSDT` | `{symbol, price}` | Free, no key | 200, ~0.97 s; client polls 60 s and keeps last on failure | Display only |
| Pyth Hermes `updates/price/latest` | Parsed price with `publish_time` | Key now required? | 401 from this host; feed id `ef0d8b6f…b56d`, `min_channel: real_time` | Price age; **U** until a key path is confirmed |
| Pyth on-chain price account | Account data via RPC | One RPC call + decode | Not tested | Keyless alternative; needs a decoder |

The client has no age field and silently keeps the last price (`ingest.mjs:64`). R9 requires
`sol_usd_age_ms` on every USD figure; past max age, show `USD unavailable` and keep SOL math — the
signal never depends on FX.

### 2.5 Yellowstone Geyser and the secondary feed

- **Role.** From M6 this is the **warm standby** in `failover.md`: a full second pipeline, not a
  sample. It is promoted only when the primary is banned, failed or out of contact beyond the
  outage threshold; it is never summed with the primary.
- **Provides:** gRPC transaction or account stream filtered by account/program, at a chosen
  commitment; carries slot, `blockTime`, instructions, pre/post balances. The only source here that
  could prove completeness rather than liveness.
- **Cost:** paid, provider-specific, **U**; own decode required. Not an M1 dependency; it gates M6.
- **License:** repository default AGPL-3.0-only; upstream explicitly licenses client and proto
  directories Apache-2.0. See `failover.md` §14 and upstream LICENSING.md; the standard Rust client
  is eligible subject to a pinned dependency-tree check. M6 ships only with a permissively licensed provider
  SDK or a client generated from permissively licensed proto definitions, and CI denies copyleft
  licences in the binary's dependency tree (`failover.md` §14).
- **Latency:** 10–100 ms is an estimate (**U**); measure against the same window as PumpPortal.
- **Failure:** provider drops are invisible without slot-continuity accounting; a filter change
  silently stops matching; reconnect needs a slot cursor or the gap returns at higher cost.
  Commitment choice changes latency and fork exposure and must be stated.
- **Cannot prove:** that every slot arrived, unless the bot counts slots itself. Raw truth about
  what arrived is not a completeness guarantee. A standby with its own gaps cannot certify a
  primary, and two feeds may share upstream infrastructure — the RPC shadow stays.

## 3. Observed payload schemas

**Observed, not documented.** PumpPortal publishes no schema (**D**: today's docs only parse and
print). M1 pins the key set in a fixture test before detectors depend on it.

### 3.1 Trade (`txType: buy | sell`)

```json
{
  "signature": "…", "mint": "…pump", "txType": "buy",
  "traderPublicKey": "…", "tokenAmount": 1234.567, "solAmount": 0.0123,
  "newTokenBalance": 1234.567, "marketCapSol": 30.12,
  "vSolInBondingCurve": 5.43, "vTokensInBondingCurve": 987654321.0,
  "bondingCurveKey": "…"
}
```

| Field | Evidence | Used for | If absent or wrong |
|---|---|---|---|
| `signature` | **O** | Dedup, RPC resolution, heal | Dedup collapses; no audit path |
| `mint` | **O** | Routing, registry key | Frame unusable; drop and count |
| `txType` | **O** | Side; "entered" vs sell | Direction unknown; wallet family must not fire |
| `traderPublicKey` | **O** | Takers, bundle membership, matching | Participation test unavailable |
| `tokenAmount` | **O** | Bundle share, supply-normalised measures | Bundle percent unavailable |
| `solAmount` | **O** (client `state.mjs:67`, `ingest.mjs:78`); observed but not guaranteed | **All volume math, percent sold, PnL** — normalized to `sol_lamports` + `sol_source` | Adapter falls back to the signed vSol delta (`ReserveDelta`); with neither, no signal — the load-bearing field |
| `newTokenBalance` | **O** (captured; unused by client) | Percent-sold denominator (R4) | Fallback to observed token-size inventory only with complete basis; never infer sold percentage from SOL proceeds; semantics **U** until C6 |
| `marketCapSol` | **O** | Card cap, ring close, price proxy | Unavailable; do not derive silently |
| `vSolInBondingCurve` | **O** | Curve price and fill | Curve regime unusable |
| `vTokensInBondingCurve` | **O** (captured; unused by client) | Curve price `vSol/vTokens` | No curve price; cap is not a substitute |
| `bondingCurveKey` | **O** (captured trade; unused by client) | Per-subscription heal (§6.3) | Block-scan heal; declare unhealed |

`REVIEW.md` B4 now lists `solAmount` as observed in the working client but absent from its captured
sample, and the client's whole volume computation reads it: the fixture must pin the full key set,
including the `ReserveDelta` fallback.

Missing by design: no timestamp, slot, block time, pool address or fee fields (**O**). Bucketing is
receive-time (R1); fees are estimated, not computed (§2C).

### 3.2 Create (`txType: create`)

```json
{
  "signature": "…", "mint": "…pump", "txType": "create", "traderPublicKey": "…",
  "name": "…", "symbol": "…", "uri": "…", "initialBuy": 0.5, "solAmount": 0.5,
  "marketCapSol": 28.1, "vSolInBondingCurve": 0.5,
  "vTokensInBondingCurve": 1000000000.0, "bondingCurveKey": "…"
}
```

| Field | Evidence | Depends on it |
|---|---|---|
| `mint`, `signature`, `traderPublicKey`, `name`, `symbol` | **O** (client) | Registration, card identity, deployer |
| `solAmount`, `marketCapSol`, `vSolInBondingCurve` | **O** (client) | Opening gate, first bucket, opening bag |
| `initialBuy` | **C** | Opening buy; client uses `solAmount` instead |
| `uri`, `bondingCurveKey`, `vTokensInBondingCurve` | **C/O** (captured; unused by client) | Metadata, heal key, curve price |
| `pool`, `quoteMint`, `venue` | **U** | Would settle the Raydium gap directly |

### 3.3 Migration (`txType: migration`)

Only `mint` is observed in use (**O**). `pool`, `quoteMint`, `quoteAmount`, `mintAmount`,
`signature`, `slot`, `user` are **C** from unrelated third-party decoders whose formats differ from
each other; none is PumpPortal's contract. Schema is **U**: build no venue attribution on it until
C1 captures a real frame. If it carries a pool, C5 is a lookup; if not, resolve the pool via RPC.

## 4. Capability matrix

| Source | Signature | Slot | Block time | Curve reserves | Pool price | Creator identity | Balances |
|---|---|---|---|---|---|---|---|
| PumpPortal create | Yes **O** | No **O** | No **O** | Yes **O** | No | Deployer **O** | Opening buy SOL **O** |
| PumpPortal trade | Yes **O** | No **O** | No **O** | Pre-migration **O**; post **U** | No | No | `newTokenBalance`, semantics **U** |
| PumpPortal migration | **U** | **U** | No | Frozen/absent **U** | No | No | No |
| PumpPortal account trade | Yes **O** (global dedup in client) | No | No | If on curve | No | No | As trade |
| `getTransaction` | Yes | Yes | Yes | No (decode account) | No | Fee payer, authority state | Pre/post balances **O** |
| `getTransfersByAddress` | Yes | Yes | Yes | No | No | No | Transfer amounts only |
| DexScreener CTO | No | No | No | No | No | No | No |
| DexScreener pairs | No | No | `pairCreatedAt` only | No | Indexed | No | No |
| SOL/USD ticker | No | No | No | No | No | No | No |
| Geyser | Yes | Yes | Yes | Decode-dependent | Decode-dependent | Fee payer | Via meta |

A secondary adapter that decodes the instruction amount records `sol_source = Instruction`; the
trade shape and detections are unchanged, and each source keeps its own dedup and generation stream
(`failover.md` §6). A source without slot and block time cannot be authoritative for the ranking
audit or for two-source replay, whatever its trade feed looks like.

Derived values, and how far each travels (label on the card):

| Value | Derivation | Validity |
|---|---|---|
| Curve price | `vSol / vTokens` from the payload | Exact at that trade; not a pool price; incomparable across regimes |
| Curve fill | `vSol / graduation constant` | Constant is config guess, recalibrated by each migration (**O**) |
| Market cap | `marketCapSol × SOL/USD` | Two hops from truth; USD age required |
| Pre-migration depth | `2 × vSol` | Client approximation (**O**); not LP |
| Bundle share | Σ non-deployer buy `tokenAmount` in window / 1e9 | Lower bound; a bundle inside socket latency is invisible |
| Percent sold | Original token fractions disposed, weighted by original SOL cost | Sale proceeds separate; missing history/transfers partial; complete history required for CLEARED |
| Realised PnL | Lamport average cost from raw buys/sells | Fees estimated; transfers excluded and counted |
| Pool price | DexScreener `priceUsd` | Third-party index; pin pair, timestamp it |

## 5. Verification plan

| # | Milestone | Test | Pass criteria |
|---|---|---|---|
| C1 | M1 | Raw fixture: log frames unparsed ≥ 1 h on free streams, plus 10 min on one gated mint; save bytes, `recv_ts`, SHA | ≥ 1 create, ≥ 50 trades, migration if it occurs; committed fixture; schema test pins the exact key set and fails on unknown keys until reviewed |
| C2 | M1 | Create schema: assert `bondingCurveKey`, `initialBuy`, `vTokensInBondingCurve` on `create` specifically | All present → heal design stands. `bondingCurveKey` absent → heal redesign before M4; record in `feed.md` |
| C3 | M1 | Cap probe on the single socket: `keys` at 1/50/100/200/500; concurrent mints at 50/200/500; account keys at 40/200 | Judged by **events delivered**, not silent acceptance; record largest accepted and first failure; no parallel sockets; run once to avoid a ban |
| C4 | M1 (gates M5) | PumpSwap coverage: fresh migration, subscribe 10 min, compare signature set with `getSignaturesForAddress(pool)` | ≥ 95% of RPC signatures seen; buys and sells present; reserves either correct or declared frozen; `dexId == pumpswap` cross-check |
| C5 | M1 (gates M5) | Raydium: same method on a known Raydium pool for a pump.fun mint | Zero events while RPC shows trades → gap confirmed; enforce exclusion and wording. Non-zero → run C4 checks, record date |
| C6 | M1 | `newTokenBalance` semantics: 20 trades vs trader's post-trade ATA balance | Equals trader post-trade balance ≥ 19/20; otherwise record what it measures and never call it a wallet balance |
| C7 | M1 | Events/min vs RPC signature count, one mint, 1 h | Within the tolerance stated in M1 acceptance; disagreement reported, not smoothed |
| C8 | M2a | Discord limits on staging webhooks: increasing rates, 3 hooks, posts and edits | 429 onset, `retry_after`, `X-RateLimit-Scope`, whether edits share the bucket; numbers dated; "5 per 2 s" stays **C** until measured |
| C9 | M2b | Shadow: ~20 mints, aligned 10-min windows, RPC signature counts vs primary | Disagreement at p50/p90 by stratum; `quiet market` blocked outside tolerance |
| C10 | M3 | CTO retention: poll 60 s for 48 h; record count, oldest/newest `claimDate`, disappearance order | Retention floor and maximum safe restart window; high-water file proven by restarting mid-run |
| C11 | M4 | Funding cost: 10 wallets through signatures+transactions and through `getTransfersByAddress` | Wall time and credits per wallet for both; confirm plan and price in dashboard; record any funder one path finds and the other misses |
| C12 | M2b/M5 | SOL/USD staleness: cut the ticker | After max age, cards show USD unavailable with `sol_usd_age_ms`; no alert suppressed or altered |
| C13 | M6 | Secondary shadow: one mint, 10 min, slot-continuous, against the primary | Signature-set disagreement reported; slot gaps counted separately from trade gaps; both sources' buckets never merged |
| C14 | M6 | Provider selection: read the client license, measure filter cap, events/s, latency and spend for at least two candidates | One candidate passes the copyleft gate and carries slot and block time; `feed.secondary.filters` set from measurement, pricing recorded with a date |
| C15 | M6 | Forced-failure drill: 20 primary failures (close, ban, stall) and 20 secondary failures | Every primary failure switches within `switch_after_s + 5 s`; secondary failures do not switch; zero duplicates, boundary frames exact, warm rings preserved (`failover.md` §12) |

## 6. Credentials and prerequisites

| Source | Credential | Balance / tier | Runs without it | Fails closed how |
|---|---|---|---|---|
| PumpPortal free streams | Key per docs **D** | None | Unkeyed client works (**O**) **U** | Free streams may run; never presented as trade coverage |
| PumpPortal trades / account | Same key | Wallet ≥ 0.02 SOL, kept funded | No | Trade families off, `subs_live=0`, health red; CTO may run |
| Solana RPC | Provider key | Paid tier for history and `getTransfersByAddress` | Public endpoint may serve some calls **U** | Heal, bundle, balances `rpc unavailable`, never zero; ranking refuses a thin watch set |
| DexScreener | None | Free | Yes | CTO degrades independently of trade health |
| SOL/USD | None (Binance); Pyth key **U** | Free / RPC | Yes | USD with age, then unavailable; SOL math unaffected |
| Discord | 3 production + 3 staging webhooks; optional bot token | None | `--dry` | Startup check before feed; dead-hook breaker |
| Geyser / secondary | Provider key + gRPC | Paid **U** | Yes until M6 | Runs single-source with `authoritative = primary`; the boot line says failover unavailable; the license gate blocks M6 |

The PumpPortal key rides in the URL query: redact the URL before logging (`operations.md`).

## 7. Risk register

| Source | Worst credible failure | Blast radius | Mitigation |
|---|---|---|---|
| PumpPortal trades | Silent loss while connected, hours long | Missed spikes, undercounted ranking, stalled bundle percent — alerts still post confidently | Shadow (C9), `unverified` footer, disagreement tags. **Missing:** the doc can never claim heal proves integrity |
| Socket + metering + balance | Reconnect storm earns a ban; wallet drains below 0.02 SOL | Every trade, wallet and migration family dark; looks like a quiet market | Single-socket assert, probe-first reconnect, `banned` state, gate, caps, cost counters (§6.1–6.2). **Missing:** daily message ceiling, balance floor with check and outage card, runbook line for waiting out a ban |
| Migration payload | Venue and pool absent | Raydium gap undetectable from the feed | C4/C5 via RPC. **Missing:** card cannot name destination until resolved |
| RPC | 429 or archive pruning during a gap | Heal declared unhealed; launch records partial; funding `unknown` | Call and duration budgets, `unknown` not zero (§6.3, §6.5). Heal day budget: `feed.heal.calls_per_day` = 50,000 (owner `feed.md`), counted `heal_day_budget_hit` |
| `getTransfersByAddress` | Helius-only, non-batchable, misses balance-delta SOL | Under-clustered bundles presented as complete | Count misses, cross-check one wallet per CTO. **Missing:** explicit fallback rule off Helius |
| DexScreener CTO | Retention shorter than a restart | Missed CTO; never posted | High-water `claimDate` plus a bounded posted-token set, boot priming without alerts. **Missing:** measured retention floor in config |
| DexScreener pairs | Stale/null liquidity; pair switch | Wrong price and depth on a card | Pin pair, label index, nullable unknown. **Missing:** max age before price is marked stale |
| SOL/USD | Silent stale rate | USD figures wrong; signals unaffected | R9 + age. **Missing:** age does not exist in the client |
| Discord | 429 storm or IP ban from invalid requests | All channels mute | Per-hook queue, error-code classes (§8). Sufficient if implemented |
| Geyser | Provider drops slots without signalling | False "complete" shadow — worse than none | Slot continuity, per-window disagreement. **Missing:** a gappy shadow may not certify the primary |
| Secondary provider (M6) | Drops slots without signalling, or shares upstream infrastructure with the primary | A promoted source that is not better; false confidence after a switch | Slot continuity, per-window disagreement, forced-failure drills. **Missing:** no proof of independence between providers |
| Secondary client license | An AGPL client is linked into an MIT binary | A license violation across the whole project | CI deny-list, C14 license read before M6 starts, `references.md` records the client and version |

## 8. Open questions and recommendations

| Question | Why it matters | Recommendation | Test |
|---|---|---|---|
| Does `create` carry `bondingCurveKey`, matching the trade's key? | The whole per-subscription heal keys off it | Block M1 acceptance; otherwise heal from the first trade and say so | C2 |
| What does `newTokenBalance` measure? | Percent sold (R4) and every wallet measure | Test before M4; assume nothing | C6 |
| Does migration name pool and venue? | Raydium gap is currently invisible | Capture; else resolve pool via DexScreener/RPC at migration | C1, C4 |
| Do PumpSwap trades carry valid reserves? | Frozen fields would silently misprice post-migration tokens | Compare payload reserves to pool reserves | C4 |
| Are Raydium trades delivered at all? | Ranking excludes the most profitable cohort if not | Treat as gap until C5; publish per-venue coverage | C5 |
| Do unkeyed trade subscriptions work? | The client runs unkeyed; docs require a key | Require a key in config; test unkeyed only as an experiment | C3 |
| Real per-message and concurrency caps, and are they per key or per connection? | Sets the gate's hard cap and the bill | Probe once, sequentially, judged by delivery; assume per key | C3 |
| Which SOL/USD source? | Age and availability differ | Binance works keyless today; Pyth on-chain as fallback; age on every figure | C12 |
| Is CTO retention long enough for a documented restart? | Restart safety is bounded, not guaranteed: a claim first seen below the high-water is not recovered | Measure 48 h; publish max downtime | C10 |
| Is the ~11-credit funding estimate right? | Bundle cost per CTO | Measure both RPC paths, choose on measured latency and credits | C11 |
| Do Bonk.fun or Meteora appear in the launch stream? | Mints outside pump.fun break curve assumptions | Attribute captured creates by program and pair `dexId`; if present, tag launchpad and refuse curve math | C1 |
| Should Geyser become primary? | Only source that could prove completeness | Not in v1: it is the M6 warm standby, and `prefer = primary` stays the default. Promoting it needs a week of two-source agreement and a written decision | C13, C15 |
| Which secondary provider passes the license gate? | The M6 adapter is gated on a permissive client or generated proto, not on the transport name | A Geyser-protocol provider whose client is MIT/Apache-2.0, or our own client generated from permissive protos; read the license before any code | C14 |

