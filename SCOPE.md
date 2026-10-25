# onchain-crime-watcher — scope

A Discord alert bot for Solana meme tokens. Rust for the data path, latency as the design axis.
Built from scratch; `~/Projects/volume-scanner` is a reference for observed payload behaviour and
hard-won lessons only — no code is carried over.

Status: revision 3, 2026-10-25, consistency pass after the five-lens review in `REVIEW.md`. No code
until explicit go on M1.

## 0. Revision log

- **2026-10-25 — initial scope.** Six decisions (Q1–Q6) answered.
- **2026-10-25 — revision 1 (post-review).** Feed subscription and cost contract added (B1);
  token registry and lifecycle added (B2); bundle data path redesigned with a persisted launch
  record and an explicit funding stage (B3); event-time rule corrected because the payload has no
  timestamp or slot (B4); delivery contract rewritten around `wait=true` and error-code
  classification (B5); volume family parameterised as priors (B6); ranking contract made
  computable (B7). Latency table split into admission versus transport; gap heal moved to
  per-subscription; watchdog and reconnect rules tightened; boot line, heartbeat and dead-man's
  switch added; watch set and message ids given restart semantics. Decisions R1–R9 record the
  choices this revision made.
- **2026-10-25 — revision 3 (consistency pass).**
  - Feed spend is one per-UTC-day budget including launch windows (`gate.feed_sol_per_day`, prior
    0.5 SOL/day), with no launch-window exemption; the cap is an open question.
  - Trades normalize to `sol_lamports` + `sol_source` (`Payload` | `ReserveDelta`); `solAmount` is
    optional and the signed vSol delta is derived when it is absent.
  - `deliver.hold.window_ms` = 45,000 (reordering) is separated from
    `deliver.budget_wait_max_ms` = 600,000 (budget admission).
  - The watch set is a discovery set: ~120 candidates/day against a 20/day wallet cap, ~83%
    suppressed and counted `wallet_budget_suppressed`.
  - CTO state persists the high-water `claimDate` and a bounded posted-token set; claims below the
    high-water are counted `suppressed_by_high_water` and are not recovered.
  - Priority is status/outage > CTO listing > wallet entry > bundle edit > volume fire > volume
    near.
  - Block time is resolved only for final candidates and the fee sample; bulk rows use receive
    time as a labelled proxy.
  - One message-id store is owned by `deliver/`; detectors keep tiers, not ids.
  - Registry states are `created | on_curve | near_graduation (display-only) | migrated | dead |
    unknown`, and `state_seq` increments on every transition.
  - M1 ships the rank-store schema only; the writer starts in M2a; M1 acceptance counts outage
    identities and one outage card, not N outage cards.
  - Six outage identities: `trade_stream`, `cto_source`, `rpc`, `discord:volume`,
    `discord:wallets`, `discord:cto`.
  - CTO caps are named `deliver.cto.hourly_cap` = 20 and `deliver.cto.daily` = 120, the latter an
    outer guard rather than a product allocation.
  - Heartbeat and outage/status cards form a status lane that bypasses hourly and daily budgets
    entirely.
  - A Near bundle tier transition does not edit the card; only Fire and Cleared do.

## 1. What it does

Watches the pump.fun launch stream and the venues pump.fun tokens trade on, and posts ranked alerts
to Discord across three signal families. Every alert carries the same shape: what fired, on which
token, why it scored where it did, and what the bot knows it cannot see.

The bot is a live feed, not an archive. It keeps enough in RAM to compare a token against its own
recent past and its cohort, and writes nothing the alert path depends on.

**Coverage is bounded and stated.** Trades are metered per mint, so the bot watches a chosen subset
of tokens. Every ranking and every card that depends on the trade stream says which subset it was
computed over. "Biggest gainer on pump.fun" means "among mints watched".

## 2. Signals

### A. Volume anomaly

Per-token volume compared against its own recent past and its age band, not a global constant.
All numbers below are **priors**, to be revisited with the first two weeks of live data; they are
not measurements.

- **Buckets.** Δ = 15 s, ring N = 30 completed buckets (7.5 min), token-anchored at the `create`
  event. The current partial bucket is never part of its own baseline; the ring is updated only
  after the decision.
- **Baseline.** Trimmed mean of the ring, trimming the top 3 buckets so one earlier spike cannot
  raise the bar that would hide the next. Warmup = 16 completed buckets (4 min); alerts suppressed
  while warming, suppression counted.
- **Sample sufficiency.** At least 8 non-empty buckets and at least 6 trades per evaluated bucket.
  Zero-volume buckets are kept in the ring; excluding them inflates the baseline and hides quiet
  tokens waking up.
- **Dispersion.** A location-only comparison treats a token moving ±5% and one moving ±200% the
  same. Fire also requires `current ≥ median + max(4 · 1.4826 · MAD, 0.5 · median)`.
- **Participation.** At least 5 distinct takers in the bucket and no single taker above 60% of its
  volume, so one wallet cannot print a spike with a buy and a sell.
- **Floor.** `floor(Δ) = max(1.5 SOL, 2 · Δ/60 s · cohort_median_trade_SOL)`, added to numerator and
  denominator. The floor is part of the signal definition, not a safety net. Signal math is in
  SOL; the USD figure on cards converts with a cached rate and shows its age.
- **SOL fields.** Trades normalize SOL as `sol_lamports` with a `sol_source` of `Payload` or
  `ReserveDelta`. The raw `solAmount` is observed but optional; when it is absent, the signed vSol
  delta is derived from the payload reserves.
- **Cohort.** An age-banded online distribution of the evaluated ratio (0–5 m, 5–30 m, 30 m–2 h,
  2–6 h, >6 h). Fire additionally requires the ratio to reach the band's p90. This is a live
  percentile over what the bot has seen, never presented as a base rate.
- **Cut-points.** Ratio ≥ 3.0 fires; [2.0, 3.0) is *near*; < 2.0 rejects. *Near* is capped at 30%
  of the channel budget and never displaces a fire.
- **Cold baseline.** If the baseline is below the floor, the ratio is not meaningful: the token
  routes to a labelled `cold_baseline` path that requires an absolute 10× floor instead of
  silently dividing by nothing.
- **Regime boundary.** On migration, the bonding-curve ring is reset and re-warmed; the card stamps
  which regime produced the statistic. Curve price is computed from the payload reserves; pool
  price comes from the pair source and is labelled as such. The two are not comparable and never
  presented as one series.

### B. CTO alerts, with bundle status

A community takeover is a curated social claim, not an on-chain fact. Source: DexScreener's
community-takeover list.

- **Source shape.** `GET /community-takeovers/latest/v1`, unauthenticated, 60 req/min, no query
  parameters, no boolean flag — membership in the list *is* the signal, and `claimDate` is the only
  takeover-specific field. Polled on a config interval (prior 60 s), filtered client-side to
  `chainId === "solana"`, and diffed against a seen-set keyed on `tokenAddress`. Both the
  high-water `claimDate` and a bounded posted-token set are persisted and primed on boot without
  alerting, so a restart does not repost a claim already sent. A claim that first appears below the
  high-water is counted `suppressed_by_high_water` and is **not** recovered: the retention window
  can hide a claim from a long restart, and the bot states that limitation rather than promising
  no missed claims.
- **Wording.** The card states that the listing certifies someone claimed the token and nothing
  about who holds it. CTO health is independent of trade-stream health and must not imply
  otherwise.
- **Non-pump.fun CTOs.** Solana claims for tokens that are not pump.fun launches are still posted,
  with bundle and price fields explicitly `unavailable`.

**Bundle status.** How much of the launch bundle has been sold.

- **Capture.** On every `create` event the bot takes a trade subscription for that mint for a
  ~10 s launch window, then keeps it only if the token passes the trading gate (section 6.1). The
  window draws from the shared per-UTC-day trade budget; when that budget is exhausted the launch
  window is refused and `gate.budget_hit` is counted, with no exemption for launch windows. The
  10 s is a **membership filter on when the buy happened**, not a deadline: clustering runs
  afterwards, asynchronously.
- **Membership.** Wallets funded by the same funder wallet, direct or one hop, that bought inside
  the launch window, where the funding transfer is at most 60 s before the buy.
- **Exclusions.** A funder with more than 50 children in the window is treated as shared
  infrastructure; its children are excluded and the bundle is marked `partial`. A known-account
  list (exchanges, routers, programs) is excluded. Two-hop funding is not followed and the card
  says so.
- **Persistence.** The launch record — mint, t0, launch buyers, opening bags, funder clusters —
  is written to the bounded side store in section 2C's contract: off the hot path, prunable,
  allowed partial. CTOs arrive weeks after launch, so the record must outlive the process. A token
  with no launch record shows `bundle: no launch record`, never 0%.
- **Percent sold.** SOL-weighted from trade events: opening bag is the first observed balance,
  sold value is the sum of sell `sol_lamports` (`Payload`, or the derived signed vSol delta where
  `solAmount` is absent), and the ratio is Σ sold SOL / Σ bought SOL. Wallet count share and
  absolute SOL are shown beside it. Balance reads are a slow reconciliation, never the headline.
  Tokens moved to another wallet and sold there are uncounted, and the card says so.
- **Minimums.** No tier is shown unless the bundle has ≥ 5 wallets, ≥ 10 SOL bought, and ≥ 2% of
  supply. Below that the granularity is meaningless (a two-wallet bundle cannot reach 60% until
  100%).
- **Tiers and escalation.** Monotonic: 60% *near* (no edit; a near transition never edits the
  card), 80% *fire*, 95% *cleared*. Any tier at or above fire posts once if no card exists,
  otherwise edits the existing card to the latest tier, coalescing skipped tiers. No downward
  transitions; a stale read marks the field stale and never lowers a tier. At most one edit per
  message per 30–60 s.
- **Price context.** Price at the first bundle sell, now, the 1 h peak, and the change during the
  sell window. Dumped if the fall through that window exceeds 30%. "Cleared" is never shown
  without this context, because selling into strength and dumping on exit are opposite signals.

### C. Gainer-wallet tracker

Tracks the wallets that were the previous week's most profitable on pump.fun and alerts when they
open new positions.

**Ranking contract** (computable, and quoted on every card):

- **Metric.** Realised SOL profit over a rolling 7-day window defined on **trade execution time**,
  not token launch date. Block time is resolved only for final candidates and the fee sample; bulk
  rows use receive time as a labelled proxy, and the card says "7d, execution time where
  resolved". Average cost in integer lamports, computed from buys and sells normalized to
  `sol_lamports` + `sol_source` at snapshot time.
- **Fees.** Priority and network fees are not in the trade stream. They are estimated per trade
  from a config constant and labelled an estimate; for final candidates they are resolved from
  `getTransaction`. The card says which.
- **Transfers.** Moving tokens out is not a sale. Wallets with outbound token transfers above a
  config share of their position are excluded from the ranking and counted, because their realised
  PnL is understated rather than known.
- **Survivorship.** Positions still open at window close are marked to market; a position with no
  observation for 24 h is valued at zero. Open positions and their cost are shown on the card.
- **Sybil and rugger control.** Wallets are clustered by the bundle funder rule first, and one slot
  per cluster is ranked. Mints where the wallet is creator or launch-bundle member are excluded
  from breadth. Breadth remains ≥ 2 distinct mints, and each counted mint must carry ≥ 1 SOL
  deployed. A minimum position size still applies.
- **Coverage.** The ranking is over mints the bot watched. Cards carry mints watched, events seen
  and per-venue coverage. Post-migration coverage is a known risk: PumpSwap is expected to be
  covered; Raydium is treated as a gap until a live test proves otherwise (section 7).
- **Watch set.** Top ~40 wallets, config value, not validated. Persisted as a bounded snapshot with
  a generation date and reloaded at boot; a missing or stale snapshot suppresses wallet alerts and
  counts `watchset_cold` rather than pretending the family is healthy. The set is a **discovery
  set, not a delivery promise**: at ~3 entries per wallet per day it yields ~120 candidates/day
  against the 20/day wallet cap, so ~83% are suppressed by budget and counted
  `wallet_budget_suppressed`.
- **Live matching.** A dedicated account subscription over the watch-set wallets in chunks, so a
  ranked wallet is seen on any mint, not only on watched ones. The subscription is metered like
  per-mint trades and draws from the same total per-UTC-day budget, `gate.feed_sol_per_day` (prior
  0.5 SOL/day), which includes launch windows with no exemption. **Buys only** fire "entered"; a
  sell is a different event and is not announced as a new position. Exits are deferred out of v1;
  the card never implies one.
- **Dedup and pacing.** Keyed `(wallet, mint, first_seen)`, cooldown 1–4 h per `(wallet, mint)`.

**Rank aggregate store.** Realised PnL needs cost basis per wallet per mint over seven days, which
cannot live in RAM at pump.fun token counts. A bounded store on disk holds per (wallet, mint):
buys, sells, weighted cost, realised PnL, first/last trade, pruned by age. Each row records the
registry state it was written under — `created | on_curve | near_graduation (display-only) |
migrated | dead | unknown` — and the `state_seq`; `state_seq` increments on every transition, so a
stored row can be checked against the mint's current state.

- Written by one background task with batched commits, never on the alert hot path; started
  passively in **M2a** so the first week of data accumulates while M3 and M4 are built.
- Never read to produce a live alert. It is read once per week to produce the watch set, and the
  card quotes the week.
- Coverage is journaled with fsync on gap start and end, so a restart cannot turn a gappy week
  into a whole one. Below a coverage threshold the job refuses to publish a watch set rather than
  publishing a thin one, and the old set stays with an age stamp.
- Rows are filtered before allocation: minimum position size and a mint volume floor, so the
  store does not fill with bot wallets that cannot rank.

## 3. Not in scope

- A full trade history or any database the alert path depends on. The rank and launch-record store
  is the bounded, prunable side store described above: off the hot path, never read for a live
  alert, allowed to lag, allowed to be partial and say so.
- Paper trading, portfolio tracking, copy-trading, or order placement.
- Replying to users, dashboards, or a web UI.
- Multi-chain. Solana meme tokens only.
- Hot failover between feeds. A shadow verifier is in scope; automatically switching the
  authoritative source is not, without measured disagreement data.
- Exit alerts for watched wallets. Deferred; not implied by any card.
- A backtest harness. With no recorded stream, a swept threshold is a preference, not validation.

## 4. Latency model

Rust does not make the feed faster or Discord less rate-limited. The budget, split so each term
can be measured:

| Stage | Realistic | Notes |
|---|---|---|
| Upstream feed → process | 100–500 ms | Vendor-dependent; dominant term |
| Decode + detect | 5–15 µs typical, < 2 ms p99 | Measured with the rate stated; a p99 without a rate is not a claim |
| Enqueue | ~0 | Detection and decode share one task; no trade channel |
| Bucket close | 0 – Δ (0–15 s) | Volume decisions are evaluated when a bucket closes |
| Budget admission | 0 – 1/rate | 10/hr volume ⇒ up to 6 min; 6/hr wallets ⇒ up to 10 min. Burst capacity 2–3 posts a cluster immediately |
| HTTP POST to 2xx | 100–300 ms | Discord, independent of our code |
| CTO end to end | minutes to hours | Curation on the vendor's side dominates; the poll adds 0–60 s |
| Volume end to end | ~0.5–15 s + admission | From event time |

Instrumentation, required before any of these numbers is repeated: `event_time` where available,
`recv: Instant`, and per-stage histograms (`feed_lag`, `decode_us`, `detect_us`, `enqueue_us`,
`budget_wait_ms`, `http_us`, `total_ms`) at p50/p99/p999 with the measured rate. A counting
`GlobalAlloc` test pushes 10k frames through decode + detect and asserts zero allocations after
warmup. A 2× peak saturation run uses `tokio-console`.

## 5. Architecture

Rust workspace, single binary, tokio.

```
crates/
  feed/        Feed session trait, PumpPortal adapter, per-adapter decode, supervisor,
               watchdog, generation counter, dedup, shadow probe
  gate/        Subscription gate and ledger: which mints get trades, caps, unsubscribe on prune
  registry/    mint -> {created_at, state, state_seq, ring, enrich, bundle, last_seen},
               lifecycle sweep, eviction, tombstones, regime resets
  detect/      volume anomaly, bundle status, CTO, wallet-hit
  wallet/      in-RAM watch set, wallet stats, live buy matching
  rank/        rolling 7-day per-(wallet, mint) aggregates, weekly snapshot job
  enrich/      RPC balance/funding reads, cache, TTLs, refresh task
  deliver/     Discord sender: serialized queue, token bucket, hold queue, dedup, message-id
               store, circuit breaker
  config/      thresholds, budgets, routing, timeouts, backoff, caps
  bin/         wiring, health endpoint, supervisor tree, graceful shutdown
```

Rules that apply across crates:

- **One decode + detect task per feed.** Frames are decoded from `&bytes` and detection runs in the
  same stack frame, so the borrow stays valid. There is no trade channel: a borrowed struct cannot
  cross one, and at pump.fun rates the split buys nothing. Channels exist after detection, where
  alerts are discrete.
- **Channel classes are explicit.** `snapshot` (idempotent, latest-wins fine), `delta` (must sum,
  never drop), `event` (discrete, drop with a count). Volume deltas are never latest-wins; a
  dropped trade is a wrong answer, not a late alert, and `dropped_trades` is red health.
- **One event enum between stages**, with fixed-width fields (`[u8; 32]` mint, `u64` lamports,
  `i64` time); strings never appear in the trade path and are resolved at the card.
- **Registry owns every per-mint allocation.** Eviction: idle TTL (prior 30–60 min) → dead; a
  hard live-mint cap with LRU; a tombstone set for recently dead mints. Re-entry allocates a cold
  ring, is counted, and never alerts while cold.
- **Registry states are fixed.** A mint is `created | on_curve | near_graduation (display-only) |
  migrated | dead | unknown`; `state_seq` increments on every transition, and a card or stored row
  quotes the state and sequence it was computed under.
- **Dedup** keyed on `(signature, mint, trader, txType, tokenAmount)` or signature plus
  transaction index where available — one transaction can contain several trades — sized by
  `rate × (replay + heal window) × 1.5`, not by time alone.
- **Supervision.** `panic = "unwind"`, every long-lived task's `JoinHandle` retained in a
  `JoinSet`; a panicked stage is respawned with jittered backoff under a restart budget, then the
  stage goes red and stops. Shutdown via `CancellationToken`; the sender flushes with a deadline
  and counts what it dropped.
- **Feed trait is a session runner over bytes**, not an object-safe async event stream. Adapters
  decode; the normalized event carries `Option` fields and a per-adapter capability record
  (`has_slot`, `has_block_time`, `has_curve_reserves`, `has_dex_trades`), so detectors declare
  behaviour when a field is absent.
- **Thresholds live in config** with units and a comment on where each number came from. A prior
  is labelled a prior.

## 6. Feed resilience and error handling

### 6.1 One primary feed, metered per mint

PumpPortal is the single authoritative trade feed for M1–M2. New-token and migration events are
free; **trades are metered per mint** (0.01 SOL per 10,000 messages), the API key must be linked to
a wallet holding ≥ 0.02 SOL, and **one websocket connection at a time** is permitted.

So the gate is part of the architecture, not an optimisation:

- **Eligibility.** Every launch gets a ~10 s trade subscription to capture its launch window (at
  roughly 0.23 launches/s that is ~2–3 concurrent). It keeps the subscription only if the token
  passes the trading gate: a config dollar volume in the first minutes, or a CTO listing.
- **Spend budget.** All trade subscriptions draw from one total per-UTC-day budget,
  `gate.feed_sol_per_day` (**PRIOR** 0.5 SOL/day), including every launch window; there is no
  launch-window exemption. At 0.5 SOL/day the budget is ~500,000 messages/day, ~5.8 msg/s
  sustained, so the spend cap binds long before the 20-slot concurrent-subscription cap. When it is
  hit, new subscriptions and launch windows are refused and `gate.budget_hit` is counted. Open
  question: confirm the spend budget, because it determines how many mints can be watched.
- **Caps and TTL.** A hard cap on concurrent subscriptions (prior 20 slots), a per-mint TTL,
  unsubscribe on prune, and a reconcile task. `subs_live`, `unsub_failed`, `subs_capped`,
  `events_per_sec` and `estimated_sol_per_day` are config values and `/health` counters.
- **Single socket invariant**, asserted at runtime. No parallel probe connections.

It can tell us it disconnected. It **cannot** tell us it dropped messages while staying connected:
no sequence numbers, no replay, no gap marker. Any "gap detection" built only on the feed's own
continuity is a liveness check, not an integrity check. Until a shadow exists the bot reports
itself as **single-source, unverified** and never claims that silence means a quiet market.

### 6.2 Failure taxonomy

| Failure | Detected by | Response | User-visible |
|---|---|---|---|
| Socket close, DNS, TLS | transport error | reconnect with capped backoff + jitter, after a probe (6.3) | health; one outage card per outage identity |
| Silent stall (connected, no events) | new-token liveness + adaptive threshold | mark suspected, probe, reconnect only if the probe disagrees | degraded tag |
| Malformed / unknown payload | decode error | count, sample first N, drop, never crash-loop | decode-error counter |
| Duplicate events | dedup key | drop | dup counter |
| Late / out-of-order trades | receive time vs bucket | bucket by receive time; late events counted and applied to aggregates, excluded from spike baselines | late counter |
| 429 / auth failure | HTTP status | backoff, escalate after N | health red, outage card |
| Provider partial fields | schema validation | reject the event, count | field-missing counter |
| Panic | `JoinHandle` join error | respawn under restart budget | restart counter |
| Backpressure | channel class | delta channels never drop; event channels drop with a count | drop-by-reason counter |
| Ban (excess connections) | repeated timeouts | stop restarting, health red, wait out the hour | banned state |

Event time: the trade payload carries **no timestamp and no slot** (observed, not documented). The
bot therefore buckets on a monotonic receive time, anchored to the arrival of that token's
`create` event, and says so. True block time is resolved via `getTransaction` only for specific
uses — the launch-window audit, a late-trade sample, and the ranking's final candidates with their
fee sample — labelled resolution, not routing.

### 6.3 Hardening the primary

- **Supervisor.** Capped exponential backoff with jitter; backoff reset only after sustained
  health; a restart storm stops restarting and goes red.
- **Watchdog.** Liveness comes from the free new-token stream, which has a near-constant global
  rate, with an adaptive threshold (`max(30 s, 6 × EWMA gap)` computed only from gaps under 60 s).
  Transport liveness (ping/pong) is tracked separately from application liveness. A suspected
  stall is probed against three active mints before any reconnect, so the watchdog does not
  manufacture the gap that healing cannot cover. `watchdog_false_positives` is a counter.
- **Generations.** Every (re)subscribe increments a generation; events from a superseded
  subscription are dropped, so a reconnect cannot double-count.
- **Gap heal is per subscription.** The `create` event carries the bonding-curve key, whose
  signature history is token-scoped: `getSignaturesForAddress(bondingCurveKey)` then
  `getTransaction`, bounded by watched mints. Program-wide heal cannot keep up (a 60 s gap at
  pump.fun rates is 30k–120k signatures, and one transaction read each). Block-level heal
  (`getBlocks` + `getBlock`) is the fallback for global truth only. Both are bounded by **calls**
  as well as duration; when the call budget is hit the window is declared unhealed, not guessed.
  Healed volume is marked and excluded from baseline updates until it agrees with the shadow
  within tolerance.
- **Warmup.** Any restart or heal leaves baselines cold; alerts are suppressed while a mint's ring
  warms, and the suppression is counted.
- **Restart semantics.** CTO high-water `claimDate` and the bounded posted-token set are primed on
  boot from the first poll without alerting; message ids are either restored from the bot-token
  path below or the card contract degrades honestly; the watch set is reloaded from its snapshot or
  the wallet family is suppressed and counted.

### 6.4 Shadow verification, not fake HA

One authoritative feed plus one **shadow** whose only job is to disagree. Not hot failover in v1.

| Shadow option | Cost | What it proves |
|---|---|---|
| RPC sampling of active mints (last-trade timestamp, trade count per aligned window) | low | primary liveness and coarse loss, on a sample |
| DexScreener / GeckoTerminal polls | low, slow | price sanity, nothing about individual trades |
| Full second ingest (Geyser, own decode) | high | near-complete integrity check; the only basis for failover later |

Start with RPC sampling in M2. Disagreement is measured per sampled mint per aligned 10-minute
window as `|primary − shadow| / max(primary, 1)`, over ~20 mints stratified across volume deciles
including at least five from the bottom, reported at p50/p90 per stratum. It assumes loss is
uniform; the doc says so.

Rules for any multi-source future: never sum two sources — exactly one is authoritative per
window; a disagreement blocks the *quiet market* conclusion and tags alerts but does not block
alerting; failover needs cross-source dedup, source priority and the same generation discipline,
and is its own milestone.

### 6.5 Error handling by stage

| Stage | Failure | Policy |
|---|---|---|
| decode | malformed JSON, unknown type | count, sample, drop; never retry the same bytes |
| detect | division by zero, NaN, invariant break | guard, route to `cold_baseline` or drop, count, never emit from an invalid statistic |
| enrich | timeout, 429, missing account | retry with jitter; serve cache with an age stamp; beyond max stale age mark `unknown` |
| rank store | disk full, corrupt file | quarantine, coverage partial, live alerts unaffected |
| deliver | see 8 |
| config | invalid at boot / reload | fail fast naming the field; keep last good on reload |
| supervisor | task panic | restart under budget, then red |

### 6.6 Degradation contract

Degradation is **per outage identity** — `trade_stream`, `cto_source`, `rpc`, `discord:volume`,
`discord:wallets`, `discord:cto` — not global, because CTO health is independent of trade health.
Component states map into the five subsystem states below; volume close-lag is a component
condition, not `degraded`.

| State | Meaning | Alerts |
|---|---|---|
| normal | source healthy, shadow agrees or absent within tolerance | normal |
| warming | restarted; baselines or watch set cold | volume suppressed; wallet suppressed if the watch set is cold; CTO posts |
| degraded | primary unresponsive, shadow alive | posted with a degraded footer |
| unverified | single source, no shadow evidence | posted; footer says data is unverified |
| stale | no event longer than threshold | suppressed; outage card only |

- **Outage identity** is `(identity, outage_start)`. Declared after 30 s, recovered after 60 s of
  health, so a blip posts nothing. The outage card is in the status lane and bypasses the hourly
  and daily alert budgets entirely; it states start, end, duration and reason.
- **One outage card per process lifetime**, unless the bot-token readback path is enabled, in which
  case the card is found by footer marker and edited. Webhooks cannot read history; the contract
  says which behaviour is in force.
- If the volume webhook is dead, the card falls back to another webhook marked `[fallback]`; if all
  are dead, health goes red and an external dead-man's switch covers the process being gone.
- **Boot line** in the channel: uptime 0, counters zero, watch-set generation and age, config hash,
  feed mode, subscription count. **Heartbeat** edited in place every N minutes with the funnel
  (seen → gated → detected → enriched → ranked → sent), drops by reason, edits by tier,
  warmup/suppression counts, shadow disagreement, `blind_seconds_1h`, and last successful
  delivery per webhook. Heartbeat and outage/status cards form the status lane and bypass hourly
  and daily budgets entirely: a 5-minute heartbeat is 12 posts/hour and would otherwise exceed the
  volume channel's 10/hour.
- Every card footer carries feed health whenever the state is not normal.

## 7. Data sources and provenance

| Need | Source | Truth status |
|---|---|---|
| New tokens, migrations | PumpPortal websocket | Free, direct; undocumented schema, observed only |
| Trades per mint | PumpPortal `subscribeTokenTrade` | Metered per 10k messages; API key linked to a wallet ≥ 0.02 SOL; one socket |
| Wallet activity | PumpPortal `subscribeAccountTrade` over watch-set keys | Same socket, same metering |
| Funding / launch clustering | Solana RPC: `getSignaturesForAddress` + `getTransaction` | On-chain truth; ~11 credits per wallet; CTO-only and async |
| Balances (reconciliation only) | Solana RPC `getMultipleAccounts` batched | On-chain truth; not the headline denominator |
| Gap heal | RPC per bonding-curve key; block-level fallback | Bounded by calls and duration |
| CTO events | DexScreener community-takeovers list | Curated claim, not on-chain; global latest list, `claimDate` only |
| Pool price after migration | DexScreener pair data | Third-party index; labelled on the card |
| Curve price | Payload reserves (`vSol`, `vTokens`) | Exact at the trade's moment |
| SOL/USD | Exchange ticker or Pyth | Display only; signal math is in SOL; rate age shown |
| Geyser (later) | Yellowstone gRPC provider | Raw truth, paid; carries slot and block time |

No provider joins the hot path until its failure mode is written down: what happens on reconnect,
on a gap, on a 429, and how the bot reports that it missed data. Post-migration coverage is a
standing unknown — the likely answer is PumpSwap covered and Raydium not — and is tested before
M5 depends on it.

## 8. Delivery

Three webhooks, one per family, each with its own serialized queue, token bucket and budget.

| Channel | Families | Budget |
|---|---|---|
| volume | volume fire, volume near | ~10/hr, capacity 2–3, *near* ≤ 30% |
| wallets | wallet entries | ~6/hr |
| cto | CTO listing, bundle tier edits | `deliver.cto.hourly_cap` = 20/hr (circuit breaker) |

Daily caps (prior 40 volume, 20 wallets) are the binding attention limit; hourly budgets are
burst control. A sliding window is used so two bursts cannot straddle a boundary.
`deliver.cto.hourly_cap` = 20 is a circuit breaker, not a product budget; `deliver.cto.daily` = 120
is an outer guard, not a product allocation. The status lane — heartbeat and outage/status cards —
bypasses both hourly and daily budgets entirely; a 5-minute heartbeat is 12 posts/hour and would
otherwise exceed the volume channel's 10/hour.

**Priority classes,** used only before send: status/outage > CTO listing > wallet entry > bundle
edit > volume fire > volume near. A token bucket alone is first-come-first-served. Non-urgent
cards — volume *near* and wallet entries — wait in a reordering hold of `deliver.hold.window_ms` =
45,000 ms; this is separate from `deliver.budget_wait_max_ms` = 600,000 ms, the maximum wait for
budget admission that applies to fire, CTO listings and bundle edits. The 6–10 minute admission
figure refers to `budget_wait_max_ms` only, not to the hold. The highest class is admitted when a
token is available. Two of the volume hour's ten tokens are reserved for fire. Priority decides
which alert survives a full budget, never whether the budget is exceeded.

**Message ids and edits.** One id store, owned by `deliver/`; detectors keep tiers, not ids. Every
post uses `?wait=true`; the response's message id is stored with `(mint, family, tier,
card_state)`. Edits rebuild the full embed from that state. A missing id is a delivery failure, not
a success. Posts and edits share the webhook's rate bucket, so one serialized queue covers both;
one edit per message per 30–60 s, coalesced to the latest tier. For bundle cards, only Fire and
Cleared tier transitions edit; a Near transition does not edit.

**Error classification** (status plus Discord error code), per webhook:

| Response | Class | Action |
|---|---|---|
| 429 | transient | honour `retry_after`; `X-RateLimit-Scope: global` pauses all sends; no breaker |
| 404 / 10008 Unknown Message | target gone | drop the id, stop editing, count, do not retry |
| 404 / 10015 Unknown Webhook, 401 / 50027 | webhook dead | breaker, health red, out-of-band alert, no retries |
| 403 | permissions / channel | breaker, health red, no retries |
| 400 / 50035 Invalid Form Body | payload | drop that payload, log redacted, count, no breaker, no retry |
| 5xx / timeouts | ambiguous | no automatic retry on POST (see semantics) |

**Delivery semantics.** At-most-once per process for posts: retry only on pre-send failures, 429,
and 4xx that cannot have created a message. An ambiguous POST (read timeout, reset after write,
5xx) is logged with the payload and counted `delivery_unknown`, never retried. PATCH is idempotent
and safe to retry.

**Content safety.** `allowed_mentions: {"parse": []}` on every payload. Token names and symbols are
attacker-controlled: strip markdown control characters and links, collapse whitespace, remove
zero-width and RTL overrides, cap length, and render the name as text. The mint address is the
canonical identifier.

**Card format.** Fixed template with a per-field character budget and a pre-send validator that
truncates the least important field first. Never truncate the mint, the percentage, the tier word,
price context, or the health footer. Colour is paired with a word (FIRE, NEAR, CLEARED, STALE),
never colour alone. Decision number first, one signal per line, units on every number.

**Operational.** `--dry` prints exact payloads without network. `--startup-check` proves token,
channel and permission on every webhook before the feed starts, against staging webhooks first.
Webhook URLs and tokens are redacted from logs, `/health` and panic messages. v1 targets plain text
channels; threads and forums are out.

## 9. Build order

Each milestone is shippable and verifiable alone. No milestone starts before the previous one's
failure behaviour is written down.

1. **M1 — Skeleton and spine.** Workspace, config, feed session trait, PumpPortal adapter with a
   **payload fixture and pinned schema**, subscription gate and ledger, supervisor, watchdog,
   generations, dedup, registry/lifecycle with eviction, receive-time bucketing, `/health` with
   boot line and heartbeat, minimal sender (`wait=true`, startup check, outage card), rank-store
   schema only (writer starts M2a). No signal alerts.
   *Acceptance:* observed events/min within X% of a reference count over one hour; N forced
   disconnects produce N reconnects, N outage identities and one outage card (or N edits with
   readback enabled), 0 duplicate trades after replay; measured watchdog false positives over
   24 h; no subscription leak over 24 h; zero allocations per trade after warmup; gap-heal success
   rate and declared-unhealed rate.
2. **M2a — Delivery proven.** Budgets, hold queue, priority classes, dedup/cooldown, degradation
   contract, rank-store passive writer, card template and validator, exercised with synthetic
   alerts into staging channels.
3. **M2b — Volume family.** Per-mint ring and parameters, cohort percentiles, dispersion and
   participation tests, cold-baseline path, shadow probe and disagreement metric. First real
   alerts.
4. **M3 — CTO family.** Poll, solana filter, diff with persisted high-water `claimDate` and
   posted-token set, latched per mint, wording.
5. **M4 — Bundle.** Launch-record persistence, funding stage with exclusion list and fan-out cap,
   percent-sold from trades, tier state machine on the CTO card.
6. **M5 — Wallets.** Ranking job, watch-set snapshot and boot reload, account subscription, buy
   matching and pacing. Gated on coverage proving acceptable, including the post-migration test.

## 10. Decisions

1. **Q1 — "fomo": answered 2026-10-25.** pump.fun only; the weekly ranking is reconstructed
   from pump.fun trades. FOMO is not a data source.
2. **Q2 — Top gainer definition: answered 2026-10-25, revised with the review.** Realised SOL
   profit over 7 days on execution time where resolved — block time is resolved only for final
   candidates and the fee sample, bulk rows use receive time as a labelled proxy, and the card
   says "7d, execution time where resolved" — average cost in lamports, estimated fees labelled as
   such, transfers excluded and counted, mark-to-market at close, sybil clustering, breadth ≥ 2
   with ≥ 1 SOL per mint.
3. **Q3 — Bundle rule: answered 2026-10-25, revised.** Same funder (direct or one hop), buy inside
   the ~10 s launch window, funding within 60 s before the buy. The window is a membership filter,
   not a deadline. Fan-out cap and known-account exclusions apply; two-hop is not followed.
4. **Q4 — Sold-out definition: answered 2026-10-25, revised.** SOL-weighted share of bundle bags
   sold, tiers at 60/80/95, minimums of 5 wallets, 10 SOL and 2% supply, price context mandatory,
   edit-in-place with coalescing.
5. **Q5 — CTO source: answered 2026-10-25, revised.** DexScreener community-takeover list, filtered
   to Solana, diffed, high-water `claimDate` and a bounded posted-token set persisted; non-pump.fun
   claims post with fields marked unavailable.
6. **Q6 — Channels: answered 2026-10-25, revised.** Three webhooks with capacity 2–3, budgets
   10/6/20 per hour, daily caps as the binding limit for volume and wallets, the CTO daily cap an
   outer guard, priority classes and a hold queue.
7. **R1 — Time basis.** Bucket on monotonic receive time anchored to `create`; the payload has no
   timestamp or slot. Resolve block time only for audits. *2026-10-25.*
8. **R2 — Feed cost contract.** Per-mint metered subscriptions, launch-window subscription for
   every create, retention only past the trading gate, hard caps, unsubscribe on prune, cost
   counters. *2026-10-25.*
9. **R3 — Launch records persist.** The launch-window record lives in the bounded side store so
   bundle status survives to CTO time. *2026-10-25.*
10. **R4 — Percent sold from trades.** SOL-weighted from trade events and `newTokenBalance`;
    balances reconcile only. *2026-10-25.*
11. **R5 — Delivery contract.** `wait=true`, one message-id store owned by `deliver/`,
    error-code classification, at-most-once posts, one serialized queue per webhook, one outage
    card per process lifetime unless readback is enabled. *2026-10-25.*
12. **R6 — Watch set persists.** Bounded snapshot with generation, reloaded at boot; cold means
    suppressed and counted. *2026-10-25.*
13. **R7 — Rank store starts in M2a.** Passive accumulation so the first watch set is available at
    M5 rather than M5 + 7 days. *2026-10-25.*
14. **R8 — Per-identity degradation.** Trade, CTO, RPC and the three Discord channels degrade
    independently; status cards bypass the alert budget. *2026-10-25.*
15. **R9 — Signal math in SOL.** Dollar display converts with a cached rate and shows its age; the
    signal never depends on an FX feed. *2026-10-25.*

Changes get a dated entry here rather than a silent edit.

## 11. Evidence limits

- PumpPortal publishes no trade or create schema. Field-level facts are observed from a captured
  payload and the working client. M1 pins the schema in a test before detectors depend on it.
- pump.fun trade and launch rates used in sizing are order-of-magnitude planning numbers, not
  measurements; M1 measures them.
- Discord webhook limits are community-documented, not contractual (observed burst 5 per 2 s per
  webhook). Measure against staging and record the numbers with a date.
- Post-migration coverage (PumpSwap versus Raydium) is unverified; the ranking treats Raydium as a
  gap until tested.
- DexScreener's CTO list retention window is a sample of one; a long restart may miss claims. The
  high-water `claimDate` and bounded posted-token set prevent reposts, but a claim below the
  high-water is counted `suppressed_by_high_water` and is not recovered.
