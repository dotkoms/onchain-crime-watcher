# onchain-crime-watcher — scope

A Discord alert bot for Solana meme tokens. Rust for the data path, latency as the design axis.
Built from scratch; `~/Projects/volume-scanner` is a reference for definitions and hard-won
lessons only — no code is carried over.

Status: scope complete, 2026-10-25. All six open questions answered. No code until explicit go
on M1.

## 1. What it does

Watches the pump.fun / fomo meme-token stream and posts ranked alerts to Discord across three
signal families. Every alert carries the same shape: what fired, on which token, why it scored
where it did, and what the bot knows it cannot see.

The bot is a live feed, not an archive. It keeps enough in RAM to compare a token against its own
recent past and its cohort, and writes nothing it needs to answer the next alert.

## 2. Signals

### A. Unusual volume on a time series scale

Per-token volume compared against an interval-aware baseline, not a global constant.

- Fixed-duration buckets per mint, held in a bounded ring (no unbounded per-token history).
- Baseline is a trimmed mean of the ring, so one earlier spike cannot raise the bar that would
  hide the next one.
- A dollar floor scales with the interval: a ratio over a tiny denominator is not signal, and a
  ratio over a large one is often just a big token breathing.
- Decision returns fire / near / reject rather than a bare threshold, so "near" alerts can be used
  for budget pressure without spamming the channel.
- A ratio with no denominator floor produces values that look exactly like signal. The floor is
  part of the signal definition, not a safety net bolted on afterwards.

### B. CTO alerts, with bundle status

A community takeover is a curated social event, not an on-chain fact. Source (Q5, answered):
the **DexScreener takeover flag**, polled. Minutes-level latency is acceptable for a social
signal; the wording must carry that a listing certifies someone claimed the token and nothing
about who is holding it. A listing is easy to read as validation; it isn't.

The new element is **bundle status**: how much of the launch bundle has been sold.

- A bundle is a set of wallets **funded by the same wallet (or by a wallet one hop away) that
  bought within ~10 seconds of launch** (Q3, answered). One identifiable actor, robust to bundlers
  that split buys across slots. Same-slot snipers with unrelated funders are a different actor
  class and are not counted, so the percentage means one thing.
- "Mostly sold out" is **tiered on the share of bundle bags sold** (Q4, answered): ~60% is
  *near*, ~80% fires, ~95% is *cleared* and edits the message already in the channel rather than
  posting again. Bag share is measured by wallet position, and the thresholds are config values.
- The card always shows the percentage, the tier, and **price versus launch**, so a bundle that
  sold into a dump cannot be misread as cleared. Selling into strength and dumping on exit are
  opposite signals and the card has to let the reader tell them apart.
- Requires balance reads per bundle wallet (RPC), which is off the hot path and cached.

### C. Gainer-wallet tracker (fomo / pump.fun)

Tracks the wallets that were the previous week's biggest gainers on fomo / pump.fun, and alerts
when those wallets take new positions.

- Weekly snapshot job ranks wallets over the prior week's tokens and produces a watch set.
- The live path is cheap: the taker wallet of every trade is checked against an in-RAM set.
  A hit emits an alert carrying what that wallet did last week that earned it a place.
- The watch set is refreshed weekly. A wallet that stops performing ages out; nothing watches
  forever.

Ranking is reconstructed **on-chain from pump.fun trade data only** (Q1, answered). No vendor
leaderboard and no scraping, so the number is ours and we can state exactly how it was computed.

Ranking rule (Q2, answered): **realised SOL profit over the 7-day window**, with

- a minimum position size, so a 0.1 SOL trade cannot define the week;
- a breadth requirement — profitable on at least 2 distinct mints — so one lottery ticket does
  not buy a wallet a week in the watch set;
- fees included in cost, and pre-graduation bonding-curve fills and post-migration pool fills
  costed the same way, since a wallet that only trades post-migration is not the same signal;
- watch-set size a config value, default ~40. That default is a starting point, not a validated
  number; it will move once we can see actual alert volume (Q6).

The card for a wallet hit quotes the rule: realised profit, how many mints, how many trades, and
the window. "Biggest gainer" is a claim with a definition and the definition travels with it.

That has one structural consequence. A weekly PnL ranking needs cost basis per wallet per mint
over seven days, which cannot live in RAM at pump.fun token counts. So the bot keeps a bounded
**rolling 7-day aggregate store on disk** — per (wallet, mint): buys, sells, weighted cost, realised
PnL, first/last trade — pruned by age. This store:

- is written by a single background task, never on the alert hot path;
- is never read to produce a live alert (live hits read the in-RAM watch set only);
- is allowed to lag, drop, or rebuild without affecting alerts;
- carries its own coverage counters, because a ranking computed over a gappy week must say so.

If the store cannot be rebuilt cheaply after a restart, the weekly job says "coverage: partial"
rather than presenting a thinner ranking as the whole week.

## 3. Not in scope

- Storing a full trade history, or any database that the alert path depends on. The weekly
  ranking aggregate (section 2C) is a bounded, prunable side store: off the hot path, never read
  to produce a live alert, and rebuildable or reportable-as-partial.
- Paper trading, portfolio tracking, copy-trading, or order placement.
- Replying to users, dashboards, or a web UI.
- Multi-chain. Solana meme tokens only.
- Hot failover between feeds in v1. A shadow verifier is in scope (section 6.4); automatically
  switching the authoritative source is not, and will not be added without measured disagreement
  data to justify it.
- A backtest harness. With no recorded stream, a swept threshold is a preference, not
  validation, and the bot will say so in its own docs rather than pretend otherwise.

## 4. Latency model

Rust does not make the feed faster or Discord less rate-limited. The honest budget:

| Stage | Realistic | Notes |
|---|---|---|
| Upstream feed → process | 100–500 ms | Data-source dependent; the dominant term |
| Decode + detect | < 2 ms p99 | Where Rust pays for itself, and it is the small term |
| Enrichment (balances, holders) | 50–300 ms async | Never blocks dispatch; alert may go with cached values |
| Delivery queue wait | 0–5 s | Discord webhook budget, not our code |
| End to end, best case | ~200 ms + queue | |

So the design goals are:

1. **Feed adapter is swappable** behind a trait (`PumpPortal` first, Geyser gRPC specified and
   built later). Swapping feeds must not touch detectors.
2. **Hot path is allocation-free where it can be.** No JSON re-serialisation, no `serde_json` in
   the trade loop, no SipHash maps, no per-trade `String`. Timestamps come from the payload.
3. **Detection never waits on network.** Balance/holder reads are a separate task with a cache
   and a result channel.
4. **Delivery queues under a budget, never silently drops.** A bounded queue with explicit
   priority and a drop counter surfaced in `/health`. Silence is indistinguishable from a quiet
   market, so every drop is counted and reported.

## 5. Architecture

Rust workspace, single binary. Async runtime: tokio.

```
crates/
  feed/        Feed trait, adapter, supervisor, watchdog, dedup, shadow probe
               pumpfun adapter now; geyser adapter stub behind the same trait
  detect/      volume anomaly, bundle status, CTO, wallet-hit
  wallet/      in-RAM watch set, wallet stats, live trade matching
  rank/        rolling 7-day per-(wallet, mint) aggregates, weekly snapshot job
  enrich/      RPC balance/holder reads, cache, refresh task
  deliver/     Discord sender: token bucket, priority queue, dedup, cooldown, circuit breaker
  config/      thresholds, budgets, channel routing, timeouts/backoff
  bin/         wiring, health endpoint, supervisor tree, graceful shutdown
```

Rules that apply across crates:

- One event enum between stages; the trade struct is `Copy`-where-possible and borrowed from the
  decode buffer.
- Per-mint state in a slab or `ahash::AHashMap`, keyed by mint bytes, not strings.
- Bounded channels between stages. If a stage falls behind, the policy is stated per channel:
  latest-wins for state updates, counted drop for alerts, never a blocking send on the hot path.
- Thresholds live in config with units and a comment on where each number came from. An invented
  constant is a bug with a comment (see Q3–Q5).

## 6. Feed resilience and error handling

### 6.1 What one source can and cannot tell us

PumpPortal is the single authoritative feed for M1–M2. It can tell us it disconnected. It **cannot**
tell us it dropped messages while staying connected. Any "gap detection" built only on the feed's
own continuity is therefore a liveness check, not an integrity check. Two consequences run through
this whole section:

- The primary gets hardened properly (6.3), because a silent drop is the failure we cannot see.
- Until a shadow source exists (6.4), the bot reports itself as **single-source, unverified** and
  never claims that silence means a quiet market.

### 6.2 Failure taxonomy

| Failure | Detected by | Response | User-visible |
|---|---|---|---|
| Socket close, DNS, TLS | transport error | reconnect, capped exponential backoff + jitter | status line in health; one outage message in channel |
| Silent stall (connected, no events) | per-subscription watchdog on inter-arrival time | mark degraded, probe RPC, force reconnect | degraded tag on any alert posted |
| Malformed / unknown payload | decode error | count + sample first N, drop the event, never crash-loop | decode-error counter |
| Duplicate events (reconnect overlap, replay) | signature dedup LRU | drop | dup counter |
| Out-of-order / late trades | payload slot + timestamp | bucket by **event time**, not arrival; count late beyond window | late-event counter |
| 429 / auth failure | HTTP status | backoff; refresh credential path; escalate after N | health red, outage message |
| Provider partial fields | schema validation | reject event, count | field-missing counter |
| Process panic | panic hook + supervisor | restart with backoff; cold per-mint state | restart counter, warmup state |
| Backpressure / queue full | channel depth | policy per channel; counted drop | drop-by-reason counter |

Clock rule: bucket assignment uses the payload timestamp, never local wall clock, so a clock skew
cannot move volume into the wrong window.

### 6.3 Hardening the primary

- **Supervisor per source.** Restart with capped exponential backoff and jitter; reset the backoff
  only after sustained healthy operation. A restart storm escalates to "stop restarting, health
  red" instead of looping quietly.
- **Watchdog.** Trades have an expected arrival rate during active hours; pump.fun is never silent
  for long. Per-subscription stale thresholds, different for trades and new tokens.
- **Subscription generations.** Every (re)subscribe increments a generation counter. Events from a
  superseded subscription are dropped, so a reconnect cannot double-count.
- **Dedup.** Bounded signature LRU, large enough to cover the longest plausible replay window and
  hard-capped in memory. One duplicate trade is a phantom volume spike, which is the most
  expensive false positive this bot can produce.
- **Gap heal.** On reconnect, attempt to fetch the missed window (RPC `getSignaturesForAddress` /
  `getTransaction` over the pump.fun program, or a provider HTTP endpoint if it exposes one).
  Bounded: heal only gaps under a configured duration. A longer gap is declared, not guessed.
- **Warmup.** Any restart or heal leaves per-mint baselines cold. Alerts are suppressed while a
  mint's ring is warming; the suppression is counted, not hidden.
- **All timings in config** with units: backoff base, cap, jitter, stale thresholds, heal window,
  warmup buckets.

### 6.4 Multiple sources: shadow verification, not fake HA

**Decision:** one authoritative feed plus one **shadow** feed whose only job is to disagree.

A shadow is cheap to add and turns the unverifiable question ("did we miss anything?") into a
measurable disagreement rate. It is **not** hot failover in v1.

| Shadow option | Cost | What it proves |
|---|---|---|
| RPC sampling of a few active mints (last-trade timestamp, trade count per window) | low | primary liveness and coarse loss, on a sample |
| DexScreener / GeckoTerminal polling | low, slow | price/volume sanity, nothing about individual trades |
| Full second ingest (Helius/Yellowstone logsSubscribe, own decode) | high | near-complete integrity check; the only basis for hot failover later |

Start with RPC sampling in M2. Upgrade to full second ingest only if the measured disagreement
justifies it.

Rules for any multi-source future:

- **Never sum two sources.** Volume counted twice becomes a spike that never happened. Exactly one
  source is authoritative for any window; the shadow only votes.
- A shadow disagreement blocks the **"quiet market"** conclusion and tags alerts, it does not
  block alerts themselves.
- Hot failover, if ever built, needs cross-source dedup by signature, source priority, and the same
  generation discipline as a reconnect. It is a milestone of its own, not a flag.

### 6.5 Error handling by stage

| Stage | Failure | Policy |
|---|---|---|
| decode | malformed JSON, unknown type | count, sample, drop; never retry the same bytes |
| detect | division by zero, NaN, invariant break | guard, count, never emit an alert from an invalid statistic |
| enrich (RPC) | timeout, 429, missing account | retry with jitter; serve cache with an age stamp; beyond TTL mark the field stale |
| rank store | disk full, corrupt file | quarantine, report coverage partial, live alerts unaffected |
| deliver | 429, 5xx, 4xx, oversized message | respect `retry_after`; retry bounded; 4xx opens a circuit breaker; truncate fields, never split |
| config | invalid at boot / at reload | fail fast naming the field at boot; keep last good config on reload |
| supervisor | task panic | restart with backoff; repeated panic stops and goes red |

### 6.6 Degradation contract

Five states, and they change what gets posted:

| State | Meaning | Alerts |
|---|---|---|
| normal | primary healthy, shadow agrees or absent within tolerance | normal |
| warming | restarted; baselines cold | volume suppressed; CTO and wallets still post |
| degraded | primary unresponsive, shadow alive | posted with a degraded footer |
| unverified | single source, no shadow evidence | posted; footer says data is unverified |
| stale | no event for longer than threshold | suppressed; outage message only |

- **One outage message per outage**, posted to the volume channel and edited on recovery. Not a
  stream of status pings, and not silence.
- Every card footer carries feed health whenever the state is not normal.
- CTO is independent of trade-stream health: the social feed can be fine while trades are down,
  and the card must not imply otherwise. Bundle percentages, however, depend on RPC reads and are
  marked stale or withheld when those fail.

### 6.7 Placement in the milestones

- **M1:** supervisor, backoff, watchdog, generations, dedup, health counters, outage message.
- **M2:** RPC-sampling shadow, disagreement counter, degradation contract.
- **M3/M4:** enrich cache TTL and stale-field rules for bundle status.
- **M5:** rank-store fault handling; revisit full second ingest only with disagreement data in hand.

## 7. Data sources and provenance

| Need | Source | Truth status |
|---|---|---|
| pump.fun trades, new tokens | PumpPortal websocket | Direct feed, but a third party over their API; treat as authoritative for trades only |
| Live balances, holders | Solana RPC (Helius or equivalent) | On-chain truth, read cost per call |
| CTO events | DexScreener token feed | Curated social event, not on-chain; alert wording must carry that |
| Weekly top gainers | PumpPortal trades, aggregated in `rank/` | Reconstructed by us from pump.fun trades (Q1); definition in section 2C |
| Geyser (later) | Yellowstone gRPC provider | Raw truth, paid, heavier parse |

No provider is added to the hot path until its failure mode is written down: what happens on
reconnect, on a gap, on a 429, and how the bot reports that it missed data.

## 8. Delivery

Three webhooks, one per family, each with its own token bucket and budget (Q6, answered):

| Channel | Families | Budget |
|---|---|---|
| volume | volume fire, volume near | ~10/hr |
| wallets | gainer-wallet hits | ~6/hr |
| cto | CTO + bundle status | unthrottled (rare, discrete) |

Each channel can be muted on its own without losing the others. Rules:

- Assume ~5 messages per 5 seconds per webhook, sustained ~1/s; verify at build time.
- Dedup key + cooldown per (mint, family). A CTO is one event, not a stream. A bundle tier change
  edits the existing message instead of posting a second one.
- Overflow is visible: when a budget is hit, the dropped alert is counted by family in `/health`
  and the reason recorded. A channel people mute is worth zero, and a budget that silently eats
  alerts looks identical to a quiet market.
- Priority within a channel decides which alert survives a full budget, never whether the budget
  is exceeded.

## 9. Build order

1. **M1 — Skeleton:** workspace, config, feed trait, PumpPortal adapter with supervisor, backoff,
   watchdog, subscription generations and signature dedup, decode to trade events, `/health`
   counters, one outage message. No alerts. Prove feed volume and reconnect behaviour.
2. **M2 — Volume family:** per-mint ring, trimmed-mean baseline, dollar floors, fire/near/reject,
   RPC-sampling shadow probe with disagreement counter, Discord sender with budget, dedup and
   degradation contract. First real alerts.
3. **M3 — CTO family:** DexScreener CTO polling, wording that marks it social, dedup.
4. **M4 — Bundle status:** launch-window wallet clustering, balance polling, percent-sold in the
   CTO card.
5. **M5 — Gainer wallets:** rolling aggregate store, weekly snapshot job, watch set, trade-match
   alerts. Depends on the rank store being correct for a full week; it is the last milestone on
   purpose.

Each milestone is shippable and verifiable on its own. No milestone starts before the previous
one's failure behaviour is written down.

## 10. Decisions

Answers are dated. Each one was a blocker; none of them is silent.

1. **Q1 — "fomo": answered 2026-10-25.** pump.fun only; the weekly ranking is reconstructed
   on-chain from pump.fun trades. See section 2C for the aggregate-store consequence. FOMO is not
   a data source.
2. **Q2 — Top gainer definition: answered 2026-10-25.** Realised SOL profit over 7 days, minimum
   position size, profitable on at least 2 distinct mints, fees in cost. See section 2C.
3. **Q3 — Bundle rule: answered 2026-10-25.** Same funder (direct or one hop) plus a buy within
   ~10 s of launch. Same-slot snipers from unrelated funders are excluded.
4. **Q4 — "Mostly sold out": answered 2026-10-25.** Tiered on share of bundle bags sold —
   60% near, 80% fire, 95% cleared with edit-in-place escalation — always shown alongside price
   versus launch.
5. **Q5 — CTO source: answered 2026-10-25.** DexScreener takeover flag, polled; worded as a
   claim on a curated feed, not as on-chain evidence.
6. **Q6 — Channel budget: answered 2026-10-25.** Three webhooks — volume (~10/hr), wallets
   (~6/hr), CTO (unthrottled). See section 8.

Resilience (section 6) is a design decision, not an open question: harden the single primary now,
add an advisory RPC-sampling shadow in M2, and build hot failover only if the shadow's measured
disagreement justifies it.

All six open questions are answered. This document is the contract for M1; changes get a dated
entry here rather than a silent edit.
