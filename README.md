# onchain-crime-watcher

A Discord alert bot for Solana meme tokens. Rust for the data path, latency as the design axis,
built from scratch.

**Status: scope revised after a five-lens review (`REVIEW.md`), no code yet. Next: M1.**

The contract is [SCOPE.md](SCOPE.md). The review that reshaped it is [REVIEW.md](REVIEW.md). This
README is the readable plan.

## Signals

**Volume anomaly.** Per-mint volume against its own recent past and its age band. 15-second
buckets over a 7.5-minute ring, trimmed-mean baseline, dispersion and participation tests, and an
interval-scaled floor in SOL. Fire / near / reject, with *near* capped so it never crowds out a
fire. Cold tokens route to a labelled cold-baseline path instead of dividing by nothing.

**CTO, with bundle status.** DexScreener's community-takeover list, filtered to Solana and diffed,
worded as a claim on a curated feed. The card adds bundle status: wallets funded by the same
funder (direct or one hop) that bought inside the ~10 s launch window, tracked as the SOL-weighted
share of their bags sold — 60% near, 80% fire, 95% cleared, escalating by editing one card. A
token with no launch record says so; it never shows a confident 0%.

**Gainer wallets.** The previous week's most profitable wallets on pump.fun, reconstructed
on-chain: realised SOL profit on execution time, average cost in lamports, sybil clustering, at
least two profitable mints with ≥ 1 SOL deployed each, mark-to-market at close. Top ~40 wallets
form a watch set that is persisted and reloaded at boot. Buys only fire "entered".

**Coverage is bounded and stated.** Trades are metered per mint, so the bot watches a chosen subset.
Every ranking and card says which subset it was computed over.

## Venues

| Venue | What it is | How we see it | Coverage status |
|---|---|---|---|
| pump.fun bonding curve | Launch venue; every token starts here | PumpPortal per-mint trade stream | Covered; schema observed, not documented |
| PumpSwap | pump.fun's AMM after graduation | PumpPortal trade stream | Expected covered, **unverified** |
| Raydium | Legacy/pool migration destination | None directly | **Treated as a gap** until a live test proves coverage |
| DexScreener index | Curated data over the above | Pair data and takeover list | Third-party index, not a venue |

If Raydium is not covered, tokens that migrate there silently leave the gainer ranking. That is
stated on the coverage line rather than papered over.

## Data sources and expected latency

All figures are **estimates from chain slot time (~400 ms), vendor behaviour and network distance**,
not measurements. M1 measures them and replaces this table with dated numbers and the rate they
were taken at.

| Source | What we pull | Transport | Expected latency | Cost / limit | Milestone |
|---|---|---|---|---|---|
| PumpPortal — new tokens, migrations | `create`, migration events | WebSocket | 100–500 ms | Free; one socket | M1 |
| PumpPortal — per-mint trades | Trades for watched mints | WebSocket (same socket) | 200–1,000 ms | 0.01 SOL / 10k messages; key wallet ≥ 0.02 SOL | M1 |
| PumpPortal — account trades | Trades by watch-set wallets on any mint | WebSocket (same socket) | 200–1,000 ms | Same metering, ~40 keys chunked | M5 |
| Solana RPC — funding graph | Funder of each launch-window buyer | HTTP `getSignaturesForAddress` + `getTransaction` | 200–600 ms per wallet; seconds for a bundle | ~11 credits/wallet; CTO-only, async | M4 |
| Solana RPC — balances | Opening bags and reconciliation | HTTP `getMultipleAccounts` batched | 50–250 ms | 1 credit per account; 60 s TTL | M4 |
| Solana RPC — gap heal | Missed trades per bonding-curve key | HTTP signatures + transactions | Seconds to minutes per gap, bounded by calls | Bounded budget; declare unhealed when hit | M1 |
| Solana RPC — block heal fallback | Global missed window | HTTP `getBlocks` + `getBlock` | ~150 calls per minute of gap; provider-dependent | Availability varies by provider | M1 fallback |
| Solana RPC — fees (top candidates) | Actual priority/network fees | HTTP `getTransaction` | 200–600 ms | Watch-set candidates only | M5 |
| DexScreener — community takeovers | CTO claims | HTTP poll | Curation (minutes to hours) + 0–60 s poll | 60 req/min, unauthenticated | M3 |
| DexScreener — pairs | Pool price after migration | HTTP poll | 300 ms + 5–30 s poll interval | Public, rate-limited | M4 |
| SOL/USD ticker | Display conversion only | WS or poll | 50–200 ms, cached 30–60 s | Free public feed | M2 |
| Shadow probe | Per-mint trade counts on aligned windows | HTTP RPC sampling | 10-minute windows, minutes behind | ~20 mints per window | M2 |
| Discord webhook | Alerts and edits | HTTPS POST/PATCH | 100–300 ms per call; **admission wait dominates** (up to 6–10 min at budget) | 5 per 2 s observed, not contractual | M1 |
| Geyser (Yellowstone) | Raw transactions, slot and block time | gRPC | 10–100 ms | Paid; own decode | Later, not built |

The chain sets the floor: a ~400 ms slot means no source can beat roughly a quarter second from
event to process. Everything we add is decode, detection and queueing on top of that.

## Plan

| Milestone | Ships | Status |
|---|---|---|
| **M1 — Skeleton and spine** | Workspace, config, feed session trait, PumpPortal adapter with a pinned payload fixture, subscription gate and ledger, supervisor, watchdog, generations, dedup, registry/lifecycle with eviction, receive-time bucketing, `/health` with boot line and heartbeat, minimal sender (`wait=true`, startup check, outage card), rank-store schema and passive writer. No signal alerts. | Next |
| **M2a — Delivery proven** | Budgets, hold queue, priority classes, dedup/cooldown, degradation contract, card validator — with synthetic alerts into staging. | Pending |
| **M2b — Volume** | Ring and parameters, cohort percentiles, dispersion and participation, cold-baseline path, shadow probe. First real alerts. | Pending |
| **M3 — CTO** | Poll, Solana filter, diff with persisted `claimDate`, per-mint latch, wording. | Pending |
| **M4 — Bundle** | Launch records, funding stage with exclusions and fan-out cap, percent-sold from trades, tier state machine. | Pending |
| **M5 — Wallets** | Ranking job, watch-set snapshot and boot reload, account subscription, buy matching and pacing. Gated on the post-migration coverage test. | Pending |

M1 acceptance is numeric: events/min against a reference count, N forced disconnects producing N
reconnects and zero duplicates after replay, measured watchdog false positives over 24 h, no
subscription leak, zero allocations per trade after warmup, gap-heal success and
declared-unhealed rates.

## Architecture

Rust workspace, single binary, tokio.

```
crates/
  feed/        Feed session trait, PumpPortal adapter, per-adapter decode, supervisor,
               watchdog, generations, dedup, shadow probe
  gate/        Which mints get trades: launch-window subscription, caps, unsubscribe on prune
  registry/    mint state and lifecycle, ring ownership, eviction, tombstones, regime resets
  detect/      volume anomaly, bundle status, CTO, wallet-hit
  wallet/      watch set, wallet stats, live buy matching
  rank/        rolling 7-day aggregates, weekly snapshot job
  enrich/      funding and balance reads, cache, TTLs
  deliver/     serialized queue per webhook, token bucket, hold queue, circuit breaker
  config/      thresholds, budgets, caps, timeouts
  bin/         wiring, health, supervisor tree, shutdown
```

Decode and detection share one task so the borrow from the frame stays valid; there is no trade
channel. Channels after detection are classed snapshot, delta or event, because a dropped volume
delta is a wrong answer rather than a late alert. The registry owns every per-mint allocation and
every eviction. The feed trait is a session runner over bytes, with a per-adapter capability
record, so a raw Geyser feed can be added without touching detectors.

## Latency, honestly

| Stage | Realistic |
|---|---|
| Upstream feed → process | 100–500 ms |
| Decode + detect | 5–15 µs typical |
| Bucket close | 0–15 s |
| Budget admission | up to 6 min at 10/hr, up to 10 min at 6/hr; first 2–3 of a cluster immediate |
| Discord POST | 100–300 ms |

Rust buys headroom at pump.fun rates; it does not make the feed faster or Discord less
rate-limited. The dominant waits are upstream, the bucket window, and our own attention budget —
and the budget is the largest of the three, which is a product decision, not a technical one.

## Resilience

The trade payload carries no timestamp and no slot, so the bot buckets on receive time anchored to
each token's create event, and says so. Gap healing is per bonding-curve key — token-scoped and
bounded — because program-wide healing cannot keep up with pump.fun's rate. The watchdog reads the
free new-token stream for liveness and probes before reconnecting, so it cannot manufacture the gap
it would then fail to heal. One socket, asserted. Dedup is keyed per trade because one transaction
can hold several. Degradation is per subsystem, and an outage card bypasses the budget it reports
on.

## Delivery

Three webhooks, separate queues and budgets (10/hr volume, 6/hr wallets, 20/hr CTO safety cap),
daily caps as the real attention limit. Every post uses `wait=true` so the id exists for edits;
errors are classified by status plus Discord code, because a breaker on all 4xx mutes a channel
after one deleted card. Posts are at-most-once per process; ambiguous POSTs are logged and counted,
never retried. Token names are attacker-controlled, so `allowed_mentions` is empty and markdown is
stripped. `--dry` and `--startup-check` exist, and staging webhooks are exercised before production.

## Decisions

| # | Decision |
|---|---|
| Q1 | pump.fun only; the ranking is reconstructed on-chain. FOMO is not a data source. |
| Q2 | Realised SOL profit over 7 days on execution time, average cost, sybil clustering, ≥ 2 mints, mark-to-market. |
| Q3 | Bundle = same funder (direct or one hop) plus a buy inside the ~10 s launch window; window is a filter, not a deadline. |
| Q4 | SOL-weighted sold share, tiers 60/80/95, minimums of 5 wallets / 10 SOL / 2% supply, price context mandatory. |
| Q5 | DexScreener takeover list, Solana-only, diffed, `claimDate` persisted. |
| Q6 | Three channels, capacity 2–3, budgets 10/6/20 per hour, daily caps binding. |
| R1 | Bucket on receive time; payload has no timestamp or slot. |
| R2 | Metered per-mint subscriptions behind a gate with caps and cost counters. |
| R3 | Launch records persist so bundle status survives to CTO time. |
| R4 | Percent sold from trade events, balances only reconcile. |
| R5 | `wait=true`, error-code classes, at-most-once posts, one serialized queue per webhook. |
| R6 | Watch set persists and reloads at boot; cold means suppressed and counted. |
| R7 | Rank store writes from M2 so the first watch set exists at M5. |
| R8 | Degradation per subsystem; status cards bypass budgets. |
| R9 | Signal math in SOL; USD is display only. |

## Not in scope

Backtesting, paper trading, portfolio tracking, order placement, dashboards, multi-chain, hot
failover between feeds, and exit alerts for watched wallets in v1.
