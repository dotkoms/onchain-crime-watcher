# onchain-crime-watcher

A Discord alert bot for Solana meme tokens. Rust for the data path, latency as the design axis,
built from scratch.

**Status: scope complete, no code yet. Next milestone: M1.**

The full contract lives in [SCOPE.md](SCOPE.md). This README is the plan as it stands.

## Signals

Three families, each shipped as its own milestone so nothing is half-built.

**Volume anomaly.** Per-mint volume compared to its own recent past, not a global constant.
Fixed-duration buckets in a bounded ring, trimmed-mean baseline so one earlier spike cannot hide
the next, dollar floors that scale with the interval so a ratio over a tiny denominator is not
mistaken for signal. Decisions are fire / near / reject, not a bare threshold.

**CTO, with bundle status.** DexScreener's community-takeover flag, worded as a claim on a curated
feed rather than as on-chain evidence. The card adds bundle status: wallets funded by the same
wallet (or one hop away) that bought inside ~10 seconds of launch, tracked as the share of their
bags sold — 60% near, 80% fire, 95% cleared, always shown next to price versus launch so a
sell-into-dump cannot be misread as cleared.

**Gainer wallets.** The previous week's most profitable wallets on pump.fun, reconstructed
on-chain from trade data: realised SOL profit, minimum position size, profitable on at least two
distinct mints, fees in cost. The weekly snapshot produces an in-RAM watch set; every live trade
checks its taker against it and alerts when a ranked wallet takes a new position.

## Plan

| Milestone | Ships | Status |
|---|---|---|
| **M1 — Skeleton** | Workspace, config, feed trait, PumpPortal adapter, supervisor with backoff and watchdog, subscription generations, signature dedup, `/health`, one outage message. No alerts. | Next |
| **M2 — Volume** | Per-mint ring, trimmed-mean baseline, dollar floors, fire/near/reject, RPC-sampling shadow feed, Discord sender with budgets and the degradation contract. First real alerts. | Pending |
| **M3 — CTO** | DexScreener takeover polling, social-source wording, dedup. | Pending |
| **M4 — Bundle** | Launch-window wallet clustering, balance polling with cache TTL, percent-sold tiers on the CTO card. | Pending |
| **M5 — Wallets** | Rolling 7-day per-(wallet, mint) aggregate store, weekly snapshot job, watch set, trade-match alerts. | Pending |

Each milestone is shippable and verifiable alone. No milestone starts before the previous one's
failure behaviour is written down.

## Architecture

Rust workspace, single binary, tokio.

```
crates/
  feed/        Feed trait, PumpPortal adapter now, Geyser stub behind the same trait
               supervisor, watchdog, dedup, shadow probe
  detect/      volume anomaly, bundle status, CTO, wallet-hit
  wallet/      in-RAM watch set, wallet stats, live trade matching
  rank/        rolling 7-day aggregates, weekly snapshot job
  enrich/      RPC balance/holder reads, cache, refresh task
  deliver/     Discord sender: token bucket, priority queue, dedup, circuit breaker
  config/      thresholds, budgets, routing, timeouts
  bin/         wiring, health endpoint, supervisor tree
```

The feed is swappable behind a trait, so the Geyser gRPC adapter can replace PumpPortal without
touching a detector. The hot path borrows from the decode buffer and never allocates per trade;
detection never waits on the network.

## Latency, honestly

| Stage | Realistic |
|---|---|
| Upstream feed → process | 100–500 ms |
| Decode + detect | < 2 ms p99 |
| Enrichment (balances) | 50–300 ms, async, never blocks dispatch |
| Discord queue wait | 0–5 s |

Rust buys headroom at pump.fun trade rates; it does not make the feed faster or Discord less
rate-limited. The design goal is a hot path that is never the bottleneck, and delivery that queues
under a budget instead of dropping silently.

## Resilience

A single feed can report that it disconnected. It cannot report that it lost messages while
staying connected. So the bot hardens the primary (backoff, watchdog, generation counters,
bounded signature dedup, bounded gap heal, warmup suppression), adds an advisory **shadow** source
in M2 whose only job is to disagree, and never sums two sources — volume counted twice is a spike
that never happened. Five degradation states (normal, warming, degraded, unverified, stale)
change what gets posted; one outage message per outage, edited on recovery.

## Delivery

Three Discord webhooks with separate budgets: volume ~10/hr, wallets ~6/hr, CTO unthrottled. Every
drop is counted by family in `/health`, because a channel people mute is worth zero and a budget
that eats alerts looks exactly like a quiet market.

## Decisions

| # | Decision |
|---|---|
| Q1 | pump.fun only; the weekly ranking is reconstructed on-chain. FOMO is not a data source. |
| Q2 | Realised SOL profit over 7 days, minimum position size, profitable on ≥2 mints, fees in cost. |
| Q3 | Bundle = same funder (direct or one hop) plus a buy within ~10 s of launch. |
| Q4 | Sold-out tiers at 60% / 80% / 95% of bundle bags sold, with edit-in-place escalation. |
| Q5 | CTO source is the DexScreener takeover flag, polled. |
| Q6 | Three channels with per-family budgets. |

## Not in scope

Backtesting, paper trading, portfolio tracking, order placement, dashboards, multi-chain, and
hot failover between feeds in v1.
