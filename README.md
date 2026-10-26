# onchain-crime-watcher

A Discord alert bot for Solana meme tokens. Rust for the data path, latency as the design axis,
built from scratch.

**Status: scope revision 5 (capture and replay, hot failover, wallet exits) after the scope
additions of 2026-10-25, no code yet. Next: M1.**

The contract is [SCOPE.md](SCOPE.md). The review that reshaped it is [REVIEW.md](REVIEW.md), the
public projects it was checked against are catalogued in [docs/references.md](docs/references.md),
and the two new subsystems have their own specs: [docs/backtest.md](docs/backtest.md) and
[docs/failover.md](docs/failover.md). This README is the readable plan.

## Signals

**Volume anomaly.** Per-mint volume against its own recent past and its age band. 15-second
buckets over a 7.5-minute ring, trimmed-mean baseline, dispersion and participation tests, and an
interval-scaled floor in SOL. Fire / near / reject, with *near* capped so it never crowds out a
fire. Cold tokens route to a labelled cold-baseline path instead of dividing by nothing. If the
participation gate proves noisy on live data, buyer breadth is the next feature — not a lower
threshold.

**CTO, with bundle status.** DexScreener's community-takeover list, filtered to Solana and diffed,
worded as a claim on a curated feed. The card adds bundle status: wallets funded by the same
funder (direct or one hop) that bought inside the ~10 s launch window, tracked as the SOL-weighted
share of their bags sold — 60% near, 80% fire, 95% cleared, escalating by editing one card. A
token with no launch record says so; it never shows a confident 0%. Repeated Jito tip payers are
recorded as corroborating evidence for a bundle, never as a membership rule on their own.

**Gainer wallets.** The previous week's most profitable wallets on pump.fun, reconstructed
on-chain: realised SOL profit on execution time where resolved (receive time otherwise, labelled),
average cost in lamports, sybil clustering, at least two profitable mints with ≥ 1 SOL deployed
each, mark-to-market at close. Top ~40 wallets form a watch set that is persisted and reloaded at
boot; it is a discovery set, not a delivery promise — at ~3 entries per wallet per day it yields
~120 entry candidates/day, and exits on observed positions can add a similar number, so ~240
against the 20/day wallet cap suppresses roughly 92% and counts `wallet_budget_suppressed`. Buys
only fire "entered"; exits fire `EXITED` on their own rule. Funder clustering follows the launch-window
rule; a public detector's 5-children-in-5-minutes default is recorded as a second opinion, not
adopted.

**Wallet exits.** A watched wallet's position fires one `EXITED` card when cumulative sells reach
90% of the position's peak (PRIOR) and proceeds reach 1.0 SOL. The card shows the sold share and
whether the position is closed, realised SOL against the observed average cost with estimated
fees, hold time, and entry versus exit price. Only positions live matching saw open are eligible
while pairing is on, because the bot cannot show a cost basis it never observed; one episode fires
once, transfers out are not exits, and exits share the wallet budget (sub-capped at 3/h) and rank
above entries. *Revision 5.*

**Coverage is bounded and stated.** Trades are metered per mint, so the bot watches a chosen subset.
Every ranking and card says which subset it was computed over. Trade spend is one total
per-UTC-day budget, `gate.feed_sol_per_day` (prior 0.5 SOL/day), that includes launch windows —
there is no launch-window exemption. At 0.5 SOL/day the budget is ~500,000 messages/day, ~5.8 msg/s
sustained, so it binds long before the subscription cap; when it is hit, new subscriptions and
launch windows are refused and `gate.budget_hit` is counted.

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
| PumpPortal — per-mint trades | Trades for watched mints | WebSocket (same socket) | 200–1,000 ms | 0.01 SOL / 10k messages; key wallet ≥ 0.02 SOL; total `gate.feed_sol_per_day` 0.5 SOL/day | M1 |
| PumpPortal — account trades | Trades by watch-set wallets on any mint | WebSocket (same socket) | 200–1,000 ms | Same metering, ~40 keys chunked, inside the same daily budget | M5 |
| Solana RPC — funding graph | Funder of each launch-window buyer | HTTP `getSignaturesForAddress` + `getTransaction` | 200–600 ms per wallet; seconds for a bundle | ~11 credits/wallet; CTO-only, async | M4 |
| Solana RPC — balances | Opening bags and reconciliation | HTTP `getMultipleAccounts` batched | 50–250 ms | 1 credit per account; 60 s TTL | M4 |
| Solana RPC — gap heal | Missed trades per bonding-curve key | HTTP signatures + transactions | Seconds to minutes per gap, bounded by calls | Bounded budget; declare unhealed when hit | M1 |
| Solana RPC — block heal fallback | Global missed window | HTTP `getBlocks` + `getBlock` | ~150 calls per minute of gap; provider-dependent | Availability varies by provider | M1 fallback |
| Solana RPC — fees (final candidates) | Actual priority/network fees | HTTP `getTransaction` | 200–600 ms | Ranking final candidates only | M5 |
| DexScreener — community takeovers | CTO claims | HTTP poll | Curation (minutes to hours) + 0–60 s poll | 60 req/min, unauthenticated | M3 |
| DexScreener — pairs | Pool price after migration | HTTP poll | 300 ms + 5–30 s poll interval | Public, rate-limited | M4 |
| SOL/USD ticker | Display conversion only | WS or poll | 50–200 ms, cached 30–60 s | Free public feed | M2 |
| Shadow probe | Per-mint trade counts on aligned windows | HTTP RPC sampling | 10-minute windows, minutes behind | ~20 mints per window | M2b |
| Raw frame capture | Arrival history for replay | Local disk, pre-decode, pre-dedup | Off the hot path; bounded and loss-counted | 2 GiB cap, 14 days, ~60–90 MB/day | M1 |
| Second trade feed | Warm standby and failover | Geyser-protocol provider, own decode | 10–100 ms (**U**) | Paid (**U**); client license is a build gate | M6 |
| Discord webhook | Alerts and edits | HTTPS POST/PATCH | 100–300 ms per call; **admission wait dominates** (up to 6–10 min at budget) | 5 per 2 s observed, not contractual | M1 |

The chain sets the floor: a ~400 ms slot means no source can beat roughly a quarter second from
event to process. Everything we add is decode, detection and queueing on top of that.

## Backtesting

A threshold swept over data the bot never recorded is a preference, not validation. So v1 records
before it replays: from M1 the bot taps raw frames — pre-decode, pre-dedup, with receive time,
generations and session lifecycle — into bounded, checksummed, compressed day segments. The tap
can drop and counts it; it never backpressures decode. A missing window is `unrecorded`, never
zero.

The replay harness runs the **production** decode, dedup, gate, detectors and delivery admission
under a virtual clock, and writes a decision journal plus a report. It is deterministic: the same
input produces the same report hash ten times. It cannot see what the feed dropped while
connected, cannot supply outcome labels, and cannot measure delivery; a forward mark over later
trades is a censored proxy, not precision. Any threshold fitted on a replay carries the train
window, the held-out window, the date and the report hash in config, or boot fails. The first
production volume alerts are gated on a report over at least seven recorded days. Spec:
[docs/backtest.md](docs/backtest.md).

## Failover

PumpPortal permits one socket, so a second connection to it is a ban risk, not a failover path.
Hot failover therefore means a **second provider** — recommended: a Geyser-protocol provider with
own decode and slot/block time — running a full parallel pipeline as a **warm standby**. Exactly
one source is authoritative at any instant. On failure of the primary (ban, transport death,
confirmed stall) the standby is promoted in seconds; its rings are already warm, so the volume
family does not go blind for four minutes. Roles swap wholesale: no source is summed, no ring is
merged, each source keeps its own dedup and generation stream, and the outgoing partial bucket is
dropped with its boundary frames counted. Switch-back needs five minutes of primary health and
aligned agreement; disagreement tags alerts but never forces a switch. A flap guard holds the
authority after three switches in an hour. The RPC shadow stays, because two feeds can share
upstream infrastructure. The secondary's client must be permissively licensed or generated from
permissive proto definitions — `yellowstone-grpc` is AGPL-3.0 and must not enter an MIT binary.
Spec: [docs/failover.md](docs/failover.md).

## Plan

| Milestone | Ships | Status |
|---|---|---|
| **M1 — Skeleton and spine** | Workspace, config, feed session trait, PumpPortal adapter with a pinned payload fixture, subscription gate and ledger, supervisor, watchdog, generations, dedup, registry/lifecycle with eviction, receive-time bucketing, `/health` with boot line and heartbeat, minimal sender (`wait=true`, startup check, outage card), rank-store schema only (writer starts M2a), raw-frame capture recorder. No signal alerts. | Next |
| **M2a — Delivery proven** | Budgets, hold queue, priority classes, dedup/cooldown, degradation contract, rank-store passive writer, card validator — with synthetic alerts into staging — plus the replay harness core and delivery-admission simulation. | Pending |
| **M2b — Volume** | Ring and parameters, cohort percentiles, dispersion and participation, cold-baseline path, shadow probe. First real alerts, gated on a backtest report over ≥ 7 recorded days with a train/test split. | Pending |
| **M3 — CTO** | Poll, Solana filter, diff with persisted high-water `claimDate` and posted-token set, per-mint latch, wording. | Pending |
| **M4 — Bundle** | Launch records, funding stage with exclusions and fan-out cap, percent-sold from trades, tier state machine. | Pending |
| **M5 — Wallets, entries and exits** | Ranking job, watch-set snapshot and boot reload, account subscription, buy matching, pacing, exit matching and the `EXITED` card with pairing and cost basis. Gated on the post-migration coverage test and a wallet/exit replay report. | Pending |
| **M6 — Hot failover** | Secondary provider adapter with own decode, warm-standby pipeline, arbitration, switchover records, per-source dedup and ledgers, two-source capture and replay, copyleft dependency gate. | Pending |

M1 acceptance is numeric: events/min against a reference count, N forced disconnects producing N
reconnects, N outage identities and one outage card (or N edits with readback enabled), and zero
duplicates after replay, measured watchdog false positives over 24 h, no subscription leak, zero
allocations per trade after warmup, gap-heal success and declared-unhealed rates.

## Agents and skills by section

Each subsystem has a lead specialist for its lens and a supporting one for the boundary it touches.
Agents are advisory roles: they write and review their section's code and tests in-session; nothing
ships autonomously. Skills are named by the phase they gate.

| Section | Spec | Lead agent | Supporting | Skills |
|---|---|---|---|---|
| Feed session, gate, dedup, heal | `docs/feed.md` | general-purpose (Rust systems) | memecoin-venues (payload truth), memecoin-alert-architect (stage boundaries) | `agent-skills:build`, `agent-skills:test` |
| Registry, lifecycle, eviction | `SCOPE.md` §5 | backend-architect | memecoin-alert-architect | `agent-skills:build`, `agent-skills:test` |
| Volume anomaly | `docs/volume.md` | alert-tuner | market-economist | `agent-skills:test`, `agent-skills:review` |
| Discord delivery and cards | `docs/delivery.md` | discord-alert-engineer | security-auditor (name injection, secret redaction) | `agent-skills:build`, `/security-review` |
| CTO ingestion and wording | `docs/bundle.md` §6 | memecoin-venues | discord-alert-engineer (card copy) | `agent-skills:build`, `agent-skills:test` |
| Bundle capture and funding graph | `docs/bundle.md` | launch-sniper | memecoin-venues (RPC funding reads), post-migration-reader | `agent-skills:build`, `agent-skills:test` |
| Gainer ranking, watch set, entry and exit matching | `docs/ranking.md` | market-economist | alert-tuner (thresholds), memecoin-venues (coverage) | `agent-skills:test`, `agent-skills:review` |
| Capture and replay backtesting | `docs/backtest.md` | alert-tuner | market-economist (denominators), general-purpose (Rust determinism) | `agent-skills:test`, `agent-skills:review` |
| Hot failover, secondary feed | `docs/failover.md` | general-purpose (Rust systems) | memecoin-venues (provider truth), security-auditor (dependency licenses, credential redaction) | `agent-skills:build`, `agent-skills:test` |
| Post-migration coverage (PumpSwap/Raydium) | `docs/sources.md` | post-migration-reader | memecoin-venues | `agent-skills:test` |
| Source verification and credentials | `docs/sources.md` | memecoin-venues | launch-sniper, post-migration-reader | `agent-skills:test` |
| Operations, config, health, runbook | `docs/operations.md` | memecoin-alert-architect | backend-architect | `agent-skills:build`, `agent-skills:ship` |
| Whole-system review before each milestone | `SCOPE.md`, `REVIEW.md` | agent-skills:code-reviewer | agent-skills:security-auditor, agent-skills:test-engineer | `agent-skills:review`, `/code-review` |

The docs phase used the lead agents directly without a skill wrapper; the skills above are the
gates for the build phase. `agent-skills:plan` is used once per milestone to break its spec into
verifiable tasks, and `agent-skills:constraints` is run before M1 to write the quality bar down.

## Architecture

Rust workspace, single binary, tokio.

```
crates/
  feed/        Feed session trait, PumpPortal adapter, secondary/Geyser adapter, per-adapter
               decode, supervisor, watchdog, generations, dedup, shadow probe, failover
               controller, warm-standby pipeline
  gate/        Which mints get trades: subscription planner, per-source ledgers, caps,
               unsubscribe on prune
  registry/    mint state and lifecycle, ring ownership, eviction, tombstones, regime resets
  detect/      volume anomaly, bundle status, CTO, wallet-hit, wallet-exit
  wallet/      watch set, wallet stats, live buy and exit matching
  rank/        rolling 7-day aggregates, weekly snapshot job, switchover coverage records
  enrich/      funding and balance reads, cache, TTLs
  deliver/     serialized queue per webhook, token bucket, hold queue, circuit breaker
  capture/     raw-frame recorder, segments, manifest, capture-gap accounting
  replay/      virtual clock, deterministic replay, delivery simulation, reports (offline)
  config/      thresholds, budgets, caps, timeouts, provenance records
  bin/         wiring, health, supervisor tree, shutdown, replay subcommand
```

Decode and detection share one task per source so the borrow from the frame stays valid; there is
no trade channel. Channels after detection are classed snapshot, delta or event, because a dropped
volume delta is a wrong answer rather than a late alert. The registry owns every per-mint
allocation and every eviction. Each source has a full pipeline; exactly one is authoritative, and
a switchover swaps roles wholesale without merging. The feed trait is a session runner over bytes,
with a per-adapter capability record, so a Geyser provider can join without touching detectors,
and a raw Geyser feed can be added without touching the primary adapter.

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
it would then fail to heal. One socket per provider, asserted, two providers maximum. Dedup is
keyed per trade because one transaction can hold several, and each source keeps its own dedup set.
Degradation is per outage identity, and the status lane — heartbeat and outage/status cards —
bypasses both hourly and daily budgets. A parsed but unresolved event is evidence of a transaction,
not of volume: it is held as `unknown`, excluded from buckets, and counted until resolved. Frames
are captured raw for replay, off the hot path and loss-counted; the alert path reads no capture
file. A provider switchover is a recorded event and a footer tag, not an outage, while another
source is authoritative.

## Delivery

Three webhooks, separate queues and budgets (10/hr volume, 6/hr wallets with a 3/hr exit sub-cap,
`deliver.cto.hourly_cap` 20/hr, with `deliver.cto.daily` 120 as an outer guard), daily caps as the
real attention limit for volume and wallets. Priority is status/outage > CTO listing > wallet exit >
wallet entry > bundle edit > volume fire > volume near. Non-urgent cards wait a 45 s reordering hold
(`deliver.hold.window_ms`), separate from the 6–10 minute budget wait (`deliver.budget_wait_max_ms` =
600,000 ms). Heartbeat and outage/status cards are the status lane and bypass both budgets; for
bundle cards only Fire and Cleared transitions edit. Every post uses `wait=true` so the id exists
for edits; errors are classified by status plus Discord code, because a breaker on all 4xx mutes a
channel after one deleted card. Posts are at-most-once per process; ambiguous POSTs are logged and
counted, never retried. Token names are attacker-controlled, so `allowed_mentions` is empty and
markdown is stripped. `--dry` and `--startup-check` exist, and staging webhooks are exercised before
production.

## Decisions

| # | Decision |
|---|---|
| Q1 | pump.fun only; the ranking is reconstructed on-chain. FOMO is not a data source. |
| Q2 | Realised SOL profit over 7 days on execution time where resolved (receive time otherwise, labelled), average cost, sybil clustering, ≥ 2 mints, mark-to-market. |
| Q3 | Bundle = same funder (direct or one hop) plus a buy inside the ~10 s launch window; window is a filter, not a deadline. |
| Q4 | SOL-weighted sold share, tiers 60/80/95, minimums of 5 wallets / 10 SOL / 2% supply, price context mandatory. |
| Q5 | DexScreener takeover list, Solana-only, diffed, high-water `claimDate` and posted-token set persisted. |
| Q6 | Three channels, capacity 2–3, budgets 10/6/20 per hour, daily caps binding for volume and wallets; CTO daily is an outer guard. |
| R1 | Bucket on receive time; payload has no timestamp or slot. |
| R2 | Metered per-mint subscriptions behind a gate with caps and cost counters. |
| R3 | Launch records persist so bundle status survives to CTO time. |
| R4 | Percent sold from trade events, balances only reconcile. |
| R5 | `wait=true`, one message-id store owned by `deliver/`, error-code classes, at-most-once posts, one serialized queue per webhook. |
| R6 | Watch set persists and reloads at boot; cold means suppressed and counted. |
| R7 | Rank store writes from M2a so the first watch set exists at M5. |
| R8 | Degradation per outage identity; status lane bypasses budgets. |
| R9 | Signal math in SOL; USD is display only. |
| R10 | Backtests replay recorded arrivals: capture raw frames from M1, run production code under a virtual clock, fit only with a train/test split and a report hash. No precision, recall or base rates. |
| R11 | Failover is a second provider with a warm standby, one authoritative source, wholesale role swaps, per-source dedup, and a copyleft dependency gate. No second PumpPortal socket. |
| R12 | Exits are one `EXITED` card per observed position episode at 90% of peak and 1.0 SOL proceeds, paired to a seen entry, sharing the wallet budget at a 3/hr sub-cap. |

## References

The public projects checked against this design — official pump.fun docs, Geyser parsers, launch
tooling, wallet clustering and PnL projects — are catalogued with stars, licenses and relevance in
[docs/references.md](docs/references.md). Nothing was cloned and no code is reused. There is no
mature open-source Discord alert bot for this domain, so the delivery layer is ours.

## Not in scope

A backtest of a period the bot did not record; paper trading, execution simulation, portfolio
tracking, order placement, dashboards; precision, recall, hit rate or base rates; multi-chain; a
second PumpPortal socket or key rotation to evade the one-socket limit; and multi-region or
multi-process HA for the bot itself. Failover covers the trade feed only; the process, Discord,
RPC and the side stores degrade independently.

## License

MIT — see [LICENSE](LICENSE). The secondary feed client is the first dependency that could change
this: M6 adds a copyleft deny-list to CI and does not start until the dependency tree is clean.
