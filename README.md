# onchain-crime-watcher

A Discord alert bot for Solana meme tokens. Rust for the data path, latency as the design axis,
built from scratch.

**Status: M1a collector is on main. The app is prepared for real data, with live integration
currently deferred. Default startup reports waiting for data; no demo trades or signals are
supplied. Recorded captures and optional live volume previews use the same volume core.
Product contract: scope revision 6. Live acceptance and Discord signal delivery remain pending.**

The contract is [SCOPE.md](SCOPE.md). The review that reshaped it is [REVIEW.md](REVIEW.md), the
public projects it was checked against are catalogued in [docs/references.md](docs/references.md),
and the two new subsystems have their own specs: [docs/backtest.md](docs/backtest.md) and
[docs/failover.md](docs/failover.md). This README is the readable plan.

## Start disconnected

Requires Rust 1.92+ on Linux/macOS. From the repository root:

```sh
cargo test --workspace --locked
cargo run --locked
cargo run --locked -- --config config/example.toml --check-config
```

Starting without arguments prints waiting-for-data status and exits. It does not read credentials,
connect to a provider, invent data, or create storage. See [the real-data runbook](docs/real-data.md)
for recorded-capture inspection and the connection steps for later.

## Connect later

Networking requires explicit --live, and paid subscriptions additionally require --enable-paid:

```sh
cargo run --locked -- --config config/example.toml --dry
cargo run --locked -- --live --config config/local.toml --preview-volume --run-for-secs 30
```

Paid subscriptions default off. Health is at `http://127.0.0.1:9898/health`. See
[the M1a runbook](docs/m1a.md) for fixture mode, spend-ledger initialization, credentials,
capture format, the one-mint paid opt-in and the remaining live acceptance checks. The collector
prints volume previews only when explicitly enabled and real observed data passes the gates.
It does not post signal alerts; later delivery and signal families below are the product plan.

## Signals

**Volume anomaly.** Per-mint volume against its own recent past and its age band. 15-second
buckets over a 7.5-minute ring, trimmed-mean baseline, dispersion and participation tests, and an
interval-scaled floor in SOL. Fire / near / reject, with *near* capped so it never crowds out a
fire. Cold tokens route to a labelled cold-baseline path instead of dividing by nothing. If the
participation gate proves noisy on live data, buyer breadth is the next feature — not a lower
threshold.

**CTO, with bundle status.** DexScreener's community-takeover list, filtered to Solana and diffed,
worded as a claim on a curated feed. The card adds bundle status: wallets funded by the same
funder (direct or one hop) that bought inside the ~10 s launch window, tracked as original token fractions sold, weighted by original SOL cost — 60% near, 80% fire, 95% cleared, escalating by editing one card. A
token with no launch record says so; it never shows a confident 0%. Repeated Jito tip payers are
recorded as corroborating evidence for a bundle, never as a membership rule on their own.

**Gainer wallets.** The previous week's most profitable wallets on pump.fun, reconstructed
on-chain from opening checkpoints plus ordered trade deltas: realised SOL profit on execution time where resolved (receive time otherwise, labelled),
average cost in lamports, sybil clustering, at least two profitable mints with ≥ 1 SOL deployed
each, mark-to-market at close. Top ~40 wallets form a watch set that is persisted and reloaded at
boot; it is a discovery set, not a delivery promise — at ~3 entries per wallet per day it yields
~120 entry candidates/day, and exits on observed positions can add a similar number, so ~240
against the 20/day wallet cap suppresses roughly 92% and counts `wallet_budget_suppressed`. Buys
only fire "entered"; exits fire `EXITED` on their own rule. Funder clustering follows the launch-window
rule; a public detector's 5-children-in-5-minutes default is recorded as a second opinion, not
adopted.

**Wallet exits.** One `EXITED` candidate when a completely observed position is depleted by
90% from peak and sale proceeds reach 1 SOL (priors). Every buy and sell updates the same
average-cost ledger as ranking. Transfers or missing opening basis suppress percentage exits;
adds after sells do not corrupt PnL. Opening observation and successful entry-alert delivery
are separate card fields. Exits share the wallet budget and expire after 30 s. *Revision 6.*

**Coverage is bounded and stated.** Trades are metered per mint, so the bot watches a chosen subset.
Every ranking and card says which subset it was computed over. Trade spend is one total
per-UTC-day budget, `gate.feed_sol_per_day` (prior 0.5 SOL/day), that includes launch windows —
there is no launch-window exemption. At 0.5 SOL/day the budget is ~500,000 messages/day, ~5.8 msg/s
sustained, so it binds long before the subscription cap; when the local stop threshold is hit, all paid mint and account subscriptions stop and new
paid commands are refused. Durable reservations survive restart. Provider teardown exposure is
measured separately; the local allowance is not a guaranteed exact billing ceiling.

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
| Capture plus input tapes | Arrival history and decision inputs | Local disk, pre-decode/pre-dedup frames plus control/external inputs | Off the hot path; bounded and loss-counted | 2 GiB cap, up to 14 days; trade-only estimate ~60–90 MB/day plus measured state/response overhead | M1–M2a |
| Second trade feed | Warm standby and failover | Geyser-protocol provider, own decode | 10–100 ms (**U**) | Paid (**U**); client license is a build gate | M6 |
| Discord webhook | Alerts and edits | HTTPS POST/PATCH | 100–300 ms estimated HTTP; per-kind expiry 15/30/120 s, stale candidates dropped | 5 per 2 s observed, not contractual | M1 |

The chain sets the floor: a ~400 ms slot means no source can beat roughly a quarter second from
event to process. Everything we add is decode, detection and queueing on top of that.

## Backtesting

A threshold swept over data the bot never recorded is a preference, not validation. So v1 records
before it replays: from M1 the bot taps raw frames — pre-decode, pre-dedup, with receive time,
generations and session lifecycle — into bounded, checksummed, compressed day segments. The tap
can drop and counts it; it never backpressures decode. A missing window is `unrecorded`, never
zero.

Complete-decision capture includes starting checkpoints, sanitized config/reloads, external
responses, timer/control inputs, boot identity and application order. Missing inputs refuse a
complete replay; changing the gate cannot invent unwatched trades. The harness runs the
**production** decode, dedup, gate, detectors and delivery admission under a virtual clock, and writes a decision journal plus a report. It is deterministic: the same
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
permissive proto definitions. Upstream Yellowstone client and proto directories are Apache-2.0;
the repository default is AGPL. Verify the pinned package tree rather than reject the client by
the repository-level label (`docs/failover.md` §14).
Spec: [docs/failover.md](docs/failover.md).

## Plan

| Milestone | Ships | Status |
|---|---|---|
| **M1 — Three spine slices** | M1a: single-source collector, bounded paid stream, durable spend stop, capture, health. M1b: one real volume signal through staging Discord with freshness. M1c: gate/TTL, watchdog/heal, supervision and checkpoint/delta storage contract. No production alerts. | M1a implemented; real-data volume core/replay prepared. Live acceptance, Discord delivery and M1c pending |
| **M2a — Delivery proven** | Budgets, hold queue, priority classes, dedup/cooldown, degradation contract, rank-store passive writer, card validator — with synthetic alerts into staging — plus the replay harness core and delivery-admission simulation. | Pending |
| **M2b — Volume** | Ring and parameters, cohort percentiles, dispersion and participation, cold-baseline path, shadow probe. First real alerts, gated on a backtest report over ≥ 7 recorded days with a train/test split. | Pending |
| **M3 — CTO** | Poll, Solana filter, diff with persisted high-water `claimDate` and posted-token set, per-mint latch, wording. | Pending |
| **M4 — Bundle** | Launch records, funding stage with exclusions and fan-out cap, percent-sold from trades, tier state machine. | Pending |
| **M5 — Wallets, entries and exits** | Ranking job, watch-set snapshot and boot reload, account subscription, buy matching, pacing, exit matching and the `EXITED` card with pairing and cost basis. Gated on the post-migration coverage test and a wallet/exit replay report. | Pending |
| **M6 — Hot failover** | Secondary provider adapter with own decode, warm-standby pipeline, arbitration, switchover records, per-source dedup and ledgers, two-source capture and replay, copyleft dependency gate. | Pending |

M1 first proves one full staged alert and measures coverage, spend-stop exposure, bytes/day,
latency, allocations and memory at a stated rate. Failure drills then prove reconnects,
generation drops, bounded heal, leak-free subscriptions and capture recovery. Production volume
still needs the seven-day M2b report; an early staging card does not waive that gate.

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

```text
crates/
  ocw/         CLI, wiring, health, shutdown, offline replay
  ocw-core/    feed, gate, registry, detect, position, rank, enrich, deliver,
               storage, capture, replay and config modules
```

Start with modules in one library and a single binary. Extract crates when reuse, dependency
isolation or measurements justify them. Position accounting has one implementation; storage
is background-only and its physical format is fixture-tested before being frozen.

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
| Budget admission | only until per-kind expiry: near 15 s, fire/entry/exit 30 s, CTO/bundle 120 s; first 2–3 can be immediate |
| Discord POST | 100–300 ms |

Rust buys headroom at pump.fun rates; it does not make the feed faster or Discord less
rate-limited. The dominant waits are upstream, the bucket window, and our own attention budget —
and cap pressure is a product decision measured through suppression and age, not hidden by
posting minutes-old signals.

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
wallet entry > bundle edit > volume fire > volume near. Non-urgent cards have a 5 s reordering hold, separate from per-kind expiry: near 15 s,
fire/entry/exit 30 s, CTO/bundle 120 s. A stale candidate is counted and dropped before sending. Heartbeat and outage/status cards are the status lane and bypass both budgets; for
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
| R12 | One exit candidate per complete episode, 90% net depletion and 1 SOL proceeds, incremental average cost, transfer ambiguity suppressed, observed and delivered pairing separated. |
| R13 | Durable paid-stream spend stop, with provider billing exposure disclosed separately. |
| R14 | Per-kind freshness deadlines and M1a/b/c slices; modules before separate crates. |

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

