# onchain-crime-watcher — scope

A Discord alert bot for Solana meme tokens. Rust for the data path, latency as the design axis.
Built from scratch; `~/Projects/volume-scanner` is a reference for observed payload behaviour and
hard-won lessons only — no code is carried over.

Status: revision 6, 2026-10-25, planning corrections after review of revision 5: capture and replay
backtesting, hot failover between feeds, and exit alerts for watched wallets, all in v1. M1a implementation has started; local verification and remaining empirical
acceptance are recorded in `docs/m1a.md` and `tasks/todo.md`. M1b/M1c remain pending.

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
- **2026-10-25 — revision 4 (reference crawl).** Five changes drawn from the public projects
  catalogued in `docs/references.md`:
  - A parsed but unresolved event is evidence of a transaction, not of volume. Unresolved
    candidates are held as `unknown`, excluded from buckets, and counted until resolved.
  - Buyer breadth is the next candidate feature for the volume family if the participation gate
    proves noisy on live data; it is not enabled in v1.
  - Repeated Jito tip payers are part of the funding-stage evidence for a launch bundle.
  - A public cluster detector's default (a funder with 5+ children inside a 5-minute window) is a
    second opinion on the bundle fan-out prior; it does not change the contract value of 50.
  - No mature open-source Discord alert bot exists for this domain, so the delivery spec remains
    ours rather than adapted from an upstream.
- **2026-10-25 — revision 5 (scope additions).** Three capabilities move from "not in scope" to
  v1, each with a written contract rather than a loosened promise:
  - **Backtesting becomes capture plus replay.** The bot records raw frames pre-decode and
    pre-dedup from M1, and a deterministic harness replays them through the production decode,
    dedup, gate, detectors and delivery admission under a virtual clock (`docs/backtest.md`). A
    replay measures the arrival history, never chain truth; it cannot yield precision, recall or
    base rates. Any threshold fitted on a replay carries a train/test split and a report hash, or
    boot fails.
  - **Hot failover between feeds.** PumpPortal permits one socket, so the second feed is a second
    provider (recommended: a Geyser-protocol provider with a permissively licensed client). Two
    sessions and two pipelines run in parallel; exactly one is authoritative at any instant; the
    standby is warm; roles swap wholesale with no merging. Switch rules, a flap guard, per-source
    dedup and the license gate are in `docs/failover.md`; decisions R11.
  - **Exit alerts for watched wallets.** A watched wallet's position fires one `EXITED` card when
    sold tokens reach `wallet.exit_share_bp` (90%, PRIOR) of the position's peak and proceeds
    reach `wallet.min_exit_sol`; pairing requires the entry to have been observed by live
    matching, and one episode fires once. Exits share the wallet budget with entries, rank above
    them in the queue, and never imply a position the bot did not see (`docs/ranking.md` §9).
  - The license stays MIT. The secondary client is the first dependency that can change that, so
    M6 adds a copyleft deny-list to CI and does not start until the dependency tree is clean.
  - Not in scope is rewritten rather than deleted: a backtest of a period the bot did not record,
    paper trading, and execution remain out.

- **2026-10-25 — revision 6 (build-readiness corrections).** Supersedes conflicting revision
  3–5 descriptions below; historical log entries remain historical.
  - Bundle sold share weights original token fractions by original SOL cost, never sale proceeds.
    Wallet exits reuse incremental average-cost accounting; transfer ambiguity suppresses them.
  - Weekly ranking reconstructs opening checkpoints and ordered deltas, excluding pre-window
    realised profit while retaining the basis of carried inventory.
  - Paid-stream exhaustion stops existing mint and account subscriptions; durable reservations
    survive restarts. Local allowance and provider-billed spend are separate, with no invented
    hard upstream charge guarantee.
  - Replay records starting state, sanitized config history, external inputs, boot identities and
    effective application order. Two-source controller replay keeps both histories isolated.
  - Alert expiry is per kind (15/30/120 s), with a separate 5 s reorder hold; delivered entries
    are distinguished from observed openings. Delivery survival and age are measured before launch.
  - Yellowstone client/proto directories are Apache-2.0 per upstream LICENSING.md; verify pinned
    packages and transitive licenses rather than rejecting the client by the repository default.
  - M1 is split into collector, staging alert and hardening slices. Start with one binary and one
    library containing modules; independent crates require a measured or ownership reason.

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
- **Buyer breadth (not enabled in v1).** If the participation test proves noisy on live data, the
  next feature is breadth — distinct buyers versus distinct sellers over the bucket — rather than
  a lower threshold. Public research bots score it alongside volume and velocity, and it separates
  a real crowd from one wallet cycling.
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
- **Corroborating evidence.** Repeated Jito tip payers across the launch wallets are recorded as
  funding-stage evidence and raise confidence, but never exclude or admit a wallet on their own.
  Public bundler tooling names same-block buys, one funder, lookup tables and repeated Jito tips as
  the observable signature; we use the first two as rules and the last two as evidence.
- **Cluster prior.** A public cluster detector defaults to a funder with 5+ children funded inside
  a 5-minute window. That is a second opinion on our 50-child fan-out cap, not a change to it: our
  rule is about the ~10 s launch burst, where a lower number would misclassify ordinary snipers.
- **Persistence.** The launch record — mint, t0, launch buyers, opening bags, funder clusters —
  is written to the bounded side store in section 2C's contract: off the hot path, prunable,
  allowed partial. CTOs arrive weeks after launch, so the record must outlive the process. A token
  with no launch record shows `bundle: no launch record`, never 0%.
- **Percent sold.** For each member, compute the observed fraction of original launch-bag
  tokens sold, then average those fractions weighted by original SOL cost. Sale proceeds are a
  separate field and cannot advance a tier. Later buys use the proportional original-bag
  allocation in `docs/bundle.md` §3. Missing history or transfers make the fraction partial;
  CLEARED needs complete history, ≤5% current holdings and price context. A stored buyer list
  without sales history cannot establish how much was sold.

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
  set, not a delivery promise**: at ~3 entries per wallet per day it yields ~120 entry
  candidates/day, and exits on observed positions can add a similar number, so ~240 candidates/day
  against the 20/day wallet cap suppresses roughly 92%, counted `wallet_budget_suppressed`.
- **Live matching.** A dedicated account subscription over the watch-set wallets in chunks, so a
  ranked wallet is seen on any mint, not only on watched ones. The subscription is metered like
  per-mint trades and draws from the same total per-UTC-day budget, `gate.feed_sol_per_day` (prior
  0.5 SOL/day), which includes launch windows with no exemption. Buys fire "entered"; adds are
  counted and never announced. Sells update the position and are the basis of the exit family
  below.
- **Exit alerts.** One `EXITED` candidate per completely observed position episode when net
  inventory depletion from peak reaches 90% (PRIOR) and observed proceeds reach 1.0 SOL (PRIOR).
  All buys and sells update the same integer average-cost ledger used for ranking. Realised
  profit is accumulated per sell, never total spending multiplied by depletion. An unexplained
  balance change suppresses percentage exits for that episode; transfers cannot become sold
  tokens. Remaining inventory, cost-basis mode, fees and source coverage are stated.
  With `wallet.exit_require_entry=true`, opening observation is required. Successful entry
  delivery is a separate fact: cards say `entry delivered at <time>` or `entry observed; alert
  not delivered`. After restart or eviction, basis is unknown until a verified zero and a new
  opening. With pairing off, unknown-basis activity is `SELL OBSERVED`, with no percent or PnL.

- **Dedup and pacing.** Keyed `(wallet, mint, entry, episode_id)` for entries and
  `(wallet, mint, exit, episode_id)` for exits; entry cooldown 1–4 h per
  `(wallet, mint)`. Exits share the wallet channel's hourly and daily budgets with entries, rank
  above them in the queue, and are sub-capped at `deliver.wallets.exit_hourly` (PRIOR 3/h) so an
  exit storm cannot crowd out entries. Suppression is counted, never silent.

**Rank aggregate store.** Realised PnL needs cost basis per wallet per mint over seven days, which
cannot live in RAM at pump.fun token counts. A bounded store on disk holds per (wallet, mint):
ordered normalized trade deltas, opening inventory/cost checkpoints and marks, pruned only when no active window needs them. Cumulative cache rows are not the weekly source of truth. Each row records the
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
  alert, allowed to lag, allowed to be partial and say so. Raw-frame capture is not a trade
  history either: it is a bounded, prunable evidence file for offline replay, never queried by a
  detector, and allowed to drop with a count.
- Portfolio tracking, copy-trading, or order placement.
- Replying to users, dashboards, or a web UI.
- Multi-chain. Solana meme tokens only.
- A backtest of a period the bot did not record. Capture starts in M1 and the replay harness only
  sees what arrived; there is no historical reconstruction and no chain-truth backfill. A
  threshold swept over unrecorded history remains a preference, not validation.
- Paper trading, execution simulation, slippage, fills, or any hypothetical portfolio. The replay
  harness re-runs decisions and admission; it places no orders and values no position it did not
  observe.
- Precision, recall, hit rate or base rates. No outcome labels exist; a replay's forward marks are
  a censored proxy and are labelled as one.
- A second PumpPortal socket, or a second key used to evade the vendor's one-socket limit. Hot
  failover is a second provider, not a second connection to the first.
- Multi-region or multi-process high availability for the bot itself. Failover covers the trade
  feed; the process, Discord, RPC and the side stores degrade per §6.6.

## 4. Latency model

Rust does not make the feed faster or Discord less rate-limited. The budget, split so each term
can be measured:

| Stage | Realistic | Notes |
|---|---|---|
| Upstream feed → process | 100–500 ms | Vendor-dependent; dominant term |
| Decode + detect | 5–15 µs typical, < 2 ms p99 | Measured with the rate stated; a p99 without a rate is not a claim |
| Enqueue | ~0 | Detection and decode share one task; no trade channel |
| Bucket close | 0 – Δ (0–15 s) | Volume decisions are evaluated when a bucket closes |
| Budget admission | Until per-kind expiry | Near 15 s; fire/entry/exit 30 s; CTO/bundle 120 s. Burst 2–3, then stale candidates drop rather than wait for a minutes-late alert |
| HTTP POST to 2xx | 100–300 ms | Discord, independent of our code |
| CTO end to end | minutes to hours | Curation on the vendor's side dominates; the poll adds 0–60 s |
| Feed switchover | Detection + switch_after_s + ≤5 s drill target | Default reconnect threshold is 30 s; measure trigger-to-authority separately from detection. Warm rings are required, and boundary loss is counted |
| Volume end to end | ~0.5–15 s + admission | From event time |

Instrumentation, required before any of these numbers is repeated: `event_time` where available,
`recv: Instant`, and per-stage histograms (`feed_lag`, `decode_us`, `detect_us`, `enqueue_us`,
`capture_tap_us`, `budget_wait_ms`, `http_us`, `switchover_ms`, `total_ms`) at p50/p99/p999 with
the measured rate. A counting `GlobalAlloc` test pushes 10k frames through decode + detect and
asserts zero allocations after warmup. A 2× peak saturation run uses `tokio-console`.

## 5. Architecture

Rust workspace, single binary, tokio.

```text
crates/
  ocw/         Binary: CLI, wiring, health, shutdown and offline replay subcommand
  ocw-core/    Library modules: feed, gate, registry, detect, position, rank, enrich,
               deliver, storage, capture, replay and config
```

These are logical boundaries, not twelve mandatory packages. Keep one normalized trade model
and one position-accounting implementation. `storage` owns bounded background persistence;
capture keeps raw bytes separately. Extract a module into a crate only for independent reuse,
dependency isolation or measured build/runtime needs. No microservice split in v1.

Rules that apply across modules:

- **One decode + detect task per source.** Frames are decoded from `&bytes` and detection runs in
  the same stack frame, so the borrow stays valid. There is no trade channel: a borrowed struct
  cannot cross one, and at pump.fun rates the split buys nothing. Channels exist after detection,
  where alerts are discrete. Each source owns a full pipeline — registry, rings, cohorts, dedup —
  and exactly one pipeline is authoritative at a time. Two sources are never merged and never
  summed; on switchover the roles swap wholesale (`docs/failover.md` §4).
- **Channel classes are explicit.** `snapshot` (idempotent, latest-wins fine), `delta` (must sum,
  never drop), `event` (discrete, drop with a count). Volume deltas are never latest-wins; a
  dropped trade is a wrong answer, not a late alert, and `dropped_trades` is red health.
- **One event enum between stages**, with fixed-width fields (`[u8; 32]` mint, `u64` lamports,
  `i64` time); strings never appear in the trade path and are resolved at the card. SOL carries
  `sol_source` of `Payload | ReserveDelta | Instruction`, so a decoded secondary feed is the same
  shape as the primary and no detector branches on the source.
- **Registry owns every per-mint allocation.** Eviction: idle TTL (prior 30–60 min) → dead; a
  hard live-mint cap with LRU; a tombstone set for recently dead mints. Re-entry allocates a cold
  ring, is counted, and never alerts while cold.
- **Registry states are fixed.** A mint is `created | on_curve | near_graduation (display-only) |
  migrated | dead | unknown`; `state_seq` increments on every transition, and a card or stored row
  quotes the state and sequence it was computed under.
- **Dedup** keyed on `(signature, mint, trader, txType, tokenAmount)` or signature plus
  transaction index where available — one transaction can contain several trades — sized by
  `rate × (replay + heal window) × 1.5`, not by time alone. Each source has its own dedup set;
  sharing one across sources would let a standby event suppress an authoritative one.
- **Capture is a tap, not a dependency.** The pipeline clones the frame handle into a bounded
  capture channel; recording may drop and count, never backpressure decode, and no alert path
  reads a capture file. Replay links the production crates and shares no state with the live
    process.
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

PumpPortal is the primary trade feed through M5. From M6 it is one of two providers, and the
vendor's **one websocket at a time** limit applies per provider, not per process: two providers
means two sockets, one per provider, each asserted independently. New-token and migration events
are free; **trades are metered per mint** (0.01 SOL per 10,000 messages), the API key must be
linked to a wallet holding ≥ 0.02 SOL, and **one websocket connection at a time** is permitted.

So the gate is part of the architecture, not an optimisation:

- **Eligibility.** Every launch gets a ~10 s trade subscription to capture its launch window (at
  roughly 0.23 launches/s that is ~2–3 concurrent). It keeps the subscription only if the token
  passes the trading gate: a config dollar volume in the first minutes, or a CTO listing.
- **Spend budget.** All trade subscriptions draw from one total per-UTC-day budget,
  `gate.feed_sol_per_day` (**PRIOR** 0.5 SOL/day), including every launch window; there is no
  launch-window exemption. At 0.5 SOL/day the budget is ~500,000 messages/day, ~5.8 msg/s
  sustained, so the spend cap binds long before the 20-slot concurrent-subscription cap. When it is
  hit, every paid mint and account subscription stops, new paid commands are refused, and
  `gate.budget_hit` is counted. Durable per-day reservations prevent reset by restart. Teardown
  margin and actual provider overshoot are measured; this local allowance does not guarantee an
  exact upstream billing ceiling. See `docs/feed.md` §3.3. Confirm the allowance before production.
- **Caps and TTL.** A hard cap on concurrent subscriptions (prior 20 slots), a per-mint TTL,
  unsubscribe on prune, and a reconcile task. `subs_live`, `unsub_failed`, `subs_capped`,
  `events_per_sec` and `estimated_sol_per_day` are config values and `/health` counters.
- **Single socket invariant per provider**, asserted at runtime: `sockets_open[provider]` is 0 or
  1 for each configured provider, and the process total is at most two. No parallel probe
  connections. A second socket to the *same* provider is a ban risk and an invariant violation,
  never a failover path.
- **Unresolved is not a fill.** A raw Geyser or ShredStream event the decoder cannot resolve to a
  concrete trade is held as `unknown`, excluded from buckets and aggregates, and counted
  `unresolved_events`. It becomes volume only when resolved against the transaction. Public
  streaming SDKs state the same rule; treating a candidate as a fill inflates volume exactly when
  the feed is degraded.

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
| Parsed but unresolved event | decoder resolution | hold as `unknown`, exclude from buckets and aggregates, count `unresolved_events` until resolved | unresolved counter |
| Panic | `JoinHandle` join error | respawn under restart budget | restart counter |
| Backpressure | channel class | delta channels never drop; event channels drop with a count | drop-by-reason counter |
| Ban (excess connections) | repeated timeouts | stop restarting, health red, wait out the hour | banned state |
| Primary provider lost (M6) | session exit, watchdog, or ban | promote the warm standby; no re-warm; drop the outgoing partial and count boundary frames | card footer names the source for 10 min; not an outage identity |
| Both providers lost (M6) | both source states | no authority, suppress alerts, one `trade_stream` outage identity | outage card, `stale` |
| Capture queue full | bounded tap | drop the record, write a marker, never block decode | `capture_dropped_frames`, amber |

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

### 6.4 Shadow verification and hot failover

One authoritative feed at any instant, plus up to two independent checks. From M2 to M5 the
check is an RPC sample whose only job is to disagree. From M6 a second provider runs a **warm
standby** — a full pipeline that is subscribed, decoded, bucketed and counting, but whose
candidates are discarded and counted — and can be promoted in seconds. The RPC sample stays: two
feeds can share upstream infrastructure, so a second ingest is not a substitute for an
independent chain read.

| Check | Cost | What it proves |
|---|---|---|
| RPC sampling of active mints (last-trade timestamp, trade count per aligned window) | low | Authoritative feed liveness and coarse loss, on a sample |
| DexScreener / GeckoTerminal polls | low, slow | Price sanity, nothing about individual trades |
| Second full ingest, warm standby (Geyser or equivalent) | high | Near-complete integrity check and the basis for failover; carries its own gaps |

Start with RPC sampling in M2. Disagreement is measured per sampled mint per aligned 10-minute
window as `|primary − shadow| / max(primary, 1)`, over ~20 mints stratified across volume deciles
including at least five from the bottom, reported at p50/p90 per stratum. It assumes loss is
uniform; the doc says so.

**Failover contract (M6).** Two providers, two session tasks, two pipelines, one authority. Switch
primary → secondary when the primary is banned, failed, or out of contact beyond the outage
threshold and the secondary is fresh; switch back only after five minutes of primary health and
aligned agreement within tolerance. A disagreement tags alerts and blocks switch-back but never
forces a switch on its own. A flap guard holds the current authority after three switches in an
hour. Roles swap wholesale: no ring is merged, no source is summed, and each source keeps its own
dedup set and generation stream. The outgoing partial bucket is dropped and the boundary frames
are counted, so no bucket ever contains two sources. The full contract is `docs/failover.md`;
decisions R11.

Rules for any multi-source operation: exactly one source is authoritative per window; never sum
two sources; cross-source agreement is evidence, never a merged series; failover needs its own
acceptance tests (forced failures, duplicates, boundary loss) before it is called hot; and a
second provider whose client is copyleft cannot enter the binary — MIT is the project's license
and M6 adds a dependency deny-list before it starts.

### 6.5 Error handling by stage

| Stage | Failure | Policy |
|---|---|---|
| decode | malformed JSON, unknown type | count, sample, drop; never retry the same bytes |
| detect | division by zero, NaN, invariant break | guard, route to `cold_baseline` or drop, count, never emit from an invalid statistic |
| enrich | timeout, 429, missing account | retry with jitter; serve cache with an age stamp; beyond max stale age mark `unknown` |
| rank store | disk full, corrupt file | quarantine, coverage partial, live alerts unaffected |
| capture | disk full, queue full, torn tail | stop or drop with a written marker and a count; never block decode; never delete a live segment |
| replay | corrupt segment, hash mismatch, missing config | refuse the report and name the segment, hash or key; a partial window is labelled, never presented as whole |
| deliver | — | see §8 |
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
| unverified | single source, or two sources whose aligned comparison is over tolerance | posted; footer says data is unverified |
| stale | no event longer than threshold | suppressed; outage card only |

- **Outage identity** is `(identity, outage_start)`. Declared after 30 s, recovered after 60 s of
  health, so a blip posts nothing. The outage card is in the status lane and bypasses the hourly
  and daily alert budgets entirely; it states start, end, duration and reason. A provider
  switchover is **not** an outage identity while another provider is authoritative; it is a
  `SWITCHOVER` record, a footer tag for ten minutes, and a counter. Only `authoritative = none`
  opens the `trade_stream` identity.
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
| Second trade feed (M6) | Geyser-protocol provider, warm standby | Raw transactions/account updates, own decode; slot and block time; paid and **U**; client license is a build gate |
| Replay capture (M1) | Local disk, raw frames | Arrival history, not chain truth; 2 GiB cap, 14 days, loss counted |

No provider joins the hot path until its failure mode is written down: what happens on reconnect,
on a gap, on a 429, and how the bot reports that it missed data. Post-migration coverage is a
standing unknown — the likely answer is PumpSwap covered and Raydium not — and is tested before
M5 depends on it.

## 8. Delivery

Three webhooks, one per family, each with its own serialized queue, token bucket and budget.

| Channel | Families | Budget |
|---|---|---|
| volume | volume fire, volume near | ~10/hr, capacity 2–3, *near* ≤ 30% |
| wallets | wallet entries, wallet exits | ~6/hr, exits sub-capped at 3/hr; shared 20/day |
| cto | CTO listing, bundle tier edits | `deliver.cto.hourly_cap` = 20/hr (circuit breaker) |

Daily caps (prior 40 volume, 20 wallets) are the binding attention limit; hourly budgets are
burst control. A sliding window is used so two bursts cannot straddle a boundary.
`deliver.cto.hourly_cap` = 20 is a circuit breaker, not a product budget; `deliver.cto.daily` = 120
is an outer guard, not a product allocation. The status lane — heartbeat and outage/status cards —
bypasses both hourly and daily budgets entirely; a 5-minute heartbeat is 12 posts/hour and would
otherwise exceed the volume channel's 10/hour.

**Priority classes,** used only before send: status/outage > CTO listing > wallet exit >
wallet entry > bundle edit > volume fire > volume near. An exit outranks an entry because the
position it describes is already open and the reader was told about it; both share one wallet
budget, and the exit sub-cap stops an exit storm from starving entries. A token bucket alone is
first-come-first-served. Non-urgent
cards — volume *near* and wallet entries — have a 5,000 ms reorder hold. It delays eligibility,
not expiry. `deliver.max_age.<kind>_ms` expires near at 15 s, volume fire and wallet entry/exit at
30 s, CTO listing and bundle edits at 120 s (PRIOR). Exits are immediately eligible. The sender
checks age before each attempt, including after 429 pauses, and never extends the original
deadline. Every card shows its age. Candidate survival, successful delivery and age by family/rank
band are release evidence; observed entries are never described as delivered without POST success.
The highest eligible class is admitted when a token is available. Two of the volume hour's ten tokens are reserved for fire. Priority decides
which alert survives a full budget, never whether the budget is exceeded.

**Message ids and edits.** One id store, owned by `deliver/`; detectors keep tiers, not ids. Every
post uses `?wait=true`; the response's message id is stored with
`(mint, family, kind, tier, card_state)`, where `kind` distinguishes a wallet entry from its exit.
Edits rebuild the full embed from that state. A missing id is a delivery failure, not a
success. Posts and edits share the webhook's rate bucket, so one serialized queue covers both;
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

1. **M1 — Spine in three verifiable slices.**
   - **M1a collector:** binary/library skeleton, minimal typed config, single PumpPortal session,
     one bounded paid mint plus free creates/migrations, normalized decode/dedup, durable spend
     stop, raw capture, health and minimal outage sender. Pin real payload fixtures and measure
     coverage, bytes/day and teardown exposure. No production signal alerts.
   - **M1b staging alert:** one complete volume path using §2A priors, registry/ring state,
     per-kind freshness, a staging webhook and `--dry`; captured fixture and live staged signal
     produce a readable card. This is the first product demonstration, not a threshold validation.
   - **M1c hardening:** expand the gate/TTL policy, generations, watchdog/probe, bounded heal,
     supervision, heartbeat and shutdown; define and fixture the checkpoint/delta storage API.
     Fault drills and measured saturation precede M2a. Zero-allocation and latency figures remain
     benchmark targets, not unmeasured claims or prerequisites to the first staging card.
   *Acceptance:* byte-identical capture; one real staged signal; expiry at the stated boundary;
   paid-stream stop survives restart; numeric reference-count tolerance set before the coverage
   run; forced disconnects/recovery and subscription leak tests; measured loss, latency, memory
   and allocation counts at the stated rate. Production stays gated by M2b's evidence report.

2. **M2a — Delivery proven.** Budgets, hold queue, priority classes, dedup/cooldown, degradation
   contract, rank-store passive writer, card template and validator, exercised with synthetic
   alerts into staging channels; **replay harness core** (virtual clock, same decode/dedup/gate,
   delivery admission simulation, decision journal) proven on the M1 fixture.
3. **M2b — Volume family.** Per-mint ring and parameters, cohort percentiles, dispersion and
   participation tests, cold-baseline path, shadow probe and disagreement metric. First real
   alerts, gated on a **backtest report over at least seven recorded days** with a stated
   train/test split (`docs/backtest.md` §6).
4. **M3 — CTO family.** Poll, solana filter, diff with persisted high-water `claimDate` and
   posted-token set, latched per mint, wording.
5. **M4 — Bundle.** Launch-record persistence, funding stage with exclusion list and fan-out cap,
   percent-sold from trades, tier state machine on the CTO card.
6. **M5 — Wallets, entries and exits.** Ranking job, watch-set snapshot and boot reload, account
   subscription, buy matching and pacing, **exit matching and the `EXITED` card** with pairing,
   cost basis and the shared wallet budget. Gated on coverage proving acceptable, including the
   post-migration test, and on a wallet/exit replay report.
7. **M6 — Hot failover.** Secondary provider adapter with own decode, warm-standby pipeline,
   arbitration, switchover records, per-source dedup and ledgers, two-source capture and replay,
   and the copyleft dependency gate. Requires a provider whose failure mode is written down and
   whose client is permissively licensed; a second PumpPortal socket is not an option.

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
16. **R10 — Backtests replay recorded inputs, not chain truth.** Raw frames are captured from M1,
    pre-decode and pre-dedup, with receive time, generations and session lifecycle; the replay
    harness runs the production code under a virtual clock and simulates delivery admission. A
    fitted threshold carries `fitted_on`, `tested_on`, `fitted_at` and a report hash, or boot fails.
    A replay cannot yield precision, recall, base rates or delivery latency. *2026-10-25 (revision
    5).*
    Revision 6 requires starting state, sanitized config/reloads, offline external-response tapes,
    boot identities and logical application order; unsupported gate/watch-set counterfactuals
    cannot justify fitted defaults. M6 controller replay uses both isolated histories.
17. **R11 — Failover is a second provider with a warm standby.** PumpPortal's one-socket limit is
    not bypassed; a second provider runs a full parallel pipeline, exactly one source is
    authoritative at any instant, roles swap wholesale with no merge, and each source keeps its own
    dedup and generations. Switch rules, flap guard, and the copyleft dependency gate are
    `docs/failover.md`. *2026-10-25 (revision 5).*
18. **R12 — Exits are one candidate per complete observed episode.** Net inventory depletion
    reaches 90% of peak and proceeds reach 1 SOL (priors). Incremental average-cost PnL, complete
    opening basis, no transfer ambiguity, separate observed/delivered pairing, shared wallet
    budget and 30 s expiry are required. See `docs/ranking.md` §9. *Revised 2026-10-25, revision 6.*
19. **R13 — Durable spend stop.** Existing paid mint/account streams stop at local exhaustion;
    durable reservations survive restart. Provider billing exposure is measured and disclosed,
    not guaranteed away. See `docs/feed.md` §3.3. *2026-10-25, revision 6.*
20. **R14 — Freshness and first slice.** Per-kind expiry, delivery survival reporting, and the
    M1a/b/c sequence replace minutes of budget waiting and a monolithic skeleton milestone.
    Logical modules start in one library and binary. *2026-10-25, revision 6.*

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
- A backtest replays what arrived. It cannot see what the feed dropped while connected, cannot
  supply outcome labels, and cannot measure delivery latency; a forward mark is a censored proxy
  over the alerts whose mint kept trading, not precision. Capture itself can drop, and the report
  must state the hole. *Revision 5.*
- Failover depends on a provider not yet selected. Its latency, spend, filter limits and client
  license are all unverified; a second feed can share upstream infrastructure with the first, so
  agreement is evidence, not proof. The secondary's own gaps are real gaps. *Revision 5.*
- Exit alerts depend on `newTokenBalance` semantics (C6) and on live matching having seen the
  entry. A pre-boot position, an evicted state, a transfer-out, or a sale on an unwatched venue
  produces no exit or no basis; the card says which. *Revision 5.*

