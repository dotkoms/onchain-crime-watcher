# Review — onchain-crime-watcher scope

Date: 2026-10-25. **Historical: this review evaluates revision 4.** Revision 5 of
[`SCOPE.md`](SCOPE.md) adds capture and replay backtesting, hot failover between providers, and
wallet exit alerts, so its "no hot failover" and "no backtest harness" statements describe the
scope at review time, not the current contract. Read-only review of `SCOPE.md` and `README.md` by
five reviewers with separate
lenses: statistical accuracy, Discord delivery, pipeline architecture, Rust latency, and data-source
provenance. This file consolidates their findings, collapses overlaps, and records where they
disagreed. Nothing here changes the contract until SCOPE.md is revised.

**Verdict.** The resilience thinking is the strongest part of the design and survives review
almost intact. The signals are the weakest: most of the statistical definitions cannot be
implemented without an implementer inventing numbers, which is the failure the doc's own rule
("an invented constant is a bug with a comment") exists to prevent. Two facts about the feed, both
verified against today's vendor docs and the working `volume-scanner` client, invalidate specific
claims in the doc rather than its overall shape.

Evidence limits are listed at the end. Anything marked *(verify)* was not confirmable today.

## Blockers

### B1. There is no feed subscription or cost model

Sections 2C, 4, 5, 7, 8. Confirmed by three reviewers independently.

PumpPortal is not a global trade firehose. Trades require `subscribeTokenTrade` **per mint**; new
tokens and migrations are free. Today's vendor page meters per-mint trades at 0.01 SOL per 10,000
messages, requires an API key linked to a wallet holding ≥0.02 SOL, and permits **one websocket
connection at a time**. The doc never chooses a subscription mode, never states an event budget,
never defines which mints earn a subscription, and never says when one is dropped. §8 budgets
Discord to the hour while the feed — the larger and less predictable cost — is unbudgeted.

Consequences: the bill scales with tokens rather than attention; the gate that decides what the
bot watches is accidental (whatever the adapter happens to do); and §2C's "previous week's biggest
gainers on pump.fun" silently means "among mints we paid to watch".

Fix: a `gate/` stage upstream of trade subscription — eligibility predicate over the free
new-token stream, hard cap on concurrent subscriptions, per-mint TTL and unsubscribe-on-prune,
`subs_live`, `unsub_failed`, `subs_capped`, `events_per_sec`, `estimated_sol_per_day` in config and
`/health`. Ranking cards quote coverage (mints watched, events seen). §7 gains the API key and
wallet-balance prerequisite.

### B2. There is no token registry, lifecycle, eviction, or regime boundary

Sections 2, 5. Architecture reviewer (primary), confirmed by the statistics and latency reviews.

Each family assumes a different token-state model. Volume rings mix bonding-curve fills with
post-migration pool fills; bundle status for a token launched before boot defaults to a
confident-looking 0%; wallet matching has no per-(wallet, mint) position state; cards assume every
field exists in every state. Meanwhile per-mint state is bounded per mint and unbounded in the
number of mints — at pump.fun launch rates that is monotonic growth, and every sweep and cohort
statistic degrades as the process ages.

Fix: `registry/` between decode and every consumer, owning
`mint -> { created_at, state, state_seq, ring, enrich, bundle, last_seen }`. States:
`created → on_curve → migrated → dead`, with `unknown` for tokens first seen mid-life;
`near_graduation` is a display-only later addition, not a lifecycle state. Eviction on
idle TTL, then LRU to a hard cap; re-entry allocates a cold ring and is counted
(`mint_reentered_cold`) and never alerts while cold. On migration, reset or rebase the volume
baseline and count `regime_reset`; cards stamp the regime. Every candidate carries `state` and
`state_seq`. This lands in M1 — it is a map, an enum and a sweep, and it is a prerequisite for
B3, B6 and several highs.

### B3. The bundle family has no data path end to end

Sections 2B, 7, Q3, M4. Architecture and provenance reviewers, independently.

Three separate gaps, each fatal alone:

1. **Same-funder is not in the trade stream.** Trade messages name only `traderPublicKey`; who
   funded that wallet is a prior SOL transfer requiring `getSignaturesForAddress(wallet)` plus
   `getTransaction` on the funding tx — roughly 11 credits per wallet (70–90 HTTP calls), ~1,000
   credits per 80-wallet CTO (~11 per wallet) *(per today's Helius pricing; verify tier)*.
   Affordable per CTO because CTOs are rare; not affordable per launch.
2. **The launch window is RAM-only, CTOs arrive weeks later.** The 10-second buyer set and opening
   bags exist only if the bot watched the launch, and idle mints are pruned. At CTO time the data
   is gone. The doc's own §3 forbids "any database that the alert path depends on", which
   contradicts §2B directly.
3. **The 10 s window is a membership filter, not a deadline.** Clustering can run after the window
   closes, off the hot path. Written as a deadline, the feature looks impossible; written as a
   filter, it is routine. The doc must say which, because the difference is the milestone.

Also: "one hop" is unbounded in fan-out (a shared funder or CEX hot wallet merges hundreds of
wallets into one bundle) and is trivially defeated by two-hop funding.

Fix: persist a compact launch-window record (mint, t0, launch buyers, opening bags, funder
clusters) in the same bounded side store as the rank aggregates — it already has the right
contract: off hot path, prunable, allowed partial. Cluster CTO-only and asynchronously. Edge is
`funder → funded`, one direction; cap fan-out (>N children = shared infrastructure, bundle
`unknown`); keep a CEX/program exclusion list; state "one-hop only" on the card. Where no launch
record exists, the card says `bundle: no launch record`, never 0%.

### B4. The payload has no timestamp and no slot

Sections 4, 6.2, 6.3. Latency and provenance reviewers, independently.

The trade payload is
`bondingCurveKey, marketCapSol, mint, newTokenBalance, signature, solAmount, tokenAmount,
traderPublicKey, txType, vSolInBondingCurve, vTokensInBondingCurve` — no `timestamp`, no `slot`, no
`blockTime` *(observed from a captured payload and the working client; vendor publishes no
schema)*. `solAmount` is observed in the working client (`state.mjs:67`, `ingest.mjs:78`) but absent
from the captured sample, so it is present-but-unpinned until the M1 fixture. The working scanner
buckets on `Date.now()` for exactly this reason.

So "bucket by event time, not arrival", "late beyond window", and the feed-lag row of the latency
table are all unimplementable as written.

Fix: bucket on a monotonic `recv_ts`, anchored to arrival of the `create` event, and say so.
Resolve `signature → blockTime` via `getTransaction` only where true block time matters (bundle
window audit, late-trade sample), labelled resolution rather than routing. Or make Geyser primary,
since raw transactions do carry slot and block time — a reason to upgrade feeds that the doc does
not currently state.

### B5. The delivery contract does not work as written

Sections 2B, 6.5, 8. Discord reviewer.

- **No message id.** Execute Webhook without `?wait=true` returns 204 with no body; edit-in-place
  is therefore unimplementable, and a 204 is acceptance, not delivery.
- **4xx breaker is misclassified.** Opening a circuit breaker on all 4xx mutes a family when a
  moderator deletes one card (404/10008) and when one oversized payload 400s. Retrying a bad
  token (401/50027) counts toward Discord's invalid-request limit (reported 10k per 10 min per
  IP) and can temporarily ban the IP, taking all three webhooks down.
- **Restart loses ids, and cooldown then swallows the escalation.** A restart between 80% and 95%
  means the "cleared" tier is neither edited nor posted. A restart mid-outage leaves a stale
  outage card above a healthy feed, and webhooks cannot read history to fix it.

Fix: `?wait=true` on every post and store
`(mint, family) -> {message_id, channel_id, webhook_id, tier, card_state}`. Classify errors by
status plus Discord error code (429 transient with `retry_after` and scope; 10008 drop id and stop
editing; 10015/50027/403 breaker + red; 50035 reject payload, no breaker). For outage cards choose
one contract: "one per process lifetime" with a timestamped card, or a bot token used for readback
only. Never let a cooldown suppress a tier that has no target — post and count
`escalation_without_target`.

### B6. The volume family is unparameterised

Section 2A. Statistics reviewer.

Absent: bucket duration, ring size, trim count, warmup length, minimum sample count, whether the
ring is calendar-aligned or token-anchored, the floor's functional form and value, and the
fire/near/reject cut-points. "The dollar floor scales with the interval" is logically empty when
buckets are fixed-duration — the floor scales with nothing. There is also no dispersion test, so a
token varying ±5% and one varying ±200% fire identically, and no distinct-taker requirement, so one
wallet can print a bucket with a buy and a sell.

Fix (numbers are priors, to be labelled as such): Δ = 15 s, ring N = 30, trim 3 from the top,
warmup 16 completed buckets, ≥8 non-empty buckets and ≥6 trades per bucket, ring updated after the
decision, current partial bucket never in its own baseline. Floor
`max($300, 2·Δ/60s · cohort_median_trade_USD)`, applied to numerator and denominator. Scale test
`current ≥ median + max(4·1.4826·MAD, 0.5·median)`. Fire ≥3.0×, near [2.0, 3.0), reject <2.0×.
≥5 distinct takers and top-taker ≤60%. Cold start routes to a labelled `cold_baseline` path gated
on an absolute 10× floor rather than being discarded by the division guard. Name a SOL/USD source
(Binance/Coinbase poll 30–60 s, or Pyth on-chain) with `sol_usd_age_ms`, or the floor's unit is
undefined.

### B7. The gainer ranking contract is invalid

Section 2C, Q2. Statistics and provenance reviewers.

- **No cost-basis rule.** "Weighted cost" is a store field, not a rule. FIFO and average cost
  diverge on adds; partial fills unaddressed. Fix: average cost in integer lamports, recomputed
  from raw buys and sells at snapshot time.
- **Fees are not computable from the stream.** `solAmount` is the curve leg; priority fees and
  tips live in the transaction. The doc claims fees included, which overstates exactly the sniper
  wallets being ranked. Fix: drop the claim, or resolve fees for watch-set wallets only
  (bounded), or state it as a config estimate.
- **Transfers evade realised PnL.** Moving tokens out is not a sale; a wallet that ships its bag
  and sells elsewhere records no profit. Fix: define transfers explicitly (treat as zero
  proceeds + note) or exclude affected wallets.
- **Window basis.** Realised PnL over 7 days must be defined on trade execution time, not token
  launch date, and the card should quote that window.
- **Survivorship.** Realised-only ranking hides held-to-zero positions (a −100% unrealised record
  shows as 0), selecting for bet-and-vanish wallets. Fix: mark-to-market at window close with
  stale >24 h treated as zero, and show open positions on the card.
- **Sybil and rugger gaming.** Breadth ≥2 is satisfied by a serial rugger's own launches, and a
  whale splitting across wallets defeats it. Fix: cluster by the Q3 funder rule before ranking,
  one slot per cluster; exclude or tag mints where the wallet is creator or launch-bundle member;
  require ≥1 SOL deployed per counted mint.
- **Window can't start at M5.** The first usable watch set then exists at M5 + 7 days. The store is
  failure-isolated and off the hot path, so start it writing passively in M2 at no alert risk.

## High findings

- **H1 — Budgets are not arbitration.** Three channels with 10/6/N per hour is routing, not
  ranking; no component produces a priority, token buckets are first-come-first-served, and `near`
  competes with `fire` for the same tokens. Fix: token bucket (capacity 2–3, refill 10/hr) plus a
  30–60 s hold queue for non-urgent cards, reserve 2/hr for fire, daily caps (e.g. 40 volume, 20
  wallets), sliding window, and a CTO safety cap (e.g. 20/hr) declared as a circuit breaker rather
  than a product budget. Consider cross-family composition from RAM when any family fires.
- **H2 — Gap heal cannot keep up.** `getSignaturesForAddress` over the pump.fun program returns
  global history with no mint attribution; at 500–2,000 tx/s a 60 s gap is 30k–120k signatures
  needing a `getTransaction` each — 10–100 minutes to heal one minute. Fix: heal **per
  subscription** via the token-scoped `bondingCurveKey` from the create event (bounded by watched
  mints), with block-level heal (`getBlocks` + `getBlock`) reserved for global truth; bound total
  calls as well as duration and declare unhealed when the call budget is hit.
- **H3 — Watchdog and reconnect are unsafe.** Inter-arrival thresholds on a bursty stream
  false-positive, and reconnect-on-suspicion manufactures the gap that H2 cannot heal. Reconnect
  also risks a one-hour ban (one socket, excess connection attempts). Fix: liveness from the
  free new-token stream (near-constant rate) with an adaptive threshold; separate transport
  liveness (ping/pong) from application liveness; probe three active mints before reconnecting;
  single-socket invariant asserted at runtime; "banned" as its own state with restarts stopped;
  count `watchdog_false_positives`.
- **H4 — Health cannot answer "quiet or dead" to a human.** `/health` is pull-only. Fix: a boot
  line (uptime 0, counters zero, watch-set generation and age, config hash, feed mode) and a
  heartbeat edited in place every N minutes with the funnel (seen → gated → detected → enriched →
  ranked → sent), drops by reason, edits by tier, warmup suppression, shadow disagreement,
  `blind_seconds_1h`, per-webhook last success; plus an external dead-man's switch.
- **H5 — Restarts break promises silently.** Message ids and the watch set are RAM-only, so a
  restart loses edit targets, can re-post CTOs on every boot, and leaves the wallet family dark for
  up to seven days while §6.6 promises it still posts. Fix: prime CTO state on boot without
  alerting, persist the bounded watch-set snapshot and outage identity, or rewrite the contract.
  Rank coverage counters need an fsync'd journal — a restart must not turn a gappy week into a
  whole one.
- **H6 — Enrichment contract is contradictory.** Fix with one sentence: alerts emit once from
  RAM, every field is fresh-with-timestamp / stale-with-timestamp / unknown, enrichment produces
  edits only, and a stronger claim requires a fresh read. Per-field TTLs with units, a max stale
  age, and a cache bounded by the registry. Percent-sold should come from `newTokenBalance` on each
  trade (exact, no polling, no TTL), with balance reads only as slow reconciliation; state the
  transfer-out blind spot.
- **H7 — Bundle math.** Denominators conflict (wallet count vs token quantity vs SOL value).
  Tier on SOL-weighted share; require ≥5 wallets and ≥10 SOL bought before showing any tier
  (a 2-wallet bundle has 50% granularity and cannot hit 60% until 100%); show price at first
  bundle sell, now, 1 h peak and % change during the sell window, and classify dumped if the fall
  exceeds 30%.
- **H8 — Wallet alerts are side-blind and mis-keyed.** Every sell matches the in-RAM set, so an
  exit is announced as a new position. Fix: buy-side fires "entered", sell-side is a separate
  "exiting" family or nothing; carry side, size, and whether the mint is new for that wallet;
  dedup key includes the wallet, cooldown per (wallet, mint).
- **H9 — Latency table hides the real waits.** Budget admission is minutes (10/hr = one per six
  minutes; 6/hr = one per ten), not 0–5 s; volume signals are additionally Δ-bound (15–60 s); and
  "~200 ms end to end" contradicts the table's own 100 ms floor. Fix: split `feed lag`, `decode`,
  `detect`, `enqueue`, `budget admission`, `http`, `total`; state the token-bucket capacity
  decision (burst 2–3 posts a cluster promptly) because it changes what "real-time" means.
- **H10 — Hot-path claims need correction.** A borrowed struct cannot cross a channel; drop the
  trade channel and decode + detect in one task. Dedup key by `(signature, mint, trader, txType,
  tokenAmount)` or signature + index — one transaction can hold several trades and signature-only
  dedup drops real volume. Size dedup by rate × window, not by time alone. "Latest-wins" is unsafe
  for volume deltas: classify channels as snapshot / delta / event and count `dropped_trades` as
  red health. Keep `panic = "unwind"`, retain every `JoinHandle`, restart via `JoinSet` with a
  budget. Make the feed trait a session runner over bytes (not object-safe async trait) with
  per-adapter decode and a capability record (`has_slot`, `has_block_time`, `has_curve_reserves`,
  `has_dex_trades`); every field nullable with defined detector behaviour.
- **H11 — Delivery mechanics.** `allowed_mentions: {"parse": []}` on every payload and sanitise
  token names (a token named `@everyone` pings a server; markdown renders links inside trusted
  cards). Edits share the webhook bucket with posts — one serialized queue per webhook, max one
  edit per message per 30–60 s, coalesce to latest tier. State delivery semantics (at-most-once
  per process; no auto-retry on ambiguous POST timeouts or 5xx; PATCH is safe to retry). Add
  `--dry`, `--startup-check`, staging webhooks, a card template with truncation priority and a
  pre-send validator, colour paired with a word, and a redacted-payload log on failure.
- **H12 — CTO source is a global list, not a flag.** Verified live: `GET
  /community-takeovers/latest/v1`, unauthenticated, 60 req/min, no parameters, no boolean field —
  inclusion is the signal, `claimDate` is the only takeover-specific field, ~6 entries in ~18.6 h
  at the 2026-10-25T17:25Z fetch.
  Fix: client-side `chainId === 'solana'` filter, diff against a seen-set keyed on `tokenAddress`,
  persist the high-water `claimDate` across restarts, and decide explicitly whether non-pump.fun
  CTOs alert.
- **H13 — Coverage gaps in the sources.** Post-migration trade delivery is unconfirmed: third
  parties conflict, the likely answer is PumpSwap covered and Raydium not, which would silently
  remove the most profitable cohort from the ranking *(verify by subscribing to a known migrated
  token)*. Add per-venue coverage counters and treat Raydium as a known gap. "Holders" appears
  three times and feeds no signal — delete it or define it as a top-20 sample from
  `getTokenLargestAccounts`. Name the SOL/USD source and the token price source for aged tokens.

## Where reviewers disagreed, and how it resolves

- **Percent-sold source.** Statistics reviewer: derive from sell/buy SOL in trade events.
  Provenance reviewer: `newTokenBalance` is exact per trade and removes the need for polling.
  These agree in direction — trades over balances — and the tighter version is `newTokenBalance`
  with trade-SOL as the fallback and balances only as reconciliation. Both reject the current text,
  which implies balance polling.
- **Gap heal mechanism.** Latency reviewer: heal by block. Provenance reviewer: heal per
  subscription via `bondingCurveKey`. They are complementary, not contradictory: per-subscription
  heal is cheap and bounded by watched mints, block heal is the correct fallback for global truth.
  Per-subscription first.
- **The feed abstraction.** Latency reviewer wants the trait to carry bytes (a session, not an
  event stream); provenance reviewer wants a normalized event boundary with a capability record.
  Both fit: session trait shipping frames, per-adapter decode, normalized enum with `Option` fields
  and per-adapter capabilities.
- **Suggested constants.** The statistics reviewer's numbers are priors, not measurements. They
  enter the doc labelled as priors with the reasoning, per the doc's own rule.

## What the review confirms is right

- §6.1, the single-source limit: a feed can report a disconnect but not a silent loss. Correct,
  and confirmed — no sequence numbers, no replay, no gap marker.
- §6.4: shadow-as-advisory, never sum two sources, no hot failover without measured disagreement.
- The rank store's contract: off hot path, never read for live alerts, allowed to lag, coverage
  partial rather than a thin ranking presented as the week. It is also the right home for the
  launch-window record.
- The delivery philosophy: per-family webhooks, counted drops, "a channel people mute is worth
  zero", priority governs survival not budget.
- The CTO framing: a curated social claim, never on-chain evidence, independent of trade-stream
  health.
- Signature dedup justified by the failure it prevents (a duplicate is a phantom spike);
  subscription generations; warmup suppression counted rather than hidden; "declare, don't guess"
  for long gaps.
- The latency table's honesty that Rust is the small term, and the refusal to present a threshold
  sweep as validation.
- Definitions travel with the claim on the card.

## Recommended revision order

1. **SCOPE.md revision 1 (unblocks M1):** B1 feed cost contract, B2 registry/lifecycle, B4 event
   time, B6 volume contract, H10 hot-path corrections, H3 watchdog/reconnect, H4 boot line and
   heartbeat. Also split M2 into M2a (delivery proven with synthetic alerts) and M2b (first
   statistics), and start the rank-store writer passively in M2.
2. **Revision 2 (unblocks M2 delivery):** B5 delivery contract, H11 mechanics, H1 arbitration,
   H5 restart semantics.
3. **Revision 3 (unblocks M4/M5):** B3 bundle data path and launch-record persistence, H7 bundle
   math, B7 ranking contract, H8 wallet semantics, H12 CTO source shape, H13 coverage.
4. Re-read the whole doc for contradictions (the §3 vs §2B conflict is one; "fees included",
   "fomo" in a heading, "holders") before M1 starts.

## Evidence limits

- PumpPortal publishes no trade or create schema. Field-level facts come from a captured raw
  payload and the working client — observed, not contracted. Log real messages in M1 and pin the
  schema in a test before the detectors depend on it.
- pump.fun trade and launch rates in the review are order-of-magnitude planning numbers, not
  measurements. The conclusions that scale with them are marked in the individual reports.
- Discord webhook limits are community-documented, not contractual; the observed burst is 5 per 2
  seconds per webhook. Measure against a staging webhook and record the numbers with a date.
- Helius credit prices and tier limits were read today and should be re-checked at build time.
- DexScreener's CTO list was fetched live today; its retention window is a sample of one, not a
  guarantee.
