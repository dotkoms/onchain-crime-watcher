# Feed subsystem — design

Builds M1's feed spine (`../SCOPE.md §9`). Revision 6 is the contract; defaults below are PRIOR.
M1a implements the bounded collector in `ocw-core` modules; `docs/m1a.md` records its supported
subset. The expanded policies and module sketch below remain M1c/M2+ design, not shipped features.

Related: `../SCOPE.md §4–7, §9`; `volume.md` (buckets); `delivery.md` (outages, heartbeat);
`ranking.md` (coverage); `bundle.md` (launch record); `sources.md` (limits); `operations.md`.

## 1. Module layout

```
crates/feed/src/
  lib.rs, event.rs, session.rs   types; FeedEvent; Session trait and wire-safe handles
  adapter/                       PumpPortal session; wire schema; decode; bounded metadata pool
  supervisor.rs, watchdog.rs, probe.rs  session loop, generation, backoff, ban; EWMA; 3-mint probe
  dedup.rs, heal/, shadow.rs     dedup set; per-curve and block heal; M2b probe
  metrics.rs, config.rs          counters and /health view; feed.* validation
crates/gate/src/
  lib.rs, policy.rs, ledger.rs, reconcile.rs, budget.rs  actor, admission, TTL, retries, cost
```

The gate is one actor with a bounded command channel; the adapter owns the socket and the registry
owns every per-mint allocation. Feed and gate hold fixed-size keys only; token metadata goes to a
bounded registry pool, never `FeedEvent`. Decode and detect share one stack frame: no trade channel;
channels after detection are `snapshot`, `delta`, or `event` (`../SCOPE.md §5`).

## 2. Session boundary and events

```rust
pub struct Generation(pub u64);   // +1 on every socket open
pub struct SubGen(pub u64);       // +1 on every subscribe/unsubscribe per mint
pub enum Source { Primary, Secondary }   // one per session; never derived from the payload
pub struct Frame { pub source: Source, pub generation: Generation, pub recv: Instant, pub bytes: Bytes }
pub enum SocketCmd {
    Subscribe { mint: [u8;32], curve: [u8;32], sub: SubGen },
    Unsubscribe { mint: [u8;32], sub: SubGen }, Close,
}
pub enum SessionExit {
    Cancelled, Closed { code: Option<u16>, reason: String }, Transport(String), Banned(String),
}
pub struct Capabilities { pub has_slot: bool, pub has_block_time: bool,
    pub has_curve_reserves: bool, pub has_dex_trades: bool,
    pub has_sol_amount: bool, pub has_tx_index: bool }
#[async_trait::async_trait]
pub trait Session: Send + 'static {
    fn name(&self) -> &'static str; fn capabilities(&self) -> Capabilities;
    async fn run(
        &mut self,
        generation: Generation,
        frames: mpsc::Sender<Frame>,          // bounded, class delta; never dropped
        commands: mpsc::Receiver<SocketCmd>, // bounded, class delta; gate backpressures
        cancel: CancellationToken,
    ) -> SessionExit;
}
pub enum FeedEvent {
    Create { mint: [u8;32], curve: [u8;32], creator: Option<[u8;32]>,
             signature: [u8;64], recv_ns: u64, generation: u64 },
    Migrate { mint: [u8;32], curve: [u8;32], pool: Option<[u8;32]>, venue: Option<Venue>,
              signature: [u8;64], recv_ns: u64, generation: u64 },
    Trade(Trade), AccountTrade(Trade),   // M5; same layout, account scope
}
pub struct Trade {
    pub mint: [u8;32], pub curve: [u8;32], pub trader: [u8;32], pub signature: [u8;64],
    pub token_amount: u64,      // raw units, 6 dp
    pub sol_lamports: u64,      // curve leg
    pub sol_source: SolSource,  // Payload | ReserveDelta
    pub side: Side, pub venue: Venue,
    pub recv_ns: u64,           // monotonic; never block time (R1)
    pub generation: u64,
    pub tx_index: Option<u16>, pub slot: Option<u64>, pub block_time: Option<i64>,
    pub reserve_sol: Option<u64>, pub reserve_tokens: Option<u64>,
    pub new_token_balance: Option<u64>, pub healed: bool,
}
```

The frame channel holds 1024 frames (PRIOR) and is `delta`: when full, the read loop stops and
TCP backpressure applies; stale frames are dropped and counted `feed.stale_generation_dropped`.
No field owns heap memory and `size_of::<Trade>()` is pinned under 320 bytes. `sol_lamports` comes
from `solAmount` if present, else the signed `vSolInBondingCurve` delta (`sol_source` records
which). `solAmount` is observed in the working client's payloads but is not guaranteed by schema
and must be treated as optional; when absent, derive the signed `vSolInBondingCurve` delta and
record `sol_source = ReserveDelta`. A secondary provider that decodes an instruction amount records
`sol_source = Instruction`; the detector sees one shape either way. PumpPortal M1 has no slot,
block time, or DEX trades, and has curve reserves; a Geyser-class secondary carries slot and
`blockTime`. Detectors branch on the capability record at wiring, never on the provider name.

## 3. Subscription gate

### 3.1 Predicate

On `Create` the gate requests a launch subscription with deadline `t0 + gate.launch_window_s`
(10 s, PRIOR). The window exists to capture the launch bundle (`bundle.md`); it is not a retention
test. Promotion into a retained slot at that deadline is provisional: without a slot the mint is
unsubscribed and its first minutes are never observed. Retention is decided at the ledger's `retain_at`,
`t0 + gate.retain_window_s` (300 s, PRIOR), on traded SOL over that window, summed from accepted,
deduplicated trades; a USD threshold uses the cached rate.
`retain = cto_listed(mint) || traded_sol_window >= gate.retain_min_sol` (3.0 SOL, PRIOR). That
3.0 SOL over 5 min is ~0.6 SOL/min and is a guess, not the old 10 s top-decile figure; it implies
keeping any mint that trades at that pace, which makes the 16 retained slots, not the threshold, the
binding cap. Revisit against the first week of counters. A slower riser is retained only by CTO (M3)
or a watch-set hit (M5); TTL then resets on any rolling 60 s with `>= 0.5 SOL (PRIOR)`. From M6 the gate is a planner plus
one ledger per source: the planner decides the desired set once and issues the same entry to both
ledgers, so a switchover does not resubscribe. Caps and refusals are counted per ledger; one
provider refusing a subscription does not change the other, and the desired set does not shrink
because the standby is unhealthy.

### 3.2 Caps and cap-hit behaviour

| Cap | Default | When hit |
|---|---|---|
| `gate.max_subs` total | 20 | No new subscription |
| retained slots | `max_subs − launch_slots` = 16 | Candidate needs score `>= min_retained × gate.evict_margin`; else refused |
| `gate.launch_slots` | 4 | Wait 2 s (PRIOR) for a slot, then skip, count `launch_skipped`, and mark the bundle `launch window missed` |
| `gate.feed_sol_per_day` | 0.5 SOL/day PRIOR | Total PumpPortal trade spend per UTC day, launch windows included, no exemption. At 0.5 it implies ~500,000 messages/day ≈ 5.8 msg/s sustained, so spend binds before the 20-slot cap. On local exhaustion: stop all paid mint and account subscriptions, refuse new paid commands, and recycle to a free-stream-only socket if unsubscribe cannot be established; see the durable spend rule below |

CTO-listed mints bypass admission, evicting the lowest-score non-CTO mint; if all retained are CTO,
the new CTO is refused and counted, and its card says coverage was capped. Every refusal is counted.

### 3.3 Daily spend enforcement

`gate.feed_sol_per_day` is a local allowance, not a provider-enforced billing cap. All received
billable messages debit it before normalization and dedup; duplicates and invalid payloads are not free.
Unknown billing classification is charged conservatively and reconciled with the provider.
Mint and account subscriptions share the allowance. Never multiply a deduplicated trade count by
price and call that the bill.

Persist a per-UTC-day spend ledger separately from prunable capture/rank files. Reserve message
allowances durably before enabling paid streams, in blocks of `gate.spend_reservation_messages`
(PRIOR 1,000). On crash, unused reserved messages remain charged locally. If the ledger is
missing after prior use, corrupt or unwritable, paid streams stay off pending operator
reconciliation; free create/migration streams may continue. A restart, reload or backward clock
step never resets the day's allowance.

Stop paid streams at allowance minus `gate.spend_stop_margin_messages` (PRIOR 10,000, 0.01 SOL
at the current published price). Cancel paid desired entries, unsubscribe **all** live mint and
account keys, block their resubscription and skip launch windows. If the provider has no usable
unsubscribe acknowledgement, close that socket immediately and reconnect with free streams only.
Account watchers cannot bypass the stop. Paid subscriptions may resume on a new UTC day only
after the new allowance is durably reserved and provider billing/credit health is acceptable.
Budget-induced blind intervals go into coverage; no wallet/volume alert implies uninterrupted
observation through them.

Frames already accepted upstream can still be billed during teardown. M1 measures that exposure
and confirms the margin under saturation and forced reconnects; without a provider contractual
bound there is **no guaranteed exact 0.5 SOL charge ceiling**. Health prints local reserved/received
spend, provider-reconciled spend when available, stop margin, overshoot and ledger state.
A strict monetary ceiling requires a verified provider-side limit or a separately bounded funded
API wallet; no automatic top-up is part of this bot.

### 3.4 Ledger, reconciliation, unsubscribe

One entry per mint: `{mint, curve, sub, kind, state, sent_at, last_msg, msgs, unsub_attempts}`,
where `kind` is `Launch{deadline}|Retained{retain_at, ttl_at}|None` and `state` is
`Unconfirmed|Live|UnsubPending|UnsubFailed`. `sub` increments on every subscribe and unsubscribe;
a heal snapshots it and discards its result if it changed. `Unconfirmed` is display, not retry:
a quiet mint sends no frame, so absence proves nothing.

Every 5 s (PRIOR): expire deadlines and TTLs; at `retain_at` keep the mint only if `cto_listed(mint)`
or traded SOL `>= gate.retain_min_sol`, else drop it from desired; `Subscribe` desired-but-unsent if
a cap allows;
`Unsubscribe` sent-but-not-desired with backoff; after `gate.unsub_retries`, set `UnsubFailed` and
**recycle the socket** — bump generation, close, reconnect, rebuild. Closing is the only guaranteed
unsubscribe without a vendor ack; a recycle is an outage (one identity, rings cold) and beats a
leak. No command bypasses the per-provider socket gate; a second admission to the same provider
sets `Failed` and counts `invariant_violations`. From M6 there are two providers and therefore two
sockets, one each; `feed.authoritative` decides which pipeline's output is live and the other is a
warm standby (`failover.md`).

## 4. Supervisor, watchdog, reconnect

### 4.1 Loop

```
Start:        admit socket; gen += 1; open; state = Connected
Connected:    frame <- frames (stale gen ? drop+count : decode+detect inline)
              cmd <- gate; exit <- session (classify); _ = cancel (close, flush)
              _ = watchdog: if suspected { probe_and_maybe_reconnect() }
Reconnecting: wait backoff + jitter; Start
Banned:       no socket, no probe; at banned_until, one Start
Failed:       no socket, health red; operator restart only
```

`Cancelled` is shutdown; `Transport`/`Closed` is transient. Three sessions under
`feed.ban.short_session_s` in 600 s (PRIOR), or a `Banned` exit, enters Ban;
`feed.restart.budget` transient exits in the same window enters Failed.

### 4.2 Backoff

Attempt 1 is 0 s (watchdog) or `feed.backoff.base_s` (transport), then 2, 4, 8, 16, capped by
`feed.backoff.cap_s`, with ±20% jitter (PRIOR); reset after `feed.healthy_reset_s` (120 s PRIOR).
Bans do not count.

### 4.3 Watchdog and probe

Liveness is the free create stream. On each create, `gap = now − last_create`; if
`gap < 60 s (PRIOR)`, update `ewma = α·gap + (1−α)·ewma` (`α` from config, initial 5 s PRIOR);
then `T = max(feed.watchdog.floor_s, feed.watchdog.multiplier·ewma)`. Transport liveness is
separate: pong within 45 s (PRIOR) or take the transport path. When `now − last_create > T` and
transport is alive, enter `Suspected` and probe 3 recent mints (PRIOR), one
`getSignaturesForAddress(curve, limit=1)` each. If ≥2 newest signatures (PRIOR) differ from
`last_seen_sig`, reconnect; if fewer, the market is quiet — clear suspicion, do not reconnect.
Two or more probe failures: stay, retry next tick, count `probe_unavailable`. Every reconnect is
probed first; after a transport failure the probe cannot cancel it, but still runs. A watchdog
reconnect that heals zero events counts `watchdog_false_positives`.

### 4.4 Ban

A provider-declared limit, or 3 consecutive sessions (PRIOR) dying under
`feed.ban.short_session_s`, enters `Banned`: no open, probe, or retry for `feed.ban.wait_s`; health
red, one outage identity, desired subscriptions unsent. At expiry try one session (first frame
within 10 s PRIOR); failure re-bans and counts `ban_retries`. A ban never drives `Failed`.

### 4.5 Reconnect sequence

1. Record `gap_start` (last create `recv_ns`, each mint's `last_seen_sig`).
2. Bump `Generation` before closing, invalidating in-flight frames.
3. `Close`; wait 2 s (PRIOR), then abort and count `close_forced`.
4. Release `sockets_open[source]`; open; first frame within 10 s (PRIOR), else fail.
5. Resubscribe every desired entry with a fresh `SubGen`.
6. Block-heal creates over the gap; curve-heal every retained mint.
7. Keep rings cold until re-warmed (`volume.md`); reset backoff after the healthy interval.

## 5. Dedup

```
K    = H128(signature ‖ mint ‖ trader ‖ side ‖ token_amount ‖ sol_lamports)
K_tx = H128(signature ‖ tx_index)     // used when tx_index exists
```

Signature alone is wrong: one transaction holds several trades, and amount plus side separates
them; two truly identical trades in one transaction are indistinguishable and one is lost (Open
questions). Structure: one-second `HashSet<u128>` shards dropped at `feed.dedup.window_s`; capacity
drops the oldest shard early and counts `dedup_evicted_early`, never refusing an insert, because
that turns a duplicate into a phantom spike.

```
capacity = ceil(1500 msg/s × 180 s × 1.5) = 405,000 keys        // PRIOR
```

405,000 keys at 24 B plus ~12 B of table overhead is ~36 B per entry, ≈ 14.6 MB, inside a 32 MB
budget; the window covers live/healed overlap plus resubscribe. Dedup runs before the registry; `dup_dropped` should be zero in steady state.

## 6. Gap heal

### 6.1 Triggers

| Trigger | Scope |
|---|---|
| Session close or reconnect | Every desired curve, plus block heal for missed creates |
| Watchdog reconnect | Probed and every desired curve |
| Hot mint silent beyond 60 s (PRIOR) | One curve, only if the probe shows newer activity |
Silence alone never fetches: a quiet token is not a gap.

### 6.2 Per-curve algorithm

1. Require a `last_seen_sig`; without one, declare unhealed `NoCursor`. Call
   `getSignaturesForAddress(curve, limit = 1000)` and page with `before` until the cursor is found
   or 1000 signatures (PRIOR) are read.
2. For each newer signature, `getTransaction` and decode with the same adapter; dedup drops
   anything seen live before the gap.
3. Emit healed trades with `slot`/`block_time` and `healed = true`. **They never enter live
   buckets**, only aggregate counters tagged with the gap id; block time is audit, never routing.
4. No baseline entry in M1: no shadow exists to agree with, so the mint re-warms live. Later,
   release per mint only on aligned shadow agreement within tolerance.
5. A prune or resubscribe mid-heal bumps `SubGen`; discard the result. On success record
   `gap_heals_ok`, recovered SOL, and the covered window.

### 6.3 Budget and declared unhealed

Per gap: `feed.heal.max_calls_per_gap` and 120 s (PRIOR). Global: `feed.heal.calls_per_min`,
`feed.heal.calls_per_day`, and at most two concurrent heals (PRIOR). Check before every call; the
last call cannot overshoot. When the daily cap is hit, count `heal_day_budget_hit` and declare the
gap unhealed (`CallBudget`).

```rust
struct UnhealedGap {
    mint: [u8;32], curve: [u8;32], from_ns: u64, to_ns: u64, sigs_seen: u32, sigs_read: u32,
    calls: u16, reason: HealStop,   // CallBudget | Duration | PageCap | NoCursor | RpcError
}
```

The window is `partial`; volume decisions are suppressed and counted `gap_unhealed`, and bundle,
ranking coverage and heartbeat state the gap. Never interpolate; call a missing cursor unhealed.

### 6.4 Block fallback

Creates missed while the socket was down have no cursor. For a socket gap only, `getBlocks` then
`getBlock(slot, "full")`, filtering to the pump.fun program; one call per slot and full blocks are
large, so cap at 150 slots and 200 calls (PRIOR). Beyond that declare `creates_unhealed` and state
the coverage gap. Per-curve heal runs first; block heal is the fallback (`../SCOPE.md §6.3`).

## 7. Shadow probe (M2b)

The shadow only disagrees. It never feeds a card and is never summed with the primary.
**Sampling.** Each aligned 10-minute window (PRIOR), after 60 s (PRIOR) of settling, sample
`feed.shadow.mints` gap-free mints, stratified by primary 10-minute SOL volume decile: 5 from the
bottom (PRIOR), the rest one or two each, rotated deterministically by `H(window_start, mint)`.
Exclude migrated mints; their curve key no longer carries the trades.

**Shadow count.** Paginate `getSignaturesForAddress(curve)` over the window and count signatures
whose `blockTime` is inside it; the primary count is accepted, deduplicated trades in the same
window. The shadow knows signatures, not side, size, or trader.

**Disagreement.** `d = |primary − shadow| / max(primary, 1)` per mint per window; report p50 and p90
per stratum and the signed median of `primary − shadow` (positive: suspected loss; negative:
duplicates or truncation). Never pool strata. A stratum p50 above `feed.shadow.tolerance` sets
`feed.unverified`, tags alert footers, and blocks a quiet-market conclusion, but not alerting.

| It can conclude | It cannot conclude |
|---|---|
| The primary is live at 10-minute resolution on sampled mints | Per-trade completeness or which trade was lost |
| Coarse loss above tolerance on curve trades | Selective loss by size, side, or trader |
| Whether silence may be called quiet | Unwatched or migrated mints |
| Aggregate error direction (loss vs duplicates) | Truth: RPC has its own lag and truncation |
| That two sources disagree enough to bar failover | Health between samples, or causality |

The metric assumes uniform loss within a mint and window. From M6 the same comparison runs against
the warm standby as a second series (`standby vs live`), because the RPC shadow and the standby
answer different questions: the sample reads chain state, the standby reads a second provider's
stream. They are reported separately and never pooled. Failover itself — the switch rules, the
warm-standby pipeline, per-source dedup and the license gate — is `failover.md`, not a loosening of
this section. A second provider is still one authoritative source per window; two sources are never
summed.

## 8. Config

All defaults PRIOR; units are in the key. Constants in prose are compiled-in priors.
`feed.secondary.*` and `feed.failover.*` are in `failover.md` §10; `capture.*` and `backtest.*`
are in `backtest.md` §7. They are not repeated here.

### feed.*
| Key | Unit | Default | Reasoning |
|---|---|---|---|
| feed.backoff.base_s | s | 1 PRIOR | First retry is cheap; the outage dominates the gap |
| feed.backoff.cap_s | s | 30 PRIOR | Keeps recovery inside the 30 s outage-declaration window |
| feed.restart.budget | sessions | 5 PRIOR | Survives transient churn, stops a crash-loop |
| feed.ban.short_session_s | s | 5 PRIOR | A handshake death, not a session that later failed |
| feed.ban.wait_s | s | 3600 PRIOR | The contract's "wait out the hour" |
| feed.healthy_reset_s | s | 120 PRIOR | Backoff resets only after this much healthy uptime |
| feed.watchdog.floor_s | s | 30 PRIOR | Above normal create gaps (~4 s) by a wide margin |
| feed.watchdog.multiplier | × | 6 PRIOR | Six EWMA gaps tolerates bursts without long blindness |
| feed.watchdog.ewma_alpha | fraction | 0.05 PRIOR | ~20-create memory; adapts to a slowing launch rate |
| feed.dedup.window_s | s | 180 PRIOR | Covers heal overlap and resubscribe; longer only costs RAM |
| feed.heal.max_calls_per_gap | calls | 1200 PRIOR | One signature page plus one tx per trade, ~2 min at 10 rps |
| feed.heal.calls_per_min | calls/min | 600 PRIOR | 10 rps, under a free-tier ceiling |
| feed.heal.calls_per_day | calls/day | 50000 PRIOR | Daily heal ceiling; validated ≤ `feed.heal.calls_per_min × 1440` |
| feed.shadow.mints | mints | 20 PRIOR | Contract sample; enough for per-stratum p50 |
| feed.shadow.tolerance | fraction | 0.05 PRIOR | A 5% median gap is detectable at this sample size |
### gate.*
| Key | Unit | Default | Reasoning |
|---|---|---|---|
| gate.launch_window_s | s | 10 PRIOR | Q3 bundle membership window; a capture deadline, not a retention test |
| gate.launch_slots | count | 4 PRIOR | 0.23 creates/s × 10 s is ~2.3; headroom for bursts |
| gate.max_subs | count | 20 PRIOR | 16 retained + 4 launch; bounds socket cost and ledger size |
| gate.retain_ttl_s | s | 1800 PRIOR | Low end of the idle band; cost scales with retention |
| gate.retain_window_s | s | 300 PRIOR | First-minutes volume window; long enough for a real signal, short enough to free 16 slots |
| gate.retain_min_sol | SOL | 3.0 PRIOR | Traded SOL over retain_window_s; ~0.6 SOL/min; unmeasured until M1 |
| gate.evict_margin | × | 1.5 PRIOR | Hysteresis: churn costs a cold ring and a resubscribe |
| gate.feed_sol_per_day | SOL/day | 0.5 PRIOR | ~$100–125 at $200–250/SOL; total spend per UTC day, launch windows included |
| gate.unsub_retries | attempts | 5 PRIOR | Then socket recycle is the only guaranteed unsubscribe |

`estimated_sol_per_day = msgs_per_s_ewma × 86400 / 10000 × 0.01` and
`sol_spent_today = msgs_charged_today / 10000 × 0.01`. Both are displayed; neither is a bill.
`gate.feed_sol_per_day` is validated in 0–200 SOL/day; values outside fail boot.

**Spend configuration.** `gate.spend_reservation_messages=1000` and
`gate.spend_stop_margin_messages=10000` are positive integers; the stop margin must be smaller
than the daily message allowance. `gate.spend_ledger_path=data/feed-spend.json` is boot-only,
0600, atomically replaced and synced, never pruned. Prices and billing units are dated provider
facts, not hidden constants.

## 9. `/health`

The boot line (`operations.md`) reads `feed.mode`, `feed.generation`, `feed.sockets_open` (per
provider) and `gate.subs_live`.

| Field | Meaning |
|---|---|
| feed.state / feed.mode / feed.unverified | Connected, Suspected, Probing, Reconnecting, Banned, Failed; `single-source` until M2b; true until shadow clears tolerance |
| feed.authoritative / feed.source_state{primary,secondary} / feed.sockets_open{primary,secondary} | Which provider's pipeline is live, each provider's state, each socket count 0/1 with total ≤2 (M6); before M6, `authoritative = primary` and the secondary is `unavailable` |
| feed.switches_total / feed.last_switch_ms / feed.switch_reason / feed.boundary_frames_dropped / feed.failover_cold_rings_total | Switchover history, reason class and its cost (M6) |
| feed.generation / feed.sockets_open / feed.session_uptime_s / feed.create_age_s / feed.trade_age_s / feed.watchdog_threshold_s | Socket identity, per-provider 0/1 invariant, uptime, event ages, current T |
| feed.msgs_in_total / feed.msgs_per_s_ewma / feed.events_per_sec / feed.events_accepted_total / feed.frames_dropped / feed.stale_generation_dropped | Messages received and accepted by type; must-be-zero drop / superseded frames |
| feed_lag, decode_us, detect_us, enqueue_us | Per-stage p50/p99/p999 with the measured stage rate; p999 is required (`../SCOPE.md §4`); `budget_wait_ms`, `http_us` and `total_ms` are delivery-owned |
| feed.dup_dropped / feed.dedup_entries / feed.dedup_evicted_early | Dedup outcome and size |
| feed.decode_errors_total / feed.field_missing_total / feed.decode_samples | Malformed and partial frames; first 5 redacted (PRIOR) |
| feed.reconnects_total / feed.watchdog_reconnects_total / feed.watchdog_false_positives / feed.probe_unavailable / feed.ban_active / feed.banned_until / feed.ban_retries | Reconnect, probe and ban accounting |
| feed.gap_heals_total / feed.gap_heals_ok / feed.gap_heal_unhealed / feed.gap_heal_calls_total / feed.heal_day_budget_hit / feed.unhealed_mints / feed.creates_unhealed_total | Heal outcomes, budget use, declared gaps |
| feed.shadow_p50 / feed.shadow_p90 / feed.blind_seconds_1h / feed.invariant_violations | Per stratum, never pooled; silence; must-be-zero |
| gate.subs_live / gate.subs_desired / gate.subs_by_kind / gate.subs_capped / gate.cost_capped_total / gate.launch_skipped_total / gate.budget_hit / gate.unsub_failed / gate.recycles_total | Sent, wanted, refusals, ledger failures, recycles |
| gate.msgs_charged_total / gate.sol_spent_today / gate.estimated_sol_per_day | Cost truth and projection |

A sample health read: `feed.msgs_per_s_ewma` 0.94, `gate.msgs_charged_total` 81,200,
`gate.sol_spent_today` 0.081, `gate.estimated_sol_per_day` 0.081; the sample satisfies
`msgs_charged / 10,000 × 0.01`.

`sockets_open[provider] > 1`, `sockets_open_total > 2`, `invariant_violations > 0`, `frames_dropped > 0`, or `gate.budget_hit` is
red; `feed.unverified` is amber in every card footer.

## 10. M1 acceptance tests
Test counts, durations and tolerances are acceptance priors; runtime defaults are §8 and the prose above.
| ID | Pass condition |
|---|---|
| F1 | Golden fixture: 100% of captured create/migrate/trade frames decode as expected; a field rename fails; unknown fields are tolerated and counted |
| F2 | Over 60 min, accepted events/min within ±5% of a simultaneous reference count (working client for messages, RPC signatures for five mints); rate and window recorded |
| F3 | 10 forced closes held 45 s each: 10 reconnects, 10 outage identities, `sockets_open[provider]` never >1, 0 duplicate trades after replay |
| F4 | Dedup and generations: a 10-minute capture replayed three times yields 0 accepted events after pass one, `dup_dropped` = 2 × accepted, and unchanged ring totals; 1k previous-generation frames after reconnect are 100% dropped |
| F5 | 24 h soak: `watchdog_false_positives ≤ 2`; every watchdog reconnect heals >0 events or is counted; no reconnect while the probe says quiet |
| F6 | 24 h at 0.5 creates/s: launch load is 5 concurrent per 10 s window against 4 slots, so skips are expected at ≈ 0.1/s (one per window on average) — `subs_live == min(subs_desired, max_subs) ≤ max_subs`, `launch_skipped` follows that rate and every skip marks the bundle `launch window missed`; no entry older than TTL + 2 reconcilers; `unsub_failed = 0`; every refusal counted |
| F7 | After warmup, 10k trade frames produce allocator delta 0; the create path stays inside its fixed metadata pool; measured rate stated |
| F8 | Injected 60 s gap over 10 mints with RPC truth: ≥90% of in-budget trades recovered; 100% of over-budget gaps declared; no budget exceeded; no healed event in a live bucket or baseline |
| F9 | Trades at t0+14.999 s and t0+15.001 s land in buckets 0 and 1; a +5 s wall-clock step moves nothing; a late event is counted and excluded from the baseline |
| F10 | 100 gate-passing creates in 10 min: `subs_live ≤ max_subs` always; CTO override admitted; every refusal counted; no subscription without a ledger entry |
| F11 | Three <5 s sessions enter Banned; zero sockets for 3600 s on a virtual clock; one retry after expiry; failure re-bans |
| F12 | 1M mutated frames: no panic, RSS bounded, decode-error count exact, no byte retried; cost accounting matches charges within 0.1% |

**Additional M1 spend acceptance:** active mint and account streams are stopped at the local
threshold; zero paid keys are resubscribed until reset. Kill during reservation, restart twice,
and step the wall clock backward: allowance never increases. Corrupt the ledger: free streams
run but paid streams remain off. Saturate teardown: measure all billable messages and report
actual provider overshoot; never claim the margin proves an upstream hard cap.

## 11. Open questions

| # | Question | Recommendation |
|---|---|---|
| 1 | Does any PumpPortal frame carry `solAmount`? Observed in the working client, absent from REVIEW B4's list. | Treat it as optional; use it when present, else the reserve delta; carry `sol_source`; pin both shapes in fixture F1. |
| 2 | Does PumpPortal acknowledge subscribe/unsubscribe? | Assume no. Sent is intent; after retries, recycle the socket. Measure in M1. |
| 3 | M1 acceptance counts N outage identities, not N outage cards; delivery posts one card per process lifetime. | Count N identities; with readback edit one card N times; without, one card plus N records. M1 asserts identities (`delivery.md`). |
| 4 | Full gate: FIFO, score, or no eviction? | Score with 1.5× hysteresis; CTO preempts. FIFO spends the cap on whoever launched first. |
| 5 | When does healed volume rejoin a baseline? | Not in M1. Later, per mint, only on aligned shadow agreement; otherwise re-warm. |
| 6 | Is block heal worth its cost beyond a minute? | No. Cap at 150 slots and declare create coverage. Geyser is the real answer (`sources.md`). |
| 7 | Do M5 account subscriptions consume gate caps? | Separate `gate.max_account_keys` (PRIOR 40) plus the same `gate.feed_sol_per_day` cap; an account trade alone does not retain a mint. |
| 8 | Two identical trades in one transaction share every key field. | Accept the single loss; add `tx_index` when available. A frame ordinal would defeat replay dedup. |
| 9 | Which secondary provider, and can its client ship under MIT? | Yellowstone client/proto directories are Apache-2.0; its server/default is AGPL. The standard client is eligible after pinned dependency-tree review (`failover.md` §14). |

