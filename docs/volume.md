# volume — anomaly detector spec

Implements [`../SCOPE.md`](../SCOPE.md) §2A. CONTRACT = fixed by SCOPE; PRIOR = a guess, with its reason, to be revisited against live counters. Buckets are receive-time (R1); signal math is SOL (R9); state is per-mint RAM in `registry/`, decisions in `detect/`. Delivery is in `delivery.md`, health in `operations.md`, gaps in `feed.md`.

The detector keeps nothing across restarts and observes nothing after an alert. It reports live cohort percentiles and fire counts; it cannot report hit rate, precision or recall, and no card may imply one.

## 1. Data structures

`registry::MintEntry` owns `volume: Option<Box<VolumeState>>`, allocated when the mint passes the trading gate, not on `create`. Only the feed task touches volume state; there is no volume channel. The adapter resolves `sol_source` before the detector runs (`feed.md` §2); the detector never branches on payload presence.

```rust
#[repr(C)]
struct Bucket {                 // one completed 15 s bucket
    vol_lamports: u64,          // raw notional, buys + sells
    top_taker_lamports: u64,    // largest per-taker contribution (upper bound, below)
    trades: u32,
    distinct_takers: u16,       // saturates at the slot count
    flags: u8,                  // PARTIAL_TRUNCATED | HEALED_UNVERIFIED | SOURCE_SECONDARY
    _pad: u8,
}                               // 24 B
struct TakerSlot { key: u64, vol_lamports: u64 }   // 16 B
struct PartialBucket {
    idx: u32,                   // since anchor; global bucket = admit_bucket_offset + idx
    trades: u32, vol_lamports: u64, distinct: u16, overflow: bool,
    takers: [TakerSlot; 32],    // 512 B
}                               // 536 B
struct VolumeState {
    anchor_t0: Instant,         // bucket 0 starts at the create recv time; migration resets it
    admit_bucket_offset: u32,   // first full bucket after the gate admits
    last_seen: Instant,
    state_seq: u32, completed: u32, last_fire_bucket: u32, cold_entries: u16,
    state: RegistryState,       // registry-owned; volume never increments state_seq
    regime: u8, flags: u8, _pad: u8,   // Curve|Pool
    ring: [Bucket; 30],         // 720 B; ring[(completed-1) % 30] is newest
    partial: PartialBucket,     // 536 B; never in the ring
}                               // ~1.33 KB
```

| Item | Bytes |
|---|---|
| Ring 30 × 24 | 720 |
| Partial bucket | 536 |
| Header | ~72 |
| Total per mint | ~1.33 KB (PRIOR: estimate; a `size_of` test pins it) |
| Map overhead ~+25% (PRIOR: open-addressed table with control byte) | ~1.65 KB effective |
| 10 000 live mints, all gated | ~16.5 MB |
| 10 000 live mints, 10% gated | ~1.65 MB plus ~0.9 MB of entries |
| Cohort cells, process-wide | ~15 KB |

PRIOR 32 taker slots: 5 distinct is the requirement; 32 satisfies it for 512 B. Beyond 32 the table becomes a bounded heavy-hitter summary and the top-taker figure an upper bound, which makes the gate stricter, never looser.

`partial.idx == completed`; the ring covers `[completed−30, completed−1]`, saturating at the start. Bucket zero is anchored at the create event, not at admission: `anchor_t0` is the create's `recv_mono`, and `admit_bucket_offset = ceil((admit_mono − anchor_t0) / 15 s)` is the first full bucket volume can observe. A trade maps to global `g = floor((recv_mono − anchor_t0) / 15 s)` and is stored at `idx = g − admit_bucket_offset`, so the first observed bucket is `idx = 0` and its deadline is `anchor_t0 + (admit_bucket_offset + 1) × Δ`; the remainder of the admission bucket is unobserved and is never counted as zero volume. The partial is frozen at close, evaluated, inserted, and never read by a baseline. Taker key is the first 8 bytes of `traderPublicKey` (PRIOR: 8 bytes is enough; random keys make collision risk at 32 entries negligible); insert sums `sol_lamports`; when full, replace the minimum slot and set `overflow`. Top share is `max(max_slot, min_slot) / vol` when overflowed, exact otherwise.

**Source tag.** `SOURCE_SECONDARY` marks a bucket built from the secondary provider (M6). A bucket
never spans sources: on switchover the outgoing partial is dropped (`failover.md` §4), so the flag is
constant within a bucket by construction. The flag exists because the invariant must be testable
and because a card may say which source produced a statistic; the detector itself never branches on
it.

## 2. Update order

All in the feed task. Closes come from a 100 ms ticker (PRIOR: 150× under Δ, cheap) over a deadline-ordered set, plus a synchronous close when a trade arrives ahead of the current bucket. At most 64 closes per tick (PRIOR: caps a stall; cost in §3.2).

**Per trade**

1. Registry lookup; drop if absent, `Dead`, or `state_seq` differs.
2. Dedup already ran in the feed layer (`feed.md`); duplicates never arrive.
3. `k = floor((recv_mono − anchor_t0) / Δ) − admit_bucket_offset`. If `k < partial.idx`: count `late_trades`, add to aggregates (bundle, rank), touch neither ring nor partial. Closed buckets are immutable.
4. If `k > partial.idx`: close `partial.idx .. k−1` in order, emitting zeros for empty intervals, then open `k`. Count `close_burst`; never skip a bucket.
5. Add to the partial: `vol += sol_lamports`, `trades += 1`, taker insert/merge.
6. Insert the trade's SOL size into the band trade histogram (§4). O(1).
7. `last_seen = recv_mono`. No decision, no allocation, no formatting.

**Per bucket close**

1. Freeze the partial into a local `Bucket`; immutable from here.
2. If `completed < 16`: no evaluation; count `warmup_suppressed` when `trades >= 6`; insert, advance.
3. If `HEALED_UNVERIFIED`: aggregates only, no ring, count `healed_excluded` (SCOPE §6.3).
4. `ring_stats` over the ring as it stands — the frozen bucket is **not** included.
5. Resolve band from the create-anchored bucket start, `(admit_bucket_offset + idx) × 15 s`, floor from the band median trade, cohort cell for `(regime, band)`.
6. `evaluate`: gates 1–4 run first; on the normal path the ratio is computed, inserted into the `(regime, band)` cohort cell, and gates 5–10 then read the post-insert distribution. Warmup, sparse, cold and healed closes insert nothing.
7. Emit the decision; enqueue an owned candidate for Fire, Near, ColdFire unless `play_hold_secs` suppresses it, counted `volume_play_hold_total`.
8. Insert the frozen bucket, overwrite the oldest after 30, `completed += 1`, zero the partial with `idx = completed`, set the next deadline.
9. Update §10 counters.

Debug assertions: `partial.idx == completed`; ring length `min(completed, 30)`; the partial index is absent from the ring; the newest ring entry is `completed−1`.

## 3. Decision

### 3.1 Signature

```rust
pub struct CohortCell { /* §4 histograms, generations; one (regime, band) cell */ }
pub struct VolumeInputs<'a> {
    pub ring: &'a [Bucket],        // len == min(completed, 30)
    pub completed: u32,
    pub frozen: &'a Bucket,        // the bucket just closed
    pub band: AgeBand,             // from the create-anchored bucket start age
    pub regime: Regime,            // Curve | Pool
}
pub enum Verdict { Fire, Near, Reject(RejectReason), ColdFire, ColdReject,
                   Suppressed(SuppressReason) }
pub enum RejectReason { Dispersion, Participation, Ratio }
pub enum SuppressReason { Warmup, SparseRing, SparseBucket, CohortCold, CohortGate,
                          HealedOnly, Stale }
pub struct VolumeDecision {
    pub verdict: Verdict,
    pub ratio: Option<f64>,          // None on the cold path
    pub current_sol: f64, pub baseline_sol: f64, pub floor_sol: f64,
    pub dispersion_floor_sol: f64, pub p90: Option<f64>,
    pub band: AgeBand, pub regime: Regime, pub cohort_n: u32,
    pub taker_share: f64, pub bucket_start_age_s: u32, pub gates: GateMask,
}
#[must_use]
pub fn evaluate(i: &VolumeInputs<'_>, cohort: &mut CohortCell, cfg: &VolumeCfg) -> VolumeDecision;
```

`evaluate` writes only the cohort cell it is given: gates 1–4 decide whether a ratio exists, the ratio is inserted into that cell before gate 10, and gates 5–10 read the post-insert distribution. The gate itself never writes. The caller enforces registry state. Candidates carry their ratio; `delivery.md` ranks within class and cuts at the budget. This detector never moves a threshold to fit a budget.

### 3.2 Arithmetic

Volumes accumulate as `u64` lamports — exact, no NaN, no float in the per-trade path. SOL appears once per close (`lamports as f64 / 1e9`). Ratios are `f64`; the card formats 2 dp. **No rounding before a comparison**: 2.9999 is near, 3.0000 is fire.

Ring statistics on every close, n ≤ 30:

1. Copy `vol_lamports` into `[u64; 30]`, `sort_unstable()` — integers, no NaN, ~200 comparisons.
2. `median_raw` = even n: `(a[n/2−1] + a[n/2]) as f64 / 2.0`; odd: `a[n/2] as f64`.
3. `mad_raw` = median of `|x − median_raw|`, same even-n rule.
4. `trimmed_mean_raw` = mean of the ascending array with the top 3 dropped; n ≥ 16, so ≥ 13 remain.
5. `nonempty` = count of `vol > 0`; `trades_total` = sum of `trades`.

MAD is not incremental: 30 values per 15 s per mint do not justify a drift-prone recurrence, and a full recompute is deterministic and testable. Target < 5 µs p99 per close (PRIOR: estimate from a 30-element sort plus one histogram increment; measured in M2b).

The floor is additive, so it does not enter the median, MAD or trim ranking:

```text
floor_lamports       = max(1_500_000_000, ceil(median_trade_lamports / 2))
                       // 1.5 SOL CONTRACT; 2·Δ/60·median = 0.5·median at Δ = 15 s
baseline_adj         = trimmed_mean_raw + floor
median_adj           = median_raw + floor
dispersion_margin    = max(4.0 × 1.4826 × mad_raw, 0.5 × median_adj)
dispersion_floor_adj = median_adj + dispersion_margin
current_adj          = frozen.vol_lamports + floor
ratio                = current_adj / baseline_adj
taker_share          = top_taker_lamports / frozen.vol_lamports
```

`ceil` is deliberate: truncation lowers the bar. The fallback term uses `median_adj`, the same floored median as `baseline_adj`: the dispersion test is in adjusted space, and a token at the floor must not be judged against a raw half-median that sits below the floor. The MAD term stays raw because deviations are unchanged by the additive floor and that branch cancels it on both sides. `baseline_adj >= 1.5 SOL`, so division is defined; the cold path handles a *raw* baseline below the floor, not a zero denominator. If `frozen.vol_lamports == 0`, `taker_share` is 0 and the bucket is counted `zero_notional` rather than divided.

### 3.3 Gate order

| # | Gate | Fail |
|---|---|---|
| 1 | `completed >= 16` | `Suppressed(Warmup)` |
| 2 | `nonempty >= 8` | `Suppressed(SparseRing)` |
| 3 | `frozen.trades >= 6` | `Suppressed(SparseBucket)` |
| 4 | `trimmed_mean_raw < floor` | cold path (§5) |
| 5 | `current_adj >= dispersion_floor_adj` | `Reject(Dispersion)` |
| 6 | `distinct >= 5` and `share <= 0.60` | `Reject(Participation)` |
| 7 | `ratio < 2.0` | `Reject(Ratio)` |
| 8 | `ratio < 3.0` | `Near` |
| 9 | `cohort.n < volume.cohort.min.samples` | `Suppressed(CohortCold)` |
| 10 | `ratio < p90` | `Suppressed(CohortGate)` |
| 11 | otherwise | `Fire` |

Interpretation: dispersion and participation gate **Near** too. A bucket one wallet printed, or one inside the token's own noise, is not a weaker claim; it is not a claim. Near skips only the cohort percentile, which is advisory on the card. A ratio ≥ 3 with a cold cohort band is suppressed rather than re-labelled near, because near would misstate the number. Both choices are §11 items. The cohort gate is read-only: the current ratio enters its cell before gate 10 reads it, so the p90 is over every evaluated ratio, and a `CohortGate` suppression stays in the distribution for later closes.

## 4. Cohort estimator

Five bands per regime (CONTRACT): `A 0–5 m, B 5–30 m, C 30 m–2 h, D 2–6 h, E >6 h`; two regimes → 10 cells. Per cell:

- `ratio_hist: [u32; 128]` over `[0.5, 32.0]`, log2-spaced, 3.3% relative resolution (PRIOR: finer
  than the ~5% sampling noise of a 200-sample p90; 512 B). Under/overflow counted; overflow shows as `>=32x`, never a precise multiple.
- `trade_hist: [u32; 64]` over `[0.01, 64]` SOL, 14.6% relative (PRIOR: feeds only the floor, which
  matters above 1.5 SOL; high-side clamping raises the floor).
- `samples: u32` per generation.

Chosen over t-digest and reservoir: the percentile is a gate with a stated bin width, so an integer histogram is sufficient, O(1) per insert, RNG-free (deterministic vectors), and rotates with a `fill(0)`.

Two generations per cell (PRIOR: two is the smallest rotation that bounds the window without per-sample timestamps), rotated every 1800 s (PRIOR: 30–60 min effective; longer mixes launch-day with quiet-night, shorter leaves bands under 200 samples). Query sums both with equal weight, so samples age out at 30–60 min with no per-sample timestamps. Below `n = 200` (PRIOR: ~15 min at 0.23 launches/s; fewer is sampling noise) the cell reports `p90: None` and fire is suppressed.

The p90 is the **upper edge of the bin holding the 90th percentile**; the card prints `n` and the window. It is a live percentile over what this process evaluated in the last 30–60 min — never a historical base rate, never "the top 10% of tokens", never an expectation of continuation.

Ratio samples enter before the percentile gate reads them, on the close that produced the ratio, tagged with band and regime; the cohort is the distribution of every evaluated ratio. Participation and dispersion failures **do** enter, and so does a ratio the cohort gate suppresses; filtering to survivors would define the p90 over alerts and make the gate circular. Cold, warming, sparse and healed buckets do not: their ratios are undefined or a different statistic. Trade sizes enter per trade, not per bucket.

## 5. Cold baseline

Trigger: gates 1–3 pass and `trimmed_mean_raw < floor`. The typical bucket is below the floor, so a fold-change is floor/floor and means nothing.

- Absolute gate: `frozen.vol_lamports >= 10 × floor` (CONTRACT) — 15.0 SOL in 15 s at the default.
- Same participation gate (≥ 5 takers, top ≤ 60%); trades ≥ 6 already holds.
- Dispersion and cohort do not apply; both need a baseline.
- Verdict `ColdFire` / `ColdReject`; `ratio = None`. Card: `cold baseline: no usable history
  (baseline 0.10 SOL < floor 1.50 SOL)`, `10x floor = 15.0 SOL`, observed SOL. Never a multiple. Carries FIRE plus the tag and consumes the volume budget.
- Exit: the frozen bucket is real and is inserted normally. At the first close with
  `trimmed_mean_raw >= floor` the token evaluates on the normal path; count `cold_exit`, log once, do not announce.
- Cold cooldown 900 s (PRIOR: high absolute gate, operator-repeatable; one card per 15 min).

## 6. Regime boundaries

Migration arrives on the free stream, so it is seen even for paid mints. In order:

1. Freeze and discard the in-flight partial as `PARTIAL_TRUNCATED`; a shorter interval would drag the new ring's mean down. Count `regime_partial_dropped`.
2. `regime = Pool`, `anchor_t0 = recv_mono`, `admit_bucket_offset = 0`, `completed = 0`, ring zeroed, `state_seq += 1`. Trades received before the event stay in aggregates only.
3. Cohort samples move to `Pool` cells; `Curve` cells stay and serve unmigrated tokens. The two are never compared.
4. Warmup and the play hold are different stages: 16 warmup buckets (240 s) suppress evaluation, while `play_hold_secs` (300 s) suppresses enqueue and is keyed by mint, so it survives the regime reset. No card appears in the new regime for 4 min, and none before the 5 min hold if the mint fired just before migration.
5. During warmup the card says `regime: pool — warming (n/16 buckets)`; count `regime_warmup_suppressed`. Afterwards: `regime: pool (PumpSwap)`, price from the pair source, labelled as such.
6. First seen mid-life: same reset; card says `age band unknown — first seen mid-life` and uses the launch record's create time if one exists, else band C (PRIOR: a middle band avoids claiming a 40-second history for a token that is hours old).
7. Eviction/re-entry allocates a cold ring, `completed = 0`, counts `mint_reentered_cold`, same warmup rule, tag `re-entered cold`.

The detector never reads curve or pool price. Volume is notional SOL on both; the split exists because the trade-size and ratio distributions differ sharply, not because the units do. `state` is the registry-owned `RegistryState` enum: volume reads it and never writes it, and never increments `state_seq` itself.

## 7. Worked example

Ring after 30 buckets (SOL): 6 × 1.0, 15 × 2.0, 6 × 4.0, 3 × 8.0. Band median trade 0.20 → floor 1.5. Each row below is a separate candidate bucket closed against this same ring.

| Step | Value |
|---|---|
| sorted | 1×6, 2×15, 4×6, 8×3 |
| median_raw | 15th,16th = **2.0** |
| deviations | 0×15, 1×6, 2×6, 6×3 → MAD = **0.5** |
| dispersion margin | max(4 × 1.4826 × 0.5, 0.5 × 3.5) = **2.9652** |
| dispersion floor (adj) | 3.5 + 2.9652 = **6.4652** |
| trimmed mean | drop 3 × 8.0 → 60/27 = **2.2222** |
| baseline_adj | 2.2222 + 1.5 = **3.7222** |
| cold? | 2.2222 ≥ 1.5 → normal path |

| Case | Current | Trades/takers/top | cur_adj | ratio | p90 (n) | Verdict |
|---|---|---|---|---|---|---|
| reject | 5.2 | 8 / 7 / 40% | 6.7 | 1.8000 | 2.10 (410) | `Reject(Ratio)`; dispersion 6.7 ≥ 6.4652 passes |
| near | 7.0 | 10 / 8 / 45% | 8.5 | 2.2836 | 2.10 (410) | `Near` (above p90, not required) |
| fire | 10.0 | 12 / 9 / 50% | 11.5 | 3.0896 | 2.10 (410) | `Fire` |

The reject moved 2.6× the median bucket and still failed: the floored baseline is the bar, not the raw ring mean. The near exceeds the live p90 and still posts, because percentile is advisory there.

## 8. Test vectors

| # | Scenario | Setup | Expected |
|---|---|---|---|
| 1 | Quiet token | 30 completed, 3 non-empty, current 0 trades | `Suppressed(SparseRing)`. Never Reject, never cold. |
| 2 | Steady token | 30 × 2.0, current 2.2, 9 trades / 7 takers / 30% | median 2, MAD 0, margin 1.75, threshold 5.25; cur_adj 3.7 → `Reject(Dispersion)`. Exercises MAD=0. |
| 3 | One-wallet fake | example ring, current 40.0, 12 trades, 1 taker 38.0 (95%) | `Reject(Participation)`; ratio 11.15 never reaches a card as fire. |
| 4 | Graduated mid-ring | completed 20, migration mid-bucket | partial dropped, ring zeroed; next 16 closes `Suppressed(Warmup)` tagged pool; `regime_reset` +1. |
| 5 | Zero-volume gaps | 10 × 0.0, 20 × 3.0, current 9.0, p90 2.90 | median 3, MAD 0, margin 2.25, threshold 6.75; trimmed 1.8889, baseline 3.3889, ratio 3.098 → `Fire`. Excluding zeros: baseline 4.5, ratio 2.333 → `Near`. Keep zeros. |
| 6a | Cold start, warming | completed 10, current 30.0 | `Suppressed(Warmup)`, `warmup_suppressed` +1. |
| 6b | Cold start, absolute | 30 × 0.1, current 16.0, 8 takers / 40% | `ColdFire` (0.1 < 1.5; 16.0 ≥ 15.0). At 14.9 → `ColdReject`. |
| 7 | Dispersion-only | 15 × 0.2, 15 × 4.0, current 9.0, 8 takers / 40%, p90 2.90 | median 2.1, MAD 1.9, threshold 14.8678; cur_adj 10.5 < 14.8678 while ratio 3.098 → `Reject(Dispersion)`. |
| 8 | Participation-only | example ring, current 30.0, 9 trades, 6 takers, top 21.0 (70%) | ratio 8.463, dispersion and p90 pass → `Reject(Participation)`. Exactly 60% passes. |

## 9. Config

All keys are `volume.`-prefixed (`volume.bucket.secs`, `volume.fire.ratio`, …); unknown keys fail boot (`config/`). Delivery owns per-kind expiry; there is no detector-side max-age alias. Boot rejects unknown legacy `deliver.budget_wait_max_ms` and `volume.card.max.age.secs` keys with the replacement field names. PRIOR rows move one at a time, with the expected counter movement stated first.

Suppression hierarchy: trade dedup (`feed.md`) → alert dedup (`delivery.md`) → cooldown (`delivery.md`). `play_hold_secs` is pre-queue suppression only and never paces delivery.

| Key | Unit | Default | Source | Rationale |
|---|---|---|---|---|
| `bucket.secs` | s | 15 | CONTRACT | Δ. |
| `ring.buckets` | count | 30 | CONTRACT | 7.5 min completed. |
| `trim.buckets` | count | 3 | CONTRACT | One prior spike cannot raise the bar. |
| `warmup.buckets` | count | 16 | CONTRACT | 4 min. |
| `min.nonempty.buckets` | count | 8 | CONTRACT | Zeros counted, not filled. |
| `min.trades.bucket` | count | 6 | CONTRACT | One trade is not a distribution. |
| `floor.base.sol` | SOL | 1.5 | CONTRACT | Absolute floor per 15 s. |
| `floor.scale` | × median/60 s | 2.0 | CONTRACT | `2·Δ/60·median_trade`. |
| `dispersion.sigma` | 1.4826·MAD multiple | 4.0 | CONTRACT | Scale test. |
| `dispersion.median.frac` | fraction of median | 0.5 | CONTRACT | Fallback when MAD=0. |
| `min.takers` | count | 5 | CONTRACT | Distinct participation. |
| `max.taker.share` | fraction | 0.60 | CONTRACT | One wallet cannot print a spike. |
| `fire.ratio` | multiple | 3.0 | CONTRACT | Fire cut. |
| `near.ratio` | multiple | 2.0 | CONTRACT | Near floor. |
| `cold.floor.mult` | × floor | 10.0 | CONTRACT | Absolute cold gate. |
| `cohort.bands.secs` | s upper edges | 300,1800,7200,21600 | CONTRACT | Five bands, last open. |
| `cohort.p90` | quantile | 0.90 | CONTRACT | Fire must reach the band p90. |
| `cohort.window.secs` | s | 1800 | PRIOR | 30–60 min effective; longer blends regimes. |
| `cohort.min.samples` | count | 200 | PRIOR | ~15 min at 0.23 launches/s; fewer is noise. |
| `cohort.ratio.bins` | count | 128 | PRIOR | 3.3% over [0.5,32]; finer is below noise. |
| `cohort.trade.bins` | count | 64 | PRIOR | 14.6% over [0.01,64] SOL; feeds only the floor. |
| `ratio.max` | multiple | 32.0 | PRIOR | Overflow shown `>=32x`, no false precision. |
| `taker.slots` | count | 32 | PRIOR | 512 B/mint; 5 distinct long before eviction. |
| `play_hold_secs` | s | 300 | PRIOR | Pre-queue suppression only, counted `volume_play_hold_total`; one play, one candidate. Delivery owns pacing: `deliver.volume.cooldown_fire_ms` 300,000, `deliver.volume.cooldown_near_ms` 1,800,000. |
| `cold.cooldown.secs` | s | 900 | PRIOR | Cold absolute spikes are rare and repeatable. |
| `ticker.ms` | ms | 100 | PRIOR | Close granularity, 150× under Δ. |
| `close.batch` | count/tick | 64 | PRIOR | Caps close work so a burst cannot stall decode. |
| `deliver.max_age.volume_fire_ms` / `deliver.max_age.volume_near_ms` | ms | 30000 / 15000 | PRIOR, delivery-owned | Detector passes original detection time; no duplicate volume age setting |

## 10. Counters and health

Process counters on `/health` and the heartbeat (`operations.md`).

| Counter | Meaning |
|---|---|
| `volume_eval_total`, `_fire_total`, `_near_total` | closes that decided; verdict counts |
| `volume_reject_{dispersion,participation,ratio}_total` | reject reasons |
| `volume_warmup_suppressed_total`, `_sparse_ring_total`, `_sparse_bucket_total` | suppressions |
| `volume_cohort_cold_total`, `_cohort_gate_total` | fire blocked by an unready or exceeded p90 |
| `volume_cold_{entries,fire,exit}_total` | cold path |
| `volume_regime_reset_total`, `_regime_warmup_suppressed_total` | migration |
| `volume_mint_reentered_cold_total`, `_late_trades_total`, `_healed_excluded_total`, `_play_hold_total` | lifecycle/late |
| `volume_ratio_overflow_total`, `_close_burst_total`, `_taker_overflow_total` | bounded-structure events |
| `volume_close_lag_ms` p50/p99, `volume_close_debt` | deadline → completion, with the measured rate |
| `volume_ring_bytes`, `volume_band_samples{regime,band}`, `_band_p90{regime,band}` | memory, cohort n and p90; a zero cell is a warning |
| `volume_floor_sol{band}` p50/p90, `_candidates_queued_total`, `_expired_total` | floor applied; handoff to `delivery.md` |
| `volume_fire_age_ms` p50/p99 | bucket close → card send, against budget wait |

`volume_state` is `normal`; `warming` when more than 50% of live gated mints have `completed < 16` (PRIOR: a restart should be visible); `degraded` when close-lag p99 exceeds 500 ms (PRIOR: a close sweep that slow is competing with decode). The heartbeat funnel carries seen → gated → warming → evaluated → fire/near → queued → sent. No counter here is a hit rate.

## 11. Open questions

1. **Near after a failed dispersion or participation gate.** Recommendation: reject (§3.3). SCOPE names them as fire requirements only; confirm before M2b.
2. **Fire with a cold cohort band.** Recommendation: suppress and count, never downgrade to near. Alternative: post as near with `cohort warming (n=…)`. Both honest; suppress is simpler.
3. **Floor base.** 1.5 SOL dominates until the band median exceeds 3 SOL. Recommendation: two weeks of live counters before touching it; if cold fires dominate, raise the cold multiple, not this.
4. **Cohort window.** 30–60 min may still blend a launch spike with a dead afternoon. Recommendation: keep 1800 s; read `volume_band_p90` against the card's own ratio before changing it.
5. **MAD = 0.** The dispersion requirement then collapses to `0.5 × median`; vector 2 is that case. Recommendation: keep 0.5 unless steady tokens never fire at plausible multiples.
6. **Late and healed trades.** SCOPE §6.3 excludes healed volume from baselines until the shadow agrees. Recommendation: keep it, count it, never blend silently.
7. **Card age.** Fire expires at 30 s and near at 15 s from detection, checked by delivery before every attempt. Measure age and survival by class before enabling production; change priors only with recorded evidence.

