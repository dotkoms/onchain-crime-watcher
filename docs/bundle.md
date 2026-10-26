# Bundle status and CTO subsystem

Status: design spec, 2026-10-25. Implements `SCOPE.md` revision 6 §2B, §7, Q3–Q5, R3–R5, R8 and
milestones M3/M4 of §9. The contract is [../SCOPE.md](../SCOPE.md); this file must not contradict
it. Every number marked **PRIOR** is invented and carries its rationale.

Sibling specs: `feed.md` (create stream, launch-window subscription, payload), `delivery.md` (`cto`
webhook, queue, message ids), `ranking.md` (shared side store, funder clustering), `sources.md`
(DexScreener, RPC), `operations.md` (config, `/health`, runbook).
## 0. Boundaries
This subsystem owns launch-window capture, the launch record, the funding stage, bundle math, the
tier state machine, the CTO poll and the card content. It does not own trade subscription policy
(`feed.md`), webhook budget and edit transport (`delivery.md`), store implementation
(`ranking.md`), provider endpoints (`sources.md`) or config plumbing (`operations.md`).

Buyer/funding fields below are logical record fields; revision 6 adds ordered window events.
The historical byte counts describe the buyer-only representation and must be remeasured with
versioned encoding and a bounded event count in M1c/M4. Overflow makes the history partial.

The launch record shares the bounded side store with rank aggregates: off the hot path, prunable,
allowed to lag, allowed to be partial. It is **never read on the volume alert path**; the CTO path
reads it under a 750 ms deadline and degrades to a stated unknown (§1.5).
## 1. Launch capture and the launch record
### 1.1 At `create` (t0)
`t0` is the monotonic receive time of the create event, not block time — the payload carries
neither timestamp nor slot (R1). The registry allocates `mint`, `creator`, `bonding_curve`
(`[u8;32]` each), `t0_mono_ms`, `t0_wall_ms` (`i64`), `init_v_sol_lamports`, `init_v_tokens_raw`
(`u64`) and `window_open`; the gate opens a per-mint trade subscription for the window.

Membership is **buys only**, receive time in `[t0, t0 + bundle.window_ms)`. The window filters when
the buy happened; clustering runs later, asynchronously, possibly weeks later (§2). A buy at
10.4 s is out by definition; the card never claims completeness.
### 1.2 At window close
Per distinct buyer, in first-buy order: `wallet`, `first_buy_off_ms` (from t0), `first_buy_wall_ms`,
`buy_sol_lamports`, `buy_tokens_raw` from its **first** buy, `bag_tokens_raw` from
`newTokenBalance` after that buy (exact, R4), `bag_source`, `sol_source`. Ordered buys and sells inside the window are retained with the record so later funding
classification can reconstruct the opening inventory. A missing trade interval makes sold share
partial; a launch buyer list alone is not a sales history. A capped, dropped or gapped subscription still writes a record, with
`window_truncated` set.
### 1.3 Records (integers LE, no strings, mint is `[u8;32]`)
**LaunchRecord** (once, ≤ 1 s after window close) — header **142 B**: `mint`32, `schema`u8 1,
`state`u8 1 (`captured`/`truncated`/`unobserved`), `t0_mono_ms`+`t0_wall_ms` 16,
`creator`+`bonding_curve` 64, `init_v_sol_lamports`+`init_v_tokens_raw` 16, `window_ms`u32 4,
`buyer_count`+`buyer_dropped` u16×2 4, `flags`u32 4 (`truncated`, `capped`, `gapped`,
`creator_is_buyer`, `twohop_not_followed`). Then **BuyerEntry, 72 B each**: `wallet`32,
`first_buy_off_ms`u32 4, `first_buy_wall_ms`i64 8, `buy_sol_lamports`8, `buy_tokens_raw`8,
`bag_tokens_raw`8, `bag_source`+`sol_source`+`reserved` 4. Max 142 + 256×72 = **18,574 B**; a
typical 40-buyer record is 3,022 B.

**FundingRecord** (once, when §2 completes) — 42 B: `mint`32, `schema`1, `funded_at_wall_ms`8,
`total_supply_raw`8, `decimals`1, `cluster_count`2, `funded_count`2, `excluded_count`2, `flags`4.
Then, per buyer in the same order: `funder`32, `hop`1, `cluster_id`2, `funding_sig`32,
`funding_block_time`8, `funding_lamports`8, `flags`1 = **84 B**.

**ProgressRecord** (rewritten on tier change) — 61 B: `mint`32, `tier`1, `pct_x10`2,
`first_sell_wall_ms`8, `observed_since_wall_ms`8, `updated_wall_ms`8, `flags`2.
### 1.4 Store, write path, pruning
`data/launch/YYYYMMDD.seg`, one segment per UTC day, `u32` length prefix, CRC32C per record, 64
records per zstd-3 block. One bounded channel (`bundle.write_channel`, PRIOR 4096) into a single
writer task; group commit every 250 ms or 64 records, `fsync` per commit. Boot rebuilds an in-RAM
index `mint -> (segment, offset)` (40 B/mint), the only read path. Prune when age >
`record_retention_days` (PRIOR 45) **or** size > `record_cap_bytes` (PRIOR 1 GiB), oldest whole day
first.

Sizing at the planning rate 0.23 launches/s (SCOPE §6.1, unmeasured): 19,870/day × 60% with a buyer
(PRIOR, unchecked) × ~1 KB compressed ≈ **12 MB/day, ~540 MB at 45 days**. If M1 measures more
launches the cap binds first, and the affected CTOs say `no launch record`.
### 1.5 Guarantees
| Event | Guarantee | Card |
|---|---|---|
| Normal close | durable within ~1 s (next commit) | none |
| Kill inside the commit window | ≤ 64 records lost, counted | `no launch record` |
| Channel full, disk full, IO error, torn tail | newest dropped or block truncated; counted; health amber | `no launch record` |
| Evicted by age or cap | pruned, counted | `no launch record` |
| Index lookup > 750 ms | post proceeds, edit queued | `launch record lookup timed out` |

No path converts a missing record into `0%`.
## 2. Funding stage
Runs on a CTO listing only, never per launch: same-funder is not in the trade stream, and the cost
model only works at CTO frequency (SCOPE §7). Starts in the background **after** the listing card
posts (§6) and produces edits.
### 2.1 Per-wallet sequence
For buyer `w` with first buy `b_w`:

1. `getSignaturesForAddress(w, {limit, before?})`, newest first, paging until
   `blockTime < b_w − 75 s`, or 2 pages, or 1,000 signatures per page.
2. Keep `blockTime ∈ [b_w − 75 s, b_w + 2 s]` (60 s rule + 15 s clock margin, PRIOR; feed lag is
   ≤ 0.5 s, so the margin is host clock skew). Cap at 4 reads.
3. `getTransaction(sig, {encoding: json, maxSupportedTransactionVersion: 0})`; collect inbound
   System Program transfers to `w`, top-level and inner.
4. Funder = source of the **largest** transfer in the window, ties by latest `blockTime`; require
   `≥ bundle.funding_min_lamports` (PRIOR 0.005 SOL — below that it is dust, not funding). None →
   `hop = 255`, buyer is not a member.
5. **One hop only if needed**: funder `F` not a launch buyer and not excluded → repeat 1–4 on `F`
   with reference `blockTime(F → w)`; accept `G → F` within 60 s before that transfer; `G` is the
   effective funder, `hop = 1`. No recursion. Two-hop is not followed and every card says
   `one-hop only`; omission biases the sold share downward if evasion wallets sell, and the card
   claims no completeness either way.
### 2.2 Batching, concurrency, budget
Neither method has a batch form; batching means JSON-RPC arrays where supported, credits counted
per call. PRIORS: 8 in-flight requests; 20 calls per array; **1,200 calls per CTO** hard cap; 25
calls/s; 8 s per call; **120 s** funding deadline. Cost: 80 wallets × ~5 + 50 funders × ~3 ≈ 550
typical, ~650 with one-hop. A 60-buyer CTO is ≈ 280 calls, ~12 s at the 25/s ceiling with arrays.
Estimated ~1,000 credits per 80-wallet CTO (~11/wallet, REVIEW B3; re-check at build) — reported,
never presented as exact.
### 2.3 Exclusions
`data/exclusions.toml`, boot-loaded, base58-validated, hashed into the config hash; a bad line
fails fast naming it. Entries: `address`, `label`, `kind` (`exchange|router|program|bridge|
known_operator`), `added`, `source`; cap 200.

An excluded address is never a funder, root or intermediate. A buyer whose only funder is excluded
is not clustered; its buy stays in the launch-buyer count and out of the bundle denominator. The
card adds `N funded from exchanges/routers, not clustered`; that alone does not set `partial`,
because a withdrawal is not evidence of a bundle.
### 2.4 Fan-out
`fanout(F)` = distinct launch buyers whose effective funder is `F`, one-hop grandchildren counted
against the root. Above `bundle.fanout_cap` (PRIOR 50 in a 10 s window — far above a hand-run
bundle, far below a CEX or router) `F` is shared infrastructure: children removed, `fanout_capped`
set, bundle `partial`, card prints `N wallets excluded (shared funder)`. The cap is unvalidated;
log the distribution per CTO. `rank.funder_fanout_cap` is also 50 but bounds identity clustering
over ranked mints; `bundle.fanout_cap` bounds launch-window bundle membership. The scopes differ.
Union-find over effective funders; a cluster of one is kept but cannot meet the wallet minimum.
### 2.5 Cache
Key `wallet` → up to 8 inbound transfers `{block_time, from, lamports, sig}` for the last 30 min;
TTL 900 s; cap 10,000 wallets; LRU. ~500 B/entry → **~5 MB**. The key is the wallet, not
`(wallet, buy_time)`: the raw signature window is re-filtered per lookup, and
`(wallet, reference_time)` is memoised within a run so a shared funder costs one fetch.
## 3. Bundle math

`B` = members after exclusion and fan-out. Revision 6 replaces sale-proceeds / purchase-spend:
price appreciation cannot turn a small sale into a fully sold bag.

| Quantity | Formula | Note |
|---|---|---|
| Opening bag | `q0_w = newTokenBalance` after first window buy | Require known opening inventory; unknown prior holdings make the basis partial |
| Weight | `c0_w = first_buy_sol_lamports` | Fixed original SOL weight, never revalued at sale price |
| Sold fraction | `f_w = allocated_original_tokens_sold / q0_w` | Raw token units, in [0,1] |
| Percent sold | `100 × Σ(c0_w × f_w) / Σ(c0_w)` | SOL-weighted token fractions; use exact integer/rational comparisons before display rounding |
| Sale proceeds | `Σ observed sell SOL` | Separate informational field, never the tier denominator |
| Wallet share | fully out / member count | Current balance ≤1% of opening bag, with complete inventory observation |
| Supply share | `100 × Σ q0_w / total_supply_raw` | Raw/raw |
| Gate | ≥5 members, ≥10 SOL original cost, ≥2% supply | All before a tier word |

Track current inventory and remaining original-bag units per member. Later buys add inventory
without increasing the original bag or its weight. A sale allocates to original-bag units in
proportion to their share of pre-sale inventory, with the same integer rounding and final-sale
dust removal as `ranking.md` §2. This is an explicit fungible-inventory accounting convention,
not an assertion about which physical token was sold. Never count the same original units twice.

A transfer mismatch or missing sales interval marks the fraction partial. Only the fraction
supported by observed sales may be shown as a lower bound; do not infer a sale from a balance
fall or print CLEARED on incomplete history. Late CTO funding classification needs the ordered
history from opening to observation; if it was not retained, the card says
`sold share unavailable — history incomplete`. The launch record alone cannot recover it.
CLEARED also requires current holdings ≤5% of the original bags; later accumulation is shown and
can withhold the word even when most original-bag units were disposed of.

### 3.1 Worked examples

Six members each buy an opening bag for 2 SOL, together ≥2% of supply. Original cost is 12 SOL.
If each sells 20% of its tokens at 5× its entry price, proceeds total 12 SOL but the sold share is
**20%**, with no tier. If each sells 80%, the share is **80%**, FIRE, regardless of sale price.
At 95%, with ≤5% remaining, complete history and the price context in §5, the state is CLEARED.

A wallet buying 100 tokens for 10 SOL, selling 50, adding 50 and selling 90 has 5 original-bag
units remaining under the proportional convention (95% disposed) but 10 current tokens held.
The current-holdings guard withholds CLEARED; the remaining and newly accumulated inventory are
shown. Transferring 60 and selling 30 never becomes 90% sold.

## 4. Tier state machine
States `NONE`, `NEAR` (≥ 60), `FIRE` (≥ 80), `CLEARED` (≥ 95), plus flags `STALE`, `PARTIAL`,
`NO_TARGET`.

| From → To | Trigger | Action |
|---|---|---|
| NONE → NEAR | `pct_raw ∈ [60, 80)` | record; no edit by default (`bundle.near_edits = false`), no post |
| NONE/NEAR → FIRE | `≥ 80` | post if no card and no restored high-water; else edit |
| FIRE → CLEARED | `≥ 95` | edit; no id → post and count `escalation_without_target` |
| NEAR → CLEARED | first read ≥ 95 | post at CLEARED, skipped tier coalesced |
| any → same/lower | fall or stale read | no tier change; stale sets `card_state.stale`, re-render under the 45 s rule |
| any → lower tier | impossible | rejected in the state setter; `monotonic_violations` must stay 0 |

Edits: at most one per message per `deliver.edit.min_interval_ms` = 45,000 (contract band 30–60 s);
inside the interval `pending_tier = max(pending, observed)` and the elapsed edit carries the latest
tier, so three jumps in 20 s are one PATCH. Edits share the `cto` queue and bucket (`delivery.md`), with
priority `bundle edit` below `CTO listing`, and rebuild the whole block from state. A 404/10008 on
PATCH makes delivery drop the id and sets `NO_TARGET`.

Restart: `ProgressRecord` restores the high-water. Bundle keeps `{mint, tier_high_water,
posted_at}` only and never stores a message id; delivery owns the single id store (R5), so a
restored tier has no target. FIRE restored + CLEARED read → post a card marked `post-restart` and count
`escalation_without_target` (a duplicate is the accepted cost of not dropping an escalation; same
rule as `delivery.md`). Already CLEARED → nothing, `tier_restored_no_target`. Sell history is not
persisted; `observed_since_wall_ms` becomes boot time and the card prints `observed since <time>`.
## 5. Price context
| Field | Source | Rule |
|---|---|---|
| `p_first` | trade event of the first bundle sell | curve price **after** that trade |
| `p_now` | latest trade event (curve) or DexScreener pair (pool) | same formula / `priceNative` |
| `p_peak_1h` | max over `peak_window_s` = 3600 of the same series | trade resolution on curve, poll resolution in pool, stated |
| `change` | `(p_now − p_first)/p_first` | shown with the trough from `p_first` |
| `dumped` | classification | below |

Curve price per whole token: `p = (v_sol_lamports / v_tokens_raw) × 10^(decimals − 9)` SOL;
`decimals` is read once in the funding stage. Reserves are exact at the trade's moment (SCOPE §7);
the post-trade state is labelled `at execution`, not a fill price.

`dumped = true` if `max_drawdown ≤ −bundle.dumped_pct` (PRIOR 30%, SCOPE) — a token that fell 30%
and recovered still dumped during the exit window. `false` if the trough never breached it.
`unknown` if an endpoint is missing or the regime changed.

Regime rule: if `p_first` is on curve and `p_now` is a pool price, show both with regime stamps and
print `dumped: unknown (regime change)`; the two are never differenced into one series (SCOPE
§2A). `CLEARED` is never printed without the context: a missing `p_first`, `p_now` or `p_peak_1h`,
or a regime change, holds the word at `FIRE` and prints `cleared withheld — price context
unavailable` (SCOPE §2B). Unavailable wording: no bundle sell → `price: no bundle sell yet`;
first sell before observation → `price: at first sell unavailable — sell before observation`; flat
hour → `1h peak unavailable (no series)`; missing pair → `price: pool price unavailable`; only
`priceUsd` → convert with cached SOL/USD and append `(usd-converted, rate 42s old)`; no rate → omit
USD, keep SOL.

**Reconciliation (balances only).** First bundle read and every `reconcile_interval_s` (PRIOR 900)
while tracked: batched `getMultipleAccounts` over member token accounts, compare `expected = buys_raw − sells_raw` with the balance. `actual ≥ expected` → no gap (later buys explain a surplus).
`actual < expected` → unobserved sells **or** transfers out; indistinguishable from one read. If
the gap exceeds 5% of `Σ bag_w` (PRIOR) the bundle is `partial` and prints `holdings below observed
sales — sells or transfers outside observation`, plus `transfers out uncounted`. Balances never
replace the trade-derived headline (R4).
## 6. CTO ingestion
**Poll.** `GET https://api.dexscreener.com/community-takeovers/latest/v1`, unauthenticated,
~60 req/min, no parameters. One in-flight request, 5 s timeout, every `poll_interval_s` (PRIOR 60).
429/5xx/timeout → jittered exponential backoff to 600 s. This poll is the `cto_source` subsystem.

**Parse and filter.** Entries need `tokenAddress` and `chainId`, else `malformed_entries`. Keep
`chainId == "solana"` (case-insensitive exact) — all else `filtered_chain`. The address must be
base58 of 32 bytes, else `invalid_address`. `claimDate` accepts RFC3339 or epoch s/ms →
`claim_date_ms`; unparsable falls back to `first_seen_wall_ms` for ordering, is excluded from the
high-water, and counts `claimdate_parse_errors`. A claim dated more than 900 s in the future is
accepted and clamped (`future_skew_clamped`).

**Diff and latch.** Seen-set keyed on `tokenAddress`: RAM, cap 20,000, TTL 30 days, with its posted
tokens mirrored to disk below — bundle's seen-set is authoritative across restarts, and delivery's
dedup is the second line. Latch keyed on mint: cap 10,000, holds `{tier_high_water, posted_at}`; a
latched mint is edited, never reposted. Bundle stores no message id. Duplicate `tokenAddress` in
one response is one post. After boot, entries above the high-water are new; entries first seen below
it and absent from the posted set are counted `suppressed_by_high_water` and are not recovered.

**High-water and posted set.** `data/cto_state.json` persists both:
`{claim_date_ms, posted_tokens, updated_wall_ms, poll_count}`, where `posted_tokens` is the bounded
posted-token set (cap 20,000, TTL 30 days, matching the seen-set). Written at most once per poll
when the maximum parsed `claimDate` advances or a token is posted, atomically (temp, fsync, rename,
dir fsync). A write failure keeps the RAM value and retries. Boot with a valid file → prime from
both without alerting; the first poll is an ordinary diff above the high-water (SCOPE §2B, Q5).
Absent or corrupt → the first poll **primes silently**, posts nothing, counts `primed_from_empty`
(SCOPE §6.3). Neither path replays the list, and no claim can be guaranteed: a claim first seen
below the high-water is counted and not recovered.

**Removal and flapping.** The list has no flag and no documented withdrawal. Disappearance is not a
signal: no edit, delete or retraction. Reappearance does not repost (seen-set + latch) and counts
`reappeared`. Six consecutive empty polls (`empty_polls_degraded`) marks `cto_source` degraded; an
empty list never implies a quiet market.

**Order.** Diff → latch → post the listing card at once with `Bundle launch record found: N launch
buyers — funding lookup in progress`, or the §7 wording when there is none → funding runs async
(§2) → first bundle edit on completion → later edits under §4. The post never waits on funding.

**Origin.** `pumpfun` if the create was observed, or if the bonding-curve PDA derived from the mint
and the pump.fun program exists (`getAccountInfo`, 1 call, 3 s). Otherwise `non_pumpfun`; a probe
failure stays `origin unknown`, never `pumpfun`. Non-pump.fun Solana claims post with `bundle:
unavailable — not a pump.fun launch` and `price: unavailable`, and never get a tier.

**Wording, fixed:** `Community takeover claim (DexScreener list). Someone has claimed this token;
the listing certifies a claim, not ownership, holders or on-chain control.`
## 7. Unknown, partial, unavailable
The card prints one of these instead of a number. No exceptions.

| Case | Text | Counter |
|---|---|---|
| No launch record | `bundle: no launch record` | `no_launch_record` |
| Record, window unobserved (gate skipped or capped at t0) | `bundle: launch window not observed` | `window_unobserved` |
| Window truncated or gapped | `partial: launch window incomplete` | `window_truncated` |
| Funding running | `bundle: funding lookup in progress` | `funding_pending` |
| Funding deadline hit | `partial: funding incomplete (N of M wallets)` | `budget_exhausted` |
| Fan-out cap hit | `partial: N wallets excluded (shared funder)` | `fanout_capped` |
| One wallet's RPC fails | omit it; `partial` if any omitted | `lookup_failures` |
| Below minimums | `below minimums (N wallets · S SOL · P% supply)` | `min_gate_rejects` |
| No sell yet, or first sell unobserved | `price: no bundle sell yet` / `at first sell unavailable` | `first_sell_unobserved` |
| Regime changed | `dumped: unknown (regime change)` | `mixed_regime` |
| Pool pair missing | `price: pool price unavailable` | `pool_price_missing` |
| Balance gap | `holdings below observed sales` + `partial` | `reconciliation_gap` |
| Non-pump.fun, probe failed | `unavailable — not a pump.fun launch` / `origin unknown` | `non_pumpfun`, `origin_probe_failed` |
| Tracker dropped, stale | `sold: not observed since <time>` / `as of <age>`, tier held | `tracker_dropped`, `stale_reads` |

Two-hop funding is a standing disclosure, not an unknown: every bundle block ends `funding:
one-hop only`.
## 8. Config
Units sit in the key or the comment; `delivery.md` owns `deliver.cto.hourly_cap` (20),
`deliver.cto.daily` (120), `deliver.edit.min_interval_ms` (45,000) and `deliver.wallets.*`.

| Key | Unit | PRIOR | Rationale |
|---|---|---|---|
| `bundle.window_ms` | ms | 10,000 | Must equal `gate.launch_window_s`; boot fails on mismatch |
| `bundle.funding_max_age_s` / `funding_clock_margin_s` | s | 60 / 15 | SCOPE Q3; host clock skew, feed lag ≤ 0.5 s |
| `bundle.funding_min_lamports` | lamports | 5,000,000 | 0.005 SOL; below it, dust |
| `bundle.signatures_limit` / `max_pages` / `tx_reads_max_per_wallet` | count | 1,000 / 2 / 4 | Provider max; 2,000 back; only window-adjacent reads |
| `bundle.min_wallets` / `min_sol` / `min_supply_pct` | wallets / SOL / % | 5 / 10.0 / 2.0 | SCOPE Q4 |
| `bundle.tier_near` / `tier_fire` / `tier_cleared` | % | 60 / 80 / 95 | SCOPE Q4 |
| `bundle.near_edits` | bool | false | SCOPE default; no edit on the least actionable tier |
| `bundle.fanout_cap` | children | 50 | Above a hand bundle, below a CEX; unvalidated |
| `bundle.rpc_concurrency` / `batch_max` / `calls_per_cto` / `calls_per_sec` | requests / calls / calls / calls/s | 8 / 20 / 1,200 / 25 | Provider-friendly; 2× the estimate; below a paid-tier ceiling |
| `bundle.rpc_timeout_ms` / `funding_deadline_ms` / `lookup_deadline_ms` | ms | 8,000 / 120,000 / 750 | Above RPC p99; edit to partial rather than wait |
| `bundle.cache_wallets` / `cache_ttl_s` / `buyers_cap` / `write_channel` | wallets / s / buyers / records | 10,000 / 900 / 256 / 4,096 | ~5 MB cache; record bound; ~2 s of launches |
| `bundle.record_retention_days` / `record_cap_bytes` | days / bytes | 45 / 1 GiB | "Weeks later"; ~540 MB at the planning rate |
| `bundle.stale_after_s` | s | 300 | Two bucket rings of silence |
| `bundle.trackers_max` / `tracker_ttl_s` | mints / s | 32 / 604,800 | ~4 CTOs/day; a week of tracking |
| `bundle.reconcile_interval_s` / `peak_window_s` / `dumped_pct` | s / s / % | 900 / 3,600 / 30 | Balances reconcile only; "1 h peak"; SCOPE |
| `cto.poll_interval_s` / `http_timeout_ms` / `poll_backoff_max_s` | s / ms / s | 60 / 5,000 / 600 | SCOPE; small body; longer than list change rate |
| `cto.seen_cap` / `seen_ttl_days` / `latch_cap` | mints / days / mints | 20,000 / 30 / 10,000 | ~120 real claims in 30 days; in-RAM latch |
| `cto.claim_future_skew_s` / `empty_polls_degraded` / `origin_probe` | s / polls / bool | 900 / 6 / true | Vendor skew; 6 min is not quiet; 1 call separates pump.fun |
## 9. `/health`
- `cto`: `state`, `last_ok_wall_ms`, `polls_total`, `polls_failed`, `http_429`, `poll_latency_ms{p50,p95}`, `high_water_claim_date` (ISO, so a stuck value is visible), `high_water_write_failures`, `empty_streak`, `primed_from_empty`, `suppressed_by_high_water`; claims: `entries_seen`, `filtered_chain`, `invalid_address`, `claimdate_parse_errors`, `future_skew_clamped`, `new_claims`, `posted`, `duplicates_suppressed`, `reappeared`, `latched`, `non_pumpfun`, `origin_probe_failed`.
- `bundle` records: `records_written`, `records_dropped`, `records_pruned`, `record_store_bytes`, `record_tail_corrupt`, `record_write_errors`, `record_lookups`, `record_hits`, `record_misses`, `lookup_timeouts`.
- `bundle` funding: `runs`, `wallets_looked_up`, `rpc_calls`, `rpc_credits_estimate` (labelled estimate), `budget_exhausted`, `lookup_failures`, `fanout_capped_wallets`, `excluded_funder_wallets`, `cache_hits`, `cache_misses`, `cache_evictions`, `deadline_hits`.
- `bundle` state: `trackers_live`, `trackers_dropped`, `tier{near,fire,cleared}`, `edits_sent`, `edits_coalesced`, `edits_rate_limited`, `escalation_without_target`, `tier_restored_no_target`, `monotonic_violations` (must be 0), `stale_reads`, `clamp_hits`, `min_gate_rejects`, `partial_records`, `reconciliations`, `gap_wallets`, `gap_tokens`.
## 10. Acceptance tests
### M3 — CTO
| # | Setup | Assert |
|---|---|---|
| M3.1 | Fixture: 3 Solana + 2 other chains | 3 posts, `filtered_chain = 2` |
| M3.2 | Same fixture on the next poll, with a duplicate `tokenAddress` in one response | 0 posts, `duplicates_suppressed = 3`, `latched = 1` |
| M3.3 | Restart with valid high-water, then one claim above it | 0 boot posts, exactly 1 new post |
| M3.4 | `cto_state.json` deleted | 0 posts, `primed_from_empty = 1`, high-water written |
| M3.5 | Entry removed at poll N, reinserted at N+3 | no edit, no repost, `reappeared = 1` |
| M3.6 | Non-pump.fun Solana claim | `unavailable — not a pump.fun launch`, `price: unavailable`, no tier |
| M3.7 | Upstream 500 × 3, then recovery | `degraded`, no "quiet market" text, outage identity stable |
| M3.8 | `claimDate` 20 min future / unparsable | clamped and counted / posted on `first_seen`, high-water not advanced |
### M4 — Bundle
| # | Setup | Assert |
|---|---|---|
| M4.1 | 6 buyers funded by one wallet, buys 0.2–9.1 s | exactly 6 members, supply 3.46% |
| M4.2 | One buy at 10.4 s | 5 members, excluded, no error |
| M4.3 | Funding 59 s vs 61 s before buy | included vs `unknown_funding` |
| M4.4 | Funder with 51 children | children excluded, `partial`, `fanout_capped = 1` |
| M4.5 | `G → F → w`, each leg within 60 s | one-hop member, card ends `one-hop only` |
| M4.6 | `G → X → Y → w` | not a member, no recursion, note unchanged |
| M4.7 | Funder in `exclusions.toml` | not clustered, counted, not `partial` for that alone |
| M4.8 | Six equal-cost buyers each sell 20% at 5× price | Proceeds equal original spend; sold share 20%, no FIRE/CLEARED |
| M4.9 | Same members sell 80%, then 95% with complete history and ≤5% holdings | FIRE then CLEARED, independent of exit price; no fraction over 100% |
| M4.10 | 3 tier jumps inside 20 s | 1 PATCH, `edits_coalesced = 2` |
| M4.11 | Percent falls after FIRE; stale read | no edit, tier held, `stale_reads ≥ 1` |
| M4.12 | Restart at FIRE, next read 96% | new card, `escalation_without_target = 1` |
| M4.13 | Restart at CLEARED, next read 97% | no post, `tier_restored_no_target = 1` |
| M4.14 | No launch record | exact `bundle: no launch record`; no `0%` in the payload |
| M4.15 | 4 wallets / 9.4 SOL / 1.8% | no tier word, exact minimums string, no edit opened |
| M4.16 | Price −20% vs −35% trough | `not dumped` vs `dumped`; trough, not endpoint |
| M4.17 | First sell curve, now pool | both prices regime-stamped, `dumped: unknown (regime change)` |
| M4.18 | Budget exhausted at 1,200 calls | edit within deadline + 5 s, `partial: funding incomplete` |
| M4.19 | Balance 8% below expected | `partial`, gap text, `transfers out uncounted` |
| M4.20 | 2,000 launch records with ordered window events | Versioned records round-trip and obey measured record/event caps; original buyer-only byte sizing is not a limit for the amended schema |
| M4.21 | Segment pruned under the cap | lookup miss, `no launch record`, `records_pruned > 0` |
| M4.22 | Transfer 60%, sell 30%; or missing sales interval | No invented 90% sold; partial/unavailable, never CLEARED |
| M4.23 | Sell, add, sell using §3 convention | Original-unit disposal and current inventory tracked separately; current-holdings guard may withhold CLEARED |

## 11. Open questions
1. **`solAmount` and the first trade.** `feed.md` already normalises to `sol_lamports` + `sol_source` (`Payload` / `ReserveDelta`); `solAmount` is observed but not guaranteed, so the `ReserveDelta` fallback is part of the contract. Remaining: the first trade on a mint has no prior reserve snapshot, so it takes the create payload; pin that and both payload shapes in the M1 fixture test.
2. **Multiple buys per wallet.** First buy fixes the original weight/bag; all later buys and sells update inventory under §3. Retaining only the first buy is insufficient for sold-share accounting.
3. **Persist tier high-water?** Yes. Without it a restart can drop a CLEARED escalation, which the escalation rule in §2B forbids.
4. **Post-then-edit, not hold for funding.** Holding ~2 min breaks the CTO latency contract for a status field.
5. **Origin probe after migration.** The PDA may be closed for old mints; fall back to `origin unknown`, never guess `pumpfun`.
6. **CTO retention is a sample of one** (SCOPE §11). 30-day TTL plus high-water, `suppressed_by_high_water` visible; revisit in a month.
7. **Fan-out cap 50 is invented.** Ship it, log the per-CTO distribution, set the cap from it next revision.
8. **CTO trade subscription.** While tracked, unsubscribe at `tracker_ttl_s`; the card states the observation window either way.
9. **Sells before observation.** Without complete retained history, print sold share unavailable or an observed lower bound and withhold CLEARED. A current balance is not proof of a sale.
10. **Near edits off.** Keep SCOPE's default; a near edit spends a token on the least actionable tier.

