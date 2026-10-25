# Ranking and live matching — v1 spec

Status: design for M5, 2026-10-25. Implements `../SCOPE.md` §2C, Q2, R6, R7 and B7/H5/H8 in `REVIEW.md`;
changes no decision. Siblings: `feed.md`, `bundle.md`, `sources.md`, `delivery.md`, `operations.md`.
Every invented number is **PRIOR** with its reasoning; §13 names what replaces it and what falsifies the design.

## 0. Pipeline and invariants

```
trade stream → rank writer (background) → day segments → weekly job → snapshot
                    │                              (bounded RPC)         │
                    └ coverage journal (fsync on gaps)                    ▼
                                                    boot reload → account subscription → "entered"
```

The store is never read on the alert path; the job is its only reader. One writer task. A cold watch
set suppresses and counts, never fabricates. Every card carries the window, the covered subset, the
fee mode, and open positions at their mark.

## 1. Store schema

| File | Contents | Bound |
|---|---|---|
| `rank/seg-NNNNNN.rcr` (+ `.idx`, 8 B/record, rebuildable) | one UTC day of 192 B records (crc32c over the first 188), append order; 32 B header: magic `RSEG`, version, day_start_ms, crc32c | 10 + today |
| `rank/coverage.jrn` | coverage journal, §6 | ~55 KB/day |
| `rank/mints.tbl` | mint 32, volume_lamports 8, trades u32, venue_mask u16, flags u16 = 64 B | ~20k mints → 1.3 MB |

Key = wallet ‖ mint.

| Field (offset) | Type | B | Notes |
|---|---|---|---|
| wallet (0), mint (32) | [u8;32] ×2 | 64 | key; base58 only at the card |
| first_ts_ms (64), last_ts_ms (72) | u64 ×2 | 16 | receive time; block time where resolved (§3); drives prune |
| buy_lamports (80), sell_lamports (88), buy_count (144), sell_count (148) | u64 ×2, u32 ×2 | 24 | gross curve leg, fees excluded |
| tokens_bought (96), tokens_sold (112), tokens_out (128) | u128 ×3 | 48 | raw units; `out` from reconciliation |
| episodes (152), venue_mask (154), flags (156), reserved (158) | u16 ×4 | 8 | venues b0 curve/b1 pumpswap/b2 raydium/b3 unknown; flags 0x1 transfer_out, 0x2 in, 0x4 creator, 0x8 bundle, 0x10 basis_incomplete, 0x20 stale, 0x40 mismatch, 0x80 excluded, 0x100 ts_resolved |
| cost_lamports (160), realised_lamports (168), balance_raw (176) | u64, i64, u64 | 24 | advisory; last `newTokenBalance`; drift counted |
| crc32c (184) | u32 | 4 | over the first 188 B |
| reserved_tail (188) | u32 | 4 | zero-filled; record length 192 B |

**Allocation filters.** A row exists only if the mint passes `rank.mint_volume_floor_sol`, cumulative
buys reach `rank.min_position_sol`, and the mint is not tombstoned. Events below the mint floor wait
in a 16-event ring (1.5 MB total); sub-threshold buys wait in a pending map capped at 200k entries
(9.6 MB), overflow counted.

**Sizing.** `bytes ≈ rows_day × retention_days × 192 × 1.08`, `rows_day = launches_day × P(mint ≥ floor) × buyers_≥min`.
PRIOR: 20,000 launches/day (0.23/s), 0.10 pass the 10 SOL floor, 30 buyers ≥ 0.5 SOL ⇒ 60,000 rows/day; at 10 days, **119 MiB**.
At 0.30 and 40 buyers, 240,000 rows/day = **474 MiB** — 7% under the 512 MiB cap; beyond it the cap shortens retention, never the window.

**Pruning.** Whole segments only, oldest first, when the day ends before `now − rank.retention_ms`,
then when bytes exceed `rank.row_cap_bytes`; a segment intersecting the active window plus look-back
is never dropped — coverage is declared partial and the look-back shortened instead.

**Writes (live from M2, R7).** Updates coalesce in a `HashMap`; flush every 2 s or 4,096 dirty rows sorted by key;
`fdatasync` every 30 s; clean shutdown writes `SHUTDOWN`. A crash loses ≤ 30 s and boot opens a gap
over it, so it is never covered.

**Corruption.** Bad crc ⇒ skip record, count. Bad header or idx ⇒ truncate to the last good record,
copy to `.quarantine`, mark the day partial; above 1% bad records the segment is excluded. Every
quarantine appears in `/health` and the job report.

## 2. Cost basis

Per row: tokens held `T` (u128 raw), basis `C` (u64 lamports), realised `R` (i64 lamports).

| Event | Update |
|---|---|
| buy q for p, fee f | `C += p + f`; `T += q`; `buy_count += 1` |
| sell q for p, f, q < T | `cost = floor(C·q/T)` in u128; `R += p − cost − f`; `C −= cost`; `T −= q` |
| sell q = T | `cost = C` exactly; no residual dust |
| sell q > T | no basis for the excess: flag `transfer_in`, exclude row, count `rank_unbacked_sells`; never credit proceeds against zero basis |
| tokens in/out by transfer | never a buy or sale; found at reconciliation (§5.4); mismatch above the share excludes row (in) or wallet (out) |
| tokens die | no observation 24 h at close ⇒ MTM 0, `stale_mark` |

`C·q` reaches 1e18 × 1e15, so the multiply is u128. Floor leaves < 1 lamport per sell in `C`,
overstating realised by at most one lamport per sell — bounded, one-directional.

**Average cost is a presentation rule, not a profit rule.** For any sequence ending flat,
`ΣR = Σproceeds − Σcost − Σfees` regardless of FIFO or average. The rule changes only the
realised/open split, which matters for the card and where open value is zeroed or missing.

**Worked example** (lamports, fee 500,000/trade):

| Step | Tokens | SOL leg | C before | Cost removed | Realised Δ | C after | T after |
|---|---|---|---|---|---|---|---|
| buy | +20,000,000 | 2,000,000,000 | 0 | — | 0 | 2,000,500,000 | 20,000,000 |
| buy | +10,000,000 | 1,500,000,000 | 2,000,500,000 | — | 0 | 3,501,000,000 | 30,000,000 |
| sell | −12,000,000 | 2,400,000,000 | 3,501,000,000 | 1,400,400,000 | 999,100,000 | 2,100,600,000 | 18,000,000 |
| sell | −18,000,000 | 3,000,000,000 | 2,100,600,000 | 2,100,600,000 | 898,900,000 | 0 | 0 |

Realised 1,898,000,000 = 1.898 SOL; cash check 5.4 − 3.5 − 4×0.0005 = 1.898. Partial sell of one raw
token from T = 30,000,000, C = 3,501,000,000 removes 116 lamports, retaining 0.7. Open case: hold
5,000,000 at C = 583,500,000, price 100 ⇒ unrealised −83,500,000; at 30 h with no trade, MTM 0.

## 3. Window semantics

- **Window and boundary.** `[close − 604,800,000 ms, close)`, evaluated once per job; "rolling 7
  days" means trailing 7 days at that instant. Trades within `rank.window_slack_ms` (300 s) of an
  edge count as `window_boundary_trades` and set `boundary_sensitive` above the SOL threshold.
- **Execution time.** The payload has no timestamp (`../SCOPE.md` R1). Block time is resolved only
  for the final candidates and the fee sample; bulk rows use receive time, labelled
  `receive-time proxy`, and `ts_resolved` marks the rows where `getTransaction` replaced it. Cards
  say `7d, execution time where resolved` and never claim execution time for an unresolved row.
- **Look-back.** Retention 10 days gives 3 days before the window. Look-back buys build `C`; sells
  before `close − 7d` realise nothing. A pair whose earliest retained record already shows `T > 0`
  predates the look-back: `basis_incomplete`, excluded. Reachable only for positions > 10 days old.
- **Round trip.** An episode is balance 0 → >0 → 0; episodes in one mint aggregate to one row and
  one breadth unit. A mint counts at ≥ 1 SOL deployed with ≥ 1 episode or an open position at close.
- **Mark at close** is mint-level: implied price of the last fill before close (curve
  `vSol/vTokens`; post-migration `sol_lamports/tokenAmount`), lamports per raw token. Open value
  `T × mark`; no observation in `rank.stale_mark_ms` (24 h) ⇒ 0.
- **Carried positions.** A position opened before the window and sold inside realises PnL on its
  carried basis, so pre-window appreciation is counted; the card says `carried` and shows cost.

| Case | Value | Card line |
|---|---|---|
| closed in window, complete basis | realised | `closed · 1.42 SOL · 3 mints` |
| open at close, fresh mark | realised + MTM | `open · mark 1.10 SOL · last trade 4 m` |
| open at close, last trade > 24 h | MTM = 0 | `open · valued 0 (no trade 31 h)` |
| bought before window, sold inside | realised, carried basis | `carried · opened before 7d window` |
| older than look-back | excluded | `excluded · basis incomplete` |
| transfer mismatch | excluded | `excluded · transfer 32% of position` |

## 4. Fees

The stream gives `sol_lamports`, the curve leg — normalized from `solAmount` when present, else the
signed `vSol` delta, with `sol_source` = `Payload` | `ReserveDelta`; `solAmount` is observed but
optional, not guaranteed. Priority fee, tip and the 5,000-lamport base fee live in the transaction
and are absent. Two paths:

- **Estimate, all rows.** `rank.fee_est_lamports_per_trade` = **500,000** (0.0005 SOL): base plus an
  asserted priority/tip allowance. Added to buy cost, deducted from proceeds; card
  `fees: estimated 0.0005 SOL/trade`.
- **Resolve, candidates only.** Top `rank.fee_resolve_top` = 80 (2× watch size, so the cut is not
  made on the estimate): sample up to 20 recent in-window signatures, `getTransaction`, sum
  `meta.fee` and prioritisation fee, apply the sample mean to the trade count; card
  `fees: resolved (20-trade sample)`.

Bound: 80 × 21 = 1,680 calls, concurrency 5 at 300 ms ≈ 100 s, weekly. Failure keeps the estimate,
labels it, counts, and never blocks the job. The bias is `(n_i − n_j) × 0.0005 SOL`: it penalises
trade count, not skill; a 600-trade week carries 0.30 SOL, 6% of a 5 SOL profit.

## 5. Sybil clustering and gaming resistance

**Cluster = shared funder, direct or one hop**, resolved only for the top
`rank.funder_resolve_top` = 400 candidates. Per wallet: `getSignaturesForAddress(limit=100)`, the
earliest inbound native-SOL transfer before the first ranked buy, `getTransaction` for its source.
Edge `funder → wallet`, one direction; union-find gives components; `cluster_score = Σ member PnL`
and the representative (one watch slot) is the highest-PnL member. Q3's 10 s window and 60 s funding
test decide *bundle membership* (`bundle.md`), not identity: a shared funder is one operator however
far apart. Bound ≈ 400 × 11 calls ≈ 4.5 min; a funder with > 50 children is shared infrastructure —
edges dropped, children counted, each its own cluster. `rank.funder_fanout_cap` (50 here) is not
`bundle.fanout_cap` (50, launch-window bundle membership); the scopes differ. Known accounts
excluded first; two-hop not followed.

| Qualification, per cluster | Rule | Counter |
|---|---|---|
| Breadth / position | ≥ 2 distinct mints at ≥ 1 SOL each; rows under `rank.min_position_sol` never allocate | `rank_short_breadth`, `rank_rows_below_min` |
| Deployment | cluster total ≥ `rank.min_cluster_deploy_sol` (3 SOL) | `rank_short_deploy` |
| Creator / bundle | mint excluded if any member created it or bought its launch bundle | `rank_mints_creator`, `rank_mints_bundle` |
| Transfer | wallet excluded if out > `rank.transfer_share_bp` of peak held | `rank_excluded_transfer` |
| Stale | open position with no observation 24 h valued 0, not dropped | `rank_stale_marks` |

Breadth is computed per wallet and per cluster; qualification uses the cluster union, which is the
stricter reading and stops a cluster splitting 0.9 SOL per wallet across the same two mints.

**Creator and bundle.** Creator comes from the captured `create` event, else
`getAccountInfo(mint).updateAuthority`, batched (~800 mints ≈ 8 calls). Bundle membership exists only
in the launch record; with none the mint is flagged `bundle_unverified`, not excluded, and the card
says `bundle: no launch record`. A rugger's pre-boot launch escapes.

**Reconciliation at job time.** For the 400 candidates read on-chain balances for every counted mint
(`getMultipleAccounts`, ≈ 20 batched calls) and compare with `balance_raw`/`T`. Lower means outbound
(exclude wallet); higher means inbound (exclude row). This closes the transfer hole only at the
ranking boundary and only as a point sample: out-and-back between trades is unseen.

**Residual attack surface — cannot be closed with trade data.** Two-hop funding (rent ≈ 0.002 SOL).
A fresh funder per batch, which defeats a rule that depends on funder reuse. Pre-funded wallets with
no resolvable edge. Cross-cluster value transfer: a watched wallet overpaid by an outside buyer books
profit that is not wealth, and netting stops at the cluster. Unverifiable bundles. A program-held
mint authority. Out-and-back reconciliation.

## 6. Coverage

`rank/coverage.jrn`, append-only: `[magic u32 "RCV1"][type u8][len u16][ts_ms u64][payload][crc32c]`
= 19 B + payload.

| Type | Payload | Sync |
|---|---|---|
| `GAP_START` | reason u8, last_event_ms u64 | fdatasync immediately |
| `GAP_END` | reason u8, first_event_ms u64, lost_estimate u32 | fdatasync immediately |
| `TICK` | rows u32, events u32, segment u32 | with the 30 s data sync |
| `VENUE` | venue u8, events u32, mints u32, wallets u32 | with TICK |
| `JOB` | close_ms u64, result u8, coverage_bp u16 | fdatasync before publish |
| `BOOT` / `SHUTDOWN` | pid u32, boot_id u64 | fdatasync |

`BOOT` without `SHUTDOWN` opens an implicit gap from the last `TICK`, so a crash cannot become
covered time; gaps are written for disconnect, decode stall, backpressure, disk error, unclean boot
and quarantine.

`coverage_bp = 10000 × (window − Σ gaps)/window`, gaps clipped to the window, an unclosed gap counted
to `close`. PRIOR `rank.coverage_min_bp` = **9,700** (≤ 5.04 h of 168 h) and
`rank.coverage_max_gap_ms` = **2 h**: below 5% loss the top 40 is stable at prior rates, but one 2 h
hole can hide a whole pump-and-dump cycle, which is the wallet the family exists to find.

**Refusal.** Publish nothing; keep the previous snapshot byte-identical; increment
`watchset_refused_coverage`; write `JOB refused`. Generation is unchanged and its age grows; past
`rank.watchset_max_age_ms` the family goes cold (§8). A broken feed silences wallets rather than
serving a month-old list.

**Per venue.** Every `VENUE` record and the snapshot carry events, mints and wallets for curve,
PumpSwap, Raydium, unknown. Raydium is a declared gap, printed, not counted as downtime. If PumpSwap
is not delivered, post-migration PnL is missing and the ranking selects pre-migration snipers only;
the M5 gate is ≥ 50 migrated mints and ≥ 100 PumpSwap events over 24 h, or M5 does not ship.

## 7. Weekly job

At `close` = Monday 00:00 UTC, misfire grace 1 h, idempotent on `(close_ms, cfg_hash, store_seq)`.

1. **Coverage gate, then freeze `store_seq`** (segments append-only, writer keeps running). Failure
   writes `JOB refused` before any store scan is paid for.
2. **Merge to one row per key**: newest segment first, first record per `(wallet, mint)` wins; a
   `HashSet<[u8;64]>` at 60k–600k keys is 4–40 MB and rows stream.
3. **Filter before aggregation**: `last_ts < close − window − lookback`, `excluded`, mint below
   `rank.mint_volume_floor_sol`, deployment below `rank.min_position_sol`, `basis_incomplete`, one
   counter each. Only survivors allocate an accumulator (pnl i64, deploy u64, breadth u16, ≤ 64
   mints, overflow excluded); ~8 MB at 60k wallets, above 200k evict the minimum PnL.
4. **Heavy-hitter prune**: min-heap of 400 keyed `(pnl_lamports, wallet)`, wallet bytes ascending on
   ties — bounded and deterministic.
5. **Cluster** (§5), then **verify** creator/bundle, balances and fees (§4), which may remove or
   revalue but never add a wallet absent from step 4.
6. **Qualify, sort, write, adopt**: sort by `(cluster_pnl, cluster_id)` descending, take 40 (fewer
   publishes short, `watchset_short`, never padded); canonical JSON + sha256, `fsync` tmp, `rename`,
   `fsync` dir, previous kept as `watchset.prev.json` (rollback only); swap the in-RAM set,
   chunk-subscribe, `JOB published`, update `/health`.

Determinism: integer only, total sort keys, no map order in output, RPC cached in
`rank/job-<close>.json` keyed `(wallet, window)`. A retry inside the grace reproduces the sha256;
after cache pruning it may differ only in cluster/fee fields, recorded `rpc_cache: miss`.

## 8. Snapshot and boot

```json
{"v":1,"generation":42,"created_ms":…,"close_ms":…,"window":{"start_ms":…,"end_ms":…},
 "watch":[{"wallet":"…","cluster_id":"…","members":3,"pnl_lamports":…,"deploy_lamports":…,
   "breadth":4,"mints":["…"],"fee_mode":"resolved","flags":[]}],
 "coverage":{"ratio_bp":9930,"max_gap_ms":420000,"mints_watched":1234,"events_seen":8421001,
   "venue":{"raydium":"gap"},"post_migration":"ok"},
 "cfg_hash":"…","store_seq":918234,"sha256":"…"}
```

40 entries × ~350 B ≈ 14 KB. Boot order: parse → `v` → sha256 → age ≤ `rank.watchset_max_age_ms` →
`cfg_hash` equals the current output-affecting keys → non-empty. Failure is cold: no alerts, **no
account subscription** (ingesting a family that cannot post is waste), `watchset_cold` +1 per boot,
boot line prints state, generation and age. States: `ok`; `missing`; `corrupt` (quarantined);
`incompatible` (`v`); `stale` (> 8 days); `cfg_mismatch` (ranking config changed).

## 9. Live matching

**Subscription.** `subscribeAccountTrade` over watch-set wallets on the existing socket (one-socket
invariant asserted), chunked at `wallet.chunk_keys` = 100 per message; 40 keys is one message and
chunking exists for a larger future set. Every reconnect increments the generation and resends all
chunks; superseded events drop. Counters `acct_sub_keys`, `acct_sub_chunks`, `acct_sub_failures`.

**Classification.** `txType` ∈ {buy, sell}; anything else drops and counts `acct_unknown_side`.
`traderPublicKey` must be in the set; the mint is unrestricted, which is the point.

**New position versus add.** State per `(wallet, mint)`: `{first_seen_ms, last_ms, balance_raw,
buy_count, sol_bought, alerted_gen}`. Balance before = `newTokenBalance − tokenAmount`, clamped at 0.
A buy is a new position iff balance before is 0, state is absent, or state is older than
`wallet.position_memory_ms` (24 h). Only a new position fires `entered`; an add increments
`wallet_add_suppressed` and is never announced — one entry per position, not per buy. Sells update
state and count `wallet_sells_seen`, never implying an exit. The touched set caps at
`wallet.touched_cap` = 100,000 entries at 96 B ≈ 9.6 MB; evict idle > 24 h, then LRU, counting
`wallet_state_evicted`; an evicted open position re-reads as new, which is accepted.

**Dedup and pacing.** Exact key `(wallet, mint, first_seen)`, `deliver.wallets.dedup_ttl_ms` =
86,400,000 (24 h) retention in the delivery set; cooldown per `(wallet, mint)`
`deliver.wallets.cooldown_ms` = 10,800,000 (3 h; band 1–4 h). Order: classify → new-position
→ gates (`wallet.min_buy_sol` 0.25) → dedup → cooldown → budget → send. Suppression is counted.

| Alert field | Source |
|---|---|
| `ENTERED`, wallet short + rank of 40, 7d PnL, cluster PnL, members | detector, snapshot |
| breadth, deployment, fee mode, window, coverage, `bundle: no launch record` | snapshot |
| buy size SOL, entry price, mint age, venue; health footer when not normal | live event, registry, `delivery.md` |

**Reachability.** The 40 wallets are a discovery set, not a per-wallet delivery promise. At 3
entries/wallet/day, 40 wallets yield ~120 candidates/day against the 20/day cap: ~83% are suppressed
and counted `wallet_budget_suppressed`. Queued alerts are ranked by `(wallet PnL desc, buy size
desc)` before the budget.

## 10. Configuration

| Key(s) | Unit | PRIOR | Rationale |
|---|---|---|---|
| `rank.window_ms` | ms | 604,800,000 | Q2 |
| `rank.basis_lookback_ms`, `rank.retention_ms` | ms | 259,200,000 / 864,000,000 | 3 d basis; window + lookback + 1 d |
| `rank.min_position_sol`, `rank.min_deploy_sol` | SOL | 0.5 / 1.0 | allocation gate; counting gate (contract) |
| `rank.min_cluster_deploy_sol`, `rank.mint_volume_floor_sol` | SOL | 3.0 / 10.0 | one lucky trade is not a record; below floor PnL is fee noise |
| `rank.breadth_min`, `rank.transfer_share_bp`, `rank.stale_mark_ms` | mints / bp / ms | 2 / 1,000 / 86,400,000 | contract; 10% of peak held; contract |
| `rank.window_slack_ms`, `rank.boundary_sensitivity_sol` | ms / SOL | 300,000 / 0.25 | boundary band and flag |
| `rank.fee_est_lamports_per_trade` | lamports | 500,000 | §4; replace with measurement |
| `rank.fee_resolve_top`, `…_sigs_per_wallet` | wallets / sigs | 80 / 20 | 2× watch size; ~100 s RPC |
| `rank.funder_resolve_top`, `rank.funder_fanout_cap` | wallets / children | 400 / 50 | ~4.5 min weekly; Q3 rule |
| `rank.watch_size`, `rank.watchset_max_age_ms` | wallets / ms | 40 / 691,200,000 | contract; one missed job tolerated, two not |
| `rank.coverage_min_bp`, `rank.coverage_max_gap_ms` | bp / ms | 9,700 / 7,200,000 | §6 |
| `rank.row_cap_bytes`, `rank.flush_interval_ms`, `rank.fsync_interval_ms` | bytes / ms | 536,870,912 / 2,000 / 30,000 | hard stop; batch latency; crash-loss bound |
| `rank.pending_ttl_ms`, `rank.job_cron`, `rank.job_misfire_grace_ms` | ms / cron UTC / ms | 3,600,000 / `0 0 * * 1` / 3,600,000 | sub-threshold buys accumulate; weekly; late, not skipped |
| `deliver.wallets.cooldown_ms`, `wallet.min_buy_sol` | ms / SOL | 10,800,000 / 0.25 | delivery-owned; 3 h in band; dust is not information |
| `deliver.wallets.dedup_ttl_ms` | ms | 86,400,000 | delivery-owned; 24 h dedup window |
| `wallet.touched_cap`, `wallet.position_memory_ms` | entries / ms | 100,000 / 86,400,000 | ~9.6 MB; 24 h idle is re-entry |
| `wallet.chunk_keys`, `wallet.add_alert` | keys / bool | 100 / false | vendor limit unverified; one entry per position |

## 11. `/health`

| Field | Meaning |
|---|---|
| `rank_rows`, `rank_segments`, `rank_bytes`, `rank_last_flush_ms`, `rank_flush_lag_ms`, `rank_basis_drift_rows`, `rank_corrupt_records`, `rank_quarantine_segments`, `rank_pruned_rows`, `rank_pruned_bytes` | store, writer, integrity, pruning |
| `coverage_bp_7d`, `coverage_max_gap_ms`, `coverage_last_gap_ms` | §6 |
| `watchset_state`, `watchset_generation`, `watchset_age_ms`, `watchset_wallets`, `watchset_clusters` | boot line |
| `watchset_cold`, `watchset_refused_coverage`, `watchset_short` | refusals |
| `wallet_alerts_sent`, `wallet_budget_suppressed`, `wallet_cooldown_suppressed`, `wallet_add_suppressed`, `wallet_sells_seen`, `wallet_state_evicted` | wallet delivery and matching |
| `acct_sub_keys`, `acct_sub_chunks`, `acct_sub_failures`, `acct_events`, `acct_unknown_side` | subscription |
| `rank_last_job_ms`, `rank_last_job_result`, `rank_job_duration_ms`, `fee_mode_{resolved,estimated}`, `venue_events{curve,pumpswap,raydium}` | job, modes, coverage |

## 12. Acceptance tests (M5)

| # | Test | Pass condition |
|---|---|---|
| 1 | 10,000 rows, 2 segments, restart; then `kill -9` mid-flush | every field equal; loss ≤ 30 s covered by a gap, never 10000 bp |
| 2 | Golden cost basis §2 | realised 1,898,000,000 exactly; partial sell removes 116 |
| 3 | Determinism, 10 runs with shuffled RPC cache | identical sha256 each run |
| 4 | Boundary: trade at `close−1`, at `close`, ±300 s | first included, second excluded, band reported |
| 5 | 30 h with no trade | MTM 0, `stale_mark`, card `valued 0` |
| 6 | Transfer out 11% / 9% of peak | excluded + counter / ranked |
| 7 | Three wallets, one funder, PnL 5/3/2 | one slot, representative 5, cluster 10 |
| 8 | Two mints 0.9 SOL each, then 1.0 each | excluded, then ranked |
| 9 | Injected 3 h gap | job refuses, old snapshot byte-identical, counter +1 |
| 10 | Snapshot deleted | `missing`, zero alerts, `watchset_cold = 1`, no subscription |
| 11 | Buy, add, sell, replay; then two positions 90 min apart | 1 / 0 / 0 / 0 posts, counters; 1 post, 1 cooldown suppression |
| 12 | 10 alerts in 1 h, ranks interleaved | ≤ 6 posted, highest PnL first, rest counted |
| 13 | Mock fee RPC at 900,000/trade | `resolved`; PnL moves by 400,000 × trades |
| 14 | 24 h PumpSwap probe | ≥ 50 mints and ≥ 100 events, or `post_migration = gap` and M5 blocked |

## 13. Open questions and recommendations

| Question | Recommendation | Falsified by |
|---|---|---|
| 40 wallets versus 20 alerts/day | keep 40 as the discovery set, rank delivery; cut to ~12 if > 1 entry/wallet/day | measured entries/wallet/day ≤ 0.5 |
| Execution-time proxy | keep the 300 s band, publish sensitivity | measured p99 feed lag > 5 min |
| Pre-window appreciation credited | add a window-start mark when history allows; `carried` is interim | a carried position flips rank when the start mark is added |
| Inbound transfers | exclude at reconciliation, as written | inbound tokens on > 5% of candidates |
| Cluster PnL as a sum | keep the sum, cap members at 20, show the count | one cluster takes > 20% of the top 40 |
| Fee constant 500,000 | use until measured | first 100 resolved medians outside [250k, 1,000k] |
| Retention 10 days | keep | > 259,000 rows/day, which trips 512 MiB inside 10 days |
| Raydium gap | the 24 h PumpSwap gate is the only defence | < 100 PumpSwap events in 24 h |
| Cooldown 3 h / weekly rebuild | keep both: 3 h is inside the band; weekly matches "the previous week", while daily would be yesterday's winners | wanted repeats inside 3 h, or noise above budget |
