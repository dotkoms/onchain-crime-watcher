# Ranking and live matching — v1 spec

Status: design for M5, 2026-10-25. Implements `../SCOPE.md` §2C, Q2, R6, R7, R12 and B7/H5/H8 in
`REVIEW.md`; changes no decision. Entry and exit matching are both M5. Siblings: `feed.md`,
`bundle.md`, `sources.md`, `delivery.md`, `operations.md`, `failover.md`, `backtest.md`.
Every invented number is **PRIOR** with its reasoning; §13 names what replaces it and what
falsifies the design.

## 0. Pipeline and invariants

```
trade stream → rank writer (background) → day segments → weekly job → snapshot
                    │                              (bounded RPC)         │
                    └ coverage journal (fsync on gaps)                    ▼
                                                    boot reload → account subscription
                                                                  → "entered" / "exited"
```

The store is never read on the alert path; the job is its only reader. One writer task. A cold watch
set suppresses and counts, never fabricates. Every card carries the window, the covered subset, the
fee mode, and open positions at their mark. Exit matching is live-only: it uses the position state
built from the account subscription, never the rank store, and never fabricates a cost basis.

## 1. Store schema

Revision 6 replaces latest-row-wins cumulative aggregates with **opening checkpoints plus
ordered trade deltas**. A cumulative row alone cannot say which profit was realised inside a
seven-day window. The storage module is one background writer; physical encoding and index
layout are selected in M1 from a measured fixture, not fixed byte offsets in this plan.

| Record | Required fields | Purpose |
|---|---|---|
| Trade delta | event id, wallet, mint, side, raw token quantity, gross SOL leg, fee estimate/resolution, receive time, optional execution time, source, boot/session, ingest sequence, registry state and state_seq | Reconstruct chronological buys and sells; immutable, idempotent by event id |
| Opening checkpoint | wallet, mint, boundary time, tokens held T, remaining cost C, lifetime realised R, counts, provenance, last applied event id | State immediately before a UTC day; never includes events at or after its boundary |
| Mint mark | mint, timestamp, price, regime, source, freshness | Last eligible price strictly before a ranking close |
| Coverage | gaps, ticks, boots, shutdowns, switchovers (§6) | Refuse an incomplete ranking rather than fabricate history |
| Candidate RPC cache | job close, wallet/mint, request identity, response hash and response | Reproduce fee, balance and clustering decisions |

The job loads a checkpoint at or before the window start, applies intervening deltas to build
opening inventory, then resets **window** realised profit to zero. Only sales in
`[close − 7d, close)` contribute realised profit; pre-window buys retain their remaining basis.
Each event has a stable total order; receive time is the bulk proxy, and any resolved execution
time correction triggers recomputation of the affected wallet/mint, not a timestamp edit on a
cumulative row. Missing opening history sets `basis_incomplete` and excludes the row.

**Allocation filters.** Minimum position and mint-volume floors decide ranking eligibility,
not which earlier cost-basis events may be discarded. Keep bounded pending histories until the
mint/position qualifies. A pending overflow or expiry makes that basis incomplete; later buys
cannot silently turn it into a complete history. Cache aggregates may accelerate the job but are
rebuildable from checkpoints and deltas.

**Retention and bounds.** Keep 10 days of deltas and the last complete checkpoint needed before
the oldest permitted window start. `rank.row_cap_bytes` (512 MiB PRIOR) includes events,
checkpoints and indexes. Measure bytes/event and events/day in M1; the old 192-byte aggregate-row
sizing is withdrawn. Prune only history no active window needs. If the cap cannot preserve a
complete opening checkpoint plus the seven-day window, stop accepting rank writes, open a
coverage gap, and refuse the job. Never silently shorten the ranking window or look-back.

**Writes and crash recovery.** Updates batch every 2 s or 4,096 deltas, with a durable high-water
sequence and coverage tick synced at most 30 s apart. A crash opens a gap from the last durable
tick. Checkpoints publish atomically with their applied event sequence. Boot discards an
incomplete checkpoint and replays from the previous complete one. Corrupt records are counted
and their interval is a coverage gap; no partially reconstructed row is ranked.

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
- **Look-back.** Ten days of deltas give three days before the window. Restore a complete
  checkpoint at or before window start, then apply any intervening deltas to build `C` and `T`.
  Pre-window sells do not contribute window realised profit. Older carried positions remain
  eligible if the checkpoint preserves their complete basis; age alone does not make it unknown.
  A missing checkpoint or missing required intervening history sets `basis_incomplete`.
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
| older than retained deltas, complete checkpoint | carried basis retained | `carried · checkpoint basis` |
| opening checkpoint/history missing | excluded | `excluded · basis incomplete` |
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
  made on the estimate): sample up to 20 recent in-window signatures, `getTransaction`, use
  `meta.fee` once (including its priority component), add separately decoded tips when present,
  and apply the sample mean to the trade count; card
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
| `SWITCHOVER` | from u8, to u8, at_ms u64, reason u8, boundary_frames u32, cold_rings u16 | fdatasync immediately |
| `BOOT` / `SHUTDOWN` | pid u32, boot_id u64 | fdatasync |

`BOOT` without `SHUTDOWN` opens an implicit gap from the last `TICK`, so a crash cannot become
covered time; gaps are written for disconnect, decode stall, backpressure, disk error, unclean boot
and quarantine. A `SWITCHOVER` is not a gap by itself: coverage follows the authoritative source,
and only the boundary loss and any blind interval count against the window. The snapshot and the
job report carry `sources_used` and `switches`, so a week that ran mostly on the secondary says so.

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

1. **Coverage gate, then freeze `store_seq`** (deltas append-only, writer keeps running). Failure
   writes `JOB refused` before any store scan is paid for.
2. **Reconstruct each wallet/mint chronologically** from the last complete opening checkpoint
   and ordered deltas (§1). Build opening inventory before the start, reset window realised PnL,
   then apply only in-window sales to it. Use the last mark strictly before close. No
   latest-row-wins merge and no summing overlapping cumulative snapshots.
3. **Filter reconstructed results**: excluded, below mint/deployment floors, incomplete basis,
   one counter each. Keep bounded accumulators (≤64 mints/wallet, overflow excluded); measure
   memory in M5. Any pruning that can affect the top set is reported in the job.
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
   "venue":{"raydium":"gap"},"post_migration":"ok","sources_used":["pumpportal"],"switches":0},
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

**New position versus add.** State per `(wallet, mint)` owns the same average-cost
`T, C, R` ledger as §2, plus `episode_id, first_seen, last_seen, tokens_peak, buy_count,
sell_count, sol_bought, sol_sold, entry_observed, entry_delivered_at, basis_complete,
transfer_ambiguous, exit_fired`. A buy opens a complete episode only when a trustworthy balance
before that buy is zero, or a recorded prior zero establishes it. For a buy,
`balance_before = newTokenBalance − tokenAmount`; a negative or inconsistent result is
unknown, never clamped into evidence of a new position. State absent with a nonzero prior balance
may emit a labelled first-observed entry, but is unpaired for exits and has no known cost basis.

An add updates `T, C` and peak, counts `wallet_add_suppressed`, and posts no entry. A known
zero closes the episode; a subsequent zero-to-positive buy creates a new episode id. Idle eviction
after 24 h and LRU beyond `wallet.touched_cap` (100,000) discard the basis and forfeit any paired
exit. RAM sizing is measured in M5; the previous 128-byte estimate is withdrawn.

**Exits.** The trigger describes observed inventory depletion, not total turnover:

```
sold_share = (tokens_peak − T) / tokens_peak >= wallet.exit_share_bp / 10_000
sol_sold >= wallet.min_exit_sol
basis_complete && !transfer_ambiguous && !exit_fired
```

A positive peak is required. `T` follows all observed buys and sells; peak is the maximum
inventory in the episode. Repeated sales followed by buys do not accumulate a false 90% exit.
The balance-derived and trade-size-derived paths reconcile against the same ledger; a mismatch
makes the episode ambiguous rather than changing the meaning of the metric. `closed` means
`T <= peak × wallet.exit_dust_bp / 10_000`; the exact remainder is always shown.

- **Realised PnL.** Apply §2 on every buy and sell, including estimated fees: buy fees join
  remaining cost, and each sell removes its proportional current cost and its own fee. The card
  shows accumulated realised `R`, never `sol_sold − sol_bought × depletion`. Adds after
  partial sells are ordinary ledger events, not a new averaging shortcut.
- **Pairing.** `wallet.exit_require_entry=true` requires a complete episode whose opening was
  observed; it does not claim the entry was delivered. The footer separately says
  `entry delivered at <time>` or `entry observed; alert not delivered`, based on the
  sender's successful POST callback. Unknown delivery is not successful delivery. With pairing off,
  an unknown-basis sell may produce `SELL OBSERVED — entry not observed`, with quantity and
  proceeds only; it must not claim percent exited, closed status or realised PnL.
- **Transfers.** Reconcile the expected post-trade inventory with any trustworthy reported balance.
  An unexplained change in either direction sets `transfer_ambiguous`, suppresses the percentage
  exit and omits PnL for the rest of that episode. Transferring 60% and selling 30% does not mean
  90% sold. A later sell cannot clear the flag; only a verified zero and a new fully observed
  episode restore eligibility. Transfers that leave and return between observations remain a
  stated blind spot.
- **Delivery and restart.** `exit_fired` latches at candidate creation, so a suppressed or expired
  candidate is counted and not recreated on each sell. Position state is RAM-only; after restart,
  prior positions are unpaired until a new complete opening is seen. Boot reports
  `position_state=cold`; it cannot report the exact number of lost positions without a persisted
  checkpoint.

**Dedup and pacing.** Entry key `(wallet, mint, entry, episode_id)`; exit key
`(wallet, mint, exit, episode_id)`; `deliver.wallets.dedup_ttl_ms` = 86,400,000 (24 h) in the
delivery set. Entry cooldown per `(wallet, mint)` `deliver.wallets.cooldown_ms` = 10,800,000 (3 h;
band 1–4 h); the cooldown **does not apply to exits**, so an entry and its exit inside three hours
are both delivered. Order: classify → entry or exit → gates (`wallet.min_buy_sol` 0.25 for entries,
`wallet.min_exit_sol` for exits) → dedup → entry cooldown → budget → send. Exits are sub-capped at
`deliver.wallets.exit_hourly` (3/h) and rank above entries in the wallet queue. Suppression is
counted, never silent.

| Alert field | Source |
|---|---|
| `ENTERED`, wallet short + rank of 40, 7d PnL, cluster PnL, members | detector, snapshot |
| breadth, deployment, fee mode, window, coverage, `bundle: no launch record` | snapshot |
| buy size SOL, entry price, mint age, venue; health footer when not normal | live event, registry, `delivery.md` |
| `EXITED`, sold share, closed/remainder, hold time, entry and exit price | live event, position state |
| realised SOL, basis (`balance` \| `sell sizes` \| `unknown`), fees mode | position state, `rank.fee_est_lamports_per_trade` |
| pairing line (`entry delivered at <time>` \| `entry observed; alert not delivered`), transfer note | position state, successful delivery callback |

**Reachability.** The 40 wallets are a discovery set, not a per-wallet delivery promise. At 3
entries/wallet/day, 40 wallets yield ~120 entry candidates/day; exits on observed positions can
add a similar number, so the combined ~240 candidates/day against the 20/day cap suppresses roughly
92%, counted `wallet_budget_suppressed`. Exits are ranked before entries and within each kind by
`(wallet PnL desc, size desc)` before the budget. The exit sub-cap means at most 3 of any hour's 6
wallet posts are exits; entries are not promised the remainder either.

An episode id is a deterministic 128-bit hash of wallet, mint and normalized opening trade identity, not a
source's receive timestamp. Delivery dedup uses that id across source changes. Missing opening
identity makes the episode unpaired. Live matching and the weekly job call the same position
accounting functions; fixtures cover sell→add→sell, transfers and carried inventory.

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
| `wallet.touched_cap`, `wallet.position_memory_ms` | entries / ms | 100,000 / 86,400,000 | measure ledger state size; idle eviction forfeits basis until a new verified opening |
| `wallet.chunk_keys`, `wallet.add_alert` | keys / bool | 100 / false | vendor limit unverified; one entry per position |
| `wallet.exit_share_bp`, `wallet.exit_dust_bp` | bp | 9,000 / 500 | 90% net depletion with complete sell-only lineage; ≤5% remaining is closed |
| `wallet.min_exit_sol`, `wallet.exit_require_entry` | SOL / bool | 1.0 / true | dust proceeds are not information; pairing keeps the basis honest |
| `deliver.wallets.exit_hourly` | posts/h | 3 | an exit storm cannot crowd out entries; shares the 6/h wallet token bucket |

## 11. `/health`

| Field | Meaning |
|---|---|
| `rank_rows`, `rank_segments`, `rank_bytes`, `rank_last_flush_ms`, `rank_flush_lag_ms`, `rank_basis_drift_rows`, `rank_corrupt_records`, `rank_quarantine_segments`, `rank_pruned_rows`, `rank_pruned_bytes` | store, writer, integrity, pruning |
| `coverage_bp_7d`, `coverage_max_gap_ms`, `coverage_last_gap_ms`, `coverage_sources_used`, `coverage_switches` | §6; switchover is not a gap, but it is part of the week's provenance |
| `watchset_state`, `watchset_generation`, `watchset_age_ms`, `watchset_wallets`, `watchset_clusters` | boot line |
| `watchset_cold`, `watchset_refused_coverage`, `watchset_short` | refusals |
| `wallet_alerts_sent`, `wallet_budget_suppressed`, `wallet_cooldown_suppressed`, `wallet_add_suppressed`, `wallet_sells_seen`, `wallet_state_evicted` | wallet delivery and matching |
| `wallet_exits_sent`, `wallet_exit_suppressed{cap,budget,unpaired,transfer,unverified}`, `wallet_exit_basis{balance,sizes,unknown}`, `wallet_exit_cold_on_restart` | exit matching and its honest denominators |
| `wallet_positions_open`, `wallet_positions_closed`, `wallet_exit_share_p50` | position state and the observed sold-share distribution; never a hit rate |
| `acct_sub_keys`, `acct_sub_chunks`, `acct_sub_failures`, `acct_events`, `acct_unknown_side` | subscription |
| `rank_last_job_ms`, `rank_last_job_result`, `rank_job_duration_ms`, `fee_mode_{resolved,estimated}`, `venue_events{curve,pumpswap,raydium}` | job, modes, coverage |

## 12. Acceptance tests (M5)

| # | Test | Pass condition |
|---|---|---|
| 1 | 10,000 deltas and 2 opening checkpoints, restart; then `kill -9` mid-flush | every field equal; loss ≤ 30 s covered by a gap, never 10000 bp |
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
| 15 | Buy, then sells at 50%, 89.9%, 90.0%, 100% of peak | 0 / 0 / 1 / 0 cards; `EXITED` once, `exit_fired` latched |
| 16 | 90% sold with 0.9 SOL proceeds, then 1.0 SOL | no card, then card; `wallet.min_exit_sol` exact at the boundary |
| 17 | Entry posts, exit 30 min later | 2 posts; entry cooldown does not suppress the exit |
| 18 | Transfer out 60%, then sell 30% of peak | `transfer_ambiguous`, no percentage exit or PnL on either event; only a new complete episode restores eligibility |
| 19 | Position opened before boot; sell unknown prior inventory | Pairing on: suppressed; pairing off: SELL OBSERVED with quantity/proceeds only, no percent/closed/PnL |
| 20 | 10 exits in one hour with entry candidates queued | 3 exits posted, 7 `wallet_exit_suppressed{cap}`; entries still use the remaining wallet tokens |
| 21 | Buy 30M tokens for 3.5 SOL, sell 27M for 4.0 SOL, fee 500,000 lamports/trade | `R = 849,050,000` lamports after proportional buy-fee basis and sell fee; 90% depleted, 10% remains |
| 22 | Replay a recorded wallet day | Identical exit cards, suppressions and order; `report_sha256` stable |
| 23 | Restart with open positions and no position checkpoint | Boot says position state cold; next activity lacks basis and cannot produce paired percentage exits; no invented count of lost positions |
| 24 | Buy 100 for 10 SOL, sell 50 for 10, buy 50 for 20, sell 90 for 18; fees zero | 10 tokens remain, depletion 90%, realised 0.5 SOL; live and ranking ledger agree |
| 25 | Realise 5 SOL before window, 1 SOL inside, with complete opening checkpoint | In-window realised = 1 SOL, never 6 SOL; carried inventory retains its basis |
| 26 | Trade exactly at start / at close; then lose needed opening history | Start included, close excluded; missing basis refuses the affected row and reports incomplete coverage |
| 27 | Entry observed but cap-dropped; eligible exit later | Footer says entry observed, alert not delivered; successful entry callback is the only delivered-state setter |

## 13. Open questions and recommendations

| Question | Recommendation | Falsified by |
|---|---|---|
| 40 wallets versus 20 alerts/day | keep 40 as the discovery set, rank delivery; cut to ~12 if > 1 entry/wallet/day | measured entries/wallet/day ≤ 0.5 |
| Execution-time proxy | keep the 300 s band, publish sensitivity | measured p99 feed lag > 5 min |
| Pre-window appreciation credited | add a window-start mark when history allows; `carried` is interim | a carried position flips rank when the start mark is added |
| Inbound transfers | exclude at reconciliation, as written | inbound tokens on > 5% of candidates |
| Cluster PnL as a sum | keep the sum, cap members at 20, show the count | one cluster takes > 20% of the top 40 |
| Fee constant 500,000 | use until measured | first 100 resolved medians outside [250k, 1,000k] |
| Retention 10 days plus required checkpoint | measure event/checkpoint bytes before M2a; refuse incomplete windows at cap | needed history cannot fit 512 MiB |
| Raydium gap | the 24 h PumpSwap gate is the only defence | < 100 PumpSwap events in 24 h |
| Cooldown 3 h / weekly rebuild | keep both: 3 h is inside the band; weekly matches "the previous week", while daily would be yesterday's winners | wanted repeats inside 3 h, or noise above budget |
| Exit threshold 90% of peak, dust 5% | keep; it catches the whale that leaves dust and never sells it, while a 50% trim is not an exit | more than 40% of exits fire with a 4–5% remainder, which reads as a trim |
| Pairing on by default | keep; an unpaired exit has no basis and is not a promise the bot can keep | unpaired exits exceed paired ones, meaning watch-set churn makes the family mostly silent |
| Exit sub-cap 3/h | keep; protects entries from an exit storm | exits fill the cap every hour while entries starve |
| Exits share the 20/day cap | keep; no separate promise, no separate channel | exits consume most of the daily cap and entry delivery collapses |
| Transfers out | suppress and count, never alert as an exit; state it if a later sale fires | transfers exceed 10% of positions that leave |

