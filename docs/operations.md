# operations — process, configuration, observability

Status: design for M1, 2026-10-25. Contract: [`../SCOPE.md`](../SCOPE.md) revision 6 (§4–§9, R5,
R8, R10–R12); this file adds runtime shape, key names and procedures and changes no decision.
Siblings: `feed.md`, `volume.md`, `delivery.md`, `bundle.md`, `ranking.md`, `sources.md`,
`backtest.md`, `failover.md`. The rank-store
checkpoint/delta contract is fixture-tested in M1c; `rank.writer` and `rank.job` start in M2a; the capture recorder ships in M1 and
replay is offline. Every invented number is **PRIOR** with its reason; contract numbers are marked
CONTRACT.

## 0. Budget, and what this file will not claim

Alert budget, stated before any threshold below: **volume 10/h, burst 3, 40/day, near ≤ 30%**;
**wallets 6/h with exits sub-capped at 3/h, burst 2, 20/day**; **CTO 20/h safety cap, burst 2,
120/day**. Status, outage and heartbeat edits bypass all of it (`delivery.md` §3); the 5-minute
heartbeat is a cadence, not an alert-budget input. Nothing in this file raises a cap.

The ~40-wallet watch set is a discovery set, not a delivery promise: ~120 entry candidates/day plus
exits on observed positions, ~240 candidates/day against the 20/day cap, so roughly 92% are
suppressed by budget and counted `wallet_budget_suppressed`. SOL values entering admission or a card
are `sol_lamports` with `sol_source = Payload | ReserveDelta | Instruction`; nothing reads raw
`solAmount`.

Observed, not recorded: counters, histograms, heartbeat, boot line, windowed rings, shadow
disagreement. Recorded, off the alert path: raw frames for replay (`backtest.md`), rank and launch
records, coverage, CTO high-water. Not recorded: trades as an archive, alert outcomes, message ids,
dedup state. So no hit rate, precision, recall or base rate; cohort context is only the live
percentile in RAM (`volume.md` §4); only windows with complete recorded inputs support full decision replay; a restart re-alerts; a
silent drop is invisible until the M2b shadow. The side stores and the capture tap are off the alert
path. Left out on purpose: a trade archive, per-event tasks, and every metric requiring outcome
labels. Backtesting (capture plus offline replay) and hot failover (a second provider, M6) are in
scope; neither is a live dependency.

Lifecycle validity: volume only `on_curve`/`migrated` with a re-warmed ring, never across the boundary; CTO/bundle only from a listing (record or `no launch record`); wallets only warm, and exits only for an episode live matching observed. A cold or evicted position forfeits its exit rather than inventing a basis.

## 1. Process and task topology

Single binary, tokio multi-thread runtime, `panic = "unwind"`. One decode+detect task per feed.
Registry is data, not a task: the pipeline task is its only mutator.

```
SIGTERM ─▶ main / supervisor (JoinSet, budget, shutdown, failover controller)
              │
   ┌──────────┼──────────────┬───────────────┬─────────────┬──────────────┐
   ▼          ▼              ▼               ▼             ▼              ▼
feed.session[p] feed.pipeline[p] feed.watchdog  gate planner  rpc.client  capture.writer
feed.session[s] feed.pipeline[s] + heal jobs    + ledger[p]   (semaphore,  (frames → disk,
 (one socket)   (decode+detect,  (≤2 gaps,       + ledger[s]    no task/call) loss counted)
   │ Frame       registry, dedup, RPC budget)         │
   └────────────▶ volume close)                       │
                 ├─▶ cto.poll ─▶ bundle.funding ─▶ bundle.writer
                 ├─▶ rank.writer ─▶ rank.job (weekly)
                 ├─▶ delivery.{volume,wallets,cto,status} (one in-flight each)
                 └─▶ heartbeat · health.server (pull) · deadman (push)
```

| Task | Count | Owns (single writer) | Receives → emits | Cost, latency |
|---|---|---|---|---|
| root + supervisor + failover | 1 | JoinSet, config ArcSwap, cancel tree, `authoritative`, flap guard | signals/states → spawn, restart, switchover | no network; 1 s arbitration tick |
| `feed.session[p]`, `feed.session[s]` | 1 each (M6) | socket, read buffer, generation | `SocketCmd` → `Frame` (delta, 1024) | upstream 100–500 ms; ~0.6 MB/s peak each |
| `feed.pipeline[p]`, `feed.pipeline[s]` | 1 each (M6) | decode scratch, dedup, registry + eviction, volume rings, cohort cells | `Frame` → gate cmds, alerts, counters | 5–15 µs/trade typical, < 2 ms p99; ~2% of a core each at 1500 msg/s |
| `feed.watchdog` + `feed.heal` | 1 each per source + ≤2 gap jobs | EWMA, probe cursor, gap cursors, call budget | tick/reconnect → probes, healed aggregates | 3 RPC calls per suspicion; heal ≤600 calls/min, ≤120 s |
| `gate` planner | 1 | shared desired set, caps, retention decisions | creates/trades/CTO/tick → desired snapshot | 5 s reconcile; no HTTP |
| `gate` ledger[p], ledger[s] | 2 (M6) | per-source subscription state, `SubGen`, retries | desired → `SocketCmd` | no HTTP |
| `capture.writer` | 1 | segments, `.idx`, manifest, capture gaps | frame tap (bounded, event class) → disk | ~1 s flush; ~60–90 MB/day; never backpressures decode |
| `cto.poll` | 1 | seen-set, latch, high-water | 60 s timer → listing alerts | 1 request/60 s; vendor body small |
| `bundle.funding` | 1 + ≤8 in-flight | funding cache, per-run budget | CTO job → record + edit | 120 s deadline, ≤1200 calls |
| `bundle.writer` | 1 | launch segments, index | records (bounded 4096) | commit ≤250 ms/64 records, fsync |
| `rpc.client` | 1 | HTTP pool, semaphore, 429 state | all callers | per-call 200–600 ms; shared |
| `rank.writer` / `rank.job` | 1 / 1 | rank segments, coverage journal, snapshot | aggregates → weekly timer | commit 2 s/4096 rows, fsync 30 s; job minutes |
| `delivery.{volume,wallets,cto}` + `status` | 4 | queues, buckets, ids, breaker, outage identity | alerts/state → POST/PATCH | 100–300 ms HTTP; admission wait dominates; status 1/5 s |
| `heartbeat` + `health.server` + `deadman` | 3 | heartbeat ref, snapshot, ping | timer/GET → edit/ping | edit/5 min; local GET; ping/60 s |

M1c defines the logical checkpoint/delta contract, versions and coverage journal; `rank.writer` and `rank.job` start
in M2a, so M1 exercises storage fixtures but no live rank task; M1b does exercise one complete staged volume alert. M1 also ships `capture.writer`; replay is an
offline subcommand and never a production task. M6 adds the second session and pipeline and the
failover controller.

**Never spawned per event, mint, alert, call or signature.** No task per trade, subscription,
mint, RPC call, CTO, buyer, heal signature, alert or send; bucket closes use one 100 ms ticker over
a deadline-ordered set, not one timer per mint. Heal uses ≤2 jobs; RPC uses a semaphore. Invariants:
`sockets_open[provider] ≤ 1` and `sockets_open_total ≤ 2`; `frames_dropped == 0`; one in-flight per
webhook; registry mutated only by its source's `feed.pipeline`; exactly one pipeline authoritative;
all channels bounded and classed; zero trade-path allocations after warmup.

**Shutdown order** (SIGTERM/SIGINT; SIGHUP is reload, not shutdown):

1. Cancel root; stop admitting alerts, CTO, heal and rank; gate stops subscribing.
2. Close the socket, wait 2 s, abort the session; count `close_forced` if it hangs.
3. Pipeline drains frames; discard the partial bucket (`shutdown_partial_dropped`).
4. Delivery flushes ready and held candidates until `ops.shutdown.flush_deadline_ms`; leftovers
   count as `drop_shutdown`.
5. Rank writer flushes and `fdatasync`s; bundle writer commits and fsyncs; capture writer seals the
   segment and manifest and records the shutdown seq; CTO high-water is already synced per advance.
6. Deadman sends a final `stopping` ping; health closes. Exit 0 only if every flush met its
   deadline, else 1, inside `ops.shutdown.hard_timeout_ms`.

A restart is not a resume: counters reset, rings are cold, ids are gone, open wallet positions are
unpaired and forfeit their exits, and the boot line says so.

## 2. Supervision, restart budget, red

- **JoinSet.** One `JoinSet<StageOutcome>` retains every stage handle; a `Fatal` return or
  `JoinError` is one restartable event, and stages cannot detach.
- **Budget.** 5 restarts/600 s per stage; backoff 500 ms × 2^n, ±20% jitter, cap 60 s; reset after
  600 s healthy. Feed socket backoff is separate (`feed.md` §4.2); a ban does not consume this.
- **Fast-fail.** Three panics within 5 s of start skip the remainder and go red; the payload is
  logged once at ERROR with the task-local stage name, never per retry.
- **Red** = exhausted budget, `Fatal`, or invariant break. That subsystem stops; the process
  lives, `/health` answers, other families post. Red lasts the process lifetime — SIGHUP does not
  re-arm it; only a restart clears it, recorded as `restart_generation`.
- **Recovery.** Automatic only for the delivery half-open breaker and a feed reconnect; everything
  else is §6.
- **Panic loss.** Pipeline: registry rebuilt cold (`mint_reentered_cold`), no alerts until warm.
  Gate: ledger rebuilt, all subscriptions re-sent under a new `SubGen`. Delivery: queue, ids and
  cooldowns lost (`deliver_state_lost`). CTO: high-water survives. Rank/bundle: at most the
  uncommitted window, and coverage opens a gap.
- **Process red** = any subsystem red, `sockets_open[provider] > 1`, `sockets_open_total > 2`,
  `frames_dropped > 0`, `authoritative = none` while a provider is configured, or
  `monotonic_violations > 0`. Red never exits; the dead-man's switch is the attention path.
  Subsystems are `trade_stream`, `cto_source`, `rpc`, `discord:volume`, `discord:wallets`,
  `discord:cto` — six outage identities; each degrades alone with its own outage identity and
  state. A switchover is not one of them.

## 3. Configuration

One TOML file (`--config` / `OCW_CONFIG`), one schema, env overrides for secrets and a few paths.
Unknown keys fail boot; any error names the field path and fails fast. Reload is SIGHUP: parse →
validate → swap the whole `ArcSwap<Config>` atomically; an invalid candidate is discarded, the last
good config stays, `config_reload_rejected_total` increments and one WARN names the path. `B` =
boot-only, `H` = hot. Secrets are always `B` and are env-only.

| Key | Type/unit | PRIOR | Validation | Reload |
|---|---|---|---|---|
| `feed.pumpportal.ws_url` | secret url | `wss://pumpportal.fun/api/data` | `wss`, no query string, host allowlist | B |
| `feed.pumpportal.api_key` | secret | — | non-empty, ≥ 32 chars | B |
| `feed.socket.{ping_interval_s,pong_timeout_s,connect_timeout_s}` | s | 20 / 45 / 10 | `0 < ping < pong < 120`, connect 1–30 | H |
| `feed.backoff.{base_s,cap_s,jitter_pct}` / `feed.healthy_reset_s` | s / s / % / s | 1 / 30 / 20 / 120 | `0 < base ≤ cap ≤ 60`, jitter 0–50, reset 60–3600 | H |
| `feed.restart.{budget,window_s}` / `feed.ban.{short_session_s,consecutive,wait_s}` | count / s / s / count / s | 5 / 600 / 5 / 3 / 3600 | ≥1, ≥60, ≥1, ≥1, ≥300 | H |
| `feed.watchdog.floor_s` / `multiplier` / `ewma_alpha` / `exclude_gap_s` / `probe_mints` / `probe_agree` | s / × / frac / s / count / count | 30 / 6 / 0.05 / 60 / 3 / 2 | floor ≥10, mult ≥2, `0<alpha<1`, `agree ≤ mints`, mints ≥2 | H |
| `feed.frames.capacity` | frames | 1024 | power of two, 256–65536 | B |
| `feed.dedup.window_s` / `peak_msgs_per_s` / `overhead` | s / msg/s / × | 180 / 1500 / 1.5 | window ≥120, peak ≥1, overhead 1.0–2.0, computed capacity ≤ 2^24 | B |
| `feed.heal.max_calls_per_gap` / `max_duration_s` / `max_sigs_per_mint` / `calls_per_min` / `calls_per_day` / `block_max_slots` / `block_max_calls` | calls / s / sigs / calls/min / calls/day / slots / calls | 1200 / 120 / 1000 / 600 / 50000 / 150 / 200 | all >0, `calls_per_min ≤ 1200`, `calls_per_day ≤ calls_per_min × 1440` | H |
| `feed.shadow.mints` / `bottom_mints` / `window_s` / `rpc_lag_s` / `tolerance` | mints / mints / s / s / frac | 20 / 5 / 600 / 60 / 0.05 | `bottom ≤ mints`, window = 600, `0 < tolerance < 0.5` | H |
| `feed.decode.sample_errors` | frames | 5 | 0–100 | H |
| `feed.metadata.name_bytes` / `symbol_bytes` | bytes | 32 / 10 | 8–64, symbol ≤ name | B |
| `feed.secondary.enabled` / `provider` / `commitment` | bool / enum / enum | false / — / confirmed | provider named only when enabled; commitment stated because it changes latency | B |
| `feed.secondary.endpoint` / `token` | secret url / secret | — | `https`/`grpcs`, host allowlist; env-only | B |
| `feed.secondary.filters` / `startup_check_s` | count / s | 256 / 10 | provider limit measured in C14; first frame within the budget | H |
| `feed.failover.prefer` / `switch_after_s` / `switch_back_after_s` / `secondary_fresh_s` | enum / s / s / s | primary / 30 / 300 / 10 | targets, outage threshold, five minutes of health, a standby with no event is not fresh (`failover.md` §3) | H |
| `feed.failover.compare_window_s` / `max_switches_1h` / `flap_cooldown_s` / `footer_window_s` / `warm_standby` / `require_secondary` | s / count / s / s / bool / bool | 600 / 3 / 3600 / 600 / true / false | aligned comparison, flap guard, card stamp, hot rings, explicit requirement | H |
| `capture.enabled` / `dir` / `retention_days` / `max_bytes` | bool / path / days / bytes | true / `data/capture` / 14 / 2 GiB | evidence path; inside data dir; two weeks; ~1.2 GiB expected | B |
| `capture.queue_capacity` / `flush_ms` / `manifest_sync_ms` / `compression` / `sources` | records / ms / ms / enum / enum | 4096 / 1000 / 60000 / `zstd-3` / `primary` | bounded tap; crash bound; metadata; ratio; `both` from M6 only | B |
| `backtest.report_min_days` / `min_candidates` / `train_share` / `max_sweeps` | days / count / frac / count | 7 / 30 / 0.7 / 20 | below these a report says insufficient evidence; sweep budget | B |
| `backtest.forward_mark_5m_s` / `_1h_s` / `report_sha256_required` | s / s / bool | 300 / 3600 / true | outcome proxy horizons; a report without a hash cannot justify a key | B |
| `gate.spend_reservation_messages` / `spend_stop_margin_messages` / `spend_ledger_path` | messages / messages / path | 1000 / 10000 / `data/feed-spend.json` | positive; margin < daily allowance; ledger synced, 0600, never pruned; fail closed for paid streams | B |
| `gate.launch_window_s` | s | 10 | 5–30, equals `bundle.window_ms/1000` | H |
| `gate.launch_slots` / `max_subs` / `launch_slot_wait_s` | count / count / s | 4 / 20 / 2 | `launch_slots < max_subs`, max 4–64 | H |
| `gate.retain_ttl_s` / `retain_window_s` / `retain_min_sol` / `keepalive_sol_60s` / `evict_margin` | s / s / SOL / SOL / × | 1800 / 300 / 3.0 / 0.5 / 1.5 | ttl 300–7200, window ≤ ttl, ≥0, ≥0, ≥1.0 | H |
| `gate.cto_override` / `feed_sol_per_day` / `reconcile_s` / `unsub_retries` / `confirm_s` / `max_account_keys` | bool / SOL/day / s / count / s / count | true / 0.5 / 5 / 5 / 5 / 40 | spend 0–200, 0.5 ≈ 500,000 msgs/day ≈ 5.8 msg/s; reconcile 1–60, retries 1–10, keys ≤100 | H |
| `volume.bucket.secs` / `ring.buckets` / `cohort.ratio.bins` / `cohort.trade.bins` / `taker.slots` | s / count / count / count / count | 15 / 30 / 128 / 64 / 32 | 5–60, 16–120, bins power of two | B |
| `volume.trim.buckets` / `warmup.buckets` / `min.nonempty.buckets` / `min.trades.bucket` | count | 3 / 16 / 8 / 6 | `trim < warmup ≤ ring`, `nonempty ≤ warmup`, trades ≥1 | H |
| `volume.floor.base.sol` / `floor.scale` / `dispersion.sigma` / `dispersion.median.frac` | SOL / × / σ-mult / frac | 1.5 / 2.0 / 4.0 / 0.5 | ≥0, 0.1–10, ≥0, 0–1 | H |
| `volume.min.takers` / `max.taker.share` / `fire.ratio` / `near.ratio` / `cold.floor.mult` | count / frac / × / × / × | 5 / 0.60 / 3.0 / 2.0 / 10.0 | `1 < near < fire`, share 0–1, cold ≥2 | H |
| `volume.cohort.bands.secs` | `[u32;4]` s | 300, 1800, 7200, 21600 | strictly ascending | B |
| `volume.cohort.p90` / `window.secs` / `min.samples` / `ratio.max` | quantile / s / count / × | 0.90 / 1800 / 200 / 32.0 | 0.5–0.99, 600–7200, ≥50, >fire | H |
| `volume.cooldown.secs` / `cold.cooldown.secs` | s | 300 / 900 | ≥60, ≥ cooldown | H |
| `volume.ticker.ms` / `close.batch` | ms / count/tick | 100 / 64 | 20–500, 1–256; expiry owned by delivery | B / H |
| `bundle.window_ms` / `funding_max_age_s` / `funding_clock_margin_s` / `funding_min_lamports` | ms / s / s / lamports | 10000 / 60 / 15 / 5,000,000 | window = gate window, age 30–120, margin 0–60 | H |
| `bundle.signatures_limit` / `max_pages` / `tx_reads_max_per_wallet` / `buyers_cap` / `write_channel` | sigs / pages / reads / buyers / records | 1000 / 2 / 4 / 256 / 4096 | 1–1000, 1–10, 1–10, 16–1024, power of two | H |
| `bundle.min_wallets` / `min_sol` / `min_supply_pct` / `tier_near` / `tier_fire` / `tier_cleared` | count / SOL / % / % / % / % | 5 / 10.0 / 2.0 / 60 / 80 / 95 | `near < fire < cleared ≤ 100` | H |
| `bundle.near_edits` / `fanout_cap` / `dumped_pct` | bool / children / % | false / 50 / 30 | near edits off; fanout 5–500, dumped 1–100; edit interval owned by `deliver.edit.min_interval_ms` | H |
| `bundle.rpc_{concurrency,batch_max,calls_per_cto,calls_per_sec,timeout_ms,funding_deadline_ms,lookup_deadline_ms}` | req / calls / calls/s / ms | 8 / 20 / 1200 / 25 / 8000 / 120000 / 750 | all >0, concurrency ≤32, calls/s ≤100, deadlines ≤300000 | H |
| `bundle.cache_{wallets,ttl_s}` / `stale_after_s` | wallets / s / s | 10000 / 900 / 300 | ≥100, 60–3600 | H |
| `bundle.record_retention_days` / `record_cap_bytes` / `trackers_max` / `tracker_ttl_s` / `reconcile_interval_s` / `peak_window_s` | days / bytes / mints / s / s / s | 45 / 1 GiB / 32 / 604800 / 900 / 3600 | days 1–365, cap ≥64 MiB, ttl ≥86400 | H |
| `bundle.record_dir` / `exclusions_path` | path | `data/launch` / `data/exclusions.toml` | inside `OCW_DATA_DIR`; parent writable; ≤200 exclusion entries | B |
| `cto.poll_interval_s` / `http_timeout_ms` / `poll_backoff_max_s` / `seen_cap` / `seen_ttl_days` / `latch_cap` / `claim_future_skew_s` / `empty_polls_degraded` / `origin_probe` | s / ms / s / mints / days / mints / s / polls / bool | 60 / 5000 / 600 / 20000 / 30 / 10000 / 900 / 6 / true | poll 15–300, timeout 1000–30000, backoff ≥ poll, caps ≥1000 | H |
| `cto.api_url` / `cto.state_path` | url / path | DexScreener v1 / `data/cto_state.json` | https, host allowlist; path in data dir | B |
| `rank.mint_volume_floor_sol` / `min_position_sol` / `pending_ttl_ms` / `retention_ms` / `row_cap_bytes` | SOL / SOL / ms / ms / bytes | 10.0 (0.10 sizing) / 0.5 / 3600000 (bounds 200k pending) / 864000000 / 512 MiB | floor ≥1, position ≤ floor, retention ≥ window+lookback | B |
| `rank.fee_est_lamports_per_trade` / `fee_resolve_top` / `fee_resolve_sigs_per_wallet` | lamports / wallets / sigs | 500000 / 80 / 20 | ≥0, ≤500, ≤100 | H |
| `rank.funder_resolve_top` / `funder_fanout_cap` / `min_cluster_deploy_sol` / `transfer_share_bp` / `stale_mark_ms` / `window_slack_ms` / `boundary_sensitivity_sol` | wallets / children / SOL / bp / ms / ms / SOL | 400 / 50 / 3.0 / 1000 (10% moved) / 86400000 / 300000 / 0.25 (¼ SOL band) | caps ≥ watch size, bp 0–10000 | H |
| `rank.watch_size` / `coverage_min_bp` / `coverage_max_gap_ms` / `watchset_max_age_ms` | wallets / bp / ms / ms | 40 / 9700 / 7200000 / 691200000 | watch 10–200, bp 0–10000 | H |
| `rank.min_deploy_sol` / `rank.breadth_min` / `rank.job_misfire_grace_ms` | SOL / mints / ms | 1.0 / 2 / 3600000 | counting gate (contract); breadth (contract); retry grace for the weekly job | H |
| `rank.job_cron` / `rank.store_dir` / `rank.job_cache_dir` | cron / path / path | Mon 00:00 UTC / `data/rank` / `data/rank/jobs` | parses; inside data dir; writable | B |
| `wallet.chunk_keys` / `position_memory_ms` / `touched_cap` / `min_buy_sol` | keys / ms / entries / SOL | 100 / 86400000 / 100000 / 0.25 | 1–100, cap ≥10000; pacing uses `deliver.wallets.cooldown_ms`, one knob | H |
| `wallet.add_alert` | bool | false | watch-set additions post nothing; the discovery set grows silently | H |
| `wallet.exit_share_bp` / `exit_dust_bp` / `min_exit_sol` / `exit_require_entry` | bp / bp / SOL / bool | 9000 / 500 / 1.0 / true | 90% net depletion with complete lineage; unknown transfers suppress; observed/delivered pairing separate (`ranking.md` §9) | H |
| `deliver.wallets.exit_hourly` | posts/h | 3 | shares the 6/h wallet bucket; exits cannot take more than half of it | H |
| `deliver.{volume,wallets,cto}.webhook_url` | secret url | — | `https://discord.com/api/webhooks/{id}/{token}` | B |
| `deliver.staging.{volume,wallets,cto}.webhook_url` | secret url | — | same shape, different id from production | B |
| `deliver.{volume,wallets,cto}.channel_id` | id | — | boot-only; required when `deliver.readback.enabled` | B |
| `deliver.{volume,wallets}.{hourly,daily,bucket_capacity}` / `deliver.cto.{hourly_cap,daily,bucket_capacity}` / `deliver.volume.{fire_reserve_per_hour,near_share_max}` | posts/h / posts/d / posts / posts/h / frac | 10/6/20 · 40/20/120 · 3/2/2 · 2 / 0.30 | hourly ≥ burst, daily ≥ hourly, share 0–0.5 | H |
| `deliver.hold.window_ms` | ms | 5000 | 0–10000, less than entry/near expiry; eligibility delay only | H |
| `deliver.max_age.volume_near_ms` / `volume_fire_ms` / `wallet_entry_ms` / `wallet_exit_ms` / `cto_ms` / `bundle_edit_ms` | ms | 15000 / 30000 / 30000 / 30000 / 120000 / 120000 | positive; reorder hold below applicable age; edits retain original transition time | H |
| `deliver.edit.min_interval_ms` / `edit.retry_max` / `http.timeout_ms` / `breaker.probe_ms` / `queue.capacity` / `ids.capacity` / `card.max_chars` | ms / tries / ms / ms / candidates / refs / chars | 45000 / 3 / 5000 / 60000 / 256 / 2048 / 2400 | interval 30000–60000; retry never extends freshness | H |
| `deliver.volume.cooldown_fire_ms` / `_near_ms` / `wallets.cooldown_ms` / `wallets.dedup_ttl_ms` / `cto.dedup_ttl_ms` | ms | 300000 / 1800000 / 10800000 / 86400000 / 2592000000 | near ≥ fire; wallet 1–4 h | H |
| `deliver.readback.enabled` / `readback.bot_token` | bool / secret | false / — | token required if enabled | B |
| `deliver.health.heartbeat_interval_ms` / `staging.prefix` | ms / text | 300000 / `[STAGING]` | 60000–900000, ≤20 chars | H |
| `deliver.deadman.ping_url` | secret url | — | https, required when `OCW_ENV=production` | B |
| `ops.health.bind` / `health.token` | addr / secret | `127.0.0.1:9898` / — | non-loopback requires token | B |
| `ops.log.level` / `log.format` | enum / enum | `info` / `json` | trace–error, json–text | H |
| `ops.shutdown.flush_deadline_ms` / `rank_sync_ms` / `hard_timeout_ms` | ms | 5000 / 2000 / 18000 | flush < hard < systemd stop | H |
| `ops.supervisor.{restart_budget,window_s,healthy_reset_s,backoff_base_ms,backoff_cap_ms,jitter_pct}` | count / s / ms / % | 5 / 600 / 600 / 500 / 60000 / 20 | base ≤ cap, budget ≥1 | H |
| `ops.outage.{declare_s,recover_s}` / `ops.deadman.{interval_s,dead_after_s}` | s | 30 / 60 / 60 / 300 | recover ≥ declare; dead ≥ 2×interval | H |
| `ops.disk.min_free_bytes` / `check_s` | bytes / s | 2 GiB / 60 | ≥ 1 GiB, 10–600 | H |
| `ops.config.reload_grace_ms` | ms | 250 (editor save burst) | 0–5000 | B |
| `ops.registry.idle_ttl_s` / `max_live` / `tombstone_cap` / `near_graduation_fill` | s / mints / mints / frac | 3600 / 20000 / 50000 / 0.80 | ttl ≥ gate TTL+60; cap ≥ 2×gate; fill 0.1–0.99 | H |

New priors: loopback health bind (operational state, not secrets); 5 s flush (Discord p99
100–300 ms, bounded queue); 18 s hard stop under systemd's 20 s; supervisor 5/600 s (mirrors feed,
no thrash); disk floor 2 GiB (above 1.6 GiB caps, room for rename); registry idle 1 h (outlives gate
churn); `max_live` 20k (~27 MB at 1.35 KB/mint); `near_graduation_fill` 0.80 is display-only. All
other prior rationale is in `feed.md` §8, `volume.md` §9, `bundle.md` §8, `ranking.md`,
`delivery.md` §11.

**Secrets.** PumpPortal key, webhook URLs, bot token, dead-man URL and health token are env-only
(`${VAR}` expansion is the sole file form), never logged, serialized, in `/health` or a panic.
`SecretString`/`SecretUrl` redact in `Debug`; the panic hook installs before config parse;
failures name the key, not the value; file mode 0600.

## 4. Health

`GET /health` returns `200` JSON, never secrets, never blocking (reads an `ArcSwap` snapshot built
each second). A non-loopback bind requires `ops.health.token`, else 401.

Types: `*_total` u64 cumulative; `*_1h`/`*_today` windowed u32; `*_s`/`*_ms` f64 durations, ages
f64|null; second counts (`window_covered_s`, `blind_seconds_1h`) u32; state is an enum string.
Groups: `feed`/`gate` transport and subscriptions; `registry` lifecycle; `volume` verdicts; `bundle`/`cto` records and claims; `rank`/`wallet` coverage and matching; `delivery` queues; `ops` resources. Counter names not shown are the sibling's, no aliases.

```json
{"schema":"ocw.health.v1",
 "process":{"pid":4812,"boot_id":"3f9c…","started_at":"…Z","uptime_s":3612.4,"version":"0.1.0","config_hash":"9f2c…",
  "restart_generation":3,"state":"amber","red_reasons":[],"dry":false,
  "tasks":[{"stage":"feed.session","state":"running","restarts":1,"last_exit":null}]},
 "feed":{"state":"connected","mode":"single-source","authoritative":"primary","unverified":true,"generation":41,
  "sockets_open":{"primary":1,"secondary":0},"sockets_open_total":1,
  "source_state":{"primary":"connected","secondary":"unavailable"},"switches_total":0,"last_switch_ms":null,"switch_reason":null,
  "boundary_frames_dropped":0,"failover_cold_rings_total":0,"switch_back_blocked_total":{"unhealthy":0,"stale":0,"disagree":0,"flap":0},"session_uptime_s":3402.1,
  "create_age_s":3.2,"trade_age_s":0.4,"watchdog_threshold_s":31.6,"msgs_per_s_ewma":0.94,"frames_dropped":0,
  "stale_generation_dropped":17,"dup_dropped":0,"dedup_entries":214553,"decode_errors_total":0,"field_missing_total":0,
  "reconnects_total":1,"watchdog_false_positives":0,"ban_active":false,"banned_until":null,"gap_heals_ok":1,
  "gap_heal_unhealed":0,"invariant_violations":0},
 "gate":{"subs_live":18,"subs_desired":18,"subs_by_kind":{"launch":2,"retained":16},"subs_capped_total":4,"cost_capped_total":0,
  "launch_skipped_total":1,"unsub_failed_total":0,"recycles_total":1,"msgs_charged_total":81200,"sol_spent_today":0.081,
  "estimated_sol_per_day":0.081},
 "registry":{"live_mints":12045,"by_state":{"created":12,"on_curve":11894,"near_graduation":31,"migrated":98,"dead":0,"unknown":10},
  "tombstones":8211,"evicted_total":19002,"reentered_cold_total":44,"regime_resets_total":98,"bytes_estimate":16700000},
 "volume":{"state":"normal","eval_total":9021,"fire_total":31,"near_total":77,"warmup_suppressed_total":4102,"cohort_cold_total":3,
  "cold_fire_total":1,"late_trades_total":2,"close_lag_ms":{"p50":0.8,"p99":11.9},"expired_total":2},
 "bundle":{"records_written":19871,"records_dropped":0,"record_store_bytes":128000000,"lookup_timeouts":0,"funding_runs":3,
  "rpc_calls":1902,"budget_exhausted":0,"trackers_live":3,"no_launch_record":1,"min_gate_rejects":0,"monotonic_violations":0},
 "cto":{"state":"normal","last_ok_wall":"…Z","polls_total":58,"polls_failed":0,"empty_streak":0,"high_water_claim_date":"2026-10-24T09:12:00Z",
  "primed_from_empty":0,"new_claims":2,"posted":2},
 "rank":{"coverage_bp":9930,"max_gap_ms":420000,"segments_bytes":124000000,"rows":590000,"writer_lag_ms":140,
  "watchset":{"state":"ok","generation":42,"age_s":259200,"sha256":"ab12…"},"watchset_refused_coverage":0,
  "venue":{"curve":8120000,"pumpswap":0,"raydium":0,"unknown":0},"post_migration":"unverified",
  "sources_used":["pumpportal"],"switches":0},
 "wallet":{"state":"normal","keys_subscribed":40,"entered_total":0,"add_suppressed":0,"sells_seen":0,"cold_events":0,
  "wallet_budget_suppressed":96,"exits_total":0,"exit_suppressed":{"cap":0,"budget":0,"unpaired":0,"transfer":0,"unverified":0},
  "exit_basis":{"balance":0,"sizes":0,"unknown":0},"exit_cold_on_restart":0,"positions_open":0,"positions_closed":0},
 "capture":{"state":"writing","bytes_today":74200000,"bytes_total":412000000,"segments":14,"oldest_day":"2026-10-18",
  "frames_written":4120000,"dropped_frames":0,"drop_intervals":0,"truncated_segments":0,"coverage_bp_24h":9998},
 "backtest":{"last_report_ms":null,"last_report_sha256":null,"last_result":"none","fitted_keys":0,"in_sample_reports":0},
 "delivery":{"started_at":"…Z","restart_generation":3,"readback":"off","webhooks":{"volume":{"state":"closed","last_ok_age_s":14,
  "posts_1h":3,"edits_1h":1,"daily_used":3,"tokens":2.4,"hold_depth":2,"queue_depth":0},"wallets":{"state":"closed"},"cto":{"state":"closed"}},
  "counters":{"posts_sent":41,"delivery_unknown":0,"drop_dedup":6,"drop_cooldown":11,"escalation_without_target":1,"outage_cards_posted":1}},
 "ops":{"rpc":{"state":"normal","http_429_total":0,"consecutive_failures":0,"inflight":0},"disk_free_bytes":5368709120,"disk_state":"ok",
  "deadman_last_ping_age_s":12.0,"config_reload_rejected_total":0,"log_dropped_total":0,"window_covered_s":3600,
  "blind_seconds_1h":{"trade_stream":0,"cto_source":0,"rpc":12,"discord:volume":0,"discord:wallets":0,"discord:cto":0}}}
```

Enums: `volume.state=normal|warming|slow`; `watchset.state=ok|missing|corrupt|incompatible|stale|cfg_mismatch`;
`post_migration=ok|gap|unverified`; webhook `state=closed|open|half_open`; `disk_state=ok|low|full`;
`feed.authoritative=primary|secondary|none`; `capture.state=off|writing|stopped_disk|stopped_cap|error`.
`sockets_open{provider}` is 0 or 1 each and the total is at most 2. `by_state.near_graduation` is
display-only; `age_s` is wall age.

**Satisfying the SCOPE §4 latency claims.** `feed_lag`, `decode_us`, `detect_us`, `enqueue_us`,
`budget_wait_ms`, `hold_wait_ms`, `http_us` and `total_ms` are exported at p50/p99/p999 with `n`
and `rate_per_s` over the same stated window; a percentile without its rate is not a claim.
`budget_wait_ms` is enqueue-to-grant for fire/CTO/bundle, `hold_wait_ms` is the 45 s near/wallet
hold, and both are split by family.

**SCOPE §6.6 mapping.** `feed.state=connected` maps to `normal`, or `unverified` when no shadow
agrees; `reconnecting`/`banned` maps to `degraded` while a shadow agrees and `stale` once no event
passes the watchdog threshold. A cold ring or cold watch set is `warming` (`volume.state=warming`,
wallet warming). `volume.state=slow` is close lag only: it is a footer tag on `normal`, never
SCOPE's `degraded`, which is reserved for a primary-unresponsive feed. Webhook breaker
`closed|half_open|open` maps to `normal|warming|degraded` for that `discord:{volume,wallets,cto}`.

**Cumulative vs windowed.** Cumulative: every `*_total`, `posts_sent`, `records_written`, `dup_dropped`,
`reconnects_total`, all reject/suppression counters. Windowed: `*_1h`, `*_today`, `tokens`, `hold_depth`,
`queue_depth`, `blind_seconds_1h`, `window_covered_s`. Gauges: ages, states, `subs_live`, `live_mints`.
A restart zeroes the first, empties the second and colds the third; the boot line is the warning.

**`blind_seconds_1h`** counts blind one-second bins in a fixed 3600-bin ring per subsystem; pre-boot
bins are excluded and `window_covered_s` is the denominator. A bin is blind when `trade_stream` is not
`connected`, a declared/unhealed gap covers it, or the last accepted event is older than `T`;
`cto_source` last succeeded more than `max(2×poll, 120 s)` ago or is in backoff; `rpc` breaker is open
or three consecutive calls failed with work pending; a `discord:{volume,wallets,cto}` has its breaker
open, a global pause, or a post pending without success for `2×heartbeat_interval`. It is a lower
bound: the primary can drop messages while connected (SCOPE §6.1). Without a shadow, 0 means "no
observed blindness", not complete data, and the heartbeat says `single-source, unverified`.

**Boot line** (volume webhook, fallback order; after `--startup-check`, one message): `ocw boot |
v0.1.0 | config <hash8> | feed single-source, unverified | mode <dry|live> | subs 0/20 | watchset gen
<g>, <age> (<state>) | restart #<n> | counters 0 | uptime 0`. It is the first heartbeat message and
is edited thereafter with readback; without it a restart posts a new one, hence `restart_generation`.

**Heartbeat** (edit-in-place every `deliver.health.heartbeat_interval_ms`, default 5 min): funnel
`seen → gated → warming → evaluated → fire/near → queued → sent`; drops by reason and edits by tier;
warmup/suppression counts; shadow p50/p90 per stratum with `n` and its assumption; `blind_seconds_1h`
and `window_covered_s`; last delivery age per webhook; feed state and generation; config hash and
boot age. A stale heartbeat and a silent dead-man's switch are one fact, both outside the process.

## 5. Observability

Logs: one JSON object per line to stdout (`ops.log.format=json`), captured by journald. Fixed fields
`ts`, `level`, `stage`, `evt`, `boot_id`; context `gen`, `sub`, `mint`, `sig8`, `gap_id`, `alert_id`,
`family`, `tier`; measures `ms`, `count`, `state_from`, `state_to`.

| Event | Level | Frequency |
|---|---|---|
| boot, config load/reload (hash only), shutdown | INFO | once, plus each reload |
| stage start/exit/restart/red, breaker transition, ban enter/exit | WARN | per transition |
| reconnect with gap window, heal summary (`sigs_seen/read/calls/reason`) | INFO | per gap |
| decode error sample, field-missing sample | WARN | first `feed.decode.sample_errors` per process, then 1/s max |
| CTO poll failure, RPC 429, funding partial | WARN | first of a streak, then every 10th |
| alert decision (verdict and inputs; no body) | INFO | per candidate admitted or dropped |
| delivery attempt failure | WARN | per attempt; sanitized payload on failure only |
| delivery success | DEBUG | per post/edit, id included |

Trades, frames, per-mint updates and per-RPC calls are never logged. Sampling never hides a state
change: transitions and red lines are unsampled. Counters carry volume, the heartbeat carries
deltas, a log line explains a counter. `log_dropped_total` counts lines lost to a full sink.

**Redaction and never-logged.** Never logged: webhook URLs, bot token, API key, dead-man URL,
health token, `Authorization` headers, query strings, config values, token name/symbol text (log
`name_sanitized` and length only), frame bodies beyond the first sample (≤256 bytes hex). Signatures
truncate to `sig8`; mints stay full. The panic hook prints `.../{id}/[redacted]` and drops payloads.

**Reconstructing an incident without a trade store.** The log is ordered and self-identifying:
boot line (version, config hash, restart and watch-set generation), feed transitions with
generation and gap window, heal counts and stop reason, each alert decision with its inputs, each
delivery attempt with class and status, heartbeat counter deltas, shutdown outcome. That proves what
the process believed, why it alerted and whether the message landed. It cannot prove what it never
received; per-window shadow disagreement is the only loss evidence, a sample with an assumption.

## 6. Runbook

A whole-process restart loses rings, ids and cooldowns, re-alerts current spikes and colds every
family. Prefer a stage restart or a hot config change; restart only when the table says so.

| # | Procedure | Detection signal | First action | Safe to restart | Must not do |
|---|---|---|---|---|---|
| 1 | Feed down | `feed.state` `reconnecting` > 2 min, `reconnects_total` rising, `create_age_s` > T | Read `feed.state`, `banned_until`, last error class; confirm the vendor is reachable from the host with one request; leave config alone | Process yes (cold rings, ids lost); session alone yes via its supervisor; never open a second connection to the same provider | Raise the restart budget, lower the watchdog floor, run a second socket to one provider or a parallel probe client, or call the market quiet |
| 2 | Ban | `feed.state=banned`, `banned_until` in the future, three short sessions or a vendor limit | Do nothing until expiry; verify `sockets_open[provider] ≤ 1` and that the key wallet holds ≥0.02 SOL; after expiry one attempt is automatic | No. A restart does not clear a provider ban and may extend it; it also consumes attempts | Retry early, rotate keys or IPs to evade, run diagnostics that open sockets, delete the ban state |
| 3 | CTO source failing | `cto.state=degraded`, `polls_failed`, `empty_streak ≥ 6` | One manual GET of the vendor list; check host clock; other families keep posting | `cto.poll` yes; process yes (high-water survives) | Treat an empty list as a quiet market, replay the list, drop the high-water, or shorten the poll below the vendor limit |
| 4 | RPC 429 storm | `ops.rpc.http_429_total` rising, `consecutive_failures > 0`, unhealed gaps rising | Check provider tier and credits; hot-reduce `feed.heal.calls_per_min` and `bundle.rpc_calls_per_sec`; expect declared-unhealed and say so | No need; process restart only drops the cache and changes nothing upstream | Retry immediately, silently disable heal, or add a second provider without writing its failure mode first |
| 5 | Discord breaker open | `delivery.webhooks.*.state=open`, `webhook_dead`/`webhook_forbidden`, `status_no_route` | Classify from the last error (401/50027, 403, 404/10015, 5xx); verify the webhook still exists and has permission; rotate the URL if dead | Process yes (breaker and ids reset); never restart only the sender to "clear" a dead URL | Retry invalid requests, open the breaker on 429 or 400/50035, or recreate the channel as a first response |
| 6 | Rank coverage partial | `rank.coverage_bp < 9700`, `max_gap_ms > 2 h`, `watchset_refused_coverage > 0` | Find the gap cause in the journal/log (feed, writer, disk); fix that; the next weekly job republishes | `rank.job` yes, `rank.writer` yes; a process restart opens another gap | Lower `coverage_min_bp`, merge two snapshots, backfill guessed rows, or publish a thin set |
| 7 | Watch set cold | `rank.watchset.state` missing/stale/corrupt/cfg_mismatch; wallet family warming; `keys_subscribed=0` | Check the snapshot file, age, hash and config hash; wallet alerts stay suppressed and counted | Process yes; do not expect a fix from restarting | Subscribe keys from an old list, lower `watchset_max_age_ms`, or post wallet alerts from a cold set |
| 8 | Disk full | `ops.disk_state=low\|full`, `record_write_errors`, rank writer errors, health amber | Stop non-essential writes; prune whole old segments and quarantined files; check caps and inodes | Writers yes once free space exists; process yes | Delete the current rank segment, current launch segment, `cto_state.json` or the watch set; disable fsync; run without caps |
| 9 | Process crash loop | systemd `NRestarts` rising, repeated boot lines, `restart_generation` climbing | Read the one ERROR panic line and the config hash it booted with; reproduce with `--dry` and `--startup-check` off the live path; fix the config or binary | Process yes after the fix; not before | Clear the systemd start limit repeatedly, run blind with no dead-man's switch, or let the boot line imply a healthy baseline |
| 10 | Vendor schema drift | `decode_errors_total` or `field_missing_total` rising with no deploy; unknown-type samples | Read the first redacted samples, compare with the pinned fixture, pin the new shape in a test before shipping; mark the feed degraded if errors exceed 5% | Process yes, but it re-alerts and does not fix a schema | Patch the decoder in production without a fixture, retry the same bytes, or ignore unknown fields silently |
| 11 | Failover flapping | `switches_total` rising, `flap_guard > 0`, `last_switch_ms` recent, both providers oscillating | Read `source_state` and `switch_reason`; fix the sick provider or pin `feed.failover.prefer`; the guard already holds the current authority for an hour | Process yes (resets the switch count and colds the standby); prefer a provider fix | Raise `max_switches_1h`, disable the flap guard, force a source without recording it, or open a second socket to one provider |
| 12 | Capture stopped | `capture.state=stopped_disk\|stopped_cap`, `dropped_frames > 0`, disk `low` | Free space, prune whole old capture days, check `capture.max_bytes`; the alert path is unaffected | Capture writer yes; process only if it is also out of disk | Delete the current capture segment, merge two days, or read a capture file on the alert path |

## 7. Deployment

| Variable | Meaning |
|---|---|
| `OCW_ENV` / `OCW_CONFIG` / `OCW_DATA_DIR` / `OCW_LOG` | `production\|staging` (production refuses unset) / config path / state root / `<level>,<json\|text>` |
| `OCW_PUMPPORTAL_KEY`, `OCW_RPC_URL`, `OCW_HEALTH_TOKEN`, `OCW_DEADMAN_URL`, `OCW_DISCORD_{VOLUME,WALLETS,CTO}_URL`, `OCW_GEYSER_ENDPOINT`, `OCW_GEYSER_TOKEN` | credentials and secrets, env-only |
| `RUST_BACKTRACE` | `1` in production; panic lines reach journald |

Precedence CLI > env > file; secrets env-only. Layout: `/usr/local/bin/ocw` → `/usr/local/lib/ocw/
releases/<sha>/ocw`; `/etc/ocw/config.toml`; `/etc/ocw/ocw.env` (0600);
`/var/lib/ocw/{rank,launch,rank/jobs,capture}`, `cto_state.json`, `watchset-GGGGGG.json`,
`exclusions.toml`, and durable `feed-spend.json` (never pruned). Logs go to journald.

```ini
[Unit]
StartLimitIntervalSec=600
StartLimitBurst=5
[Service]
User=ocw
EnvironmentFile=/etc/ocw/ocw.env
ExecStart=/usr/local/bin/ocw --config /etc/ocw/config.toml
Restart=on-failure
RestartSec=5
TimeoutStopSec=20
NoNewPrivileges=true
ProtectSystem=strict
ReadWritePaths=/var/lib/ocw
MemoryMax=512M
```

Assumptions: 0.23 launches/s, 1500 msg/s peak, 20 subscriptions, 10k live mints. RSS ≈ 120 MB
single-source (registry 27, dedup 15, rank index 14, bundle cache 5, queues and code the rest);
with the M6 standby pipeline add ~27 MB registry, ~15 MB dedup, ~13 MB rings and ~5 MB decode
buffers, so ~180 MB and `MemoryMax` 512 MB from M6. CPU < 0.5 core average, ~1 at peak, ~2 cores
worst case with two sources; inbound ~0.6 MB/s per source; Discord negligible. Disk caps 512 MiB
rank + 1 GiB launch + 2 GiB capture = 3.6 GiB; keep the 2 GiB free floor above them. All PRIOR until
M1 measures.

Upgrade: build; `--dry` and `--startup-check --staging`; `systemctl stop` (flush); repoint the
symlink; start; verify boot line, `/health`, one heartbeat. Rollback: repoint and start; no alert
state to migrate. A rank segment version bump is one-way: the old binary refuses new segments and
the wallet family goes cold rather than reading them, which is safe and goes in the release note.

## 8. M1 acceptance checklist

Run in the SCOPE M1a/b/c order. M1a proves collection/capture and durable paid-stream stopping;
M1b proves one real staged volume card and exact freshness expiry; M1c completes the fault drills
below. No production alerts before M2b. Store size estimates must be replaced by measurements
for ordered deltas/checkpoints, not reused from the retired cumulative-row layout.

Spend drills include active mint+account shutdown, kill during reservation, corrupt ledger,
same-day restart and a backward clock step. Capture/replay fixtures include a boot boundary,
config reload, response tape, initial checkpoint and silent interval ending the report. Delivery
reports show received candidates, successes/unknowns, cap/age drops and age percentiles per kind.

Run against staging webhooks and a recorded fixture; state rate and window beside every number.

1. Fixture: every captured frame decodes as pinned; a renamed field fails; unknown fields tolerated and counted. Pass: 100%.
2. Rate: one hour against a simultaneous reference (client messages; RPC signatures for five mints). Pass: events/min ±5%.
3. Disconnects: 10 forced closes, 45 s each. Pass: 10 reconnects, 10 outage identities, `sockets_open[provider]` never >1, 0 duplicates after replay.
4. Replay: a 10-minute capture three times. Pass: 0 accepted after pass one, `dup_dropped = 2 × accepted`, ring totals unchanged.
5. Soak 24 h: `watchdog_false_positives ≤ 2`; every watchdog reconnect heals >0 or is counted; no reconnect while the probe says quiet.
6. Gate 24 h at 0.5 creates/s: `subs_live == subs_desired ≤ max_subs`; no entry older than TTL + 2 reconcilers; `unsub_failed = 0`; every refusal counted.
7. Allocation benchmark: 10k frames after warmup; record allocator delta and rate. Zero trade-path allocations remains the M1c optimization target; this benchmark does not block the M1b staging demonstration. Production sizing records any remaining allocation cost.
8. Heal: 60 s gap over 10 mints with RPC truth. Pass: ≥90% recovered in budget, 100% of over-budget gaps declared, no healed event in a live bucket.
9. Generations: 1k stale frames after reconnect. Pass: 100% dropped, exact counter, no ring change.
10. Buckets: trades at t0+14.999 s and t0+15.001 s; +5 s wall-clock step. Pass: buckets 0 and 1, the step moves nothing, the late event is counted and excluded.
11. Ban: three <5 s sessions on a virtual clock. Pass: zero sockets for 3600 s, one retry after expiry, failure re-bans.
12. Fuzz: 1M mutated frames. Pass: no panic, RSS bounded, exact decode-error count, no byte retried.
13. Cost: `msgs_charged_total/10_000 × 0.01 = sol_spent_today` ±0.1%; projection ±20% of the next hour.
14. Ops: `--startup-check --staging` proves POST, PATCH and permission on all three; `--dry` prints valid payloads; a revoked token exits 3 naming the key, not the URL.
15. Capture: one hour recorded across a forced 10-minute disconnect. Pass: frames round-trip byte-identically, the manifest gap matches the disconnect within 1 s, a ±5 s wall-clock step reorders nothing, and a queue saturation for 60 s writes an exact drop count without changing `frames_dropped`.

## 9. Three reliability improvements, with cost

| Improvement | Gain | Cost |
|---|---|---|
| Shadow verifier moved from M2b to M1 as a 20-mint RPC sample per 10 min | Turns `unverified` into a measured p50/p90 and makes a quiet-market claim possible on samples | ~2,900 RPC calls/day, one sampling job, ~3 days build; still cannot prove per-trade completeness |
| Bot-token readback for status and heartbeat only | One outage card per channel across restarts; heartbeat edits survive a restart; no duplicate CTO card | Second credential, Read Message History + Manage Messages in three channels, one `GET` of 100 messages at boot, a wider redaction surface |
| RAM-only 15-minute frame ring (signature hash + length, no payload) | Post-incident estimate of messages lost between heartbeats, still nothing on disk and off the alert path | ~40 MB RAM, a privacy decision, and a new bounded-ring code path that must never feed a detector |
| Warm-standby second provider (M6) | Removes the single-provider outage for volume and wallets; a switch keeps warm rings and loses seconds, not four minutes | A paid provider, own decode, ~2× pipeline CPU, ~60 MB RAM, two feeds to monitor, and a copyleft dependency gate (`failover.md` §13–14) |

## 10. Open questions

| # | Question | Recommendation |
|---|---|---|
| 1 | Four docs reference `sources.md` | Resolved 2026-10-25: `sources.md` written |
| 2 | Do edits consume a daily slot? (`delivery.md` Q1) | No, hourly only, as implemented; needs a one-line SCOPE amendment |
| 3 | Which webhook carries the heartbeat? | The healthiest one at boot, named in the boot line; volume by default |
| 4 | Should SIGHUP re-arm a red stage? | No. Red is terminal until restart; a config edit must not reset a crash count |
| 5 | Is `near_graduation` a real lifecycle state? | Keep it display-only at 0.80 curve fill or drop it; no detector may read it |
| 6 | One outage card or N identities after N disconnects? (`feed.md` Q3) | Count N identities; without readback post one card and N log records |
| 7 | Expose `/health` beyond loopback? | Not in v1. If remote, terminate TLS and auth at a reverse proxy; never bind publicly |
| 8 | Rollback across a rank segment format bump | Refuse in the release note; the old binary must go cold, never guess at new rows |


