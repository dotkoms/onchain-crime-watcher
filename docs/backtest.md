# Backtesting — capture and replay

Status: v1 design, 2026-10-25. Contract: [`../SCOPE.md`](../SCOPE.md) revision 6 (§3, §9, R10).
This file reverses revision 4's "no backtest harness" and defines what replaces it. Siblings:
`feed.md`, `volume.md`, `ranking.md`, `bundle.md`, `delivery.md`, `operations.md`, `failover.md`.

Tags: **CONTRACT** = fixed by SCOPE; **PRIOR** = a guess with its reason, revisited on live data.

## 0. Position, stated before any number

A threshold swept over data the bot never recorded is a preference dressed as a result. The
harness below does not change that. It narrows the gap in one direction only: it replays **what
the bot actually received**, including the feed's gaps, duplicates, late frames and decode
failures, so a parameter can be judged against the stream that produced it.

Three consequences follow, and they are the whole contract:

1. **The recording ships before the harness.** Capture starts in M1; no replay exists before there
   is a stream to replay.
2. **A replay never sees what the feed dropped while connected.** It replays receive time, not
   chain truth. Silence in a recording is the absence of a frame, never evidence of a quiet market.
3. **Nothing the alert path does depends on either artifact.** Capture is best-effort and counted;
   a missing window makes the report say `unrecorded`, never zero.

What a v1 backtest is *not*: paper trading, execution simulation, a PnL engine for hypothetical
trades, or an estimator of precision and recall. Those need outcome labels the bot does not have.
It is a deterministic re-run of the live decision path over a recorded arrival history.

## 1. What is recorded

Capture raw frames pre-decode and pre-dedup, and record every external input that can change a
decision. Revision 6 distinguishes a byte/decoder fixture from a complete decision replay.

| Captured | Why |
|---|---|
| Raw frame bytes, source, per-source seq, session generation and subscription generation | Exercise decode/dedup against the original arrival history |
| Boot id, monotonic offset, receive wall time, logical application sequence | Durations survive process restarts; cross-source/control ordering is explicit |
| Session lifecycle, subscription commands/results, watchdog/probe observations, timer ticks, UTC-day rollovers | Reproduce health, gate, cap and timer decisions |
| Sanitized output-affecting config snapshot at boot and each accepted reload | A hash cannot reconstruct parameter values; secrets and credential URLs are excluded |
| Initial registry/rings/cohorts, dedup/cooldown/budget state, authority state, watch set and live position state, or a complete cold-boot prefix | An arbitrary date slice does not start with empty live state |
| RPC/HTTP request identity, response or error, and application sequence/time | Reproduce heals, funding, fees, balances, CTO lists, prices and SOL/USD inputs without live network access |
| Delivery result classes and entry-success callbacks, if reproducing live pairing | Observed opening and delivered entry are different facts |
| M6 authority decisions and boundary records | Audit switches and compare with independently replayed controller decisions |

Store snapshots as versioned, checksummed side artifacts referenced by the manifest. A bounded
recorder may miss an input; that interval becomes `unrecorded`, and a complete-decision report is
refused. A decoder-only fixture may still run with its narrower claim. RPC responses and captured
metadata remain untrusted, size-bounded inputs. Never store secrets, webhook tokens, authorization
headers or credential-bearing endpoint URLs.

Capture does not persist live message ids for recovery. A replay uses logical delivery-result
events, not Discord ids. The production alert path never reads capture files.

**Replay scope.** Detector/delivery threshold comparisons hold the observed subscription universe
fixed. Changing gate selection, watch-set membership or provider filters can require trades that
were never recorded. Such a run is labelled `unsupported_counterfactual`, cannot justify fitted
defaults and never treats absent data as zero. A report over existing arrivals is not an
evaluation of the market coverage a different gate would have bought.

## 2. Recorder

### 2.1 Tap and ordering

The frame path is already `session → Frame` on a bounded channel. The pipeline clones the
`Bytes` handle into a `capture` channel (capacity 4096, PRIOR; class `event`, drop with a
written marker). A cloned `Bytes` is a refcount, so the tap costs no copy on the hot path; the
writer owns compression and disk.

The session assigns per-source `seq` in receive order. Each effective state input also carries
a process-wide logical application sequence; ticks, control inputs and source inputs participate
in that ordering. Live application of a switch is a barrier, not a race between pipelines.
Replay follows boot order and application sequence, never a sort by wall time. Durations use
boot-local monotonic offsets; captured UTC-day rollover events drive daily budget resets.

### 2.2 Files

```
capture/<source>/<YYYY-MM-DD>/seg-<HH>.ocwcap     zstd-framed records
capture/<source>/<YYYY-MM-DD>/seg-<HH>.idx        rebuildable sequence/offset index
capture/<source>/<YYYY-MM-DD>/manifest.json      counts, coverage, hashes, config
```

Segment headers carry magic `OCWC`, schema version, source and boot identity. Records are
length-bounded and checksummed, with per-source receive sequence, logical application sequence,
boot-local monotonic offset, wall timestamp, generation, subscription generation, kind and
payload. Indexes are rebuildable. M1 pins the actual binary encoding and byte sizes with
round-trip fixtures; this plan does not prescribe conflicting hand-counted offsets.

The manifest references segment hashes, sanitized configuration versions, initial checkpoint
hashes, UTC-day rollovers, supported replay modes and missing-input intervals. Seal and sync at
segment close; sync progress every 60 s (PRIOR). A missing snapshot or response is visible evidence
loss, never an implicit default.

### 2.3 Loss, crash, retention, disk

| Event | Behaviour | Counter |
|---|---|---|
| Capture queue full | Drop the record, write `CaptureDrop` with the seq range, never block the pipeline | `capture_dropped_frames` |
| Writer behind | Same; the marker is the truth, not a silent hole | `capture_drop_intervals` |
| Crash mid-segment | Boot drops the torn tail, rebuilds `.idx`, writes `capture_truncated_segments` | `capture_recovered_bytes` |
| Disk below `ops.disk.min_free_bytes` | Stop capture before the writer risks the feed or the side stores | `capture_stopped_disk` |
| Byte cap reached | Prune whole oldest days first; if still over, stop capture | `capture_stopped_cap`, `capture_pruned_days` |
| Clock step | `recv_mono` is monotonic; a step is visible in `recv_wall` only and never reorders | — |

**Sizing.** At the 0.5 SOL/day budget the stream is ~500,000 messages/day. A trade frame is
~350 B raw (PRIOR: the observed payload in `sources.md` §3.1, base58 addresses dominate), so
~175 MB/day raw, ~60–90 MB/day for trade frames alone at zstd-3 (PRIOR: 2–3× on base58-heavy JSON).
Config, checkpoints, free streams and external-response tapes add measured overhead. Trade-only fourteen days is
~1.2 GiB; the cap is 2 GiB, which leaves room for a busier measured rate before retention
shortens. If M1 measures 1,500 msg/s sustained instead of ~5.8, the cap binds and capture
prunes, never the alert path.

**Security.** Capture holds attacker-controlled token names, symbols and URIs verbatim. Files
are `0600` under `OCW_DATA_DIR`, never logged, never served by `/health`, and treated as
untrusted input at replay — the same decoder and fuzz corpus apply.

## 3. Replay harness

```
ocw replay --from 2026-11-01 --to 2026-11-07 --config <toml> --out <dir>
           [--mode decoder|decisions|controller] [--source pumpportal|secondary]
           [--train 5d] [--simulate-delivery]
```

`controller` mode requires both source tapes plus control order; it rejects `--source` because
arbitration cannot be checked from one feed. `decisions` mode requires the complete input/state
manifest. `decoder` mode explicitly makes no full-decision claim.

The harness links the production library and calls the same adapter decode, dedup, registry,
gate, detectors and delivery **admission** code. It does not reimplement them; a replay that
shares no code with the live path proves nothing about the live path. It writes only under
`--out`, refuses to open the live `OCW_DATA_DIR`, and never emits an alert.

### 3.1 Virtual clock

Every duration read uses a `Clock` boundary. Restore the initial checkpoint or replay the full
cold-boot prefix, then apply recorded inputs in logical application order. The virtual scheduler
executes **all** due bucket closes, hold expiries, budget refills and watchdog ticks, including
silent intervals and the final requested report boundary. It does not wait for another trade
before advancing a timer. Recorded boot boundaries reset monotonic epochs explicitly; captured
UTC-day rollovers control daily counters.

External clients are replaced by offline response tapes matched on request identity and
application order. A missing response, incompatible checkpoint or unknown config version refuses
complete-decision mode. No request falls through to a live endpoint. Randomized supervisor
jitter is fixed-seed; the seed, code revision, schema versions and initial-state hashes are
report inputs.

### 3.2 Determinism

A report is only evidence if it is reproducible. Rules:

- Integer accumulation in the trade path, as in `volume.md`; floats only at the same points the
  live code uses them.
- No `HashMap` iteration in ordering decisions; sort keys are total and stated.
- Boot order and logical application order, including cross-source/control inputs, and a fixed seed for anything random.
- The output carries code/schema versions, sanitized config history, checkpoint and response-tape
  hashes, source set, segment hashes and a `report_sha256`. The hash excludes wall-clock run time
  and the hash field itself.
  Ten runs of the same input produce the same hash; a mismatch is a bug in the harness, not
  variance.

### 3.3 Outputs

| File | Contents |
|---|---|
| `decisions.jsonl` | One line per candidate: family, verdict, inputs (ratio, baseline, MAD, takers, tier), suppression reason, virtual time, mint, wallet |
| `summary.json` | Counts by family/verdict/suppression, budget survival, coverage denominators, forward marks with `n` and censoring, fitted-config provenance |
| `report.md` | Human report: window, days, frames, coverage, train/test split, every sweep run, and the five things it cannot answer |

`--simulate-delivery` runs the real admission algorithm (dedup, cooldown, token bucket, daily
caps, hold expiry, priority) against a null transport. It answers "which of these candidates
would have posted", not "would Discord have accepted them". It never sends, never edits, never
retries, and writes no message ids.

### 3.4 Cost

Replay throughput is measured in M2a and M2b with the fixture size, external-input volume,
storage format and code revision stated. The previous under-one-minute estimate omitted state
restoration and external inputs and is withdrawn. Replay is an offline binary path; it reads
bounded streams and never opens the production data directory.

## 4. What a replay can and cannot answer

| It can answer | It cannot answer |
|---|---|
| How many candidates, fires, nears and suppressions this config produces on this arrival history | Precision, recall, hit rate, or base rates — no outcome labels exist |
| Which gate, floor, cohort cell or budget rule produced each outcome | What the feed dropped while connected; the recording is the arrival history, not chain truth |
| Whether the delivery budget would have admitted a candidate, in priority order | Whether Discord would have delivered it, or how long admission really waited |
| Threshold sensitivity **within the recorded window**, with the train/test split stated | How the thresholds behave in a regime the recording never contained |
| Dedup, generation, gate and cap behaviour on real duplicate/replay patterns | Upstream network latency or vendor behaviour outside the recorded days |
| A forward mark for a volume alert from later trades in the same recording, where the mint kept trading | The token's value after the recording ends, off-feed venues (Raydium), or a wallet's true PnL |
| Two-source switch decisions and duplicate counts from a two-source capture (M6) | Whether the secondary was complete when it was not carrying the window |
| Exit-alert pairing and budget survival from a wallet replay (M5) | Whether a watched wallet's exit was profitable after transfer-out or off-feed sales |

**Forward marks, precisely.** For a volume fire at virtual time `t` on mint `m`, the report may
compute the mint's implied price at `t + 5 min` and `t + 1 h` from later trades in the recording,
and report the distribution **only over alerts whose mint has at least one trade in the horizon**.
The denominator, the number censored by capture gaps or silence, and the venue mix are printed
beside it. This is an outcome proxy on a selected subset, never precision, and no default
threshold may be fitted to it without the train/test protocol in §5.

**No quiet-market claims.** A replay with no candidates in a window says "no candidate was
emitted", not "the market was quiet" and not "the detector works".

## 5. Anti-overfitting protocol

The harness is only useful if it is harder to fool than a sweep.

1. **Pre-register the hypothesis.** Before a run, write down the key, the direction, the metric,
   the train window and the test window. The report lists every configuration run, not only the
   best (`sweeps_total`, `sweeps_reported`).
2. **Split, don't reuse.** Tune on the first `backtest.train_share` of the recorded days; report
   on the remainder. A key fitted and tested on the same days is recorded as `in_sample` and
   cannot justify a default.
3. **Minimum evidence.** Fewer than `backtest.report_min_days` recorded days or
   `backtest.min_candidates` candidates yields `insufficient_evidence`, not a recommendation.
4. **Provenance on the key.** A changed default carries:

   ```toml
   [provenance.volume.fire_ratio]
   fitted_on = "2026-11-01..2026-11-05"
   tested_on = "2026-11-06..2026-11-07"
   fitted_at = "2026-11-08"
   report_sha256 = "…"
   prior = 3.0
   ```

   A fitted key with no `tested_on`, or whose report hash does not exist on disk, **fails boot**.
   The boot line and `/health` print `fitted_keys` and the count. This is the same rule as "an
   invented constant is a bug with a comment", enforced.
5. **Defaults stay priors until forward-tested.** A replay over one week is evidence about that
   week. A fitted key may be set in a deployment's config with complete provenance after one
   held-out window; the **shipped default** changes only after a second, non-overlapping recorded
   window agrees, and the change gets a dated SCOPE entry rather than a silent edit.
6. **One decision per change.** A sweep that moves fire ratio, floor and warmup together cannot
   attribute the effect; the report says so and the change is split.

## 6. Reports and milestone gates

| Gate | Requirement | Blocks |
|---|---|---|
| M2b volume | ≥ 7 recorded days, ≥ 30 candidates, train/test split, `report.md` in the release note | First production volume alerts. A waiver is allowed only in writing in the release note |
| M5 wallets and exits | Replay of the rank store and live matching over the recorded window; watch-set stability, entry/exit pairing, budget survival, coverage denominators | M5 acceptance |
| M6 failover | Two-source capture; forced-failure timeline replayed; switch decisions, duplicate counts, boundary drops, warm-ring state | M6 acceptance |

A report that cannot meet its minimum says `insufficient evidence` and lists the missing
denominator. It never fills the gap with a recommendation.

## 7. Configuration

All keys `capture.*` and `backtest.*`; unknown keys fail boot. PRIOR numbers carry their reason.

| Key | Unit | Default | Rationale |
|---|---|---|---|
| `capture.enabled` | bool | true | The product's only path to evidence; opt-out must be recorded in the release note |
| `capture.dir` | path | `data/capture` | Inside `OCW_DATA_DIR`; parent writable |
| `capture.retention_days` | days | 14 | Two weeks covers a weekly job plus a held-out day |
| `capture.max_bytes` | bytes | 2 GiB | ~1.2 GiB expected; headroom before retention shortens |
| `capture.queue_capacity` | records | 4096 | ~1–2 s at peak; the tap must never backpressure decode |
| `capture.flush_ms` | ms | 1000 | Bounds a crash to one second of records plus the tail |
| `capture.manifest_sync_ms` | ms | 60000 | Manifest is metadata, not the alert path |
| `capture.compression` | enum | `zstd-3` | 2–3× on base58 JSON; level 3 keeps replay CPU low |
| `capture.sources` | enum | `primary` | `both` from M6; doubles disk and is for failover reports only |
| `backtest.report_min_days` | days | 7 | One weekly cycle; fewer cannot test a weekly job |
| `backtest.min_candidates` | count | 30 | Below this a rate is noise |
| `backtest.train_share` | fraction | 0.7 | First 70% tune, last 30% report |
| `backtest.max_sweeps` | count | 20 | Bounds multiple comparisons and run time |
| `backtest.forward_mark_5m_s` / `_1h_s` | s | 300 / 3600 | Horizons a reader can interpret |
| `backtest.report_sha256_required` | bool | true | A report without a hash cannot justify a key |
| `provenance.<key>.{fitted_on,tested_on,fitted_at,report_sha256,prior}` | table | — | Required for any key whose value differs from its prior; an incomplete block fails boot naming the missing field |

## 8. `/health` and counters

| Field | Meaning |
|---|---|
| `capture.state` | `off \| writing \| stopped_disk \| stopped_cap \| error` |
| `capture.bytes_today`, `capture.bytes_total`, `capture.segments`, `capture.oldest_day` | Disk truth |
| `capture.frames_written`, `capture.dropped_frames`, `capture.drop_intervals`, `capture.truncated_segments` | Capture loss, never hidden |
| `capture.coverage_bp_24h` | Frames written vs frames accepted by the feed, not vs chain truth |
| `backtest.last_report_ms`, `backtest.last_report_sha256`, `backtest.last_result` | Latest offline run |
| `backtest.fitted_keys`, `backtest.in_sample_reports` | Provenance warnings on the boot line and heartbeat |

`capture.coverage_bp_24h` is the honest denominator: it says the recording matches what the bot
accepted, not that the bot accepted everything. `capture_dropped_frames > 0` is amber, because a
report over that window must state the hole.

## 9. Acceptance tests

| # | Test | Pass condition |
|---|---|---|
| B1 | 24 h capture with a 10-minute forced disconnect | Frames round-trip byte-identically; manifest gap matches the disconnect within 1 s; no record reordered by wall-clock step |
| B2 | Kill -9 mid-segment, restart | Torn tail dropped and counted; `.idx` rebuilt; every earlier record decodes |
| B3 | Capture queue saturated for 60 s | `capture_dropped_frames` exact, the marker written, the feed's `frames_dropped` unchanged |
| B4 | Replay a 1 h fixture ten times | `report_sha256` identical; candidate counts identical; no wall-clock or RNG dependency |
| B5 | Replay the same fixture under a ±5 s wall-clock step | Identical output; bucket assignment unchanged |
| B6 | Replay a day that overlaps a capture gap | Report marks `unrecorded` over the window; no rate computed over it |
| B7 | Delivery simulation with a full budget | Same admissions, drops and order as `delivery.md` §3; no network, no ids |
| B8 | Fitted key with no `tested_on` | Boot fails naming the key; with a hash that does not exist, boot fails naming the hash |
| B9 | Sweep 25 configs | Run refused at `backtest.max_sweeps`; the report lists every run, not only the best |
| B10 | Forward mark over an alert whose mint never trades again | Censored and counted; no zero return invented |
| B11 | Two-source capture (M6) | `--source` runs a single-source detector audit; controller mode replays both isolated pipelines plus control inputs in application order, reproducing FO9 without merging trade histories |
| B12 | Capture byte cap reached | Whole oldest day pruned first, then capture stops; neither the feed nor the rank store writes fail |

| B13 | Slice starts with open positions and warm rings | Checkpoint/prefix restores the same decisions; missing state refuses complete-decision mode |
| B14 | Captured RPC response, reload, UTC midnight and process restart | Offline run reproduces results; missing response refuses; no live network request |
| B15 | Ten-minute silence then report ends | All due ticks and expiries run before report close |
| B16 | Sweep a gate or watch-set change needing uncaptured trades | `unsupported_counterfactual`; no fitted default or invented zero |
| B17 | Two boots with overlapping monotonic offsets and a wall-clock step | Boot/application order is preserved; duration and daily-reset semantics match capture |

## 10. Open questions

| # | Question | Recommendation | Falsified by |
|---|---|---|---|
| 1 | Capture on by default in production? | Yes. It is the only path to evidence and it holds public chain data; make the opt-out a recorded release-note line | A disk or privacy constraint the operator states before M1 |
| 2 | Frame size and compression ratio | Measure in M1 C1 and replace the 350 B / 2–3× estimate | Measured bytes/day outside 60–300 MB |
| 3 | Replay of delivery admission | Keep it, but never claim delivery latency; 429s and readback are not simulated | A production incident where the simulation's order differs from the live order |
| 4 | Forward marks | Keep as a censored outcome proxy, never as precision | A user reads it as a hit rate; rename the report field |
| 5 | Two non-overlapping windows before a default changes | Keep; it is the only defence against one lucky week | A measured regime where two weeks cannot be collected before a launch |
| 6 | Minimum seven days | Keep for M2b; one day may be enough for a decoder test, never for a threshold | A week where the recorded rate does not match the next week within tolerance |

