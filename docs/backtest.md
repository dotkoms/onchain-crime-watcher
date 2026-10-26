# Backtesting — capture and replay

Status: v1 design, 2026-10-25. Contract: [`../SCOPE.md`](../SCOPE.md) revision 5 (§3, §9, R10).
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

Raw WebSocket frame bytes, exactly as received, **before decode and before dedup**, plus the
receive time and the session identity. Decode happens at replay with the same adapter, so a
decoder change is testable against old bytes and a schema drift stays visible.

| Captured | Why |
|---|---|
| raw frame bytes | Decode, field-presence and schema tests; no lossy re-encode |
| `recv_mono_ns` | The live bucketing input (R1). Replay must reproduce it exactly |
| `recv_wall_ns` | Human time for reports; never a routing input |
| `seq` (per source, monotonic) | Arrival order survives compression and segment rotation |
| `generation`, `sub_gen` | Replay reproduces generation drops and superseded subscriptions |
| `source` (`pumpportal` \| `secondary`) | Two-source replay from M6; one stream today |
| session lifecycle records | Open/close/ban/probe/reconnect, so replay has a health timeline, not just frames |
| config hash | Provenance. The config itself is not stored, and secrets never are |

Not captured: decoded events, alert candidates, message ids, dedup state, config values, secrets.
A replay re-derives all of it.

**Why pre-dedup.** A recording of accepted events would bake in the live dedup window and could
never test it. Replaying raw frames exercises dedup, generations, late trades and the gate in the
same order they occurred.

## 2. Recorder

### 2.1 Tap and ordering

The frame path is already `session → Frame` on a bounded channel. The pipeline clones the
`Bytes` handle into a `capture` channel (capacity 4096, PRIOR; class `event`, drop with a
written marker). A cloned `Bytes` is a refcount, so the tap costs no copy on the hot path; the
writer owns compression and disk.

The session assigns `seq` in receive order. Replay orders by `(segment, seq)` and never by
`recv_wall_ns`: wall clocks step, and a replay that sorts by wall time is not the live path.

### 2.2 Files

```
capture/<source>/<YYYY-MM-DD>/seg-<HH>.ocwcap     zstd-framed records
capture/<source>/<YYYY-MM-DD>/seg-<HH>.idx        8 B per record: seq, offset, len
capture/<source>/<YYYY-MM-DD>/manifest.json      counts, coverage, hashes, config
```

Segment header (32 B): magic `OCWC`, version u8, source u8, flags u16, day_start_ms i64,
first_seq u64, crc32c of the header. Record:

```text
[len u32][crc32c u32][seq u64][recv_mono_ns u64][recv_wall_ns i64]
[generation u32][sub_gen u32][kind u8][payload len bytes]
```

`kind` is `Frame`, `SessionOpen`, `SessionClose`, `Ban`, `Probe`, `CaptureDrop`. One transaction
can hold several trades; they are separate frames or separate records and keep their order.

Manifest, fsynced at segment close and every 60 s (PRIOR): `source`, `day`, `seq` range, frame
count, byte count, sha256 of the segment, config hash, feed mode, and a list of
`{from_seq, to_seq, reason}` **capture gaps**.

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
~175 MB/day raw, ~60–90 MB/day at zstd-3 (PRIOR: 2–3× on base58-heavy JSON). Fourteen days is
~1.2 GiB; the cap is 2 GiB, which leaves room for a busier measured rate before retention
shortens. If M1 measures 1,500 msg/s sustained instead of ~5.8, the cap binds and capture
prunes, never the alert path.

**Security.** Capture holds attacker-controlled token names, symbols and URIs verbatim. Files
are `0600` under `OCW_DATA_DIR`, never logged, never served by `/health`, and treated as
untrusted input at replay — the same decoder and fuzz corpus apply.

## 3. Replay harness

```
ocw replay --from 2026-11-01 --to 2026-11-07 --config <toml> --out <dir>
           [--source pumpportal|secondary] [--train 5d] [--simulate-delivery]
```

The harness links the production crates and calls the same adapter decode, dedup, registry,
gate, detectors and delivery **admission** code. It does not reimplement them; a replay that
shares no code with the live path proves nothing about the live path. It writes only under
`--out`, refuses to open the live `OCW_DATA_DIR`, and never emits an alert.

### 3.1 Virtual clock

Every time read in the pipeline goes behind a `Clock` trait. Live uses `Instant`; replay uses
`VirtualClock`, anchored at the first record and advanced to each record's `recv_mono`. Bucket
closes, cooldowns, budget refills, hold expiry and watchdog thresholds all read the same clock.
The 100 ms ticker becomes a virtual tick driven by the record timeline, not a wall timer.
Randomised jitter in the supervisor is disabled or seeded from a fixed seed; nothing in decode
or detection uses RNG.

### 3.2 Determinism

A report is only evidence if it is reproducible. Rules:

- Integer accumulation in the trade path, as in `volume.md`; floats only at the same points the
  live code uses them.
- No `HashMap` iteration in ordering decisions; sort keys are total and stated.
- Segment order, seq order, and a fixed seed for anything random.
- The output carries `config_hash`, `source`, the segment sha256 list and a `report_sha256`.
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

Decode + detect is 5–15 µs per trade typical (`SCOPE.md` §4). One 500,000-frame day is ~5–8 s of
CPU plus zstd decompression; a seven-day report is under a minute. Replay is an offline binary
path, not a production task; it shares the frame channel capacities so RSS stays bounded by the
same caps as live.

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
| B4 | Replay a 1 h fixture three times | `report_sha256` identical; candidate counts identical; no wall-clock or RNG dependency |
| B5 | Replay the same fixture under a ±5 s wall-clock step | Identical output; bucket assignment unchanged |
| B6 | Replay a day that overlaps a capture gap | Report marks `unrecorded` over the window; no rate computed over it |
| B7 | Delivery simulation with a full budget | Same admissions, drops and order as `delivery.md` §3; no network, no ids |
| B8 | Fitted key with no `tested_on` | Boot fails naming the key; with a hash that does not exist, boot fails naming the hash |
| B9 | Sweep 25 configs | Run refused at `backtest.max_sweeps`; the report lists every run, not only the best |
| B10 | Forward mark over an alert whose mint never trades again | Censored and counted; no zero return invented |
| B11 | Two-source capture (M6) | Both streams replay independently; `--source` selects one; a combined run is refused |
| B12 | Capture byte cap reached | Whole oldest day pruned first, then capture stops; neither the feed nor the rank store writes fail |

## 10. Open questions

| # | Question | Recommendation | Falsified by |
|---|---|---|---|
| 1 | Capture on by default in production? | Yes. It is the only path to evidence and it holds public chain data; make the opt-out a recorded release-note line | A disk or privacy constraint the operator states before M1 |
| 2 | Frame size and compression ratio | Measure in M1 C1 and replace the 350 B / 2–3× estimate | Measured bytes/day outside 60–300 MB |
| 3 | Replay of delivery admission | Keep it, but never claim delivery latency; 429s and readback are not simulated | A production incident where the simulation's order differs from the live order |
| 4 | Forward marks | Keep as a censored outcome proxy, never as precision | A user reads it as a hit rate; rename the report field |
| 5 | Two non-overlapping windows before a default changes | Keep; it is the only defence against one lucky week | A measured regime where two weeks cannot be collected before a launch |
| 6 | Minimum seven days | Keep for M2b; one day may be enough for a decoder test, never for a threshold | A week where the recorded rate does not match the next week within tolerance |
