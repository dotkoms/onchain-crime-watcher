# Real-data-only operation

The application has no demo mode, generated trade stream, packaged sample signal, or automatic
provider connection. Starting it without a source prints `waiting_for_data` and exits. Empty
state means no observations: trade count is zero and last-trade time is null. Credential
environment variables are ignored in status and recorded-capture replay modes.

## What works now

```sh
cargo run --locked
cargo run --locked -- --config config/local.toml --check-config
cargo run --locked -- --replay-capture /absolute/path/to/recorded.ocwcap
cargo run --locked -- --replay-capture /absolute/path/to/recorded.ocwcap --json
```

Replay reads an existing OCWC capture without changing it, uses its recorded monotonic receive
times, and streams payloads through the same exact decoder and volume engine. JSON output
includes bucket decisions and eligible card previews; human output labels previews as historical.
Neither mode posts a message. A segment missing launch/history stays cold or unknown; it does
not invent a creation time, trades, or a baseline. An unfinished final bucket is not evaluated.
Checksums, decompression bounds, monotonic clock and boot identity are checked. Torn/corrupt
captures fail clearly; replay does not repair or silently truncate input. One invocation is
limited to one segment, 30,000 records and 20,000 evaluated buckets. This is diagnostic replay,
not the complete M2a controller/delivery/backtest harness.

The observed create-only fixture is retained for regression tests with documented provenance.
Artificial edge cases exist only inside automated tests; no synthetic fixture is supplied for
application use. A create-only capture correctly yields zero trades and zero volume alerts.

## Volume core

The core consumes normalized events and a monotonic receive clock, with no credential or network
dependency. Scope §2A priors are implemented: create-anchored 15-second buckets, 30-bucket ring,
top-three-trimmed baseline, 16-bucket warmup, eight non-empty buckets, six trades, five distinct
takers, at most 60% concentration, median/MAD dispersion, age-banded floor and preceding-sample
p90, and a labelled absolute cold-baseline path. Near expires at 15 seconds and fire at 30 seconds,
including the exact expiry boundary. A failed fire test does not silently become a near.

Lamport totals use checked integers. Statistical comparisons use f64; even-sized medians average
the middle two observations and p90 uses nearest rank. The current bucket is excluded from its
own history and cohort comparison. Cohorts separate curve/pool regimes and retain at most 1,024
prior ratios and trade sizes per age band. These are estimates over the watched subset, not
market-wide percentiles. A cold baseline uses the absolute 10× floor rule, without claiming a
meaningful ratio/p90. Candidates are previews, not validated predictions.

State is bounded to 256 mints, 256 takers and 1,024 trades per open bucket. Saturation or numeric
overflow invalidates that bucket and re-warms its history. Missing/unresolved SOL invalidates
open buckets conservatively. Gaps and session replacement discard partial observations and
re-warm; migration resets history once into the pool regime. Unknown launch history suppresses
the signal and is counted. No metadata name or URL enters an alert; the base58 mint is canonical.
Every card disables mentions and states coverage is unverified and delivery has not occurred.

## Connect real data later

The existing PumpPortal adapter is ready behind `--live`; it is never selected by default:

```sh
# Free collection, real observations only; no paid trade subscription.
cargo run --locked -- --live --config config/local.toml --run-for-secs 30

# When deliberately enabled later: one configured mint, with a durable spend ledger.
cargo run --locked -- --live --config config/local.toml --enable-paid --preview-volume --run-for-secs 300
```

`--preview-volume` wires live decoded events and real one-second timer ticks to the same core and
prints fresh candidates to stdout/capture; health exposes active mints, warmup, candidate,
unknown-history and saturation counters. Preview alone sends no signal webhooks. The separately
opted-in staging sender is described in [staging-delivery.md](staging-delivery.md). The existing optional
status/outage webhook can still post on a live collector failure if separately configured.
The stdout preview path is diagnostic and has not been benchmarked as a production hot path.

The paid window defaults to 10 seconds, which cannot establish the four-minute volume baseline.
For a deliberately enabled later observation, configure `feed.paid_window_s` up to 300 seconds,
select one mint, and start before its creation is observed. Attaching to an already-created mint
does not fabricate launch history and therefore cannot immediately emit volume alerts. A free
stream alone cannot establish a trade-volume signal.

The executable reads environment variables, not `.env` files automatically. Set a replacement
`OCW_PUMPPORTAL_KEY` through your local/deployment secret manager when ready; no wallet signing
key is required. Initialize/reconcile the spend ledger explicitly as documented in [m1a.md](m1a.md).
Keep the chosen daily allowance in `config/local.toml`; the allowance is not an upstream hard
billing cap. Default capture needs 2 GiB of free disk and retains a separate 2 GiB owned-data cap.

## Still needed before operation

1. Provider credentials and funded data allowance, a selected mint/watch policy, sufficient disk,
   and a measured live paid-trade capture. Verify decoding, reference coverage and actual billing.
2. A staging Discord signal channel/webhook and real delivery acceptance. The staging sender is
   implemented with local expiry/rate/budget/ambiguous-POST tests; write permissions and a real
   live staged alert still need verification. The minimal outage sender stays separate.
3. M1c reconnect/watchdog/heal and supervision, fault drills, and measured runtime resource use
   before unattended deployment. No automatic restart loop for the current collector.
4. CTO, bundle and wallet families remain later scope milestones. Production volume delivery
   still needs the M2b evidence/backtest gate; passing unit tests does not satisfy that gate.

No new provider or webhook calls are made as part of this preparation.

## Verification evidence

Local checks on 2026-10-25 Eastern: 38 tests passed, none ignored; formatting, Clippy with
warnings denied, and the locked workspace build passed. RustSec found no known vulnerabilities
in 190 locked dependencies using advisory DB `21d909e4` (2026-10-24; 1,352 advisories).
Raising the fire cut-point from 3× to 5× caused the numerical spike contract test to fail;
the implementation was restored and the full suite passed. No dependencies were added.

The executable's default startup reported disconnected/waiting with no last trade. Read-only
replay of the earlier real 20-second provider capture processed 13 records / 7 received frames,
four creations, zero trades, zero decode errors and zero alerts; no new provider connection was
made. The capture had no trade history and no volume baseline, so no signal was claimed.
Artificial unit-test inputs prove boundary behavior only, not live coverage or signal quality.
