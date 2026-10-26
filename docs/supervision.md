# Task supervision and crash containment

The collector observes its capture writer, optional staging worker, and loopback health server
while collection runs. A completion cannot stay hidden until shutdown. Pipeline panics unwind
inside a bounded recovery boundary; no external error or panic payload is copied into health.
Default startup remains disconnected. No demo stream or external acceptance claim is added.

## Pipeline recovery

A panicked attempt is dropped before recovery, releasing its socket and local paid credits.
Decoder, dedup, and volume state are rebuilt cold. Capture records `pipeline_panic` with
`history: discarded_cold`; volume-engine statistics restart with that engine, while frame,
panic, and restart counters remain process-local totals. Unknown history never becomes a quiet
bucket. Newly admitted candidates wait for a fresh observed launch and normal warmup.

The restart policy permits five restarts in a rolling 600-second window. Backoff begins at
500 ms, doubles with jitter of up to 20% in either direction, and never exceeds 60 seconds.
An attempt lasting 600 seconds resets the backoff step. Three consecutive panics in attempts
that each lasted less than five seconds fast-fail to red. Red is latched for this run and does
not recover when the budget window passes.

Every attempt after a panic is free-only. Durable paid reservations remain consumed and paid
observation never resumes automatically. Transport reconnect credits live outside the attempt
and are not replenished by a pipeline panic. Transport failures still use the separate opt-in
[recovery policy](recovery.md); an ordinary provider error is not a panic restart opportunity.
The abandoned attempt's transport-backoff gauge is cleared during panic recovery.

While red, the pipeline admits no new signals and waits for shutdown or the configured run
limit. The health server remains available. Already-started external POSTs cannot be undone;
the existing durable intent and unknown-outcome rules still apply.

## Background workers

| Stage | Observed failure behavior |
| --- | --- |
| Capture | Error or panic latches the capture stage red. Live collection continues with capture loss/degradation visible. No automatic disk-worker rebuild is attempted. A cap or free-space stop remains a counted capture degradation, not a pipeline crash. |
| Staging delivery | Unexpected return or panic latches delivery red and closes admission. The collector does not replay uncertain writes or automatically reopen a failed journal/webhook. |
| Health server | Unexpected completion marks health red and stops collection with an error, because the local diagnostic endpoint is no longer available. |

`/health` includes `supervision.state`, per-stage states, pipeline panic/restart counts, and
`failed_stages`. Each stage's red transition is counted once. A red stage stays red through
shutdown; the collector returns an error instead of reporting success. Successful shutdown
reports the supervisor stopped. Capture flush and coverage remain separate outcomes; see
[capture shutdown](shutdown.md). A capture panic can leave unrecorded in-flight/queued records;
the exact loss may be unknown, and a red capture state is not complete history.

## Cancellation and diagnostics

Dropping the enclosing collector future aborts its async health service and signals the staging
worker to stop. Pending staging sends retain the existing conservative unknown-outcome rules.
Blocking capture, spend, staging journal-open and receipt workers retain directory ownership
until disk operations finish. A started blocking operation still cannot be forcibly cancelled;
the async shutdown deadlines are not hard OS/process guarantees.

The executable installs a redacting panic hook before reading CLI configuration. It emits a
fixed message without payload or source location. Stage failure events contain only fixed
stage names. Library hosts choose their own global panic hook and may call
`ocw_core::supervision::install_panic_hook()` before loading secrets.

## Bounds and remaining work

This implements containment and monitoring for today's collector stages. It does not implement
the complete planned JoinSet task topology, automatic rebuild of disk/delivery stages,
heartbeat/dead-man reporting, independent probes, gap healing, or the rest of M1c. Root/runtime
abort and OS termination cannot be recovered by an unwind boundary. External supervision is
still required for unattended operation. A test panic proves local failure behavior, not
provider completeness, signal quality, billing correctness, or real Discord acceptance.

## Verification

Local tests cover restart-window boundaries, jitter/caps, fast failure, red latching, a panicked
pipeline rebuilding cold/free-only with retained transport credits, clearing abandoned backoff,
health remaining available while red, live observation of capture I/O failure, staging fatal
exits, cleanup on cancellation, and panic-output redaction in an isolated subprocess.
Artificial inputs are confined to tests.

On 2026-10-26 Eastern, all 85 workspace tests passed with none ignored. Formatting,
Clippy with warnings denied, and the locked build passed. A fresh RustSec audit found no known
vulnerabilities in 190 locked dependencies (DB `21d909e4`, 2026-10-24; 1,352 advisories).
An off-by-one fast-fail mutation failed its policy test, was restored, and was followed by the
passing complete checks. The stale backoff-gauge regression failed before its reset was added.
No dependencies were added. Local builds disabled debug symbols to reduce disk use; repository
build profiles and quality constraints are unchanged. Real provider/Discord acceptance remains pending.
