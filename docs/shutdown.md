# Capture shutdown integrity

The collector gives capture shutdown one five-second async deadline. That deadline includes
admitting any final capture-loss marker and waiting for the blocking writer to drain, sync,
and seal its segment and manifest. Previously, a saturated queue could wait before the timer
started, and an I/O failure could still produce a successful collector result.

## Outcomes

`capture.shutdown` in local health and a successful final snapshot describes the shutdown
attempt separately from capture coverage:

| State | Meaning |
| --- | --- |
| `not_requested` | Collection is running; no shutdown attempt has started. |
| `draining` | Final loss-marker admission and writer completion are pending. |
| `complete` | The writer returned without an I/O error or panic before the deadline. |
| `failed` | The writer failed or panicked. The collector returns an error. |
| `pending` | The deadline expired. Disk work may still be running; the collector returns an error. |

An otherwise successful collection run with capture I/O failure or timeout now exits
unsuccessfully (CLI exit code 2), with a fixed error that does not expose paths or payloads.
An earlier feed error remains the primary error if both collection and capture shutdown fail.

`complete` does not mean every event was recorded. Queue loss, retention, disk-floor stops,
and capture-cap stops retain their existing states and loss counters. Capture remains a
best-effort tap and never backpressures live decoding. A normal seal has a closed manifest;
a failed or interrupted write can require torn-tail recovery on the next start.

The capture `shutdown` control record precedes flushing. Its payload uses `operation_clean`
for the collection result and `capture_flush: pending`; it cannot certify its own completed
flush. A manifest written by the draining worker may likewise show `draining`. The collector's
post-join snapshot is the completion observation, not a promise embedded before disk sync.

## Ownership and limits

The live collector's blocking capture worker retains the data-directory process lock through
all of its writes. Cancelling the collector, abandoning a join, or exceeding the async deadline
does not let another collector acquire that directory while the old writer is still alive.
This matches the existing spend-reservation and staging-journal ownership rules.

A started blocking disk operation cannot be aborted by Tokio. The async deadline bounds the
caller's wait, not an OS write or runtime teardown. A library host can still wait for blocking
workers when dropping its runtime; a hard process stop can leave an incomplete tail. Unattended
operation still needs external process supervision and a hard shutdown limit. This slice does
not implement task restart supervision, delivery flush guarantees, independent probes,
heartbeat/dead-man monitoring, or the full M1c shutdown contract.

## Local verification

Regression tests cover a real capture-directory I/O error and nonzero executable exit,
a full queue with an unconsumed final loss marker, a panicked writer, cancellation/abort of a
started capture worker's join while retaining exclusive directory ownership, and normal
byte-identical capture with a closed manifest. Artificial inputs exist only in isolated tests.
No provider connection or real Discord request is needed.

On 2026-10-26 Eastern, all 75 workspace tests passed with none ignored; formatting, Clippy
with warnings denied, and the locked workspace build passed. RustSec found no known
vulnerabilities in 190 locked dependencies (advisory DB `21d909e4`, dated 2026-10-24;
1,352 advisories). No dependencies were added. Inverting the capture-error guard made the
I/O-failure regression fail; the guard was restored before the complete passing checks.

The blocking-worker limit above was checked against the locked Tokio 1.54.1 source documentation
in `src/task/blocking.rs`. Real provider, billing, and Discord acceptance remain pending.
