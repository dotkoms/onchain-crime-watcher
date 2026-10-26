# M1a collector implementation

Use a single binary and library. Single-source and one optional paid mint; no signals or ranking.

1. Typed config, exact decoder, bounded dedup and durable daily spend reservations.
   Prove corrupt ledger/restart/clock rollback, missing SOL and multiple fills per signature.
2. Bounded raw recorder and health snapshots. Prove byte-identical round trips, torn tails,
   corruption detection, queue saturation and disk cap behaviour.
3. Single WebSocket session runner, local fixture mode, minimal outage sender and CLI.
   Prove subscription/teardown on a local WS server, health without secrets and clean shutdown.
4. Run free-provider smoke, record fixture provenance, document commands and external checks.

Paid traffic defaults off. Init-spend is a separate explicit command. When paid observation
expires or the allowance exhausts, close that socket before any free-only replacement connects.
Capture I/O runs on a blocking worker with nonblocking admission; it never backpressures decode.
The spend ledger is not a prunable side store. A process lock prevents two collectors sharing data.

Verification: workspace tests/build, formatting/clippy and executable fixture/health smoke.
Live trade payload fixture, one-hour reference comparison, provider-billed overshoot and Discord
staging acceptance remain empirical work until credentials are configured. M1b and M1c are separate.

## Real-data-only preparation (live integration deferred by the user)

The user explicitly rejected demo data. No packaged demo or generated signal may run in the
application. Default startup reports waiting for data with no credentials or network. Keep the
existing collector gated by --live, and keep paid streams independently opt-in. External M1a
acceptance remains incomplete, not waived or marked passed.

1. Test then implement bounded token/bucket/cohort state and the scope §2A volume priors.
   Keep exact lamport totals; exclude the current bucket from its baseline/cohort thresholds.
   Suppress unknown launches, invalid buckets, warmup and insufficient participation. Reset
   regimes on migration and histories on gaps. Check near/fire expiry boundaries.
2. Stream recorded OCWC capture segments through the decoder/volume engine using recorded
   monotonic receive times. Reject torn/corrupt captures, mixed boots, rollback and oversized
   input. No generated ticks or trades, no fabricated launch history and no simulated delivery.
3. Wire the same engine to an opt-in --live --preview-volume path and real timer ticks. Default
   startup is disconnected. Keep raw inspection and --dry modes. Prove mode conflicts are
   rejected before side effects, and observed create-only frames produce no trades or alerts.
4. Document the real-data/live boundary and credential hookup. Run fmt, Clippy, tests, build and
   audit; push a reviewable branch. Production delivery and full M2a replay remain gated.

Risks: tests cannot validate thresholds or provider completeness; cohort estimates cover a bounded
observed sample only; gaps re-warm instead of fabricating quiet volume. A capture segment missing
its launch/baseline cannot produce a confident alert. Existing paid/capture safeguards remain.

## Staging Discord volume sender

This extends the real-data path. Default startup, replay and previews stay disconnected from
Discord. No demo data or real external POSTs are used during implementation.

1. Contract tests for fresh/safe staging payloads, bounded priority queue, 5-second near hold,
   15/30-second expiry, rolling hour/day attention limits and reserved fire capacity.
2. Durable, bounded pre-send intent/budget journal and returned message IDs. Ambiguous outcomes
   remain consumed and never retry. Check restart, corrupt state, rollback and disk failures.
3. HTTPS webhook transport: exact Discord host, redirects and automatic retries disabled,
   bounded response/timeouts, wait=true, mention suppression, target-channel readiness GET,
   429/header pauses and permanent failure breaker. Prove behavior against localhost servers.
4. Nonblocking real-live admission and serialized sender worker; separate staging flag and
   environment webhook. Replay cannot send. Surface outcomes in health, cancel pending work on
   feed generation changes, bound shutdown and respect shared global Discord pauses.
5. Runbook and verification evidence; fmt, Clippy, tests, build and audit; commit/push a PR.

Defaults: 10/hour and 40/rolling-day volume attempts, burst two with six-minute refill, near at
most 30% of each budget, two hourly slots reserved for fire. Failed/unknown attempts consume
capacity conservatively. Staging is labelled; production remains gated. Readiness GET checks
webhook/channel identity, not proof of POST permissions; real Discord acceptance is separate.

## Bounded transport recovery (first M1c slice)

Real staging acceptance still lacks credentials. Keep it pending while building the next
independent reliability slice; no provider connection, real webhook POST or demo data is used.

1. Write scalar-clock contract tests for a lifetime retry cap, capped exponential jittered
   backoff, sustained-health backoff reset, correlated ping/pong deadlines and adaptive create
   liveness. Application silence is suspicion only; it must not trigger an unprobed reconnect.
2. Integrate opt-in recovery after definitive transport failures. Default retry count remains
   zero. Drop the old socket before retrying; all recovery subscriptions are free-only. Paid
   interruption consumes unused reservations and cannot resume in the same run. Auth/429 stop.
3. Separate matching-pong/transport-idle liveness from create-stream EWMA, suppress staging
   admission while application liveness is suspected, and reset histories on real disconnects.
   Expose classifications, retries, backoff and liveness in capture/health without secrets.
4. Local WebSocket fault drills prove paid teardown, duplicate/reserve reset, bounded retry
   exhaustion, refusal after auth/rate-limit rejection, traffic-without-pong timeout and prompt
   cancellation during backoff. Then run complete checks, document limits and push a PR.

This is not the full M1c supervisor/probe/heal milestone. Three-mint independent probes, RPC gap
healing, task-panic supervision, heartbeat and storage checkpoints remain separate. Quiet-market
or application-stall suspicion never fabricates a gap or heals missing data in this slice.

## Capture shutdown integrity (next M1c slice)

Close capture admission and bound the final gap enqueue plus writer join with one five-second
async deadline. A writer I/O error, panic, or missed deadline must not return a successful
collector result. Keep capture loss/cap degradation distinct from flush success. The capture
worker must retain the data-directory lock through all pending writes, including after its
caller is cancelled or times out. Expose a separate shutdown outcome in local health; a
pre-flush capture record cannot certify a completed disk flush.

Prove real directory I/O failure, normal sealed capture, a saturated unconsumed queue, worker
panic, and lock retention during a timed-out/cancelled await. Then run the complete locked
checks and document OS/runtime shutdown limits. No new provider, webhook or demo inputs.
Full task restart supervision and the rest of M1c remain separate.
