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
