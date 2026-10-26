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
