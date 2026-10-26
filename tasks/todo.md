# M1a tasks

- [x] Typed configuration, exact decoding and bounded dedup; meaningful contract tests.
- [x] Durable spend reservation and fail-closed tests.
- [x] Bounded checksummed raw capture, recovery and loss accounting tests.
- [x] Local health endpoint, single-session runner and minimal outage sender.
- [x] Local fake-provider integration and executable fixture smoke.
- [x] Free-provider smoke and honest fixture provenance.
- [x] Reproducible checks and updated usage/status documentation.

External acceptance (not replaced by fixtures):

- [ ] Paid trade fixture and payload/ATA comparisons with provider key.
- [ ] One-hour reference-count and billing/teardown reconciliation.
- [ ] Staging Discord webhook acceptance.

## Real-data-only volume preparation

- [x] Bounded volume engine, scope priors, gap/migration reset and expiry contract tests.
- [x] Recorded-capture replay with real receive timing and no generated data.
- [x] Disconnected default and opt-in live volume previews, with mode conflict tests.
- [x] Runbook and complete local checks.
- [x] Push review branch (`codex/real-data-ready`).

Live acceptance above is deliberately deferred at the user's request, not marked passed.

## Staging Discord sender

- [x] Safe payload, queue/priority/expiry and rolling-budget tests.
- [x] Durable pre-send reservation and message receipt journal.
- [x] Webhook transport classification, rate limits and localhost HTTP tests.
- [x] Explicit staging opt-in, live integration, health and safe shutdown.
- [x] Documentation and complete local checks.
- [x] Push staging sender review branch (`codex/staging-discord-sender`).

## Bounded transport recovery

- [x] Retry/backoff and independent transport/application liveness contracts.
- [x] Opt-in free-only reconnection with paid-interruption and auth/429 guards.
- [x] Local fault drills, history reset, cancellation and health/capture evidence.
- [x] Complete local checks and runbook.
- [x] Push bounded recovery review branch (`codex/bounded-recovery`).

Independent probing/heal, full task supervision and real staging acceptance remain pending.

## Capture shutdown integrity

- [x] Reproduce capture I/O failure incorrectly returning successful collection.
- [x] One deadline for final loss-marker admission and blocking writer completion.
- [x] Propagate writer failure/panic/timeout and expose capture shutdown outcome.
- [x] Preserve data-directory ownership through capture work after cancelled joins.
- [x] Fault tests and runbook; complete locked checks before committing.

Full task supervision, delivery shutdown guarantees, heartbeat/dead-man monitoring, independent
probing/heal and real external acceptance remain pending.

## Task supervision and crash containment

- [x] Monitor current worker completion during live collection and latch stage failures.
- [x] Bound pipeline panic recovery; rebuild cold and recover free-only without refilling transport credits.
- [x] Preserve health while pipeline recovery is red; redact executable panic output.
- [x] Clean up async health and staging workers on cancellation; retain pending disk ownership.
- [x] Fault tests, policy mutation check, runbook and complete locked checks.

Full JoinSet topology, automatic disk/delivery rebuild, heartbeat/dead-man reporting, probes/heal,
checkpoint/delta storage, saturation measurements and real acceptance remain pending.

## Requested completion sequence (no deployment)

- [x] Heartbeat, readiness and external dead-man monitoring (implementation; real external notification acceptance pending).
- [ ] Independent probes, bounded gap healing, durable coverage/checkpoints and remaining supervision.
- [ ] Complete recorded subscription/delivery replay and backtest evidence workflow.
- [ ] CTO polling, persistence, dedup and staging alerts.
- [ ] Launch bundle storage, funding analysis, sale tracking and partial coverage.
- [ ] Wallet cost basis, weekly ranking, watch matching and observed-position exits.
- [ ] Hosting/secrets/persistence/backup/rollback preparation (no actual deployment).
- [ ] Independent secondary feed, warm authority switching and no merged signal histories.

Live accounts, credentials, real historical capture collection and empirical acceptance are
separate from implementation and are not marked complete by local tests.
