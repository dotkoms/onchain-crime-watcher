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
- [x] CTO polling, persistence, dedup and staging alerts (real source/channel acceptance deferred).
- [ ] Launch bundle storage, funding analysis, sale tracking and partial coverage.
- [ ] Wallet cost basis, weekly ranking, watch matching and observed-position exits.
- [ ] Hosting/secrets/persistence/backup/rollback preparation (no actual deployment).
- [ ] Independent secondary feed, warm authority switching and no merged signal histories.

Live accounts, credentials, real historical capture collection and empirical acceptance are
separate from implementation and are not marked complete by local tests.

Reliability foundations (increment within the requested reliability step):

- [x] Bounded checksum-chained normalized journal, durable prefix and explicit restart/loss gaps.
- [x] Independent progress clocks for history, capture, delivery and monitoring workers.
- [x] Deterministic bounded automatic watch admission on the shared single paid socket.
- [x] Bounded read-only RPC transport and strict native Pump curve event decoding library.
- [x] Wire independent three-mint watchdog probes with live-race and stale-RPC guards.
- [x] Bound curve gap scans, recover ordered native audit deltas and preserve partial coverage.
- [ ] Missed-create block fallback, migrated-pool recovery and live/history reconciliation.
- [ ] Complete family state checkpoints and full decision replay before claiming coverage recovery.

See [history and watch runbook](../docs/history.md). No live provider or Discord acceptance
was performed for these foundations; no deployment was created.

Hosting preparation increment:

- [x] Offline selected-mode preflight with redacted missing-input reports.
- [x] SIGTERM shutdown, non-root Docker/Compose and bounded systemd templates.
- [x] Offline checksummed snapshots, safe restore and live-start reconciliation barrier.
- [x] Hosting, secret, persistent-storage, monitoring and rollback runbook.
- [x] Container image build, disconnected startup and offline preflight in CI.
- [ ] Actual target-host systemd validation and live hosted acceptance.
- [ ] Live staging and provider acceptance when explicitly authorized.

No host, external monitor account, backup schedule or deployment was created.

Recorded analysis execution increment:

- [x] Record exact frame application, watch operations, timer ticks and volume reset clocks.
- [x] Read-only cold-prefix decoder/volume replay with deterministic input/decision hashes.
- [x] Reject missing execution evidence; preserve observed admission and exclude healed audit batches.
- [ ] Replay controller choices, delivery admission/results and external response tapes.
- [ ] Multi-segment/checkpoint replay and real historical train/test threshold evaluation.

See [analysis replay](../docs/analysis-replay.md); this is not a complete delivery backtest.

CTO implementation:

- [x] Bounded Solana polling and strict optional claim-date handling.
- [x] Restart priming, persisted high-water, bounded tombstones and conservative send intents.
- [x] Dedicated verified staging channel, rolling budgets and ambiguous-outcome suppression.
- [x] Independent worker health, trade-failure isolation and checked shutdown.
- [x] Accurate claim wording and unavailable bundle/price history.
- [ ] Real DexScreener/Discord acceptance after explicit live authorization.

Bundle accounting increment:

- [x] Bounded half-open launch membership and ordered buyer-event capture model.
- [x] Original-bag proportional allocation, later-buy handling and balance-gap guards.
- [x] Native-cost-weighted sold fractions, minimums and conservative exact tier comparisons.
- [x] Direct/one-hop funding classification, known-account exclusions and fan-out cap.
- [ ] Wire live capture and durable launch storage into the collector.
- [ ] Fetch confirmed funding/supply/price facts and connect persisted progress to CTO cards.

The pure bundle model is not yet connected to live cards. CTO bundle fields continue to say
unavailable. No launch or funding facts are fabricated by these unit-testable rules.
