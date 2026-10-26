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
- [ ] Push bounded recovery review branch.

Independent probing/heal, full task supervision and real staging acceptance remain pending.
