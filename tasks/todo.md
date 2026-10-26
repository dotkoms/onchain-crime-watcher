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
- [ ] Push review branch.

Live acceptance above is deliberately deferred at the user's request, not marked passed.
