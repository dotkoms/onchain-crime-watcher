# Heartbeat and external monitoring

Default startup stays disconnected. Monitoring runs only during explicit live collection with
`[monitor].enabled = true`; a recorded fixture/replay never opens reporting connections.
No deployment or external check is created by the application.

The checks answer three operational questions: is the collector being scheduled, is its feed
pipeline advancing, and are its observed dependencies healthy? Neither liveness nor readiness
proves complete market data or validated signals.

## Local checks

`GET /health` remains a diagnostic snapshot. `GET /ready` returns 200 only when both collector
and feed-pipeline progress clocks are fresh, the feed is connected with observed application
activity, and required stages have no known failure. It returns 503 with fixed reason codes
for startup, stalls, suspected application silence, red stages and capture/delivery degradation.
The default freshness boundary is five seconds, measured on a monotonic clock. `production_ready`
is always false until the separate release-evidence gates are implemented and satisfied.

A five-minute structured JSON heartbeat records the boot identity, actual counters, uptime and
readiness. Empty counters describe actual empty observation; no artificial trades or signals
are generated. The monitor is off the feed path and has bounded network timeouts. Its worker
is observed by supervision, signalled on cancellation and retains the process lock during disk
work. A hung OS disk operation can still outlast async shutdown deadlines.

## Optional Discord status card

Set `monitor.discord_status = true`, `monitor.status_channel_id`, and the environment secret
`OCW_DISCORD_STATUS_URL`. Use a status webhook distinct from signal delivery. Startup checks the
webhook/channel identity before opening the provider connection; that GET is not proof of POST
permissions or supported channel type. Use a plain text staging channel for real acceptance.

The initial POST reserves a durable intent in `status-card.json` before sending. Its returned
message ID is reused with PATCH, including after restart. Unknown initial POST outcomes block
new creation, preventing duplicate heartbeats. A missing/deleted message or changed target also
requires operator action. Resolve an ambiguous card manually: identify its message in Discord
and reconcile the saved ID, or retire the old card before resetting the private journal. Never
reset it merely to retry an unknown POST. Corrupt/oversized/symlinked journals fail closed.

Heartbeats bypass signal attention budgets but honor Discord rate-limit/global pauses. Mentions
are disabled and payloads contain fixed labels and observed counters. Endpoint URLs and tokens
are never included in logs or health. Real posting permission and restart behavior still need
staging acceptance with the eventual webhook.

## Optional external dead-man check

Create an independently hosted Healthchecks.io check and configure its notification integration.
Set its period to 60 seconds (matching `monitor.deadman_s`) and choose a grace period appropriate
for host startup/network delays; 120 seconds is an initial operational prior, not a measured SLO.
Set `monitor.deadman = true` and provide its UUID ping URL in `OCW_DEADMAN_URL` through the host's
secret manager. The supported URL shape is HTTPS on `hc-ping.com/<uuid>`; it is secret.

The application sends a regular ping only while observed operational checks pass, otherwise
`/fail`. Graceful stopping attempts a bounded final failure ping. If the process, host or network
disappears, the external service detects missing pings independently. Without that external
check and notification configuration there is no process-disappearance alert. The process
cannot monitor its own absence. No automatic account creation or test notification is supplied.

## Runbook

For `collector_stalled` or `pipeline_stalled`, inspect process load, blocking disk work and the
last stage transition; do not call the feed complete because HTTP health still responds.
For suspected/unprobed silence, check the independent probe once available and retain the
uncertainty label. For `stage_red`, fix the classified problem before an explicit restart.
For an external missing-ping alert, check host/process/network availability first. Pause the
external check deliberately during planned maintenance, then resume and verify fresh pings.
For heartbeat ambiguity, reconcile its durable identity instead of posting another card.

References: [Healthchecks ping protocol](https://healthchecks.io/docs/http_api/),
[failure signals](https://healthchecks.io/docs/signaling_failures/),
[check periods and grace](https://healthchecks.io/docs/configuring_checks/), and
[Discord webhook/message endpoints](https://docs.discord.com/developers/resources/webhook).
These document transport behavior; actual host/Discord notification delivery remains an
external acceptance check.

Local verification on 2026-10-26: 94 workspace tests passed, none ignored; formatting,
Clippy with warnings denied, and the locked build passed. RustSec found no known vulnerabilities
in 190 locked dependencies using DB `21d909e4` (2026-10-24; 1,352 advisories). No dependencies
were added. The `/ready` test failed with the previous missing route; an exact-boundary stall
mutation also failed its test and was restored before the full checks. Local HTTP tests cover
card identity across restart, unknown POST suppression, global rate limiting, failure pings,
URL redaction and stopping. Real external notifications remain untested until configured.
