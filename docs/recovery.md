# Bounded transport recovery

The first M1c reliability slice is implemented. Default startup is still disconnected; no
provider, webhook, API key or demo stream is activated by this work. Existing live operation
still exits on transport failure unless recovery is deliberately configured.

## Enable later

Set `[recovery].reconnect_attempts` in ignored `config/local.toml` to a value from 1 to 5, then
use the existing explicit `--live` command. Zero is the default and disables reconnects. The
cap is a lifetime allowance for this run, not an allowance reset after every successful dial.
Budget/window expiry still performs the existing free-only replacement even with retry zero.

Delay starts at `initial_backoff_ms`, doubles on consecutive failures, adds bounded jitter up
to 25%, and never exceeds `max_backoff_ms`. After sustained observed application health,
`healthy_reset_ms` resets the exponential step only; it never replenishes retry credits.
HTTP 401/403 or 429 handshake rejections stop immediately instead of entering a restart storm.
The earlier socket is closed/dropped before any next connection is attempted. Authentication
values and raw server errors are excluded from classifications and capture controls.

Every recovery connection subscribes only to free creates/migrations. A paid interruption
sets `paid.state=interrupted`, discards remaining in-memory credits, and preserves durable
reservations as consumed. Paid subscriptions never resume automatically in that run. A new
explicitly enabled run is required, subject to the same persistent allowance. The local ledger
does not guarantee upstream billing has stopped; already in-flight messages can still be billed.

## Transport versus application liveness

The client sends a numbered WebSocket ping at `ping_interval_ms` and requires a matching pong
before `pong_timeout_ms`. A different, unsolicited or late pong does not satisfy that deadline.
JSON traffic cannot extend it. An independently bounded transport-idle timer also exists.
Incoming server pings use Tungstenite's automatic pong and a bounded flush; no duplicate manual
pong is emitted. WebSocket writes and close attempts are bounded, and the caller can cancel during
connect, receive, close or backoff. Backoff does not block the local health endpoint.

Application liveness uses valid free-create observations, with threshold
`max(30 seconds, 6 × EWMA gap)`. The integer EWMA uses alpha 0.2 and only gaps strictly between
zero and 60 seconds. A long stall is not learned as a new normal. These are priors, not a proof
of market activity or feed completeness.

When the create stream is quiet beyond that threshold, the feed is marked suspected/unprobed.
Fresh candidate emission and staging admission pause. A matching pong can prove transport
responsiveness, not application integrity. Application silence alone never triggers a
reconnect, clears histories, claims missing data healed, or increments a confirmed false-positive
count. A later valid create restores observed application liveness and counts a quiet recovery;
it does not establish why the stream was quiet. Independent three-mint probing is not implemented.

## Real gaps, generation and ownership

A definitive transport failure records an unhealed transport gap, clears reserve-delta state,
drops the outgoing partial volume bucket and re-warms the baseline. Dedup identities remain
bounded and survive session replacement. Within the configured dedup window/capacity, a
recognized duplicate fill cannot move reserves twice. This is not an unlimited replay guarantee.
The next connection attempt advances the generation; the staging queue rejects prior-generation
work. Already-started external POSTs cannot be undone and retain their existing unknown-outcome
rules. No missing trades, launches or gap-heal events are generated.

A durable spend-reservation worker retains process ownership even when its awaiting future is
cancelled. A replacement collector cannot acquire the data directory until that writer finishes.
The same rule applies to the staging receipt and capture workers. [Capture shutdown](shutdown.md)
uses one async deadline for final loss-marker admission and disk completion, and returns an error
on I/O failure, panic or timeout. Hung OS disk operations still require hard process supervision;
the async deadline is not a hard wall-clock process shutdown guarantee.

## Diagnostics

- `recovery`: actual reconnects, current scheduled backoff, exhausted budgets and last failure class.
- `transport`: state, received-frame time, matching and wrong/late pongs, and timeout count.
- `application`: state, last create, EWMA/threshold, suspicions, quiet recoveries and suppressed candidates.
- Capture: session close/open, unhealed transport gap, paid stop, retry schedule and terminal reason.

Only terminal failure attempts the existing optional outage notification once for this run.
Recovered transient failures are visible in capture/health; a complete persistent outage-identity
and heartbeat service is still pending. Source verification remains single-source/unverified.

## Remaining M1c and acceptance work

This is not the complete hardening milestone. Independent active-mint probes, bounded RPC healing,
expanded mint/TTL admission policy, task-panic supervision, heartbeat/dead-man monitoring and
checkpoint/delta storage remain to be implemented. Measured saturation, real provider disconnect
behavior, post-migration coverage, billing/teardown exposure and a real staging alert are still
acceptance work. There is no new unattended deployment or automatic application restart loop.

The WebSocket control-frame behavior was checked against the locked Tungstenite 0.27 source;
timer intervals use Tokio 1.54 with missed ticks skipped. The provider's
[one-WebSocket contract](https://pumpportal.fun/data-api/real-time/) remains in force.

## Local verification evidence

On 2026-10-26 Eastern, all 69 tests passed with none ignored. Formatting, Clippy with warnings
denied, and the locked workspace build passed. Refreshed RustSec audit found no known
vulnerabilities in 190 locked dependencies (DB `21d909e4`, 2026-10-24; 1,352 advisories).
No dependencies were added. A deliberate off-by-one retry-cap mutation failed its contract
test, was restored, and was followed by the passing complete suite.

Local fault drills cover paid interruption followed only by free subscriptions; duplicate fill
and reserve-cache isolation; finite reconnect exhaustion; auth/429 refusal; data flowing without
matching pongs; responsive quiet transport; cancellation during long backoff; quiet-application
signal suppression; and process-lock retention during a cancelled spend reservation. No real
provider or webhook connection is made for these tests. The application supplies no demo data.
