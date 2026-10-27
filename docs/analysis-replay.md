# Recorded analysis execution replay

New captures include `execution_schema: 1`, their cold-boot configuration, the exact clock
used by each decoded/discarded frame, watch attachment/removal steps, timer ticks and volume
reset boundaries. These records distinguish a captured frame from its later application:
subscription bookkeeping may happen between the two. Decoder/volume replay follows those
recorded steps rather than advancing the engine at every incidental control/log timestamp.

```sh
ocw --replay-analysis /private/path/segment.ocwcap --json
```

This command is read-only and never reads credentials or calls a provider/Discord. It uses the
recorded configuration; `--config` is rejected for this mode. It preserves the observed watch
universe, including ignored creates, late inactive trades and partial initial subscriptions.
Recovered RPC audit batches stay separate and never warm a live-volume ring.

The supplied segment must contain the uninterrupted initial cold-boot prefix. Sequence gaps,
mixed boots, rollback, missing frame-application records, unsupported schema and torn tails
fail. An arbitrary rotated segment cannot be reconstructed from empty state. Older captures
remain available through the narrower `--replay-capture` diagnostic command; no execution
records are invented to upgrade them.

New live captures also declare `watch_schema: 1`. For paid automatic watching, the session
records a cold controller start and every frame/timer planning step, including empty action
lists. Replay runs the production watch planner against recorded raw creates and decoded
trades, compares its ordered actions with the capture, and checks that watch attachment/removal
and frame admission follow those actions. Inactive trade discards must match membership.
Session boundaries clear controller state; incomplete planning/application tails fail.

The summary includes deterministic input, volume-decision and watch-decision hashes,
`watch_plans` and `watch_sessions`. `watch_controller_replayed:true` means at least one automatic
watch session was recomputed from that evidence. Older captures without `watch_schema` retain
analysis replay and report this flag false; missing evidence is not invented. Free-only and
manual-mint sessions do not claim automatic-controller verification.

This verifies desired subscription choices, not successful upstream subscription delivery or
billing. New captures with `spend_schema: 1` also record starting ledger usage and each credit
check/reservation. Replay reuses the production reservation calculation, checks rollover and
rollback behavior, and consumes credits for every recorded billable frame, including duplicates
and inactive frames. `financial_admission_replayed:true` means those local reservations and
credits were checked; it never verifies upstream billing. Missing/unfinished reservations and
redacted billable frames fail instead of guessing. Older captures report this flag false.

`subscription_transport_replayed:false`,
`delivery_replayed:false` and `external_responses_replayed:false` make those limits explicit.
Discord queue/admission/results, durable-write failure response tapes, RPC response tapes and full
multi-source authority switching are not replayed. Full M2a replay, multi-segment/checkpoint
restart, train/test reports, real seven-day captures and empirical threshold evaluation remain
pending. No fitted threshold or historical performance result is claimed without that evidence.
