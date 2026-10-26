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

The summary includes deterministic input/decision hashes and states `delivery_replayed:false`
and `external_responses_replayed:false`. It verifies recorded analysis execution, not the
independent subscription controller, Discord queue/admission/results, RPC response tapes or
whole-provider coverage. Full M2a replay, multi-segment/checkpoint restart, train/test reports,
real seven-day captures and empirical threshold evaluation remain pending. No fitted threshold
or historical performance result is claimed without that evidence.
