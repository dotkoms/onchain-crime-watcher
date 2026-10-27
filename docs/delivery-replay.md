# Recorded staging-delivery policy replay

The staging-volume sender can record its actual policy operations in the optional normalized
history journal. Enable `[history].enabled = true` before an explicitly authorized live run.
This does not enable a provider, paid subscriptions or Discord delivery; their existing flags,
credentials and guards still apply. Defaults remain disconnected. No demonstration tape or
sample notifications are supplied.

After a clean shutdown, inspect that real journal offline:

```sh
ocw --replay-delivery /private/path/data/history/observations.ocwh --json
```

The command reads no secrets, creates no state, sends nothing and rejects `--config`, live,
paid and delivery flags. It reuses the recorded policy configuration and durable journal's
starting state, including prior reservations, receipts and burst usage. The delivery journal
is reconstructed only in memory, using the same admission/accounting functions as the sender.
The original files remain unchanged.

Recorded evidence includes:

- Queue insertions, replacements and duplicates; effective expiry/generation pruning; selected
  candidate order; and final clearing. Empty idle polls are omitted to bound idle storage.
- Candidate decision, boot identity, generation, hold/deadline and retry state.
- Rolling attention admission, burst credits, reservation result and resulting state hash.
- Each attempted payload's hash, exact age clock and observed feed/generation guards.
- Normalized transport outcomes, rate-limit pause boundaries and durable receipt changes.

Replay recomputes queue priority, holding, expiry, reservation budgets and payloads. Only an
explicit rate-limit result permits a fresh bounded retry under the same reservation. Unknown
outcomes cannot be resent. Confirmed receipts must match the recorded result; failed writes or
incomplete operations fail replay rather than resetting usage. The collector stops delivery
before sealing history, preserving the final receipt and queue-clear evidence.

A result includes deterministic input/decision hashes, session/operation counts, attempts,
`observed_sent` and `observed_unknown`. These are historical outcomes, not new sends:
`alerts_sent` remains zero and `network` is disabled. Captures without the required start/end
evidence are not upgraded into a successful policy replay. Corrupt/torn journals, recorded
history loss, unsupported operations and changed decisions fail. Input is bounded at 512 MiB
and one million history entries.

## Limits

This command verifies the recorded **staging-volume lane**, not the whole Discord system.
CTO posts, bundle edits, status notifications and wallet delivery are outside this tape. Feed
health and normalized HTTP outcomes are observed inputs. Raw HTTP parsing/classification,
upstream subscription delivery, billing, shared global-pause causes and external coverage are
not independently reconstructed. `http_classification_replayed` and `other_lanes_replayed`
remain false. No provider or Discord acceptance is implied.

[Analysis replay](analysis-replay.md) separately verifies recorded decoder/volume decisions,
automatic watch membership, and local feed-spend credits. There is no claim yet that these two
recordings form one fully linked end-to-end backtest. Historical evaluation, multi-segment
state reconstruction, threshold calibration and additional response tapes remain outstanding.
