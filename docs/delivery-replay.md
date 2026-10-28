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

## Paired analysis and delivery verification

```sh
ocw --replay-linked /private/path/cold-boot.ocwcap /private/path/history/observations.ocwh --json
```

New captures declare `route_schema: 1` and record a route for every still-fresh detector
candidate, including health suppression, disabled delivery and queue rejection. Analysis replay
recomputes those candidates and checks the route's decision, clock and generation. Paired replay
then matches every initial queue insertion to its recomputed source route, including rejection
results. Missing routes, extra candidates, changed decisions/admission, mixed boots and
incomplete tails fail. Retries stay attached to their original reservation; they are not new
detector candidates. A zero-candidate result explicitly reports zero; it supplies no signal-quality
evidence.

The command is read-only, uses recorded configuration, and requires a single uninterrupted
cold-boot capture and same-boot delivery evidence. It verifies at most 100,000 candidate routes,
with at most 4,096 eligible decisions awaiting route records. Older captures lacking routing
records cannot pass this stronger mode. `volume_delivery_link_replayed:true` covers the
volume detector-to-staging-policy path, not raw HTTP classification or whole-provider truth.
Other notification lanes and historical evaluation retain their separate coverage limits.
Multi-segment state reconstruction and threshold calibration remain outstanding.
