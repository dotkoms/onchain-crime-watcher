# Observation journal and automatic launch watching

These are opt-in reliability foundations. They do not establish complete provider coverage,
restore paid subscriptions after failure, or enable production delivery.

## Durable observations

Set `[history] enabled = true` in your private configuration for an explicitly live run.
The writer records normalized real observations and collector controls in
`data/history/observations.ocwh`. Each record has a sequence number, boot identity, wall time
and a SHA-256 chain checksum. `checkpoint.json` records the durable prefix and coverage state.
The default journal cap is 512 MiB, with 8 MiB reserved for coverage/checkpoint termination.
No automatic pruning deletes historical evidence.

The bounded nonblocking queue stops recording on overflow and reports lost observations.
Worker progress is checked independently from collector progress by `/ready`. Restart records
an unverified coverage gap; it never converts elapsed downtime into zero trades. An incomplete
EOF record is recoverable, while checksum corruption and clock rollback fail closed. Only one
process can own the data directory and journal. A failed writer cannot continue appending.

The checkpoint describes journal durability, not a complete bundle/wallet/accounting snapshot.
Full historical decision replay and bounded chain healing are separate work. Raw capture is
still needed to reproduce provider decoding. Keep the data directory private and backed up.

## Automatic watch admission

Set `feed.auto_watch = true` instead of `feed.mint`. Both cannot be configured together.
Automatic watching additionally requires `--live --enable-paid`, a fresh provider credential,
and the explicitly initialized shared spend ledger, just like manual paid watching.
`--dry --enable-paid` prints its subscription policy without connecting or reserving spend.

The default planner allows 20 total mints, including four launch slots. Each launch receives
10 seconds of observation, then provisional retention up to five minutes. Retention requires
at least 3 SOL of observed turnover. Inactive subscriptions expire after 30 minutes unless
observed recent turnover renews their lifetime. The waiting queue is bounded to 32 entries
and two seconds; late admission marks its first bucket incomplete. Unsubscribed trades remain
billable and captured but cannot update reserve caches or volume decisions.

All subscriptions use the existing single socket and shared daily message allowance. There is
no second paid connection. Failure, exhausted spend or admission bookkeeping exhaustion closes
that socket before free-only recovery. Automatic paid watching does not have the manual
10-second whole-session window: its exposure is bounded by mint admission and the daily ledger.
The allowance is a local estimate, not a provider-side billing guarantee.

## Read-only chain foundations

The library includes a bounded Solana JSON-RPC client: HTTPS only, eight concurrent calls,
two-second timeout, 2 MiB responses, durable per-call reservations and persisted rolling rate
limits/429 pauses. It has no transaction submission method. Set `[probe] enabled = true` and supply `SOLANA_RPC_URL` from an independent provider
to use it for live watchdog probes. The shared daily/rate limits apply to all probe calls.
Setting the URL alone does not enable networking. Probes do not implement gap healing.

The Pump curve TradeEvent decoder binds signatures and validates the emitting program/log
stack, complete current layout, explicit transaction success, slot and timestamp. It preserves
individual event indices and exact native amounts. Unknown layouts, non-SOL quote currencies,
truncation and unavailable transactions remain unresolved. A whole-transaction fee is metadata,
not automatically a fee on every fill. PumpSwap decoding and live schema acceptance are pending.

Protocol sources: [Solana getTransaction](https://solana.com/docs/rpc/http/gettransaction),
[getSignaturesForAddress](https://solana.com/docs/rpc/http/getsignaturesforaddress), and
[Pump public IDL](https://github.com/pump-fun/pump-public-docs/blob/main/idl/pump.json).
The IDL snapshot inspected for the decoder had SHA-256
`7616681ea557975f9f623ac211080f22a93060781eaf4765b0a488d980997207`.

## Independent watchdog samples

When the create stream is suspected stalled, the collector samples three actually subscribed
curves with recent accepted trade cursors. Fewer than three, failed calls, pruned/migrated mints,
unknown logs and same-slot ordering remain unavailable. Changed signatures must decode to a
native trade for the sampled mint at a confirmed slot later than the observed anchor. This
avoids confusing a lagging RPC index or unrelated account activity with missing trades.

Before acting, results are rechecked against trades accepted while RPC ran. Two genuinely
missing newer trades permit a watchdog reconnect, subject to the existing finite recovery
budget. The interrupted paid stream remains stopped. Quiet samples suppress another probe
for the configured 30–300 seconds and remain explicitly single-source/global-unverified.
Probe tasks have bounded request deadlines and are cancelled with their owning socket session.
Transport failures remain independently actionable; per-gap healing and block fallback are
still pending. None of this runs during configuration checks, disconnected startup or replay.
