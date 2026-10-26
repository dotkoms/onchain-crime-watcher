# Staging Discord volume delivery

The sender is implemented but disabled by default. Starting with no arguments still reports
`waiting_for_data`; no credentials are read and no network or demo data is used. Recorded
capture replay cannot send. Production delivery remains disabled.

## Later operator setup

Use a dedicated ordinary text channel for staging and its incoming webhook. Keep the status
webhook separate. Set `delivery.staging_channel_id` to the public channel ID in ignored
`config/local.toml`, and set `OCW_DISCORD_STAGING_URL` outside chat through your secret manager.
The executable does not automatically load `.env` files and never needs a wallet signing key.
The URL is accepted only on HTTPS `discord.com`, with a webhook ID/token path and no supplied
query, fragment or user credentials. Errors, health and Debug output exclude the URL/token.

When real provider validation is ready, explicitly opt in:

```sh
cargo run --locked -- --live --config config/local.toml --enable-paid --enable-staging-delivery --run-for-secs 300
```

This automatically enables the real volume core. Paid data still independently requires a
configured mint, provider key, sufficient linked allowance and an explicitly reconciled spend
ledger. Configure `feed.paid_window_s` deliberately: its default 10 seconds cannot establish the
four-minute volume baseline. Unknown launch history stays suppressed; no launch time or trades
are fabricated. The watch/discovery policy and live coverage acceptance remain unfinished.

Before the provider connects, the sender GETs the webhook and checks incoming-webhook type,
webhook ID and exact configured channel ID. Failure stops startup. This checks identity and
token access; it does not prove POST permission or that the channel is an ordinary text channel.
Confirm those against a real staging message later. No readiness test message is generated.

Inspect real recorded candidates without any external request:

```sh
cargo run --locked -- --replay-capture /absolute/path/to/recorded.ocwcap --json
```

Eligible decisions include both `preview` and the exact `staging_payload` envelope used by the
sender. Historical records are labelled `recorded_capture` and cannot be routed to delivery.
The existing `--dry` command remains a subscription-plan inspection, not a generated signal.

## Admission and budgets

One bounded priority queue, up to 64 candidates, feeds one serialized worker. Admission uses
`try_lock` and never awaits network or disk on the feed path. A fire can replace a queued near
when full; a near cannot replace a fire. Duplicate queued intents are rejected. Fire is eligible
immediately; near has a five-second reorder hold. Eligibility never changes the original
deadline: near expires at 15 seconds and fire at 30 seconds, including the exact boundary.

The sender prunes stale/generation-invalid candidates while paused, prefers the highest eligible
class, and checks feed connection, generation and freshness again after durable reservation and
before each HTTP attempt. Expired candidates never become posts. Payload age is rebuilt for
every permitted attempt, with request time bounded by the earlier of its deadline and the
configured timeout (default two seconds). Only live decoded decisions are admitted.

Default attention limits are 10 attempts per rolling hour and 40 per rolling 24 hours. A burst
of two refills at the hourly rate (one token per six minutes by default). Near is capped at 30%
of both budgets, with two hourly slots reserved for fire. Limits may be lowered within the
configuration ranges, never raised above these defaults in this slice. There is no waiting for
an exhausted attention budget: that candidate is counted as suppressed and dropped.

`data/staging-delivery.json` is a bounded, checksummed, atomic 0600 journal. A reservation reaches
durable storage before the first POST. Each reservation consumes attention budget even if the
send fails, remains ambiguous or is cancelled. An explicit 429 retry uses the same reservation,
not a new budget slot. Confirmed returned message IDs are stored against stable intent hashes
derived from boot, mint, bucket and tier. Pending/unknown intents are not replayed after restart.
Rolling usage and burst credits survive ordinary restart; backward wall-clock steps cannot
reset them. Keep the journal with the data directory; it is not capture-retention data.

A missing journal is initialized on first reservation. Corruption, unreadable state or failed
durable writes block the sender instead of resetting usage. If recording a successful receipt
fails, the pre-send reservation still prevents retry and further delivery stops. The process
lock is held by the worker, including while an old disk write is pending.

## HTTP outcomes and pauses

Every payload disables mentions, uses the canonical base58 mint, omits untrusted token names and
URLs, stays within 2,000 characters, and says STAGING with unverified coverage and prior thresholds.
The shared card formatter supplies the same units, regime and statistical values for previews
and staging. A successful POST requires `wait=true`, a valid returned message ID and the expected
channel ID; a missing receipt is unknown, not success.

| Response | Action |
|---|---|
| Valid success receipt | Count sent; persist the returned ID |
| Explicit 429 | Honor the longest valid body/header delay; retry at most twice, only while fresh |
| 401, 403 or 404 | Block this sender; no retry |
| Other 4xx / redirects | Reject this payload; no retry or redirect following |
| 5xx, transport errors, timeouts, missing/invalid receipt | Count unknown; never retry |
| Invalid rate-limit metadata | Fail closed rather than guess an earlier retry |
| Response names a different channel | Stop further posts; mark the result unknown |

`X-RateLimit-Remaining: 0` and reset-after pause subsequent staging requests. A global 429 also
pauses the optional status lane; a status notification arriving during that pause is counted
and skipped by the existing minimal outage sender. Unreadable or oversized global 429 bodies
still respect global header information. Redirects and Reqwest automatic retries are disabled.
No claim of exactly-once delivery is made: a timeout/5xx can have created a message remotely.

Queue/generation loss, budget suppression, attempts, confirmed sends, unknowns, rejections,
rate limits and blocked/journal-error state are visible under `/health` → `staging_delivery`.
`signals_enabled` reflects the staging opt-in; `production_signals_enabled` stays false.

Shutdown stops admission, cancels an in-flight HTTP operation conservatively as unknown, drops
pending work and waits up to three seconds for the sender. A hung OS disk operation may outlive
that join timeout and retain the process lock; hard process supervision remains M1c work.

## Acceptance still outstanding

No real Discord webhook or PumpPortal feed requests are made while implementing/testing this sender. The
automated HTTP tests use localhost; edge-case inputs exist only in tests. Real webhook write
permissions, ordinary-channel targeting, end-to-end live staged alerts, provider completeness,
billing reconciliation and production signal evidence remain operator acceptance work.

Primary references: [Discord webhook execution](https://docs.discord.com/developers/resources/webhook#execute-webhook)
and [Discord rate-limit headers and 429 responses](https://docs.discord.com/developers/topics/rate-limits).

## Verification evidence

Local verification on 2026-10-26 Eastern: all 58 tests pass, none ignored; formatting, Clippy
with warnings denied and the locked workspace build pass. RustSec audit refreshed its database
and found no known vulnerabilities in the 190 locked dependencies (DB `21d909e4`, 2026-10-24,
1,352 advisories). No dependencies were added. Changing 5xx outcomes from unknown to retryable
caused the HTTP outcome contract test to fail; the implementation was restored and the complete
suite passed again. This is a targeted mutation check, not a mutation score.

Local HTTP tests cover readiness without POST, expected channel identity, target changes,
mention suppression, successful receipts, single-attempt ambiguous failures, explicit 429
retries, global pauses, oversize responses, expiry/generation changes, disk failures and shutdown
in flight. Worker integration proves process ownership lasts through the final journal write.
Default startup ignores even an invalid staging environment value and remains waiting for real
data; configuration validation succeeds without credentials. No real acceptance is inferred.
