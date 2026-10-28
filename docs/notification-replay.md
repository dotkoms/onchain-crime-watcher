# Replay notification receipts

New runs with `[history].enabled = true` record normalized request/result evidence for all
currently implemented non-volume Discord transports: CTO listing posts, bundle-card edits,
status-card posts/edits, and the minimal trade-outage notification. Existing connection and
staging opt-ins still apply. Recording does not start a provider or send a test notification.

```sh
ocw --replay-notifications /private/path/data/history/observations.ocwh --json
```

This command reads only recorded evidence. It rejects live/delivery flags and configuration
overrides, reads no credentials, writes no state, and cannot send HTTP. A result identifies the
lanes actually observed; an empty session supplies no evidence of a notification being sent.
`observed_sent` and `observed_unknown` describe historical normalized results; `alerts_sent`
remains zero.

The recorder stores target digests, public message/intent identities, safe formatted payloads,
remaining request lifetime, and normalized receipts/pauses. Webhook URLs and keys are not
recorded. Cancellation emits an unknown result before closing the request. Recording is
best-effort through the bounded history queue; loss or incomplete evidence fails replay.

Replay checks:

- Session/boot identity, ordered request IDs and complete matching outcomes.
- Positive bounded request lifetime, message IDs, payload hashes and disabled mentions.
- CTO POST intent uniqueness and bundle edit suppression after unresolved/rejected outcomes.
- Status checkpoints: known card identity, pending initial POST suppression, matching PATCH
  receipts, and safe initial retry only after explicit rejection/rate limiting.
- Minimal outage intent scope per boot, including process restarts reusing local session IDs.

Status and outage workers finish before history is sealed. Bounds are 512 MiB, one million
history entries, 100,000 requests, 4,096 sessions and 64 open requests per session. Unknown
schemas, changed references, missing results, torn/corrupt history and recorded loss fail.
Old runs without this evidence cannot be silently upgraded.

## Coverage limits

`notification_receipts_replayed:true` verifies conservative normalized transport handling.
It does **not** independently recompute CTO source selection/dedup/budgets, funding decisions,
status health decisions, raw HTTP classification, global-pause causes, or receipt-reference
checkpoints from outside the supplied history. `admission_replayed:false` and
`http_classification_replayed:false` expose those limits. Wallet delivery is not implemented
and has no invented replay lane. Real webhook/provider acceptance is separate.

For recomputed volume decisions and staging queue/attention admission, use
[paired analysis/delivery replay](delivery-replay.md#paired-analysis-and-delivery-verification).
Historical threshold evaluation, complete family checkpoints and additional external input
tapes remain outstanding. Nothing in these offline commands deploys the application.
