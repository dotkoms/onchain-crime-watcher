# Curated community-takeover listings

CTO polling is disabled by default. Set `cto.enabled=true` in the private configuration and
start the collector with explicit `--live` to poll the public DexScreener takeover list. No API
key is needed for this source. It is independent of PumpPortal trade health: exhausted trade
recovery leaves the process and CTO worker alive, with the trade failure visible in health.

Polling defaults to once per minute, with a two-second request timeout, no redirects, a 1 MiB
body bound and at most 1,000 rows. Solana mint addresses and RFC3339 claim dates are validated.
A missing, malformed or implausibly future date remains unknown; receive time never replaces
it. Names, descriptions, links and vendor-provided URLs are not used in alert cards.

Every boot primes the current list without posting. `data/cto/state.json` durably stores its
claim-date high-water mark, bounded seen-token set, candidate outcomes, spend reservations,
Discord receipts and source/channel rate-limit pauses. The file is checksum-protected and
written atomically with checked synchronization. Invalid state, rollback or exhausted storage
capacity fails closed. The set holds at most 20,000 tokens and never silently discards tombstones.
Back up the whole data directory with the stopped-process snapshot tooling.

A newly appearing token below the previous high-water is counted as suppressed, not recovered.
New tokens tied at the high-water remain eligible. The vendor’s global latest list has limited
retention: downtime or late curation can miss claims. This feature does not promise complete
coverage of community takeovers.

Without delivery opt-in, newly observed listings produce clearly labeled real-data previews.
For staging delivery, also set `cto.staging_delivery=true`, `cto.channel_id`, a separate
`OCW_DISCORD_CTO_URL`, and `--enable-staging-delivery`. The global staging flag also retains its
existing volume-staging configuration requirements. Both CTO webhook and channel must be distinct
from the configured volume/status targets. Live startup verifies webhook/channel identity;
offline `--preflight --enable-staging-delivery` checks local inputs only.

The CTO channel has independent rolling caps (maximum 20/hour and 120/day) and a two-message
burst allowance. Candidates expire 120 seconds after observation. Durable intent is recorded
before POST; a timeout, ambiguous response or interrupted attempt never triggers a duplicate
send, including after restart. Rejected, expired, budget-limited and rate-limited candidates
are not queued indefinitely. Source 429 pauses and channel pauses survive restart; Discord
global pauses are shared with other channels. Unauthorized or changed webhooks block the worker.

The card says **curated claim** and explicitly states that it does not verify ownership,
control or who holds tokens. Optional bundle integration supplies verified funding and partial
observed sale bounds; absent evidence and historical price context remain unavailable. A
non-pump Solana listing remains eligible with the same limits.

`/health` reports CTO source/worker state, last successful poll, unknown dates, high-water
suppression, observed listings, send outcomes and budget drops. `/ready` distinguishes CTO
failure/stall from trade failure. No real vendor polling or Discord acceptance was performed
while preparing this implementation; localhost tests exercise the HTTP path and persistence.
Production delivery remains disabled under the current project contract.

Source: [DexScreener API reference](https://docs.dexscreener.com/api/reference),
`GET /community-takeovers/latest/v1` (documented limit 60 requests/minute).

### Optional bundle observations

`[bundle]` is disabled by default. Capture requires paid automatic launch watching; funding
also requires CTO enabled and `SOLANA_RPC_URL`, using the existing `[probe]` RPC budgets.
Persist the entire data directory, including `launch/` and `bundle-analysis/`, so snapshots,
funding caches and edit intents survive restart. Back up these directories with the existing
offline snapshot workflow. Cards state partial observed history, never fabricated zeroes or
CLEARED. Funding edits target the existing verified CTO staging message and never retry an
ambiguous HTTP outcome. See [bundle implementation](bundle.md#implementation-progress-2026-10-27).
