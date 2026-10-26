# Failover — two feeds, one authority

Status: v1 design, 2026-10-25. Contract: [`../SCOPE.md`](../SCOPE.md) revision 6 (§6.4, §9, R11).
Replaces revision 4's "shadow, not hot failover" with a two-source contract. Siblings:
`feed.md`, `backtest.md`, `sources.md`, `operations.md`, `ranking.md`, `delivery.md`.

Tags: **CONTRACT** = fixed by SCOPE; **PRIOR** = a guess with its reason; **U** = unverified.

## 0. What "hot failover" can mean here

PumpPortal permits **one websocket at a time** (`sources.md` §2.1, **D**). A second PumpPortal
key or socket is not a failover path: it is a ban risk and an evasion of the vendor's stated
limit, which the runbook already forbids. So a second feed means a **second provider**.

Failover is *hot* only if the standby has been carrying the same subscriptions and building the
same state before the switch. A second source that starts cold on failure is a faster outage:
every ring re-warms for four minutes and the volume family is blind through the event that
caused the switch. This design therefore runs **two pipelines in parallel**, one per source, with
exactly one authoritative at any instant and the other a warm standby.

What failover does not do: recover events the standby did not receive, make either provider
complete, keep the process alive, or repair Discord, RPC or the side stores. It removes the
single-provider outage from the volume and wallet paths, and it bounds the blind interval to the
switchover latency plus the frames in flight.

## 1. Provider options

| Option | Transport | What it carries | Latency | Cost | License gate | Verdict |
|---|---|---|---|---|---|---|
| Yellowstone Geyser provider | gRPC, server-side filters on the pump.fun program and bonding-curve accounts | Raw transactions and account updates; own decode; slot and `blockTime` | 10–100 ms (**U**) | Paid (**U**) | Client/protos Apache-2.0; server default AGPL; check pinned dependency tree (§14) | **Recommended secondary** |
| Provider "enhanced" websocket / LaserStream | WS or gRPC, provider-specific | Raw or decoded transactions | **U** | Paid | Verify before use | Fallback if permissive and it carries slot |
| Public RPC `logsSubscribe` / `programSubscribe` | WS, per-filter | Raw logs; own decode | 400 ms–2 s (**U**) | Free/cheap | Permissive | Degraded secondary: subscription caps, no slot continuity, not a true peer |
| Second PumpPortal key | WS | Identical | 200–1,000 ms | Double metering | Vendor limit; runbook forbids key/IP rotation | **Rejected** |

The secondary must satisfy `has_slot` and `has_block_time`; both are optional in the adapter
capability record, and a secondary without them cannot be authoritative for the ranking audit or
the swap replay. **M6 gates on a provider whose client can be built without copyleft code.**

## 2. Topology

```
             ┌────────────── feed.session[pumpportal] ──frames──▶ feed.pipeline[primary]
 capture ◀───┤                                                        │ decisions
 (both)      └────────────── feed.session[secondary]  ──frames──▶ feed.pipeline[secondary]
                                                                        │ decisions (standby)
             desired set ◀── gate planner ──▶ ledger[primary] / ledger[secondary]
                                    ▲
                         failover controller ── authoritative = primary | secondary
```

| Component | Count | Owns |
|---|---|---|
| `feed.session[p]`, `feed.session[s]` | 2 | One socket each, one generation stream each |
| `feed.pipeline[p]`, `feed.pipeline[s]` | 2 | Decode, dedup, registry, rings, cohorts, detection — one full copy per source |
| `gate planner` | 1 | The desired subscription set, caps, retention decisions |
| `gate ledger[p]`, `ledger[s]` | 2 | Per-source subscription state, `SubGen`, unsubscribe retries |
| `failover` | 1, inside the supervisor | `authoritative`, switch decisions, flap guard, switchover record |
| `capture` | 1 | Both sources, tagged (M6) |

**Two pipelines, not one.** A single pipeline fed by two sources would have to merge them, and
merging is forbidden: the same trade would arrive twice with different timing, and the ring
would count it twice or the dedup would hide one source's genuine gap. Two pipelines keep each
source's history honest and make the standby's rings warm by construction. The cost is memory
and CPU, stated in §13.

**One desired set.** Retention is a product decision, not a per-source one. The planner is fed by
the authoritative pipeline's creates, migrations and gate decisions, and publishes
`Desired { mint, curve, kind, deadline }` snapshots. Each ledger subscribes its own socket to that
set. On switchover the set does not change; both sockets are already subscribed. The same planner
issues the watch-set account filters — the secondary's transaction filter is keyed on the watched
wallet addresses, not on a mint list — so a promoted secondary can match wallet entries and exits
without a resubscribe.

## 3. Arbitration

All conditions are evaluated on the failover tick (1 s, PRIOR) and on any source state change.

| Switch | Requires |
|---|---|
| Primary → secondary | Secondary `Connected`, subscribed to the desired set, all promoted rings ready or explicitly cold **and** fresh (`last_event` within `failover.secondary_fresh_s` = 10 s) **and** primary is `Banned`, `Failed`, reconnecting longer than `failover.switch_after_s` (30 s, the outage-declare threshold), or `Suspected` with a probe that disagrees |
| Secondary → primary | Primary `Connected` for `failover.switch_back_after_s` (300 s) **and** fresh **and** aligned disagreement over `failover.compare_window_s` (600 s) at or below `feed.shadow.tolerance` (0.05) per stratum **and** switches in the last hour below `failover.max_switches_1h` (3) |
| Neither | Any of the above fails: stay on the current authority; count `switch_back_blocked{reason}` |
| Both unhealthy | No authority: feed state `stale`, one `trade_stream` outage identity, alerts suppressed, outage card; the process and the other families continue |

Rules:

- **Disagreement never forces a switch.** The same rule as the shadow (`SCOPE.md` §6.4): a
  discrepancy tags alerts and blocks a quiet-market claim, but the primary is not demoted because
  a sample disagrees.
- **Disagreement blocks switch-back.** Returning to a source that disagrees with the current
  truth is worse than staying on a healthy secondary.
- **Flap guard.** At `max_switches_1h`, the controller holds the current authority for
  `failover.flap_cooldown_s` (3600 s, PRIOR), records `failover_flap_guard`, and requires either
  cooldown expiry or a source `Failed` (not merely reconnecting) to move again. A controller that
  ping-pongs is an outage with extra steps.
- **Preference.** `feed.failover.prefer = primary | secondary` sets the target after both are
  healthy; default primary. A boot-time `--force-source` is for an operator who knows a provider
  is degraded; it is logged and counted, never silent.
- **Bans are per provider.** A PumpPortal ban switches to the secondary; it never stops it. A
  secondary provider ban is the same in reverse. `Banned` on both is the only ban that silences
  both.

## 4. Switchover procedure

The swap is one supervisor action, not a message race:

1. **Stop admission.** Mark the outgoing pipeline non-authoritative; it stops emitting
   candidates. Frames already in its channel are drained and discarded, counted
   `switchover_boundary_frames`.
2. **Freeze the partial.** Each mint's in-flight partial bucket on the outgoing pipeline is
   dropped as `PARTIAL_TRUNCATED`; the new authority's partial continues from its own timeline.
   The switchover takes effect immediately, not at a bucket boundary: waiting up to 15 s on a
   feed that has failed is worse than one truncated bucket.
3. **Swap state wholesale.** The new pipeline's registry, rings, cohort cells, dedup set and gate
   ledger become live. Nothing is copied or merged. The old pipeline continues to run, cold, as
   the standby: it keeps its own rings and dedup and re-warms.
4. **Record.** Write `SWITCHOVER {from, to, at, generations, boundary_frames, reason}` to the rank
   coverage journal (`ranking.md` §6) and the capture control stream (`backtest.md` §1), and log
   one WARN with both generations and the reason class.
5. **Stamp cards.** For `failover.footer_window_s` (600 s), every card footer carries
   `source: geyser (switched 12 s ago)`; the boot line and heartbeat show `feed.authoritative`.
6. **Do not touch delivery.** Queues, ids, cooldowns and budgets are source-independent and
   continue. An alert detected 200 ms before the switch is still delivered.

Counters: `failover_switches_total{from,to,reason}`, `failover_switchover_ms`,
`failover_boundary_frames`, `failover_cold_rings_total`, `failover_switch_back_blocked_total`,
`failover_flap_guard_total`.

## 5. Warm standby and bucket semantics

The standby pipeline runs the same detectors and discards its candidates, counted
`standby_candidates` by family and verdict. That discard is free evidence: `standby_fires_total`
versus `live_fires_total` over the same window is a second disagreement series, reported per
stratum and never pooled.

**Bucket skew.** Both sources bucket on receive time (R1). The same trade arrives at a different
instant on each, so it can land on opposite sides of a 15 s boundary. During dual operation the
harness measures `source_bucket_skew`: for sampled mints, the per-bucket count difference between
sources in an aligned window, reported p50/p90. The difference is bounded by the receive-time
delta (≤ ~1 s), so the expected effect is boundary trades only; a p90 above 5% of a bucket is a
signal that one source's delay is not what it claims. It is measured, never corrected: rings are
never merged or resampled.

**Warm rings on switch.** The promoted pipeline's rings are already populated from its own
arrival history. They are not identical to the outgoing rings, and the card says so for 10
minutes. With `failover.warm_standby = false` the standby decodes and counts but does not own
rings; a switch then allocates cold rings, suppresses volume and counts `failover_cold_rings_total`.
Warm standby is the default and the reason failover is called hot.

**Boundary assertion.** Every bucket and partial carries the `source` it was built from (a flag
bit in `volume.md`'s `Bucket.flags`). A bucket containing two sources is an invariant violation;
the switchover drops the outgoing partial precisely so that it cannot happen.

## 6. Dedup and generations

Each source has its own dedup set and its own generation stream. The key is source-independent
(`(signature, mint, trader, side, token_amount, sol_lamports)`, `feed.md` §5), so it behaves the
same on a Geyser decode, but sets are **not shared**:

- Sharing one set across sources would let the standby's event suppress the authoritative one,
  or vice versa, depending on arrival order. That is a lost trade, not a dedup.
- On switchover the sets swap roles wholesale. A promoted source's set already contains every
  trade it saw while on standby, so its own overlap is still deduplicated.
- The outgoing source's late frames are discarded with the pipeline; they are not replayed into
  the new authority. The boundary count says how many.

Generations are per source. `Frame.generation` identifies the session that produced it; a
superseded generation on either source is dropped by that source's pipeline. `sockets_open` is
therefore per source (`sockets_open[p]`, `sockets_open[s]`), each 0 or 1; the process-wide total
is allowed to be 2. The old "one socket, asserted" invariant becomes **one socket per provider,
two providers maximum**, and the ban logic in `feed.md` §4.4 applies per session.

## 7. Gap accounting and coverage

The coverage journal (`ranking.md` §6) gains a `SWITCHOVER` record carrying both sources'
coverage at the instant of the swap, the reason, the switch duration and the boundary frames
lost. The weekly job reads it the same way it reads `GAP_START`/`GAP_END`:

- A window covered by the authoritative source is covered, whichever source it was. Switching
  does not create a coverage gap by itself; the boundary loss and any blind interval do.
- A source that was on standby and has its own gaps contributes nothing to coverage while on
  standby, and its gaps are not charged to the authoritative window. On promotion, its warm
  history is used and its own gaps are declared from its ledger, not from the outgoing source.
- The report and the watch-set snapshot carry `sources_used` and the switch count. A week that
  ran mostly on the secondary says so; a ranking that silently changed source mid-week is a
  coverage fact, not a footnote.

If warm standby is off, the promoted source starts at the switch instant and the interval until
its first accepted event is `failover_cold_s`, declared unhealed, not zero.

## 8. The secondary adapter

The secondary is a `Session` implementation like any other (`feed.md` §2), with its own decoder
and capability record:

| Capability | PumpPortal | Geyser secondary |
|---|---|---|
| `has_slot` | No | **Yes** |
| `has_block_time` | No | **Yes** |
| `has_signature` | Yes | Yes |
| `has_curve_reserves` | Yes, pre-migration | Only with account updates for the curve; otherwise derive after decode |
| `has_dex_trades` | Post-migration **U** | Decode-dependent |
| `has_sol_amount` | Optional payload field | Decoded instruction amount |
| `has_tx_index` | No | Yes |

A third `SolSource::Instruction` joins `Payload` and `ReserveDelta` for the decoded SOL leg.
Detectors never branch on the source; they branch on the capability record, as today. The
secondary's trade identity includes the instruction index, which is exactly the key `feed.md`
§5 already prefers when it exists.

**Own decode is the cost.** The Geyser adapter must parse pump.fun buy/sell instructions,
creator/fee accounts, bonding-curve account data and migration transactions, and it must pin
those layouts in a fixture test before M6 acceptance. A provider that ships a decoded pump.fun
stream with a permissive client is materially cheaper to build and should be preferred if one
exists at M6 time; the contract is the capability record and the trade shape, not the transport.

## 9. Failure taxonomy

| Failure | Detected by | Response | User-visible |
|---|---|---|---|
| Primary transport dead | session exit | switch if secondary fresh; else reconnect budget | one outage identity; footer source after recovery |
| Primary silent, connected | watchdog + 3-mint probe | switch only if the probe disagrees and the secondary is fresh | degraded/stale per `SCOPE.md` §6.6 |
| Primary banned | `SessionExit::Banned` | switch; do not probe or retry until expiry | ban state, one outage card |
| Secondary transport dead | session exit | no switch; continue primary; count | `feed.secondary.state`, no card unless both down |
| Secondary silent/dropping | its own watchdog, shadow comparison | no switch away from a healthy primary; count disagreement | `feed.unverified` if the primary's shadow is the secondary and it disagrees |
| Both unhealthy | both states | no authority, `stale`, suppress, one outage identity | outage card |
| Secondary returns after a gap | its session + heal | it becomes a warm standby again only after its rings re-warm and a probe agrees; count `secondary_reready_s` | no card |
| Switch-back disagrees | aligned comparison | stay; `switch_back_blocked` | none on cards; heartbeat |
| Flap | switch count in 1 h | hold current authority 1 h | heartbeat, one WARN |

## 10. Configuration

| Key | Unit | Default | Rationale |
|---|---|---|---|
| `feed.secondary.enabled` | bool | false | Off until a provider passes the license and capability gate |
| `feed.secondary.provider` | enum | — | `yellowstone \| provider_name`; names the adapter and its license note |
| `feed.secondary.endpoint`, `feed.secondary.token` | secret url | — | Env-only; redacted like every other secret |
| `feed.secondary.commitment` | enum | `confirmed` | Stated because it changes latency and fork exposure |
| `feed.secondary.filters` | count | 256 | Bonding-curve account filters per stream; provider limit **U** |
| `feed.failover.prefer` | enum | `primary` | Target when both are healthy |
| `feed.failover.switch_after_s` | s | 30 | Same as outage declaration; a blip does not switch |
| `feed.failover.switch_back_after_s` | s | 300 | Five minutes of health before trusting the primary again |
| `feed.failover.secondary_fresh_s` | s | 10 | A standby with no event in 10 s is not fresh enough to promote |
| `feed.failover.compare_window_s` | s | 600 | Aligned with the shadow window; comparison needs a settled window |
| `feed.failover.max_switches_1h` | count | 3 | Three moves is already a bad hour |
| `feed.failover.flap_cooldown_s` | s | 3600 | Hold long enough for the operator to see it |
| `feed.failover.footer_window_s` | s | 600 | How long cards name the source |
| `feed.failover.warm_standby` | bool | true | The only setting that makes failover hot |
| `feed.failover.require_secondary` | bool | false | When true, boot is red and the boot line says failover unavailable if the secondary fails its check |
| `feed.secondary.startup_check_s` | s | 10 | Same as the primary's first-frame budget |

The secondary's provider spend is tracked in the provider dashboard until measured; no internal
dollar cap is invented before M6's C14 records real pricing. The PumpPortal budget
(`gate.feed_sol_per_day`) remains primary-only; the secondary's filters are capped by
`feed.secondary.filters` and its provider plan.

## 11. `/health`

| Field | Meaning |
|---|---|
| `feed.authoritative` | `primary \| secondary \| none` |
| `feed.source_state{primary,secondary}` | `connected \| suspected \| reconnecting \| banned \| failed \| unavailable` |
| `feed.sockets_open{primary,secondary}` | Each 0 or 1; total ≤ 2 |
| `feed.switches_total`, `feed.last_switch_ms`, `feed.switch_reason` | History |
| `feed.switch_back_blocked_total{reason}` | `unhealthy \| stale \| disagree \| flap` |
| `feed.failover_cold_rings_total`, `feed.boundary_frames_dropped` | The cost of the switch |
| `feed.secondary.{filters, events_per_s, gaps, reconnects, reready_s}` | Secondary truth, kept separate from the primary's |
| `feed.source_bucket_skew_ms{p50,p90}` | Boundary effect, per stratum, never pooled |
| `standby_candidates{family,verdict}`, `standby_fires_total` | What the standby would have done; evidence, not alerts |
| `feed.unverified` | Amber unless a shadow agrees; two sources do not remove the tag when the comparison is over tolerance |

`authoritative = none` is red. `switch_back_blocked` is amber. A secondary that is configured but
`unavailable` is amber and the boot line says `failover unavailable`.

## 12. Acceptance tests (M6)

| # | Test | Pass condition |
|---|---|---|
| FO1 | 20 forced primary failures (close, ban, silent stall), secondary ready; space trials beyond flap cooldown | Each switches within `switch_after_s + 5 s`; switches counted; no duplicated eligible alert; boundary loss, cap drops and freshness expiry accounted separately; outage identity only when no authority remains |
| FO2 | 20 forced secondary failures | No switch; `secondary.state` cycles; alert stream unchanged; no card |
| FO3 | Both providers down 5 min | `authoritative = none`, one outage card, zero alerts, recovery switches once and edits the card |
| FO4 | Alternating failures every 60 s | ≤ 3 switches in the hour, then `flap_guard`; no ring merge; no duplicate candidate |
| FO5 | Primary recovery with a 12% count disagreement | No switch-back; `switch_back_blocked{disagree}`; alerts carry `unverified` |
| FO6 | Warm standby for 10 min, then switch | Zero warmup suppression on promoted mints; `cold_rings_total = 0`; each bucket has one source flag |
| FO7 | Warm standby off, then switch | N promoted mints re-warm; `cold_rings_total = N`; volume suppressed and counted |
| FO8 | Switch during an open partial bucket | Outgoing partial dropped; no bucket contains two sources; boundary frames counted |
| FO9 | Replay a recorded two-source day with the recorded health timeline | Identical switch decisions, boundary counts and candidate order; `report_sha256` stable across 10 runs |
| FO10 | Secondary startup check fails at boot with `require_secondary = true` | Boot red, `failover unavailable` in the boot line, primary still runs if its own check passes |
| FO11 | Per-source dedup | The same trade delivered on both sources while one is authoritative is counted once in that source's ring; on switch, the promoted ring contains it once |
| FO12 | Generation swap | 1k frames from the demoted source after switchover are discarded 100%; no candidate references them |
| FO13 | Card footer | For 600 s after a switch every card names the source and the switch age; after 600 s it does not |
| FO14 | Ban on primary and secondary | No authority, no sockets reopened before each provider's own `banned_until`, no key/IP rotation |

FO9 uses the controller replay mode in `backtest.md`: both source histories stay isolated, while
control inputs and switches share a logical application order. Single-source mode cannot claim
to validate arbitration. New POSTs still obey their original freshness deadline after a switch.
Promotion does not reconstruct a missing wallet basis; transfer/incomplete episodes stay unpaired.

## 13. Cost, stated

| Item | Estimate | Notes |
|---|---|---|
| Second pipeline CPU | ~2× decode + detect: ~4% of a core typical, < 1 core at peak | The pipeline is the same code; no second detector implementation |
| Memory | +~27 MB registry, +~15 MB dedup, +~13 MB rings at 10k gated mints, +~5 MB decode buffers | RSS ~120 → ~180 MB; raise `MemoryMax` from 384 to 512 MB before M6 |
| Provider spend | Paid, **U** | Measure and record in M6 C14; no internal dollar cap until then |
| Build | Geyser adapter + own decode + arbitration + replay of two sources | The largest single milestone after M1; it is M6, not an M2 detail |
| Ongoing | Two feeds to monitor, two sets of gaps, one more credential | The runbook gains a provider column and a failover row |

## 14. Licensing gate

Verified 2026-10-25: upstream [LICENSING.md](https://github.com/rpcpool/yellowstone-grpc/blob/master/LICENSING.md)
states the repository default is AGPL-3.0-only, but `yellowstone-grpc-client`,
`yellowstone-grpc-client-nodejs`, `yellowstone-grpc-proto` and examples are **Apache-2.0**.
The Rust [client manifest](https://github.com/rpcpool/yellowstone-grpc/blob/master/yellowstone-grpc-client/Cargo.toml)
and [proto manifest](https://github.com/rpcpool/yellowstone-grpc/blob/master/yellowstone-grpc-proto/Cargo.toml)
also declare Apache-2.0. Revision 5's blanket client prohibition was incorrect.

M6 may use the standard client after recording the exact package versions, source revision,
component license files and complete locked dependency tree. CI runs `cargo deny` against that
tree with the project's permissive-license policy. Do not copy the AGPL server implementation
into the bot. A generated client is an option, not a workaround required by the current client
license. Recheck upstream at dependency selection; these moving links are evidence for the plan,
not a substitute for pinned release evidence.

## 15. Open questions

| # | Question | Recommendation | Falsified by |
|---|---|---|---|
| 1 | Which secondary provider? | A Geyser-protocol provider with a permissive client or generated client; prefer a decoded pump.fun stream if one exists | No provider passes the license gate, or none carries slot and block time |
| 2 | Warm standby cost | Keep it on: cold failover defeats the purpose | Measured RSS above 512 MB or CPU above 1 core sustained |
| 3 | Switch-back on disagreement | Stay on the secondary; do not return to a disagreeing primary | A week where the secondary is the worse source on every stratum |
| 4 | Switchover during a partial bucket | Drop the partial; do not wait up to 15 s | Measured boundary loss above one bucket on > 1% of switches |
| 5 | Two sources in the shadow metric | Report `standby vs live` as a second series, never pooled with the RPC shadow | The standby disagrees for reasons that are not loss (stated per report) |
| 6 | Per-source dedup memory | Keep two sets; sharing loses trades | Measured dedup memory above 32 MB per source at peak |
| 7 | Secondary provider caps | Measure `filters` and events/s in C14 before fixing `feed.secondary.filters` | A provider limit below the desired set, forcing a gate change |
| 8 | Requirement flag | Default `require_secondary = false`; production sets true once the provider is proven | An operator runs production single-source and believes failover is on |

