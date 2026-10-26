# Delivery — Discord subsystem

Status: M2a design, 2026-10-25. Contract: [`../SCOPE.md`](../SCOPE.md) §8, decisions R5/R8.
Siblings: `feed.md`, `volume.md`, `bundle.md`, `operations.md`. Transport and admission only; signals
come from `detect/`. No outbox, no spool, no store the alert path reads.
Tags: `[doc]` documented, `[obs]` observed on staging, `[folk]` community-reported planning number.

## 1. Component layout

```
detect ──Alert──▶ per-webhook Scheduler
   ├─ admission: dedup → cooldown → budget → priority → hold
   ├─ render + validator (sanitize, truncate, count chars)
   ├─ id store (RAM, keyed (webhook, mint, family, wallet, episode, kind))
   ├─ sender: one task, one serialized queue, posts and edits
   └─ buckets, breaker, 429 pause, health counters
                   ▼  POST ?wait=true  ·  PATCH /webhooks/{id}/{token}/messages/{mid}
```

One task per webhook runs the scheduler and awaits its own HTTP call, so exactly one request is in flight per webhook; `render` is pure: `render(&Alert, &RenderCtx) -> RenderedEmbed`.

## 2. Data structures

```rust
pub type Mint = [u8; 32];
pub enum Family { Volume, Wallet, Cto }
pub enum WebhookId { Volume, Wallet, Cto }
pub enum Tier { Near, Fire, Cleared }        // bundle 60/80/95; volume uses Near/Fire
pub enum Target { NewPost, Edit(u64), Escalate }
pub enum CandState { Held, Admitted, InFlight, Done, Dropped(DropReason) }
#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord)] pub enum Priority { Status = 0, CtoListing = 1, WalletExit = 2, WalletEntry = 3, BundleEdit = 4, VolumeFire = 5, VolumeNear = 6 }
pub enum WalletKind { Entry, Exit }
pub struct Alert {
    pub id: u64, pub family: Family, pub priority: Priority, pub mint: Mint,
    pub symbol: String, pub name: String,        // attacker-controlled; sanitized at render
    pub key: AlertKey, pub cooldown: Option<CooldownKey>, pub detected_at: Instant,
    pub body: AlertBody, pub card_state: CardState, pub target: Target,
}
pub enum AlertBody {                            // numbers only; strings resolved at the card
    Volume { ratio: f32, sol: f64, baseline_sol: f64, mad_sol: f64, takers: u16,
             top_taker_share: f32, nonempty_buckets: u8, regime: Regime, usd_rate_age_ms: u64 },
    Wallet { kind: WalletKind, wallet: [u8; 32], size_sol: f64, share_of_launch: Option<f32>,
             rank: Option<RankQuote>, first_seen_ms: i64, new_for_wallet: bool,
             exit: Option<ExitView> },
    Cto { claim_date: i64, bundle: BundleView, price: PriceContext, non_pumpfun: bool },
}
pub struct ExitView {                          // present iff kind == Exit
    pub sold_share: Option<f32>, pub proceeds_sol: f64, pub realised_sol: Option<f64>,
    pub closed: Option<bool>, pub hold_ms: Option<i64>, pub entry_price: Option<f64>, pub exit_price: f64,
    pub basis: ExitBasis, pub transfer_out: bool, pub entry_delivered_ms: Option<i64>,
}
pub enum ExitBasis { Balance, SellSizes, Unknown }
pub struct Candidate { pub alert: Alert, pub seq: u64, pub enqueued_at: Instant, pub not_before: Instant, pub deadline: Instant, pub state: CandState }
pub enum DropReason { Dedup, Cooldown, DailyCap, HourlyCap, NearCap, NearReserve, ExitCap, Stale, Overflow, NoChange, WebhookDead, Shutdown }
pub struct MessageRef { pub webhook: WebhookId, pub message_id: u64, pub channel_id: u64,
    pub family: Family, pub kind: Option<WalletKind>, pub mint: Mint, pub tier: Tier, pub card_state: CardState,
    pub marker: String, pub posted_at: Instant,  // "ocw:v2:<mint>:<family>[:<wallet>:<kind>:<episode>]" in the footer
    pub last_edit_at: Option<Instant>, pub pending: Option<(Tier, CardState, Instant)> }
pub struct Scheduler {
    pub id: WebhookId, pub bucket: TokenBucket,  // capacity 2–3, refill hourly_cap/3600 per s
    pub status_lane: TokenBucket,                // capacity 1, refill 1 per 5 s
    pub hold: BinaryHeap<Reverse<Candidate>>,     // min-heap on (Priority, seq); never a max-heap
    pub ready: VecDeque<Candidate>,
    pub hourly: SlidingWindow, pub daily: DailyCounter,   // 3600 s posts; UTC day; near/fire sub-counts
    pub breaker: Breaker, pub paused_until: Option<Instant>, pub inflight: Option<Inflight>,
}
pub struct TokenBucket { pub capacity: f32, pub tokens: f32, pub refill_per_sec: f32, pub last: Instant }
pub struct DeliveryState {
    pub started_at: Instant, pub restart_generation: u32, pub config_hash: [u8; 32],
    pub messages: LruMap<(WebhookId, Mint, Family, Option<[u8;32]>, Option<WalletKind>, u128), MessageRef, 2048>,
    pub dedup: Dedup, pub cooldown: Cooldown, pub outage: [OutageState; 6], pub counters: Counters,
}
```

`Priority` derives `Ord` with `Status` lowest, and the hold is a **min-heap**
(`BinaryHeap<Reverse<Candidate>>`), so selection pops `Status` first, then CTO, wallet exit,
wallet entry, bundle, fire, near. Within a tier `seq` is FIFO; `WalletExit` orders ties by rank PnL
desc, then sold share desc, then `seq`; `WalletEntry` by rank PnL desc, then buy size desc, then
`seq`. The comparison is stated once here so an implementation cannot invert it. Delivery owns
every message id; detectors keep tiers only, never ids. A wallet message id is keyed by wallet, mint, episode and kind,
because multiple wallets and episodes can share a mint. Successful entry POSTs emit a bounded
`EntryDelivered(episode_id, at)` callback to live matching. Failed/unknown POSTs never set it. The upstream values behind `sol` and
`size_sol` are `sol_lamports` with `sol_source = Payload | ReserveDelta | Instruction` (`feed.md`
§2); no card or admission path reads raw `solAmount`.

## 3. Admission algorithm

Per webhook, on enqueue and every 250 ms, in this order. Drops precede tokens, so a duplicate never
costs budget.

1. Ingest: assign `seq` and `deadline = detected_at + deliver.max_age.<kind>_ms`.
   Volume fire, wallet entry and wallet exit expire at 30 s; volume near at 15 s; CTO listings
   and bundle edits at 120 s (PRIOR product limits). `VolumeNear` and `WalletEntry` have
   `not_before = detected_at + deliver.hold.window_ms` (5 s) for reordering; other classes
   are immediately eligible. Status has no alert deadline. Expire stale candidates before
   selection and recheck immediately before every POST/PATCH attempt, including after a 429.
2. **Dedup.** Key `Queued|InFlight|Sent` → drop `Dedup`; key `Unknown` (ambiguous POST) → drop,
   `drop_ambiguous_replay++`; absent → insert `Queued`.
3. **Cooldown.** Exits carry no cooldown (`alert.cooldown = None`): one exit per episode is already
   enforced by its dedup key, and the entry cooldown must not swallow the exit. For entries, inside
   TTL → drop `Cooldown`; a tier escalation with a live `MessageRef` is exempt and becomes an edit
   (§5).
4. **Status.** Skips 5–9 and the near sub-cap, uses the status lane, `status_bypass++`; still obeys
   429 and the breaker. Status and heartbeat edits consume no alert token, hourly slot, daily slot
   or near sub-cap.
5. **Daily cap.** Posts only; used ≥ cap → `DailyCap`. Edits exempt (§5).
6. **Hourly window.** Posts in the last 3600 s ≥ cap → `HourlyCap`.
7. **Near sub-cap, both windows.** `near_posts_1h ≥ ceil(0.30 · hourly_cap)` **or**
   `near_posts_today ≥ ceil(0.30 · daily_cap)` → `NearCap`, counted `drop_near_cap` with the
   window in the log; step 9 still applies.
8. **Selection.** Admit the **minimum** of `(Priority, seq)` —
   `Status < CtoListing < WalletExit < WalletEntry < BundleEdit < VolumeFire < VolumeNear` — among eligible, unexpired candidates when
   a token exists; `Status` waits only for its own lane. The hold is a min-heap, never a max-heap.
   `WalletExit` ties order by rank PnL desc, then sold share desc, then `seq`; `WalletEntry` ties
   order by rank PnL desc, then buy size desc, then `seq`.
9. **Fire reservation, both counters.** Hourly: `reserve = max(0, fire_reserve_per_hour −
   fire_posts_1h)`, `non_fire = tokens − reserve`: near needs `non_fire ≥ 1.0`. Daily:
   `reserve_day = max(0, fire_reserve_per_hour · daily_cap / hourly_cap − fire_posts_today)`;
   near needs `daily_remaining − reserve_day ≥ 1` (`daily_remaining = daily_cap − posts_today`),
   else `NearReserve`, counted `drop_near_reserve`. Fire, CTO, wallet and bundle may take any token
   and the reserved daily slot. Recomputed each tick.
10. **Exit sub-cap.** `exit_posts_1h ≥ deliver.wallets.exit_hourly` (3) → `ExitCap`, counted
    `wallet_exit_suppressed{cap}`. Exits still share the wallet hourly and daily caps; the sub-cap
    only stops them from taking every token. Entries are never refused in order to save a token for
    an exit that may never come.
11. **Burst and grant.** Capacity 3 (volume) / 2 (wallets, CTO) spendable at once; refill
    `hourly_cap / 3600` per s. On grant take the token, increment hourly and daily, set dedup
    `InFlight` *before* the HTTP call — that flag makes an ambiguous POST non-retryable. Status
    and heartbeat edits increment neither window.
12. **Deadline expiry.** `now >= deadline` → `Stale`, counted
    `drop_stale{family,kind}`, whether waiting for reordering, tokens, transport or a 429.
    No deadline is extended by a refill, retry, source switch or newer price lookup. A stale
    candidate is not posted later as a live event. Shutdown counts remaining `drop_shutdown`;
    Status has no alert expiry.
13. **Overflow.** `hold + ready > 256` → drop lowest priority, highest `seq`; status never dropped.

Priority orders admission and survival; it never changes a cap. Only `Status` bypasses, only on its
own lane.

**Watch-set expectation.** The ~40-wallet watch set is a discovery set, not a delivery promise:
~120 entry candidates/day plus exits on observed positions, ~240 against the 20/day cap, so roughly
92% are suppressed by budget and counted `wallet_budget_suppressed` — the Wallet-family daily and
hourly cap drops.

**Freshness and survival.** Every card carries event/receive time where known, detection time
and age at send. Measure receive→detect, detect→POST start, HTTP completion and expiry separately;
feed lag cannot be inferred from a payload without execution time. The pre-production report
lists candidates, delivered/unknown/expired/cap-dropped counts and age p50/p95/p99 by family and
wallet rank band. Show paired exit coverage separately for openings observed and entries
successfully delivered. Keep the 40-wallet discovery set and the existing caps initially; the
~92% suppression estimate is a prior to measure, not acceptance evidence. A release note states
whether the observed survivors meet the product's timeliness and attention goals before enabling
the family. A dry replay cannot prove Discord delivery latency.

## 4. Dedup and cooldown are separate structures

Dedup asks *have I already acted on this exact event*; cooldown asks *is a new event too soon*.

| | Dedup | Cooldown |
|---|---|---|
| Key | `AlertKey` (event) | `CooldownKey` (subject) |
| TTL | volume 90 s, wallet 24 h, CTO 30 d, bundle = card life | volume fire 300 s, near 1800 s, wallet 3 h, bundle edit `deliver.edit.min_interval_ms` (45 s) |
| State, on hit | RAM only; drop, no budget consumed | RAM only; drop, counted |

```rust
pub struct AlertKey { pub mint: Mint, pub wallet: Option<[u8;32]>, pub family: Family, pub kind: Option<WalletKind>, pub window: u128 }
// window: volume = bucket_close_unix / 15 · wallet entry or exit = episode_id
//         cto = claim_date · bundle = tier as u64
pub enum Signal { Volume, Cto, Bundle }
pub enum CooldownKey { Mint { mint: Mint, signal: Signal },
                       WalletMint { wallet: [u8; 32], mint: Mint } }   // entries only
```

**Wallet key correction.** Dedup is `(wallet, mint, kind, episode_id)`. The kind separates
an entry and its exit on the same position, which share the episode start; the episode start
separates a re-entry weeks later from the position it replaced. Without both, an exit is dropped as
a duplicate of its own entry forever. Cooldown stays `(wallet, mint)` and applies to entries only:
two entries inside 3 h are one play developing, while an exit is the resolution of the position the
reader was already told about. Dedup first. Both are LRU (4096 / 8192) by insert order; a restart may
re-send, which is accepted.

## 5. Message lifecycle

**Post.** `POST {url}?wait=true` `[doc]`; a 204 means no id, which is a contract violation — count
`wait_ignored`, treat as `Unknown`, never as success. On 2xx store `id`, `channel_id`, tier and
`card_state`; a missing id is a delivery failure.

**Edit coalescing.** Only a bundle Fire or Cleared transition with a ref edits: it sets `pending`,
and a newer Fire/Cleared transition replaces it, so Near→Fire→Cleared in 40 s is one PATCH to
Cleared (`edits_coalesced += 2`). A Near transition never PATCHes; it updates `card_state.tier` in
the ref and does nothing else. Send when `now − last_edit_at ≥ deliver.edit.min_interval_ms` (45 s)
and a token is free. Alert edits take a token and the hourly window but **not** a daily slot: a full
daily cap must not hide the strongest state. Status and heartbeat edits use the status lane and
bypass the hourly, daily and near sub-cap; `deliver.health.heartbeat_interval_ms` (5 min) is a
delivery cadence, independent of the alert budgets.

**Tiers.** Bundle is monotonic Near → Fire → Cleared, never downward. A Near transition updates
internal state and posts nothing unless no card exists and the tier is already at or above Fire;
it never PATCHes. A stale read sets `card_state.stale` and re-renders with the STALE word;
it never lowers a tier. Same or lower tier → `NoChange`, no PATCH. Volume does not transition; a
second fire is a new `window`, governed by dedup and cooldown.

**Deleted message.** Invisible until the next PATCH returns 404/10008: drop the ref,
`edit_target_gone++`, and at Fire or above post a replacement, counting `escalation_without_target`.
Near simply stops. Never retry a dead id.

**Restart.** The store is RAM-only, so it is empty; `restart_generation++` and the boot line in
`operations.md` says so. An escalation posts a new card marked `post-restart` and counts
`escalation_without_target`, rather than a cooldown swallowing the tier. CTO idempotency is a
persisted high-water `claimDate` **plus** a bounded posted-token set, not high-water alone; priming
both on boot does not alert, and a claim first seen below the high-water is counted
`suppressed_by_high_water` and is not recovered. The bundle seen-set is authoritative for bundle
edits; delivery dedup is a second line. Wallet cooldowns are lost; count `cooldown_lost_on_restart`
and accept it. At-most-once is per process; re-sending beats a silent drop.

**Readback.** Optional: `readback.enabled`, a bot token, `deliver.<name>.channel_id`, Read Message
History, and Manage Messages to edit `[doc]`. At boot, `GET /channels/{id}/messages?limit=100`, parse
footer markers, rebuild refs. It turns "one card per process" into "one per channel". Off by default.

## 6. Error classification

Status is checked before the body is parsed, so a Cloudflare HTML 403 never lands in the payload class.

| Response | Code | Class | Action | Counter |
|---|---|---|---|---|
| 429 scope `user` | — | transient | pause this webhook to `retry_after`; requeue at its priority head; no breaker | `rate_limited_429` |
| 429 scope `shared` | — | transient | pause this route only; requeue | `rate_limited_shared` |
| 429 scope `global` | — | transient | pause all three webhooks; health degraded; no breaker | `rate_limit_global_pause` |
| 404 on PATCH | 10008 | target gone | drop id, stop editing, replacement per §5, no retry | `edit_target_gone` |
| 404 on POST | 10015 | webhook dead | breaker, health red, out-of-band alert, no retry | `webhook_dead` |
| 401 | 50027 | webhook dead | as above; never retry (invalid-request budget `[folk]`) | `webhook_dead` |
| 403 | 50001/50013 | permissions | breaker, health red, no retry | `webhook_forbidden` |
| 400 / 413 | 50035 | payload | drop payload, log redacted, no breaker, no retry | `payload_rejected` |
| 5xx, timeout, reset | — | ambiguous | POST: log payload, no retry. PATCH: retry ×3, idempotent | `delivery_unknown` / `edit_retried` |
| DNS, connect refused | — | pre-send | retry ×3 with jitter, then breaker | `delivery_retry` |

`retry_after` is seconds, possibly fractional `[doc]`; the transport bound is 5 posts per 2 s per webhook
`[folk, observed burst, not contractual]`, far above these budgets. Honour `max(body, Retry-After) + 0.25 s`
(PRIOR margin for clock skew). A 429 never opens the breaker — repeated 429s mean the budget is too
high, a config decision surfaced in `/health`. Invalid requests can temporarily ban the IP
`[folk, 10k per 10 min]`, so they are never retried.

Breaker: `Closed`; `Open { since, class }` for dead or forbidden; `HalfOpen` after 60 s with one probe. Success closes
(`breaker_recovered++`), failure reopens. While open, that family's posts drop `WebhookDead`; nothing queues indefinitely.

## 7. Card specification

Limits `[doc]`: 10 embeds/message, 6000 chars/embed, title 256, description 4096, 25 fields, field
name 256, field value 1024, footer 2048, author 256, content 2000. Budgets (PRIOR, headroom for
edits and growing numbers): title 180, description 900, field name 60, field value 240, footer 180,
whole embed 2400.

**Layout.** Description line 1 is the decision and fits 100 chars on a phone: tier word plus one
number with units. Then one signal per line. The mint sits on its own line in full (44 chars, never
truncated). Links live in one field, not in prose. Footer: family, tier word, budget position,
health, readback mode. **Colour is paired with a word** in title and footer: FIRE `0xED4245`, NEAR
`0xFEE75C`, CLEARED `0x3BA55D`, STALE `0x95A5A6`, CTO `0xFEE75C`, ENTERED `0x3BA55D`, EXITED
`0xED4245`, OUTAGE/RECOVERED `0xED4245`/`0x3BA55D`.

**Truncation priority** — cut from the bottom until it fits. Never cut: the mint, the tier word and decision number,
the percentage or primary metric (including the exit sold share and realised SOL when known),
bundle price context, the health footer. Then, first to last: why-it-fired
diagnostics (last line first), coverage and cohort detail, chart links (dropped whole), name and symbol (falling back to
the mint prefix), timestamps and rate age (logged as `truncated`).

The validator runs before send: no field over budget, a number never separated from its unit, a URL
never cut, embed under 2400 chars, non-empty title, mint matches the alert. Local failure →
`card_validation_failures++` and drop; Discord never gets to truncate the number that mattered.

| Family | Title | Description lines | Fields |
|---|---|---|---|
| volume | `FIRE 3.7x \| SMPL` | decision, mint, why (ratio/MAD/takers/buckets), price + rate age, regime + coverage | `Chart` |
| wallet entry | `ENTERED 4.2 SOL \| 9xQe` | decision, mint, wallet + rank, size + share, new-for-wallet, rank basis | `Chart` |
| wallet exit | `EXITED 90% \| 9xQe` | decision, mint, wallet + rank, sold share + remaining, realised + basis, entry/exit price, hold time, pairing | `Chart` |
| CTO + bundle | `CTO  FIRE 82% \| SMPL` | decision, mint, bundle arithmetic, price + dump word, basis, CTO wording | `CTO listing`, `Chart` |

```json
{"username":"ocw","allowed_mentions":{"parse":[]},"embeds":[{
 "title":"FIRE 3.7x | SMPL","color":15548997,"description":"**3.7x baseline — 7.7 SOL in 15 s**\nMint: 7f3aQ9m2Kd4Vx1s8Rb6TpNyWc5HjLzUe3AoGvB2nMqSd\nWhy: baseline 2.1 SOL · MAD 0.31 · 12 takers, top 41% · 8/30 buckets non-empty\nPrice: 0.00000041 SOL (+12% / 15 s) · USD 0.000074 (rate 62 s old)\nRegime: bonding curve · watched 14 min · shadow agrees (4% diff)",
 "fields":[{"name":"Chart","value":"[DexScreener](https://dexscreener.com/solana/7f3aQ9m2Kd4Vx1s8Rb6TpNyWc5HjLzUe3AoGvB2nMqSd) · [pump.fun](https://pump.fun/coin/7f3aQ9m2Kd4Vx1s8Rb6TpNyWc5HjLzUe3AoGvB2nMqSd)","inline":false}],
 "footer":{"text":"volume FIRE · 3/40 today · 2/10 this hour · feed unverified, single source"},
 "timestamp":"2026-10-25T17:12:15Z"}]}
```

```json
{"username":"ocw","allowed_mentions":{"parse":[]},"embeds":[{
 "title":"ENTERED 4.2 SOL | 9xQe","color":3908957,"description":"**ENTERED — 4.2 SOL, new position on SMPL**\nMint: 7f3aQ9m2Kd4Vx1s8Rb6TpNyWc5HjLzUe3AoGvB2nMqSd\nWallet: 9xQeWvG816bUx9EPfN5nQhHk3pQyYbTgWz8r4mLc2VdA\nWallet rank: #7, week to 2026-10-19 · +41.3 SOL realised · 5 mints · 38.8 SOL deployed · fees estimated\nSize: 4.2 SOL = 6.9% of the 61 SOL bought in the first 90 s\nPrice: 0.00000041 SOL · quote 3 s old",
 "fields":[{"name":"Chart","value":"[DexScreener](https://dexscreener.com/solana/7f3aQ9m2Kd4Vx1s8Rb6TpNyWc5HjLzUe3AoGvB2nMqSd)","inline":false}],
 "footer":{"text":"wallets ENTERED · 2/20 today · watch set gen 4, 3 days old · entries and exits"},
 "timestamp":"2026-10-25T17:14:02Z"}]}
```

```json
{"username":"ocw","allowed_mentions":{"parse":[]},"embeds":[{
 "title":"EXITED 90% | 9xQe","color":15548997,"description":"**EXITED — 90% of the position sold, 10% left**\nMint: 7f3aQ9m2Kd4Vx1s8Rb6TpNyWc5HjLzUe3AoGvB2nMqSd\nWallet: 9xQeWvG816bUx9EPfN5nQhHk3pQyYbTgWz8r4mLc2VdA\nWallet rank: #7, week to 2026-10-19 · +41.3 SOL realised · 5 mints · 38.8 SOL deployed · fees estimated\nSold: 90% of peak 30,000,000 · 4.02 SOL proceeds · realised +0.85 SOL · basis balance, fees estimated\nPrice: entry 0.000000117 SOL → exit 0.000000149 SOL (+27%) · held 42 min\nPaired: entry delivered 42 min ago · transfer out: none",
 "fields":[{"name":"Chart","value":"[DexScreener](https://dexscreener.com/solana/7f3aQ9m2Kd4Vx1s8Rb6TpNyWc5HjLzUe3AoGvB2nMqSd)","inline":false}],
 "footer":{"text":"wallets EXITED · 4/20 today · 1/3 exits this hour · watch set gen 4, 3 days old"},
 "timestamp":"2026-10-25T17:56:02Z"}]}
```

```json
{"username":"ocw","allowed_mentions":{"parse":[]},"embeds":[{
 "title":"CTO  FIRE 82% | SMPL","color":16705372,"description":"**Bundle 82% sold — tier FIRE**\nMint: 7f3aQ9m2Kd4Vx1s8Rb6TpNyWc5HjLzUe3AoGvB2nMqSd\nBundle: 14 wallets · 61.2 SOL bought · 3.1% supply · 82% sold (50.2 SOL) · 11/14 wallets fully out\nPrice: 0.00052 at first sell → 0.00032 now · peak 0.00068 · −38% during sell window (DUMPED)\nBasis: same funder, one hop only; two-hop not followed; moved wallets and Raydium sells uncounted\nCTO: a claim on a curated list. It certifies someone claimed the token, not who holds it.",
 "fields":[{"name":"CTO listing","value":"claimDate 2026-10-24T09:12:00Z · DexScreener community takeovers · bundle edit T4→FIRE","inline":false},{"name":"Chart","value":"[DexScreener](https://dexscreener.com/solana/7f3aQ9m2Kd4Vx1s8Rb6TpNyWc5HjLzUe3AoGvB2nMqSd) · [pump.fun](https://pump.fun/coin/7f3aQ9m2Kd4Vx1s8Rb6TpNyWc5HjLzUe3AoGvB2nMqSd)","inline":false}],
 "footer":{"text":"cto FIRE · 4/120 today · 9/20 this hour (safety cap) · feed normal"},
 "timestamp":"2026-10-25T17:20:00Z"}]}
```

## 8. Sanitization

Names are attacker-controlled and arrive inside a card people trust. Layers, none load-bearing alone:

1. NFKC normalize; strip `Cf` (U+200B–200D, U+2060, U+FEFF) and bidi controls (U+202A–202E, U+2066–2069, U+061C).
2. Remove `` \ ` * _ ~ | [ ] ( ) < > `` — `<`/`>` kill mention, emoji and timestamp syntax; brackets and parentheses kill links.
3. Collapse whitespace, strip newlines and control characters, trim.
4. Render inside one code span (backticks already gone), so Markdown is inert and even `@everyone` does not ping inside inline code.
5. Cap at a char boundary (symbol 32, name 64), `…` only if it fits; fall back to the mint prefix if the result is empty or punctuation.
6. Every payload carries `{"allowed_mentions":{"parse":[]}}` — the actual control; rule 4 is the second line.

Worked example, input symbol:

```
**@everyone** [claim free SOL](https://evil.example/x)​‮ɐpuɐ
```

(two zero-width spaces after the link, an RTL override before `ɐpuɐ`.)

| Step | Result |
|---|---|
| NFKC | unchanged |
| strip `Cf`, bidi | `**@everyone** [claim free SOL](https://evil.example/x)ɐpuɐ` |
| remove Markdown chars | `@everyone claim free SOLhttps:evil.examplexɐpuɐ` |
| collapse, cap 32, code span | `` `@everyone claim free SOLhttps:e` `` |

The mention is inert, the URL is destroyed rather than shortened (a short link in a trusted card is worse than none),
and the mint stays canonical. `name_sanitized++` on change; our own links, the mint, numbers and footer markers are never sanitized.

## 9. Outage, status, fallback, dead-man's switch

Identity `(subsystem, outage_start_unix)`; six subsystems: `trade_stream`, `cto_source`, `rpc`,
`discord:volume`, `discord:wallets`, `discord:cto`. States `Healthy`, `Suspected`,
`Declared { start, message }`, `Recovering`: declared after 30 s continuous unhealthy, recovered
after 60 s healthy (`ops.outage.declare_s` / `recover_s`, owned by `operations.md`); a shorter blip
posts nothing and increments `outage_declined_blip`.

Declare card: title `OUTAGE | trade_stream`; start UTC, duration so far, reason class
(`socket_closed`, `no_events_90s`, `http_503`), what is suppressed (volume; warmup on return), what is
**not** (CTO posts; wallets if warm), `blind_seconds_1h`, last event age. Recovery **edits** the same
card: `RECOVERED | trade_stream`, end, duration, gap, re-warm estimate. One card per process lifetime
per subsystem; with readback off a restart cannot find the old card and posts a new one stamped
`since <process start>`, while the old card keeps its timestamps and the boot line carries
`restart_generation`. With readback on, `ocw:v1:status:<subsystem>` finds it and the limit is one per
channel.

Status bypasses budget, not transport: a status lane (capacity 1, one token per 5 s) stops three
failures racing into a 429. Never dropped for daily, hourly, near or hold expiry; `status_sent++`.

**Fallback.** If a family's webhook is open, the status card goes to the next closed webhook in order
volume → wallets → cto, titled `[fallback]`, `fallback_posts++`. It skips that channel's daily slots and
uses its status lane; all three open → `status_no_route++`, `discord` red, one error line, nothing buffered.

**Dead-man's switch.** Discord cannot report a dead process. `operations.md` owns an external pinger:
`deliver.deadman.ping_url` every 60 s, alerting after 5 minutes of silence. The heartbeat stops
updating when delivery is red, so a stale heartbeat and a silent pinger agree.

## 10. Operational modes

| Mode | Behaviour | Exit |
|---|---|---|
| `--dry` | render all three families from built-in fixtures; print exact JSON, char counts, validator results; no sockets | 0, or 2 if any payload fails |
| `--dry --alerts <file>` | same, from a JSONL fixture | as above |
| `--startup-check` | per webhook: POST `wait=true`, assert id, PATCH once, DELETE `[doc]`; print ids, never tokens | 0, or 3 on failure |
| `--startup-check --staging` | against `deliver.staging.*`; refuse if any URL equals production | as above |
| normal | startup check on staging first, then production; feed starts only if every enabled webhook passes | — |

`OCW_ENV=staging` prefixes titles with `[STAGING]`, multiplies hourly caps by 0.1 (floor 1), uses
staging URLs only; production refuses to start if unset.

**Redaction.** URLs are `https://discord.com/api/webhooks/{id}/{token}`. A `SecretUrl` newtype with a
custom `Debug` prints `.../{id}/[redacted]`; the token never enters a `Debug`-able field, and the panic
hook is installed before config parse. A failed POST logs the full sanitized payload, alert key, class
and status — enough to reconstruct the alert — never the URL. Startup failure names the key, not the value.

## 11. Config keys

All under `deliver.`, durations suffixed, PRIOR numbers carry their reason in the config file.

| Key | Unit | Default | Rationale |
|---|---|---|---|
| `deliver.{volume,wallets,cto}.webhook_url` | secret | — | required |
| `deliver.staging.{volume,wallets,cto}.webhook_url` | secret | — | required for staging |
| `deliver.{volume,wallets,cto}.channel_id` | id | — | boot-only; required when `deliver.readback.enabled` |
| `deliver.{volume,wallets}.hourly` / `deliver.cto.hourly_cap` | posts/h | 10 / 6 / 20 | SCOPE §8; CTO is a breaker, not a budget |
| `deliver.{volume,wallets}.daily` / `deliver.cto.daily` | posts/day | 40 / 20 / 120 | first two binding (SCOPE §8); CTO PRIOR outer guard |
| `deliver.{volume,wallets,cto}.bucket_capacity` | posts | 3 / 2 / 2 | cluster burst; 2 would drop a same-block pair |
| `deliver.volume.fire_reserve_per_hour` / `deliver.volume.near_share_max` | posts/h / fraction | 2 / 0.30 | 20% held for fire; near capped at 30% (SCOPE §2A) |
| `deliver.hold.window_ms` | ms | 5000 | Reorder delay for near and wallet entry only; 0–10000 and strictly below their expiry |
| `deliver.max_age.volume_near_ms` | ms | 15000 | Near expiry from detection |
| `deliver.max_age.volume_fire_ms` / `wallet_entry_ms` / `wallet_exit_ms` | ms | 30000 / 30000 / 30000 | Freshness overrides token wait and 429 pauses |
| `deliver.max_age.cto_ms` / `bundle_edit_ms` | ms | 120000 / 120000 | Claim/enrichment cards may wait longer; still expire before an attempt |
| `deliver.wallets.exit_hourly` | posts/h | 3 | exits share the 6/h wallet bucket but cannot take more than half of it; stops an exit storm crowding out entries |
| `deliver.edit.min_interval_ms` / `deliver.edit.retry_max` | ms / attempts | 45000 / 3 | owner: delivery (bundle has no interval key); one edit per card per 45 s; PATCH is idempotent, 3 bounds a flapping edit |
| `deliver.http.timeout_ms` | ms | 5000 | Discord p99 100–300 ms `[obs]`; 5 s bounds a hung socket |
| `deliver.breaker.probe_ms` | ms | 60000 | half-open cadence |
| `deliver.volume.cooldown_fire_ms` / `deliver.volume.cooldown_near_ms` | ms | 300000 / 1800000 | matches `volume.md` 300 s: one play, one card; near is cheap and must not become the channel |
| `deliver.wallets.cooldown_ms` | ms | 10800000 | 3 h inside the 1–4 h band |
| `deliver.wallets.dedup_ttl_ms` | ms | 86400000 | same position within a day is a duplicate |
| `deliver.cto.dedup_ttl_ms` | ms | 2592000000 | 30 d; high-water plus posted-token set also guard |
| `deliver.queue.capacity` / `deliver.ids.capacity` | cands / refs | 256 / 2048 | 2× the largest plausible hourly arrival; far above cards alive in a day |
| `deliver.health.heartbeat_interval_ms` | ms | 300000 | 5 min; cadence only, independent of the alert budgets; staleness visible within two beats |
| `deliver.readback.enabled` / `deliver.readback.bot_token` / `deliver.deadman.ping_url` | bool / secret / secret | false / — / — | webhook-only is the shipped contract; pinger required in production |
| `deliver.card.max_chars` | chars | 2400 | under 6000 with headroom for edits |

**Budget independence.** Status and heartbeat edits use the status lane and bypass the hourly,
daily and near sub-cap; the heartbeat interval is a delivery cadence, not an alert-budget input.

## 12. `/health`

```json
{"delivery":{"started_at":"2026-10-25T17:00:00Z","restart_generation":3,"config_hash":"9f2c…","readback":"off",
 "webhooks":{
  "volume":{"state":"closed","last_ok_age_s":14,"last_error":null,"posts_1h":3,"edits_1h":1,
            "daily_used":3,"daily_cap":40,"tokens":2.4,"status_tokens":1.0,
            "hold_depth":2,"queue_depth":0,"paused_until":null},
  "wallets":{"state":"closed"},"cto":{"state":"closed"}},
 "counters":{"posts_sent":41,"posts_failed":1,"delivery_unknown":0,"payload_rejected":0,
  "card_validation_failures":0,"edit_target_gone":1,"escalation_without_target":1,
  "edits_sent":9,"edits_coalesced":4,"edit_retried":0,"webhook_dead":0,"webhook_forbidden":0,
  "breaker_recovered":0,"rate_limited_429":2,"rate_limited_shared":0,"rate_limit_global_pause":0,
  "drop_dedup":6,"drop_cooldown":11,"drop_daily_cap":0,"drop_hourly_cap":1,"drop_near_cap":2,
  "drop_near_reserve":0,"drop_stale":4,"drop_shutdown":0,
  "drop_overflow":0,"drop_no_change":5,"drop_ambiguous_replay":0,
  "status_sent":2,"status_bypass":2,"status_no_route":0,"fallback_posts":0,"wait_ignored":0,
  "name_sanitized":1,"cooldown_lost_on_restart":0,"readback_found":0,"readback_missing":0,
  "wallet_budget_suppressed":96,"wallet_exit_suppressed_cap":2,"wallet_exit_suppressed_budget":0,"suppressed_by_high_water":0,
  "outage_declined_blip":4,"outage_cards_posted":1},
 "blind_seconds_1h":{"trade_stream":0,"cto_source":0,"rpc":12,"discord:volume":0,"discord:wallets":0,"discord:cto":0}}}
```

`blind_seconds_1h` sums intervals with no fresh event or no successful delivery in the last hour, per
subsystem. It is the number that separates quiet from dead; a gap is reported, never inferred from silence.

## 13. Acceptance tests for M2a

Synthetic alerts, staging webhooks, assertions on counters and payloads.

1. **Burst and freshness.** Enqueue 30 distinct volume fires at t=0 with a full capacity-3 bucket: 3 POSTs promptly; remaining 27 expire at 30 s, none posts at the 360 s refill. New candidates arriving after refill may use it.
2. **Daily binding.** 45 fires over a simulated day: 40 posts, 5 `drop_daily_cap`, no 429.
3. **Fire reserve.** 10 near with one fire at t=0: 1 near posts, 2 fire tokens remain spendable, the rest are `drop_near_cap` or stale expiry.
4. **Priority.** Fire enqueued after near with one token: fire sends; near is not eligible before 5 s and expires at 15 s into `drop_stale` if no token is available.
5. **Dedup and cooldown.** Same volume window twice: one post, `drop_dedup = 1`, no token consumed. Two fires on one mint 3 min apart: 1 post, 1 `drop_cooldown`; at 11 min: 2.
6. **Wallet key.** `(W,M,100)` twice → 1 post; `(W,M,200)` inside 3 h → `drop_cooldown`; outside 3 h → 2 posts.
7. **429.** `retry_after: 2.0`: no send before 2.25 s, no breaker, one `rate_limited_429`, identical bytes on retry. Scope `global` pauses all three.
8. **10008.** Delete a card, escalate: `edit_target_gone = 1`, replacement posted, `escalation_without_target = 1`, id cleared.
9. **Breaker classes.** Rotate the token: opens, one out-of-band alert, zero retries, red in `/health`. Force an oversized field: nothing sent, `payload_rejected`, breaker closed, payload logged redacted.
10. **Ambiguous POST.** Listener accepts and never replies: one `delivery_unknown`, no retry, payload logged.
11. **Restart.** Kill between Near and Fire: Fire posts a new card, `restart_generation` increments, `escalation_without_target = 1`, no CTO duplicate.
12. **Sanitization and truncation.** The §8 symbol renders inert with `name_sanitized = 1`; a 300-char name over a 900-char description drops the chart field, keeps mint and tier, stays under 2400 chars.
13. **Outage.** 20 s blip: nothing. 40 s outage: one card, edited on recovery, bypasses a full budget, one per process.
14. **Dry and startup.** `--dry` prints three valid payloads, exit 0; `--startup-check` proves POST, PATCH, DELETE; revoked token exits 3 naming the key, not the URL.
15. **Exit after entry.** Entry posts at T; exit at T+30 min on the same position: 2 posts, no `drop_cooldown`; the exit's footer shows the exit sub-cap position.
16. **Exit sub-cap.** Pace candidates just after wallet token refills so they remain fresh: at most 3 exits in the hour; remaining eligible tokens may go to entries. A simultaneous storm is tested separately and loses stale candidates rather than waiting ten minutes.
17. **Exit deadline.** An exit is immediately eligible; a token freed at 29 s permits a send,
but at 30 s the exit is `drop_stale`. A 429 crossing the deadline cannot extend it.
18. **Key separation.** The same `(wallet, mint, episode_id)` with kind entry and kind exit produces 2 posts; dedup does not collapse them, and the message-id store keeps both refs under different kinds.
19. **Exit card truncation.** A 300-char token name over a 900-char description drops the chart and name before the sold share or realised SOL is cut; the card stays under 2400 chars.

20. **Truthful pairing.** Suppressed/ambiguous entry POST followed by an eligible exit:
`entry observed; alert not delivered`. Only a successful entry response sets the delivered time.
21. **Per-kind expiry.** Every class expires at its configured boundary; a 5 s reorder hold is
an eligibility delay, not an expiry. Unknown/failed sends are not successful delivery in reports.
22. **Two wallets on one mint.** Entry and exit refs for both wallets and distinct episodes
coexist; dedup and readback do not overwrite a different wallet's state.

## 14. Open questions

1. **Edit accounting.** Edits share the webhook bucket (SCOPE §8). Recommendation: edits take a token and the hourly window but not a daily slot, or an escalation is hidden by a volume cap. Needs a one-line contract amendment.
2. **Idempotency across restarts.** Recommendation: keep at-most-once per process. Persisting ids would make the alert path depend on a store, which §3 forbids.
3. **Readback default.** Recommendation: on for outage and heartbeat only. It needs a bot in three channels with Read Message History and Manage Messages — a second credential to redact.
4. **Bundle post vs CTO cap.** Recommendation: a first bundle post counts against the 20/hr safety cap, since two cards for one token is already the maximum a channel should show.
5. **Cross-family duplication.** Volume fire then CTO for one mint is two cards in two channels. Recommendation: no merge; add a `See also` line only when readback can resolve the other card.
6. **`retry_after` margin.** 0.25 s is a PRIOR. Measure 0, 0.25 and 0.5 s on staging; keep the smallest that never produces an immediate second 429.
7. **Near presentation.** Recommendation: keep one template, put `near` in the footer; revisit contrast only after readers report confusion.
8. **Staging fidelity.** Staging lives in another guild and another Cloudflare bucket. Recommendation: treat it as a payload/permission test, and measure real limits in a dedicated production channel before M2b.
9. **Exit freshness.** Exits bypass the 5 s reorder hold and expire at 30 s. Report the fraction
of observed exits dropped by caps or age; an observed entry does not imply its alert was delivered.
10. **Exit sub-cap 3/h.** Recommendation: keep it and count `ExitCap`. Exits outrank entries, so without a sub-cap an exit storm can take the whole wallet channel; with it, the worst hour still leaves three tokens for entries.
11. **Exit message-id key.** Key wallet refs on `(webhook, mint, family, wallet, kind, episode)` and include those identities in readback markers; otherwise an exit PATCH can rewrite the entry card. The store grows by at most the number of positions, which is far below `deliver.ids.capacity`.

