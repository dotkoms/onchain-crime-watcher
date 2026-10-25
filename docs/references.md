# Reference projects

Crawled from GitHub on 2026-10-25 via the GitHub API and raw READMEs. Nothing was cloned; every
repo here was read only. Stars, dates and licenses are as reported by the API that day.

**How to use this list.** MIT and Apache-2.0 projects may inform implementation directly with
attribution. AGPL-3.0 code must not be copied into this repo. Repos with no detected license are
reference for ideas only — no code reuse. Many pump.fun "bot" repos are thin reskins or scam
adjacent; stars are not a quality signal, and this list excludes the ones that only advertise
Telegram handles.

## Official protocol material

| Repo | Stars | License | What it is | Use |
|---|---|---|---|---|
| `pump-fun/pump-public-docs` | 469 | none | Pump's public program docs: accounts, instructions, events | Protocol truth for the decoder; reference only |
| `pump-fun/pump-fun-skills` | 34 | none | Agent skills describing pump.fun operations | Semantics for create/buy/sell events |
| `pump-fun/pump-segments-sdk` | 0 | none | Segments SDK | Not needed |

## Streaming and Geyser parsing

| Repo | Stars | Lang | License | Why it matters |
|---|---|---|---|---|
| `rpcpool/yellowstone-grpc` | 1015 | Rust | AGPL-3.0 | The Geyser gRPC service itself. Use it as a server, never copy code into our crate |
| `Shyft-to/solana-defi` | 354 | Rust/TS | README claims MIT; API detects none | gRPC parsing examples for Pump, Raydium, Orca, Meteora. Strong reference for per-program decoders |
| `0xfnzero/solana-streamer` | 187 | Rust | MIT | Rust facade over a parser SDK: Pump/PumpSwap/Raydium/Meteora/Orca, zero-copy, micro-batching, backpressure, benchmark. Closest match to our Geyser adapter plan |
| `0xfnzero/pumpfun-sdk` | 83 | Rust | MIT | Rust SDK with pump.fun log and Yellowstone subscriptions; `PumpfunEvent` enum |
| `lamports-dev/richat` | 150 | Rust | none | Geyser stream server, alternative to Yellowstone |
| `jito-foundation/geyser-grpc-plugin` | 146 | Rust | other | Geyser plugin emitting account and slot updates |
| `rpcpool/solana-geyser-park` | 138 | — | none | Index of Geyser plugins |
| `solstackapp/geyserbench` | 80 | Rust | MIT | Benchmarks multiple Yellowstone endpoints; use to choose a provider |
| `tomwangcn/solana_meme` | 221 | Rust | none | Minimal Rust new-token monitor over Helius WebSocket logs |

**Lesson taken:** `solana-streamer` states that unresolved ShredStream candidates must not be
treated as fills. That is the same discipline as our `unknown` states and belongs in the feed
contract: a parsed-but-unresolved event is evidence of a transaction, not of volume.

## Pump.fun launch bots and research

| Repo | Stars | Lang | License | Why it matters |
|---|---|---|---|---|
| `chainstacklabs/pumpfun-bonkfun-bot` | 1041 | Python | Apache-2.0 | Four listener modes (logs, blocks, geyser, shreds), per-platform validation, token-bucket limiter, 429 backoff, committed decode fixtures. Best public reference for our gate and listener design |
| `WSOL12/Solana-Arbitrage-Bot` | 525 | TS | MIT | Multi-venue (pump.fun, Meteora, Raydium, Orca) with Jito and gRPC |
| `cicere/pumpfun-bundler` | 443 | TS | none | Documents how bundles are built: 20 keypairs funded from one wallet, atomic same-block buys, lookup tables, Jito tips. Turns our Q3 rule into observable signatures |
| `coffellas-cto/Solana-Copy-Trading-Bot` | 412 | Rust | none | Rust copy-trading over ShredStream |
| `nirholas/pumpfun-rust-client` | 10 | Rust | NOASSERTION | Instruction builders for pump and pump_amm |
| `SolanaSpectre/Spectre` | 2 | JS | MIT | Paper-only research bot scoring launches on curve progress, SOL volume, trade velocity, buy ratio, buyer breadth, sniper crowding, wallet history. Closest public match to our volume family's feature set |
| `Mason333xbt/sol-terminal` | 2 | TS | MIT | PumpPortal + DexScreener feeds with rug checks |

**Lesson taken:** Spectre's buyer-breadth and trade-velocity features overlap our distinct-taker
test; if our participation gate proves noisy on live data, breadth is the next feature to add.
The bundler repo confirms same-block, same-funder, Jito-tipped clustering is the signature to
detect, and that warmup/profile randomisation reduces but does not remove it.

## Wallet clustering and PnL

| Repo | Stars | Lang | License | Why it matters |
|---|---|---|---|---|
| `sohailblockchain/Solana-Cluster-Monitoring-Backend` | 20 | Python | MIT | Parent wallet funding 5+ children in a 1–30 min window; `BUY_CLUSTER` / `SELL_CLUSTER`; coordination score. Directly comparable to our sybil clustering rule |
| `DoradoDevs/solana-wallet-pnl-tracker` | 1 | TS | MIT | FIFO cost basis, balance-delta parsing instead of instruction decoding, Helius pagination and checkpoints, price waterfall DexScreener → Jupiter → Birdeye |
| `hi-tech-AI/dexscreener-api-to-get-top-traders` | 12 | Python | none | Top-100 traders per pair from DexScreener; a cross-check for our reconstructed ranking |
| `DracoR22/handi-cat_wallet-tracker` | 206 | TS | none | Wallet tracking and notification patterns |
| `jcb07/Solana-Scam-Wallet-Database-Jacob-Schmidt` | 0 | Python | none | Seed-based wallet cluster builder |

**Lesson taken:** the cluster backend's tunable "5 children within 5 minutes" is close to our
fan-out cap and gives an empirical starting point. Its parent→child direction matches Q3. The PnL
tracker confirms balance-delta parsing works across PumpSwap and pump.fun, which is the same
approach behind our `ReserveDelta` fallback.

## Data and API wrappers

| Repo | Stars | Lang | License | Use |
|---|---|---|---|---|
| `keiveulbugs/Dexscreener_pricebot` | 28 | Rust | none | Rust DexScreener usage |
| `hedgey-finance/dexscreener-api` | 21 | TS | none | DexScreener wrapper |
| `janswist/mcp-dexscreener` | 19 | JS | none | DexScreener MCP server |
| `chainstream-io/chainstream-go-sdk` | 0 | Go | none | Streaming API SDK, reference for stream semantics |
| `chainstream-io/dex-openapi` | — | JS | none | OpenAPI stream spec |

## Discord bots

| Repo | Stars | License | Note |
|---|---|---|---|
| `TsahiP/pumpFunDiscordBot` | 0 | none | Tiny and unmaintained; no useful delivery logic |
| `pubkeyapp/pubkey-link-v1` | 5 | MIT | Deprecated Solana Discord bot, cNFT role linking. Not relevant to alerts |
| `UcubeHunter/ctogodv3`, `ctogodv5` | 0 | none | Old community-takeover bots; no license, stale, ideas only |

**Lesson taken:** there is no mature open-source Discord alert bot for this domain. Delivery is
ours to build; the spec in `docs/delivery.md` is not reinventing a solved public problem, and no
upstream can be leaned on for rate-limit or edit semantics.

## What this crawl changes in our docs

1. **Feed:** add the "unresolved is not a fill" rule from `solana-streamer`; add `blocks` and
   `shreds` as future adapters alongside Geyser, as `chainstacklabs` validates the listener-mode
   split we already have.
2. **Volume:** Spectre's feature set is a useful checklist; buyer breadth is the next candidate
   feature if the participation gate misbehaves.
3. **Bundle:** the bundler repo confirms our detection signatures and suggests adding repeated
   Jito tip payers to the funding-stage evidence.
4. **Ranking:** the cluster backend's default (5 children / 5 min) is a second opinion on our
   fan-out prior; the PnL tracker's balance-delta parsing corroborates the reserve fallback.
5. **Delivery:** no upstream reference; keep the spec as written.
6. **Geyser:** `solana-streamer` and `Shyft-to/solana-defi` are the places to start when the
   Geyser adapter is scheduled; both are MIT/no-license at the repo level, so read before reuse.

## Licensing summary

| License | Repos | Us |
|---|---|---|
| MIT | solana-streamer, pumpfun-sdk, geyserbench, Spectre, cluster backend, PnL tracker, arbitrage bot | Direct use with attribution |
| Apache-2.0 | chainstacklabs/pumpfun-bonkfun-bot | Direct use with notice |
| AGPL-3.0 | yellowstone-grpc | Server use only; do not copy into our binary |
| None detected | pump-public-docs, Shyft, solana_meme, bundler, copy-trading, DexScreener wrappers | Concepts only; verify the LICENSE file before any reuse |
