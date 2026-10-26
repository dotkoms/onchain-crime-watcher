# M1a collector: implementation and verification

One Rust binary and library. The collector subscribes to free create/migration streams and,
only with `--enable-paid`, one configured mint for a bounded observation window. It does not
emit volume, CTO or wallet signals. M1b and M1c remain separate milestones.

## Commands

Requires Rust 1.92.0 or later on Linux/macOS. Run from the repository root.

```sh
cargo build --workspace --locked
cargo run --locked -- --config config/example.toml --check-config
cargo run --locked -- --config config/example.toml --dry
cargo run --locked -- --config config/example.toml --run-for-secs 30
```

While collection is running, inspect health from a second terminal:

```sh
curl http://127.0.0.1:9898/health
```

The collector prints a boot JSON line and a final health snapshot. Ctrl-C shuts down collection,
flushes capture and stops health. A transport failure attempts one optional outage POST and exits;
automatic reconnect/backoff/watchdog policy belongs to M1c. Do not deploy this first slice in a
restart loop. The process lock prevents two collectors using the same data directory; it cannot
detect a separate application or host using the same provider key.

For offline fixture inspection (not the full M2a replay harness):

```sh
cargo run --locked -- --config config/example.toml --fixture tests/fixtures/synthetic.jsonl
cargo run --locked -- --inspect-capture data/capture/day-<UTC-epoch-day>/boot-<id>-<seq>.ocwcap
```

Synthetic fixtures are labelled as such and never establish provider schema or completeness.
The separately observed free-stream fixture and its checksum are documented in
`tests/fixtures/README.md`. Noisy optional reserve values are counted and excluded from reserve
math without rejecting a valid create. Concrete trade amounts remain exact raw-unit parses;
a missing/unusable SOL leg is unresolved, never rounded into a fill.
Fixture mode makes no provider/webhook calls and does not charge the spend ledger. Raw JSONL
bytes, including line endings, round-trip through the same recorder and decoder.

## Paid observation

Copy `config/example.toml` to ignored `config/local.toml`, set exactly one `feed.mint`, and set
`OCW_PUMPPORTAL_KEY` in your environment. Do not put secrets in TOML or CLI arguments. Before the
first paid run, reconcile the linked wallet/provider allowance, then explicitly initialize:

```sh
cargo run --locked -- --config config/local.toml --init-spend-ledger
cargo run --locked -- --config config/local.toml --enable-paid --run-for-secs 30
```

Initialization never overwrites a ledger. A missing/corrupt/unwritable ledger blocks paid streams;
health states `ledger_unavailable` while free streams can continue. Restore/reconcile a lost
ledger; do not reset usage to regain credits. Reservations persist before subscription, in
1,000-message blocks by default. Unused reservations remain spent locally after a restart.
A backward clock step cannot reset the day. A still-active observation crossing UTC midnight
discards old credits and first obtains a fresh durable reservation. A stopped observation never
automatically resumes; it requires a new explicitly enabled run.

Every received paid-scope trade is charged before normalization/dedup. Unknown response types
are charged conservatively; free create/migration events are excluded. Local counters are not
the provider's invoice. At exhaustion or observation-window expiry, the socket closes before a
new free-only socket connects; no paid subscription is sent on that replacement. The vendor can
still bill buffered/in-flight frames. The stop margin is not an upstream hard cap and there is
no automatic wallet top-up. The code does not place orders or hold a signing key.

## Capture format v1

`data/capture/day-<UTC-epoch-day>/boot-<boot-id>-<first-seq>.ocwcap`, mode 0600, parent directories
0700. Rotate on UTC hour changes, using a fresh file for each boot. Each file begins `OCWC` plus
version byte 1. Each record is little-endian compressed length u32, SHA-256 of compressed bytes
(32 bytes), then an independent zstd level-3 frame. Decompressed content is metadata length u32,
UTF-8 metadata JSON, then the exact raw payload. Metadata carries seq, boot id, generation,
monotonic offset, wall time and kind. Decompression, metadata and record sizes are bounded.

This M1a format uses SHA-256 rather than the older illustrative CRC32C layout. There is no index
yet; the streaming scanner is the bounded read/recovery path. Per-segment manifests carry the
file hash, sequence range, physical closed flag and capture-loss snapshot. The mode is explicitly
`decoder-only`: these files do not yet contain every external input/state needed by M2a's
complete-decision replay.

The bounded tap clones a `Bytes` handle and never awaits disk on the frame path. Queue loss emits
sequence-range markers when space returns; shutdown flushes a pending marker. A stopped/failed
writer counts queued unwritten records. Files sync at least every second while the writer runs,
and on shutdown. Boot truncates a torn final record, preserving all earlier verified records;
recovery rebuilds the manifest with a recovered flag and the truncated byte count. Actual read
errors propagate without truncation. A complete checksum error stops capture rather than
silently discarding history.

Retention prunes only whole older day directories containing exclusively owned regular files.
Symlinks and operator files are never followed or pruned. Current-day exhaustion stops capture.
Health reports stopped/error states while collection continues. Disk caps include owned files;
the free-space floor is the safeguard for other data on the volume.

## Security and boundaries

TLS verification stays enabled. The production provider URL is fixed; tests inject localhost
servers privately. Provider errors are reduced to classifications, never serialized with URLs.
The status webhook accepts only Discord HTTPS URLs, disables redirects and sets `wait=true`
and empty allowed mentions. Failed/ambiguous POSTs are not retried; success requires a returned
message id. API-key echoes are excluded from capture with a redaction marker. Health contains
counts/states only and refuses a public bind. Incoming token metadata is not rendered or fetched.

## Verification

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --locked
cargo build --workspace --locked
cargo audit
```

Local verification on 2026-10-26 UTC (2026-10-25 Eastern): all 22 tests passed, with no ignored
tests; formatting, Clippy with warnings denied, and the locked workspace build passed. RustSec
audit found no known vulnerabilities in the 190 locked dependencies using advisory DB
`21d909e4` (2026-10-24, 1,352 advisories). Reversing the ledger's day-reset comparison caused
the restart/clock-rollback contract test to fail; the original implementation was restored and
the complete suite passed again. This is one targeted mutation check, not a mutation score.

The preserved four-frame free-provider fixture also passed through the executable: two creates,
zero decode errors, one excluded noisy optional reserve, zero capture drops and zero paid messages.
The live free-stream probe established connectivity only. Its bounded smoke configuration used
a 1 MiB capture cap and 64 MiB free-space floor because the development volume had less than the
default 2 GiB floor; production defaults remain unchanged.

Local integration tests use a real localhost WebSocket server and HTTP endpoint. They verify
the exact subscription sequence, budget exhaustion, closure before free-only replacement,
health output, no repeat ambiguous POST, executable fixture round trip and explicit ledger init.
Empirical acceptance is tracked in `tasks/todo.md`: real paid-trade fixtures, one-hour RPC/reference
coverage, actual billing exposure and staging Discord still need operator credentials.

## Sources used

- [PumpPortal subscription/billing and one-socket contract](https://pumpportal.fun/data-api/real-time/).
- [Tokio bounded channels and clean shutdown](https://docs.rs/tokio/1.54.1/tokio/sync/mpsc/index.html).
- [tokio-tungstenite 0.27 connection configuration](https://docs.rs/tokio-tungstenite/0.27.0/tokio_tungstenite/fn.connect_async_with_config.html).
- [Reqwest 0.12 client timeout and redirect policy](https://docs.rs/reqwest/0.12.28/reqwest/struct.ClientBuilder.html).
- [Axum 0.8.9 serving and graceful shutdown](https://docs.rs/axum/0.8.9/axum/fn.serve.html).
- [RustSec cargo-audit](https://github.com/rustsec/rustsec/tree/main/cargo-audit).
