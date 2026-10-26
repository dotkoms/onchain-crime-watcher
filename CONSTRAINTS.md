# Collector quality bar

The approved SCOPE revision 6 defines the product limits. This file records the M1a checks.
No skipped tests, unfinished implementations, committed secrets or lint suppressions. Do not
weaken a constraint to pass a check. No production signal delivery in M1a.

| Rule | Reason | Check |
|---|---|---|
| Zero build/type errors | A runnable increment | `cargo build --workspace --locked` |
| Zero formatting/lint errors | Keep maintenance simple | `cargo fmt --all --check`; `cargo clippy --workspace --all-targets --locked -- -D warnings` |
| All tests pass, none ignored | Failure behaviour is part of the feature | `cargo test --workspace --locked` |
| Paid streams opt-in; one mint; durable reservations before subscription | Bound billing exposure | Collector/spend integration tests |
| Exact integer normalization; invalid and unresolved events counted | No invented volume | Decoder contract tests |
| Capture queue, records, disk use and decoder state bounded | Untrusted stream cannot grow memory/disk indefinitely | Capture and decoder tests |
| Localhost health only; secrets excluded from errors and snapshots | Keep credentials private | Config and runtime tests |

The external dependency check is `cargo audit` (RustSec), run at task end and in CI; no known
vulnerabilities may be ignored to make this increment pass. The advisory DB date and lockfile
are recorded with verification evidence. Private fixtures and localhost tests do not prove
provider coverage or billing correctness.

Coverage and performance numbers remain measured targets until M1c; no coverage percentage has
been invented for this first increment. Provider completeness, bill reconciliation and staging
Discord acceptance require external credentials and are reported separately from local tests.

Real-data-only preparation adds these constraints: default startup is disconnected and shows no
sample events/signals; networking requires explicit `--live`; recorded replay is read-only and
does not invent launch history or complete a partial bucket. Unit tests may construct artificial
edge cases in isolation, but the application must not embed or run demo data. Signal candidates
remain previews without explicit staging opt-in. Existing build, spending and capture limits still apply.

The staging sender extension requires `--live --enable-staging-delivery`, a separate environment
webhook and verified configured channel identity. Replay never sends. Reserve a durable intent
before POST; ambiguous outcomes never retry. Honor freshness, rolling attention caps and global
pauses. Bounded nonblocking admission and serialized sends are required. Artificial inputs stay
inside isolated tests; no demo stream or test notification is supplied by the application.
Production delivery remains disabled and real Discord/provider acceptance is reported separately.
