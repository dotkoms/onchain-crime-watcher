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

Coverage and performance numbers remain measured targets until M1c; no coverage percentage has
been invented for this first increment. Provider completeness, bill reconciliation and staging
Discord acceptance require external credentials and are reported separately from local tests.
