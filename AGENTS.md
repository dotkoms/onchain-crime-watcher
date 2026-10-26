Read SCOPE.md and CONSTRAINTS.md before writing code. Do not weaken constraints to pass checks.
Start with one binary and one library; see tasks/plan.md. Keep fixture provenance explicit.
Run cargo fmt, clippy, tests and build with the committed lockfile. Never log provider URLs,
webhook URLs or API keys. Capture is best-effort; missing history is never zero activity.
