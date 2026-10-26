# Hosting preparation — nothing deployed

The repository provides an offline preflight, Docker image/Compose template, systemd unit and
checksummed offline snapshots. These run the implemented collector and staging-volume path.
Bundle tracking, wallet rankings, full decision backtesting and independent
secondary-feed switching remain unfinished. Adding keys does not enable those missing features.

## Supply fresh inputs

Copy `.env.example` to a private environment file and `config/example.toml` to
`config/local.toml` for a local checkout. For either hosting template, use
`deploy/config.example.toml` as the starting config: it uses `/var/lib/ocw`, the persistent
runtime directory. Never place secrets in TOML, command arguments or version control.

| Input | When required |
| --- | --- |
| `OCW_PUMPPORTAL_KEY` | Explicit paid observation: one configured mint or `feed.auto_watch=true`. |
| `OCW_DISCORD_STAGING_URL` and `delivery.staging_channel_id` | Explicit staging signal delivery; channel identity is verified at live startup. |
| `OCW_DISCORD_STATUS_URL` and `monitor.status_channel_id` | Optional Discord status card; use a separate webhook from staging. |
| `OCW_DISCORD_CTO_URL` and `cto.channel_id` | Optional dedicated CTO staging delivery; see [CTO setup](cto.md). |
| `SOLANA_RPC_URL` | `probe.enabled=true`; use a provider independent from the primary stream. |
| `OCW_DEADMAN_URL` | `monitor.deadman=true`; independently configure Healthchecks period/grace and notifications. |

The current Discord integration uses webhooks, so it does not require a bot token. No wallet
private key or signing permission is needed. The binary does not automatically source `.env`:
Compose or systemd supplies the environment; a local shell must export the required values.

## Check setup without networking

```sh
cargo build --release --locked
./target/release/ocw --config config/local.toml --check-config
./target/release/ocw --config config/local.toml --preflight
./target/release/ocw --config config/local.toml --dry --enable-paid
```

For a paid/staging configuration, deliberately initialize the local paid ledger once, then
check that selected mode. Initialization never connects and refuses to replace an existing ledger.

```sh
./target/release/ocw --config config/local.toml --init-spend-ledger
./target/release/ocw --config config/local.toml --preflight --enable-paid --enable-staging-delivery
```

Preflight reads configuration, format-checks only the selected environment inputs, checks
private storage metadata and reads ledger integrity. It never creates storage, reserves calls,
contacts Discord or tests a key. Exit 0 means local configuration checks passed; exit 2 reports
missing/invalid inputs. `production_ready` remains false. Network access, provider plan/billing,
actual payload coverage and Discord acceptance must be tested separately when live work is authorized.

## Docker path (future operator actions)

The image defaults to disconnected `--status`. Compose requires the explicit `live` profile,
private `.env`, a real `config/local.toml` and an existing private `data` directory. Use the
hosting config path `/var/lib/ocw`; bind-mounted data must be owned by UID/GID 10001 and mode 0700.
Compose never creates a missing host path. The runtime is unprivileged, read-only except its
persistent data mount and bounded temporary space. It publishes no health port.

Build with `docker build -t ocw:<commit> .`; record the image ID/digest with its commit and
configuration version. Set `OCW_IMAGE` to that immutable tag/digest. When you choose to host,
`docker compose -f deploy/compose.yaml --profile live up -d` starts free collection and volume
previews. The checked-in command does not enable paid data or staging sends. Add their explicit
flags only after the matching preflight. Never run two collectors against one data directory.

`/ready` drives container health; Docker health failure alone does not restart the process.
Finite process restart limits protect against a crash loop; in-process red latches stay visible
for operator review. Use external notification monitoring for unhealthy containers as well as
process disappearance. Docker was unavailable on the preparation machine. The [hosting CI run](https://github.com/dotkoms/onchain-crime-watcher/actions/runs/40322677312) passed image build, disconnected startup and offline container preflight.
Live hosted operation remains untested.

## systemd path (future operator actions)

Use a dedicated `ocw` account, `/etc/ocw/config.toml`, root-owned mode-0600
`/etc/ocw/ocw.env` and versioned binaries under `/opt/ocw/releases/<commit>/ocw`.
`/opt/ocw/current` selects the active release. The unit creates private `/var/lib/ocw`, runs the
offline preflight before free collection, bounds restart frequency and allows 45 seconds for
shutdown. Both SIGINT and SIGTERM request checked collector shutdown. Add any paid/staging flags
to both `ExecStartPre` and `ExecStart` when selecting those modes.

Review `deploy/ocw.service` for the selected Linux host and run `systemd-analyze verify` there
before installation. No unit has been installed or started here. Keep `/health` and `/ready`
on loopback; use a host-side monitor or tunnel rather than exposing them publicly.

## Backups and restore

Python 3.9+ and POSIX file locks are required. Stop the collector first. The snapshot tool
acquires the same exclusive data-directory lock and rejects running collectors, links, special
files, excessive size and existing destinations. Its default limits are 10,000 files/16 GiB.
It copies data only, excludes lock files, writes mode-0600 archives, and records per-file hashes
plus a whole-archive checksum. Config and secrets need their own private backup. Copy archives
and checksums to independent private/encrypted storage; no remote upload or scheduler is installed.

```sh
python3 scripts/storage_snapshot.py backup /var/lib/ocw /private-backups/ocw-<timestamp>.tar.gz
python3 scripts/storage_snapshot.py verify /private-backups/ocw-<timestamp>.tar.gz
python3 scripts/storage_snapshot.py restore /private-backups/ocw-<timestamp>.tar.gz /var/lib/ocw-restored
```

Restore requires a new destination and never overwrites live data. It validates the archive
before writing, uses safe regular-file extraction, checks each file hash and leaves a
`restore-review-required.json` marker. Live startup refuses that marker. A snapshot may predate
provider spend, Discord attempts or ambiguous POST outcomes: reconcile with the newest known
journals and real external records before removing it. Do not blindly reset a spend ledger or
message receipts. Historical replay and inspection do not send messages.

Hashes detect damage; they do not authenticate an archive from an untrusted source. Practice
restoring a stopped snapshot to a separate directory and verify its capture/journal evidence
before relying on a backup. The local tests cover exact-byte round trips, lock exclusion,
corruption, path rejection, private permissions and the live-start restore barrier.

## Rollback

Stop the collector gracefully, preserve current data, and point the image tag or
`/opt/ocw/current` link at the last verified compatible release. Roll back the binary/config
without rolling back external spend or delivery receipts. A release must understand the
current on-disk schemas; otherwise keep it stopped and reconcile using a separate restored
copy. Run offline preflight, then recheck readiness and external monitoring after an explicitly
authorized restart. New source sessions start cold; no rollback invents detector history.

Sources: [Docker multi-stage builds](https://docs.docker.com/build/building/multi-stage/),
[Compose services](https://docs.docker.com/reference/compose-file/services/),
[systemd service options](https://www.freedesktop.org/software/systemd/man/latest/systemd.service.html).
