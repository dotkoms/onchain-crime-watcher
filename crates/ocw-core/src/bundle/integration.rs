use super::{capture::Launch, rpc::Report, runtime::Config};
use crate::{
    health::Health,
    rpc::Client,
    storage::{ProcessLock, atomic_write, private_dir},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, UNIX_EPOCH},
};
#[derive(Clone)]
pub(crate) struct Integration {
    pub root: PathBuf,
    pub rpc: Option<Client>,
    pub config: Config,
    pub boot: String,
    pub lock: Arc<Mutex<()>>,
    pub reads: Arc<tokio::sync::Semaphore>,
}
impl Integration {
    pub async fn launch(
        &self,
        mint: [u8; 32],
        guard: Arc<ProcessLock>,
        health: &Health,
    ) -> Result<Option<Launch>, &'static str> {
        if !health.bundle_state.lock().is_ok_and(|s| *s == "recording") {
            return Err("capture_unavailable");
        }
        let tick = crate::health::value(&health.bundle_tick_ms);
        let now = health
            .monitor_origin
            .get()
            .map(|origin| origin.elapsed().as_millis() as u64 + 1);
        if now.is_none_or(|now| {
            tick == 0
                || tick > now
                || now - tick >= crate::health::value(&health.monitor_stall_ms).max(3000)
        }) {
            return Err("capture_stalled");
        }
        let permit = tokio::time::timeout(
            Duration::from_millis(50),
            self.reads.clone().acquire_owned(),
        )
        .await
        .map_err(|_| "launch_reader_busy")?
        .map_err(|_| "launch_reader_closed")?;
        let root = self.root.clone();
        let boot = self.boot.clone();
        tokio::time::timeout(
            Duration::from_millis(750),
            tokio::task::spawn_blocking(move || {
                let _guard = guard;
                let _permit = permit;
                let mut launch =
                    super::store::read(&root, mint).map_err(|_| "launch_unreadable")?;
                if let Some(row) = &mut launch
                    && row.boot != boot
                {
                    row.gap();
                    row.closed = true;
                }
                Ok(launch)
            }),
        )
        .await
        .map_err(|_| "launch_lookup_timeout")?
        .map_err(|_| "launch_lookup_failed")?
    }
    fn aux(&self) -> PathBuf {
        self.root.with_file_name("bundle-analysis")
    }
    pub async fn cache(&self, report: Report, guard: Arc<ProcessLock>) -> Result<(), &'static str> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let _lock = this
                .lock
                .lock()
                .map_err(|_| "bundle auxiliary lock failed")?;
            write(
                &this.aux(),
                &format!("report-{}.json", report.mint),
                &report,
                this.config.max_bytes / 4,
            )
        })
        .await
        .map_err(|_| "bundle cache task failed")?
    }
    pub async fn cached(&self, mint: [u8; 32], guard: Arc<ProcessLock>) -> Option<Report> {
        let path = self
            .aux()
            .join(format!("report-{}.json", bs58::encode(mint).into_string()));
        let exclusions = self
            .config
            .exclusions
            .iter()
            .filter_map(|s| crate::feed::key::<32>(s).ok())
            .collect();
        let policy = super::rpc::exclusions_hash(&exclusions);
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            read::<Report>(&path)
                .ok()
                .flatten()
                .filter(|r| r.exclusions_sha256 == policy)
        })
        .await
        .ok()
        .flatten()
    }
    pub async fn edit_meta(
        &self,
        mint: [u8; 32],
        update: Option<Edit>,
        guard: Arc<ProcessLock>,
    ) -> Result<Option<Edit>, &'static str> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let _lock = this
                .lock
                .lock()
                .map_err(|_| "bundle auxiliary lock failed")?;
            let name = format!("edit-{}.json", bs58::encode(mint).into_string());
            if let Some(value) = update {
                write(&this.aux(), &name, &value, this.config.max_bytes / 4)?;
                Ok(Some(value))
            } else {
                read(&this.aux().join(name))
            }
        })
        .await
        .map_err(|_| "bundle edit task failed")?
    }
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Edit {
    pub schema: u8,
    pub message: String,
    pub tier: u8,
    pub wall_ms: u64,
    pub pending: bool,
    pub enriched: bool,
    pub target_sha256: String,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope<T> {
    schema: u8,
    value: T,
    sha256: String,
}
fn read<T: serde::de::DeserializeOwned + Serialize>(
    path: &Path,
) -> Result<Option<T>, &'static str> {
    let meta = match std::fs::symlink_metadata(path) {
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Ok(m) if m.is_file() && !m.file_type().is_symlink() && m.len() <= 256 * 1024 => m,
        _ => return Err("invalid bundle cache"),
    };
    let _ = meta;
    let envelope: Envelope<T> =
        serde_json::from_slice(&std::fs::read(path).map_err(|_| "bundle cache unreadable")?)
            .map_err(|_| "invalid bundle cache")?;
    if envelope.schema != 1
        || envelope.sha256
            != format!(
                "{:x}",
                Sha256::digest(
                    serde_json::to_vec(&envelope.value).map_err(|_| "invalid bundle cache")?
                )
            )
    {
        return Err("invalid bundle cache");
    }
    Ok(Some(envelope.value))
}
fn write<T: Serialize>(root: &Path, name: &str, value: &T, cap: u64) -> Result<(), &'static str> {
    private_dir(root).map_err(|_| "bundle cache directory unavailable")?;
    let sha256 = format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).map_err(|_| "bundle cache serialization failed")?)
    );
    let bytes = serde_json::to_vec(&Envelope {
        schema: 1,
        value,
        sha256,
    })
    .map_err(|_| "bundle cache serialization failed")?;
    if bytes.len() > 256 * 1024 {
        return Err("bundle report capacity reached");
    }
    let mut total = bytes.len() as u64;
    let mut reports = Vec::new();
    let mut count = 0;
    for entry in std::fs::read_dir(root).map_err(|_| "bundle cache unreadable")? {
        let entry = entry.map_err(|_| "bundle cache unreadable")?;
        let filename = entry
            .file_name()
            .into_string()
            .map_err(|_| "invalid bundle cache file")?;
        let key = filename
            .strip_prefix("report-")
            .or_else(|| filename.strip_prefix("edit-"))
            .and_then(|s| s.strip_suffix(".json"))
            .ok_or("invalid bundle cache file")?;
        crate::feed::key::<32>(key).map_err(|_| "invalid bundle cache key")?;
        let meta =
            std::fs::symlink_metadata(entry.path()).map_err(|_| "bundle cache unreadable")?;
        if !meta.is_file() || meta.file_type().is_symlink() || meta.len() > 256 * 1024 {
            return Err("invalid bundle cache file");
        }
        count += 1;
        if count > 20_256 {
            return Err("bundle auxiliary entry cap");
        }
        if filename != name {
            total = total
                .checked_add(meta.len())
                .ok_or("bundle cache capacity reached")?;
            if filename.starts_with("report-") {
                reports.push((
                    meta.modified().unwrap_or(UNIX_EPOCH),
                    entry.path(),
                    meta.len(),
                ));
            }
        }
    }
    reports.sort_by_key(|r| r.0);
    while total > cap || reports.len() >= 256 {
        let Some((_, path, len)) = reports.first().cloned() else {
            return Err("bundle auxiliary capacity reached");
        };
        std::fs::remove_file(path).map_err(|_| "bundle cache pruning failed")?;
        total -= len;
        reports.remove(0);
    }
    atomic_write(&root.join(name), &bytes).map_err(|_| "bundle cache write failed")
}
pub(crate) fn card(
    claim: &crate::cto::Claim,
    launch: Option<&Launch>,
    report: Option<&Report>,
    unavailable: bool,
) -> serde_json::Value {
    let mut card = claim.card();
    let description = if unavailable {
        "Bundle: unavailable — launch storage is unreadable or timed out".into()
    } else if let Some(report) = report {
        let best = report
            .clusters
            .iter()
            .filter_map(|c| c.progress.as_ref().map(|p| (c, p)))
            .max_by_key(|(_, p)| {
                (
                    p.minimums_met && p.tier >= super::Tier::Fire,
                    p.sold_bps_lower,
                )
            });
        if let Some((cluster, p)) = best {
            let tier = if p.minimums_met && p.tier >= super::Tier::Fire {
                " — FIRE (observed lower bound)"
            } else {
                ""
            };
            format!(
                "Bundle: {} same-funder wallets; observed original-bag sales ≥{}.{:02}%{tier}\nCoverage: partial; holdings/transfers and historical price context unverified. One hop only. No clearance claim. Sales observed through <t:{}:f>.",
                cluster.wallets,
                p.sold_bps_lower / 100,
                p.sold_bps_lower % 100,
                report.observed_wall_ms / 1000
            )
        } else {
            "Bundle: sold share unavailable — funding or opening inventory could not be verified\nCoverage: partial. One hop only; no completeness or clearance claim.".into()
        }
    } else if let Some(launch) = launch {
        format!(
            "Bundle: launch observed ({} buyers); sold share unavailable — funding/history not verified{}",
            launch.buyers.len(),
            if launch.partial {
                "; incomplete history"
            } else {
                ""
            }
        )
    } else {
        "Bundle: no launch record; sold share unavailable".into()
    };
    if let Some(content) = card["content"].as_str() {
        card["content"] = serde_json::Value::String(content.replace(
            "Bundle: unavailable — verified launch history not loaded",
            &description,
        ));
    }
    card
}

#[cfg(test)]
mod tests {
    #[test]
    fn absent_or_unreadable_launch_never_becomes_zero_sold() {
        let claim = crate::cto::Claim {
            mint: [1; 32],
            date_ms: 1,
        };
        for unavailable in [false, true] {
            let payload = super::card(&claim, None, None, unavailable);
            let content = payload["content"].as_str().unwrap();
            assert!(content.contains("unavailable"));
            assert!(!content.contains("0%"));
            assert!(!content.contains("FIRE"));
            assert!(!content.contains("CLEARED"));
        }
    }
}
