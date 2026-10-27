//! Atomic bounded launch snapshots; capture writes never run on the trade task.
use super::capture::Launch;
use crate::{
    feed,
    storage::{atomic_write, private_dir},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
pub const MAX_FILE: u64 = 16 * 1024 * 1024;
const RESERVE: u64 = 8 * 1024 * 1024;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    schema: u8,
    launch: Launch,
    sha256: String,
}
fn invalid() -> io::Error {
    io::Error::other("invalid or unavailable launch storage")
}
fn hash(launch: &Launch) -> io::Result<String> {
    Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(launch)?)))
}
pub fn read(root: &Path, mint: [u8; 32]) -> io::Result<Option<Launch>> {
    let path = root.join(format!("{}.json", bs58::encode(mint).into_string()));
    match std::fs::symlink_metadata(&path) {
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Ok(m) if m.is_file() && !m.file_type().is_symlink() && m.len() <= MAX_FILE => {}
        _ => return Err(invalid()),
    }
    let envelope: Envelope =
        serde_json::from_slice(&std::fs::read(path)?).map_err(|_| invalid())?;
    if envelope.schema != 1
        || envelope.launch.mint != mint
        || !envelope.launch.validate()
        || envelope.sha256 != hash(&envelope.launch)?
    {
        return Err(invalid());
    }
    let mut launch = envelope.launch;
    if root.join("coverage-loss.json").exists() {
        launch.gap();
    }
    Ok(Some(launch))
}
pub(crate) struct Store {
    root: PathBuf,
    cap: u64,
    bytes: u64,
    sizes: BTreeMap<[u8; 32], u64>,
    pub pruned: u64,
}
impl Store {
    pub fn open(root: PathBuf, cap: u64, retention_days: u64) -> io::Result<Self> {
        if !(32 * 1024 * 1024..=2 * 1024 * 1024 * 1024).contains(&cap)
            || !(1..=90).contains(&retention_days)
        {
            return Err(invalid());
        }
        private_dir(&root)?;
        let mut sizes = BTreeMap::new();
        let mut bytes = 0u64;
        let mut pruned = 0;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| invalid())?
            .as_secs();
        for item in std::fs::read_dir(&root)? {
            let item = item?;
            let path = item.path();
            let name = item.file_name().into_string().map_err(|_| invalid())?;
            if name == "coverage-loss.json" {
                continue;
            }
            let key = feed::key::<32>(name.strip_suffix(".json").ok_or_else(invalid)?)
                .map_err(|_| invalid())?;
            let metadata = std::fs::symlink_metadata(&path)?;
            if !metadata.is_file() || metadata.file_type().is_symlink() || metadata.len() > MAX_FILE
            {
                return Err(invalid());
            }
            let modified = metadata
                .modified()?
                .duration_since(UNIX_EPOCH)
                .map_err(|_| invalid())?
                .as_secs();
            if now
                .checked_sub(modified)
                .is_some_and(|age| age > retention_days * 86_400)
            {
                std::fs::remove_file(path)?;
                pruned += 1;
                continue;
            }
            bytes = bytes.checked_add(metadata.len()).ok_or_else(invalid)?;
            sizes.insert(key, metadata.len());
            if sizes.len() > 10_000 {
                return Err(invalid());
            }
        }
        let mut store = Self {
            root,
            cap,
            bytes,
            sizes,
            pruned,
        };
        store.make_room(0, None, &BTreeSet::new())?;
        Ok(store)
    }
    pub fn contains(&self, mint: &[u8; 32]) -> bool {
        self.sizes.contains_key(mint)
    }
    pub fn write_protected(
        &mut self,
        launch: &Launch,
        protected: &BTreeSet<[u8; 32]>,
    ) -> io::Result<()> {
        if !launch.validate() {
            return Err(invalid());
        }
        let old = self.sizes.get(&launch.mint).copied().unwrap_or(0);
        let bytes = serde_json::to_vec(&Envelope {
            schema: 1,
            sha256: hash(launch)?,
            launch: launch.clone(),
        })?;
        if bytes.len() as u64 > MAX_FILE
            || fs2::available_space(&self.root)? < bytes.len() as u64 + 2 * 1024 * 1024 * 1024
        {
            return Err(invalid());
        }
        self.make_room(bytes.len() as u64, Some(launch.mint), protected)?;
        let total = self
            .bytes
            .checked_sub(old)
            .and_then(|n| n.checked_add(bytes.len() as u64))
            .ok_or_else(invalid)?;
        let path = self
            .root
            .join(format!("{}.json", bs58::encode(launch.mint).into_string()));
        if path.is_symlink() {
            return Err(invalid());
        }
        atomic_write(&path, &bytes)?;
        self.bytes = total;
        self.sizes.insert(launch.mint, bytes.len() as u64);
        Ok(())
    }
    fn make_room(
        &mut self,
        new_bytes: u64,
        target: Option<[u8; 32]>,
        protected: &BTreeSet<[u8; 32]>,
    ) -> io::Result<()> {
        let old = target
            .and_then(|k| self.sizes.get(&k))
            .copied()
            .unwrap_or(0);
        while self.bytes - old + new_bytes > self.cap - RESERVE
            || new_bytes > 0 && old == 0 && self.sizes.len() >= 10_000
        {
            let victim = self
                .sizes
                .keys()
                .filter(|key| Some(**key) != target && !protected.contains(*key))
                .map(|key| {
                    let path = self
                        .root
                        .join(format!("{}.json", bs58::encode(key).into_string()));
                    std::fs::symlink_metadata(&path)
                        .map(|meta| (meta.modified().unwrap_or(UNIX_EPOCH), *key, path))
                })
                .collect::<io::Result<Vec<_>>>()?
                .into_iter()
                .min_by_key(|row| (row.0, row.1))
                .ok_or_else(invalid)?;
            std::fs::remove_file(&victim.2)?;
            self.bytes -= self.sizes.remove(&victim.1).ok_or_else(invalid)?;
            self.pruned += 1;
        }
        Ok(())
    }
    pub fn loss(&self) -> io::Result<()> {
        atomic_write(
            &self.root.join("coverage-loss.json"),
            b"{\"schema\":1,\"coverage\":\"partial\"}",
        )
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn snapshots_survive_restart_preserve_gaps_and_never_replace_a_corrupt_record_as_zero() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("launch");
        let mut store = Store::open(root.clone(), 32 * 1024 * 1024, 45).unwrap();
        let launch = Launch::new([1; 32], "boot", 100, 200).unwrap();
        store.write_protected(&launch, &BTreeSet::new()).unwrap();
        drop(store);
        assert!(
            Store::open(root.clone(), 32 * 1024 * 1024, 45)
                .unwrap()
                .contains(&[1; 32])
        );
        assert!(!read(&root, [1; 32]).unwrap().unwrap().partial);
        assert!(read(&root, [2; 32]).unwrap().is_none());
        Store::open(root.clone(), 32 * 1024 * 1024, 45)
            .unwrap()
            .loss()
            .unwrap();
        assert!(read(&root, [1; 32]).unwrap().unwrap().partial);
        std::fs::write(
            root.join(format!("{}.json", bs58::encode([1; 32]).into_string())),
            b"{}",
        )
        .unwrap();
        assert!(read(&root, [1; 32]).is_err());
    }
}

#[cfg(test)]
mod capacity_tests {
    use super::*;
    #[test]
    fn capacity_eviction_preserves_active_records_and_returns_missing_instead_of_zero() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("launch");
        let mut store = Store::open(root.clone(), 32 * 1024 * 1024, 45).unwrap();
        for key in 1..=2 {
            store
                .write_protected(
                    &Launch::new([key; 32], "boot", 100, 200).unwrap(),
                    &BTreeSet::new(),
                )
                .unwrap();
        }
        store.cap = RESERVE + store.bytes + 10;
        store
            .write_protected(
                &Launch::new([3; 32], "boot", 100, 200).unwrap(),
                &BTreeSet::from([[1; 32]]),
            )
            .unwrap();
        assert!(read(&root, [1; 32]).unwrap().is_some());
        assert!(read(&root, [2; 32]).unwrap().is_none());
        assert!(read(&root, [3; 32]).unwrap().is_some());
        assert_eq!(store.pruned, 1);
    }
}
