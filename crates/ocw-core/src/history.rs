//! Bounded checksummed observation/coverage journal. A recovered tail is a gap, never history.
use crate::storage::{ProcessLock, atomic_write, new_private, private_dir};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
};
const MAGIC: &[u8] = b"OCWH\x01";
const MAX_ENTRY: usize = 32 * 1024;
const CHECKPOINT_RESERVE: u64 = 8 * 1024 * 1024;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub seq: u64,
    pub wall_ms: u64,
    pub boot: String,
    pub kind: String,
    pub data: Value,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Checkpoint {
    pub schema: u8,
    pub seq: u64,
    pub wall_ms: u64,
    pub hash: String,
    pub boot: String,
    pub state: Value,
}
pub struct Journal {
    file: File,
    path: PathBuf,
    cap: u64,
    bytes: u64,
    seq: u64,
    wall: u64,
    hash: [u8; 32],
    boot: String,
    pub recovered_bytes: u64,
    failed: std::cell::Cell<bool>,
    _lock: ProcessLock,
}
impl Journal {
    pub fn open(root: &Path, cap: u64, boot: &str, now: u64) -> io::Result<Self> {
        if cap < CHECKPOINT_RESERVE + MAX_ENTRY as u64
            || boot.is_empty()
            || boot.len() > 64
            || !boot.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err(invalid());
        }
        private_dir(root)?;
        let ownership = ProcessLock::acquire(root)?;
        let path = root.join("observations.ocwh");
        if path.is_symlink() {
            return Err(invalid());
        }
        let (bytes, seq, wall, hash, torn) = if path.exists() {
            scan(&path, cap, |_| Ok(()))?
        } else {
            let mut file = new_private(&path)?;
            file.write_all(MAGIC)?;
            file.sync_all()?;
            (
                MAGIC.len() as u64,
                0,
                0,
                Sha256::digest(MAGIC).into(),
                false,
            )
        };
        if now < wall {
            return Err(io::Error::other("history clock rollback"));
        }
        let recovered = fs::metadata(&path)?.len() - bytes;
        let file = OpenOptions::new().read(true).append(true).open(&path)?;
        if torn {
            file.set_len(bytes)?;
            file.sync_all()?;
        }
        let mut gap_from = wall;
        let checkpoint = root.join("checkpoint.json");
        if checkpoint.exists() {
            if checkpoint.is_symlink()
                || !fs::metadata(&checkpoint)?.is_file()
                || fs::metadata(&checkpoint)?.len() > CHECKPOINT_RESERVE / 2
            {
                return Err(invalid());
            }
            let saved: Checkpoint =
                serde_json::from_slice(&fs::read(&checkpoint)?).map_err(|_| invalid())?;
            if saved.schema != 1 || saved.seq > seq || saved.wall_ms > wall {
                return Err(invalid());
            }
            let mut matched = saved.seq == 0 && saved.hash == hex(&Sha256::digest(MAGIC));
            let mut running: [u8; 32] = Sha256::digest(MAGIC).into();
            scan(&path, cap, |entry| {
                let bytes = serde_json::to_vec(&entry)?;
                running = chain(&running, &bytes);
                if entry.seq == saved.seq {
                    matched = saved.hash == hex(&running) && saved.wall_ms == entry.wall_ms;
                }
                Ok(())
            })?;
            if !matched {
                return Err(invalid());
            }
            gap_from = saved.wall_ms;
        }
        let mut journal = Self {
            file,
            path,
            cap,
            bytes,
            seq,
            wall,
            hash,
            boot: boot.into(),
            recovered_bytes: recovered,
            failed: std::cell::Cell::new(false),
            _lock: ownership,
        };
        // Even an orderly shutdown does not observe the interval before the next boot.
        journal.append(now,"boot",json!({"gap_from_wall_ms":if seq>0 {Some(gap_from)}else{None},"gap_to_wall_ms":now,"recovered_tail_bytes":recovered,"coverage":"unverified"}))?;
        journal.sync()?;
        Ok(journal)
    }
    pub fn append(&mut self, wall: u64, kind: &str, data: Value) -> io::Result<u64> {
        if self.failed.get()
            || wall < self.wall
            || kind.is_empty()
            || kind.len() > 64
            || !kind.bytes().all(|b| b.is_ascii_lowercase() || b == b'_')
        {
            return Err(invalid());
        }
        let seq = self.seq.checked_add(1).ok_or_else(invalid)?;
        let entry = Entry {
            seq,
            wall_ms: wall,
            boot: self.boot.clone(),
            kind: kind.into(),
            data,
        };
        let bytes = serde_json::to_vec(&entry)?;
        if bytes.len() > MAX_ENTRY
            || self.bytes + bytes.len() as u64 + 36 + CHECKPOINT_RESERVE > self.cap
        {
            return Err(io::Error::other("history capacity exceeded"));
        }
        let hash = chain(&self.hash, &bytes);
        self.failed.set(true);
        self.file.write_all(&(bytes.len() as u32).to_le_bytes())?;
        self.file.write_all(&hash)?;
        self.file.write_all(&bytes)?;
        self.bytes += 36 + bytes.len() as u64;
        self.hash = hash;
        self.seq = seq;
        self.wall = wall;
        self.failed.set(false);
        Ok(seq)
    }
    pub fn sync(&self) -> io::Result<()> {
        if self.failed.get() {
            return Err(invalid());
        }
        let result = self.file.sync_all();
        if result.is_err() {
            self.failed.set(true);
        }
        result
    }
    pub fn checkpoint(&self, state: Value) -> io::Result<()> {
        self.sync()?;
        let data = serde_json::to_vec(&Checkpoint {
            schema: 1,
            seq: self.seq,
            wall_ms: self.wall,
            hash: hex(&self.hash),
            boot: self.boot.clone(),
            state,
        })?;
        if data.len() as u64 > CHECKPOINT_RESERVE / 2 {
            return Err(io::Error::other("checkpoint capacity exceeded"));
        }
        let result = atomic_write(&self.path.with_file_name("checkpoint.json"), &data);
        if result.is_err() {
            self.failed.set(true);
        }
        result
    }
    pub fn sequence(&self) -> u64 {
        self.seq
    }
}
fn invalid() -> io::Error {
    io::Error::other("invalid or corrupt history journal")
}
fn chain(previous: &[u8; 32], bytes: &[u8]) -> [u8; 32] {
    let mut hash = Sha256::new();
    hash.update(previous);
    hash.update(bytes);
    hash.finalize().into()
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
fn scan(
    path: &Path,
    cap: u64,
    mut visit: impl FnMut(Entry) -> io::Result<()>,
) -> io::Result<(u64, u64, u64, [u8; 32], bool)> {
    if path.is_symlink() || !fs::metadata(path)?.is_file() || fs::metadata(path)?.len() > cap {
        return Err(invalid());
    }
    let mut file = File::open(path)?;
    let mut magic = [0u8; 5];
    file.read_exact(&mut magic)?;
    if magic != MAGIC {
        return Err(invalid());
    }
    let mut offset = MAGIC.len() as u64;
    let mut seq = 0;
    let mut wall = 0;
    let mut hash: [u8; 32] = Sha256::digest(MAGIC).into();
    loop {
        let mut size = [0u8; 4];
        if file.read(&mut size[..1])? == 0 {
            return Ok((offset, seq, wall, hash, false));
        }
        if let Err(e) = file.read_exact(&mut size[1..]) {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok((offset, seq, wall, hash, true));
            }
            return Err(e);
        }
        let len = u32::from_le_bytes(size) as usize;
        if len == 0 || len > MAX_ENTRY {
            return Err(invalid());
        }
        let mut expected = [0; 32];
        let mut bytes = vec![0; len];
        if let Err(e) = file
            .read_exact(&mut expected)
            .and_then(|_| file.read_exact(&mut bytes))
        {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Ok((offset, seq, wall, hash, true));
            }
            return Err(e);
        }
        let next = chain(&hash, &bytes);
        if next != expected {
            return Err(invalid());
        }
        let entry: Entry = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
        if entry.seq != seq + 1
            || entry.wall_ms < wall
            || entry.boot.is_empty()
            || entry.boot.len() > 64
        {
            return Err(invalid());
        }
        seq = entry.seq;
        wall = entry.wall_ms;
        hash = next;
        offset += len as u64 + 36;
        visit(entry)?;
    }
}
/// Streams only validated complete records and refuses torn input for offline evidence.
pub fn read(path: &Path, cap: u64, visit: impl FnMut(Entry) -> io::Result<()>) -> io::Result<()> {
    if scan(path, cap, visit)?.4 {
        return Err(io::Error::other(
            "torn history journal; recover before inspection",
        ));
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    const CAP: u64 = 16 * 1024 * 1024;
    #[test]
    fn journal_checkpoint_and_restart_preserve_order_and_declare_unobserved_time() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("history");
        let mut journal = Journal::open(&root, CAP, "boot1", 100).unwrap();
        journal
            .append(101, "trade", json!({"tokens_raw":"18446744073709551615"}))
            .unwrap();
        journal.checkpoint(json!({"applied_seq":2})).unwrap();
        drop(journal);
        let journal = Journal::open(&root, CAP, "boot2", 200).unwrap();
        assert_eq!(journal.sequence(), 3);
        drop(journal);
        let mut entries = Vec::new();
        read(&root.join("observations.ocwh"), CAP, |entry| {
            entries.push(entry);
            Ok(())
        })
        .unwrap();
        assert_eq!(entries[2].data["gap_from_wall_ms"], 101);
        assert_eq!(entries[2].data["gap_to_wall_ms"], 200);
        assert_eq!(entries[1].data["tokens_raw"], "18446744073709551615");
    }
    #[test]
    fn partial_tail_is_recovered_but_checksum_corruption_is_not_silently_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("history");
        let journal = Journal::open(&root, CAP, "a", 100).unwrap();
        drop(journal);
        let path = root.join("observations.ocwh");
        let good = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[8, 0])
            .unwrap();
        assert!(read(&path, CAP, |_| Ok(())).is_err());
        let journal = Journal::open(&root, CAP, "b", 200).unwrap();
        assert_eq!(journal.recovered_bytes, 2);
        drop(journal);
        let mut bytes = fs::read(&path).unwrap();
        bytes[good as usize - 1] ^= 1;
        fs::write(&path, &bytes).unwrap();
        assert!(Journal::open(&root, CAP, "c", 300).is_err());
        assert_eq!(fs::read(&path).unwrap(), bytes);
    }
    #[test]
    fn rollback_capacity_and_forged_checkpoint_fail_closed() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("history");
        let mut journal = Journal::open(&root, CAP, "a", 100).unwrap();
        assert!(journal.append(99, "trade", Value::Null).is_err());
        assert!(
            journal
                .append(100, "trade", json!("x".repeat(MAX_ENTRY)))
                .is_err()
        );
        journal.checkpoint(Value::Null).unwrap();
        drop(journal);
        let path = root.join("checkpoint.json");
        let mut saved: Checkpoint = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
        saved.hash = "0".repeat(64);
        fs::write(path, serde_json::to_vec(&saved).unwrap()).unwrap();
        assert!(Journal::open(&root, CAP, "b", 200).is_err());
    }
}

mod runtime;
pub(crate) use runtime::Handle;
pub(crate) use runtime::Runtime;
