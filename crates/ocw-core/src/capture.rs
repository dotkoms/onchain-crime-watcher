use crate::{
    config::CaptureConfig,
    health::{Health, inc, set, set_state},
    storage::{ProcessLock, atomic_write, new_private, private_dir},
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

const MAGIC: &[u8; 5] = b"OCWC\x01";
const MAX_RECORD: usize = 70_000;
const MANIFEST_RESERVE: u64 = 2048;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Record {
    pub seq: u64,
    pub boot_id: String,
    pub generation: u64,
    pub mono_ns: u64,
    pub wall_ms: u64,
    pub kind: String,
    pub payload: Bytes,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Meta {
    seq: u64,
    boot_id: String,
    generation: u64,
    mono_ns: u64,
    wall_ms: u64,
    kind: String,
}
impl Record {
    pub fn frame(
        seq: u64,
        boot: &str,
        generation: u64,
        mono_ns: u64,
        wall_ms: u64,
        payload: Bytes,
    ) -> Self {
        Self {
            seq,
            boot_id: boot.into(),
            generation,
            mono_ns,
            wall_ms,
            kind: "frame".into(),
            payload,
        }
    }
    pub fn control(
        seq: u64,
        boot: &str,
        generation: u64,
        mono_ns: u64,
        wall_ms: u64,
        kind: &str,
        payload: Bytes,
    ) -> Self {
        Self {
            kind: kind.into(),
            ..Self::frame(seq, boot, generation, mono_ns, wall_ms, payload)
        }
    }
    fn encoded(&self) -> io::Result<Vec<u8>> {
        let meta = Meta {
            seq: self.seq,
            boot_id: self.boot_id.clone(),
            generation: self.generation,
            mono_ns: self.mono_ns,
            wall_ms: self.wall_ms,
            kind: self.kind.clone(),
        };
        let json = serde_json::to_vec(&meta)?;
        if json.len() > 4096 || json.len() + 4 + self.payload.len() > MAX_RECORD {
            return Err(io::Error::other("capture record too large"));
        }
        let mut raw = (json.len() as u32).to_le_bytes().to_vec();
        raw.extend(json);
        raw.extend_from_slice(&self.payload);
        zstd::bulk::compress(&raw, 3)
    }
}

pub struct Segment {
    file: File,
    bytes: u64,
    hash: Sha256,
    records: u64,
    first_seq: u64,
    last_seq: u64,
}
impl Segment {
    pub fn create(path: &Path) -> io::Result<Self> {
        let mut file = new_private(path)?;
        file.write_all(MAGIC)?;
        let mut hash = Sha256::new();
        hash.update(MAGIC);
        Ok(Self {
            file,
            bytes: 5,
            hash,
            records: 0,
            first_seq: 0,
            last_seq: 0,
        })
    }
    pub fn append(&mut self, record: &Record) -> io::Result<()> {
        self.append_encoded(record, &record.encoded()?)
    }
    fn append_encoded(&mut self, record: &Record, compressed: &[u8]) -> io::Result<()> {
        let len = (compressed.len() as u32).to_le_bytes();
        let checksum = Sha256::digest(compressed);
        self.file.write_all(&len)?;
        self.file.write_all(&checksum)?;
        self.file.write_all(compressed)?;
        self.hash.update(len);
        self.hash.update(checksum);
        self.hash.update(compressed);
        self.bytes += 36 + compressed.len() as u64;
        if self.records == 0 {
            self.first_seq = record.seq;
        }
        self.last_seq = record.seq;
        self.records += 1;
        Ok(())
    }
    pub fn sync(&mut self) -> io::Result<()> {
        self.file.sync_data()
    }
    fn manifest(&mut self, path: &Path, closed: bool, health: &Health) -> io::Result<()> {
        self.sync()?;
        let manifest = serde_json::json!({"schema":1,"replay_mode":"decoder-only","closed":closed,"capture":health.snapshot()["capture"],"records":self.records,"first_seq":self.first_seq,"last_seq":self.last_seq,"bytes":self.bytes,"sha256":format!("{:x}",self.hash.clone().finalize())});
        atomic_write(
            &path.with_extension("json"),
            &serde_json::to_vec(&manifest)?,
        )
    }
}
pub struct ReadResult {
    pub records: Vec<Record>,
    pub torn_tail: bool,
}
pub fn read_segment(path: &Path) -> io::Result<ReadResult> {
    let mut records = Vec::new();
    let mut bytes = 0;
    let (_, torn_tail) = scan(path, |record| {
        bytes += record.payload.len();
        if records.len() >= 10_000 || bytes > 4 * 1024 * 1024 {
            return Err(io::Error::other(
                "inspection limit exceeded; use streaming scan",
            ));
        }
        records.push(record);
        Ok(())
    })?;
    Ok(ReadResult { records, torn_tail })
}
pub fn recover_segment(path: &Path) -> io::Result<u64> {
    let (valid, torn) = scan(path, |_| Ok(()))?;
    if !torn {
        return Ok(0);
    }
    let file = fs::OpenOptions::new().write(true).open(path)?;
    let lost = file.metadata()?.len() - valid;
    file.set_len(valid)?;
    file.sync_all()?;
    let mut records = 0u64;
    let mut first_seq = 0;
    let mut last_seq = 0;
    scan(path, |record| {
        if records == 0 {
            first_seq = record.seq;
        }
        last_seq = record.seq;
        records += 1;
        Ok(())
    })?;
    let mut input = File::open(path)?;
    let mut hash = Sha256::new();
    let mut buffer = [0u8; 8192];
    loop {
        let size = input.read(&mut buffer)?;
        if size == 0 {
            break;
        }
        hash.update(&buffer[..size]);
    }
    let manifest = serde_json::json!({
        "schema": 1, "replay_mode": "decoder-only", "closed": true,
        "recovered": true, "recovered_tail_bytes": lost,
        "records": records, "first_seq": first_seq, "last_seq": last_seq,
        "bytes": valid, "sha256": format!("{:x}", hash.finalize())
    });
    atomic_write(
        &path.with_extension("json"),
        &serde_json::to_vec(&manifest)?,
    )?;
    Ok(lost)
}
/// The visitor API keeps recovery and whole-file inspection bounded even for large captures.
pub fn scan(path: &Path, visit: impl FnMut(Record) -> io::Result<()>) -> io::Result<(u64, bool)> {
    if path.is_symlink() {
        return Err(io::Error::other("capture cannot be a symlink"));
    }
    scan_reader(File::open(path)?, visit)
}
fn scan_reader(
    mut file: impl Read,
    mut visit: impl FnMut(Record) -> io::Result<()>,
) -> io::Result<(u64, bool)> {
    let mut magic = [0u8; 5];
    file.read_exact(&mut magic)?;
    if &magic != MAGIC {
        return Err(io::Error::other("capture header/version mismatch"));
    }
    let mut valid = 5u64;
    loop {
        let mut len = [0u8; 4];
        let n = file.read(&mut len[..1])?;
        if n == 0 {
            return Ok((valid, false));
        }
        if !read_tail(&mut file, &mut len[1..])? {
            return Ok((valid, true));
        }
        let len = u32::from_le_bytes(len) as usize;
        if len > MAX_RECORD + 1024 {
            return Err(io::Error::other("invalid capture record length"));
        }
        let mut checksum = [0u8; 32];
        let mut compressed = vec![0u8; len];
        if !read_tail(&mut file, &mut checksum)? || !read_tail(&mut file, &mut compressed)? {
            return Ok((valid, true));
        }
        if Sha256::digest(&compressed).as_slice() != checksum {
            return Err(io::Error::other("capture checksum mismatch"));
        }
        let decoder = zstd::stream::read::Decoder::new(compressed.as_slice())?;
        let mut raw = Vec::new();
        decoder
            .take((MAX_RECORD + 1) as u64)
            .read_to_end(&mut raw)?;
        if raw.len() > MAX_RECORD || raw.len() < 4 {
            return Err(io::Error::other("invalid capture decompressed size"));
        }
        let meta_len = u32::from_le_bytes(raw[..4].try_into().map_err(io::Error::other)?) as usize;
        if meta_len > 4096 || 4 + meta_len > raw.len() {
            return Err(io::Error::other("invalid capture metadata"));
        }
        let m: Meta = serde_json::from_slice(&raw[4..4 + meta_len])
            .map_err(|_| io::Error::other("invalid capture metadata"))?;
        let payload = Bytes::copy_from_slice(&raw[4 + meta_len..]);
        visit(Record {
            seq: m.seq,
            boot_id: m.boot_id,
            generation: m.generation,
            mono_ns: m.mono_ns,
            wall_ms: m.wall_ms,
            kind: m.kind,
            payload,
        })?;
        valid += 36 + len as u64;
    }
}

fn read_tail(reader: &mut impl Read, bytes: &mut [u8]) -> io::Result<bool> {
    match reader.read_exact(bytes) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(error),
    }
}

pub struct Tap {
    sender: mpsc::Sender<Record>,
    health: Arc<Health>,
    gap: Option<Record>,
}
pub fn channel(capacity: usize, health: Arc<Health>) -> (Tap, mpsc::Receiver<Record>) {
    let (sender, rx) = mpsc::channel(capacity);
    (
        Tap {
            sender,
            health,
            gap: None,
        },
        rx,
    )
}
impl Tap {
    pub fn record(&mut self, record: Record) {
        if let Some(gap) = self.gap.take()
            && let Err(err) = self.sender.try_send(gap)
        {
            self.gap = Some(err.into_inner());
        }
        if self.gap.is_none() && self.sender.try_send(record.clone()).is_ok() {
            return;
        }
        inc(&self.health.capture_dropped);
        let mut gap = self.gap.take().unwrap_or_else(|| {
            Record::control(
                record.seq,
                &record.boot_id,
                record.generation,
                record.mono_ns,
                record.wall_ms,
                "capture_gap",
                Bytes::new(),
            )
        });
        gap.payload =
            Bytes::from(serde_json::json!({"from_seq":gap.seq,"to_seq":record.seq}).to_string());
        self.gap = Some(gap);
    }
    pub async fn finish(mut self) {
        if let Some(gap) = self.gap.take() {
            let _ = self.sender.send(gap).await;
        }
    }
}

fn owned_files(root: &Path) -> io::Result<Vec<(u64, PathBuf, u64)>> {
    let mut files = Vec::new();
    if !root.exists() {
        return Ok(files);
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        let Some(day) = name
            .strip_prefix("day-")
            .and_then(|s| s.parse::<u64>().ok())
        else {
            continue;
        };
        for f in fs::read_dir(entry.path())? {
            let f = f?;
            if !f.file_type()?.is_file() {
                continue;
            }
            let n = f.file_name();
            let n = n.to_string_lossy();
            if n.starts_with("boot-") && (n.ends_with(".ocwcap") || n.ends_with(".json")) {
                files.push((day, f.path(), f.metadata()?.len()));
            }
        }
    }
    Ok(files)
}
fn prune(root: &Path, day: u64, cfg: &CaptureConfig) -> io::Result<u64> {
    let files = owned_files(root)?;
    let mut total = files.iter().map(|x| x.2).sum::<u64>();
    let mut days = files.iter().map(|x| x.0).collect::<Vec<_>>();
    days.sort_unstable();
    days.dedup();
    for old in days {
        if old >= day {
            continue;
        }
        if old + cfg.retention_days >= day && total + MANIFEST_RESERVE < cfg.max_bytes {
            continue;
        }
        let dir = root.join(format!("day-{old}"));
        let all_owned = fs::read_dir(&dir)?.all(|e| {
            e.ok()
                .is_some_and(|e| files.iter().any(|f| f.1 == e.path()))
        });
        if !all_owned {
            continue;
        }
        for (_, path, len) in files.iter().filter(|f| f.0 == old) {
            fs::remove_file(path)?;
            total = total.saturating_sub(*len);
        }
        fs::remove_dir(dir)?;
    }
    Ok(total)
}
pub fn spawn(
    root: PathBuf,
    cfg: CaptureConfig,
    health: Arc<Health>,
) -> (Tap, tokio::task::JoinHandle<()>) {
    spawn_inner(root, cfg, health, None)
}

pub(crate) fn spawn_owned(
    root: PathBuf,
    cfg: CaptureConfig,
    health: Arc<Health>,
    ownership: Arc<ProcessLock>,
) -> (Tap, tokio::task::JoinHandle<()>) {
    spawn_inner(root, cfg, health, Some(ownership))
}

fn spawn_inner(
    root: PathBuf,
    cfg: CaptureConfig,
    health: Arc<Health>,
    ownership: Option<Arc<ProcessLock>>,
) -> (Tap, tokio::task::JoinHandle<()>) {
    let (tap, mut rx) = channel(cfg.queue_capacity, health.clone());
    let task = tokio::task::spawn_blocking(move || {
        // Blocking I/O cannot be aborted. Keep exclusive ownership even if the caller leaves.
        let _ownership = ownership;
        let mut in_progress = false;
        let result = (|| -> io::Result<()> {
            // A pre-existing capture directory may contain operator files; never chmod or delete them.
            if !root.exists() {
                private_dir(&root)?;
            } else if root.is_symlink() {
                return Err(io::Error::other("capture directory cannot be symlink"));
            }
            for (_, path, _) in owned_files(&root)? {
                if path.extension().is_some_and(|e| e == "ocwcap") {
                    health.capture_recovered.fetch_add(
                        recover_segment(&path)?,
                        std::sync::atomic::Ordering::Relaxed,
                    );
                }
            }
            let mut current: Option<(u64, u64, PathBuf, Segment)> = None;
            let mut total = owned_files(&root)?.iter().map(|x| x.2).sum::<u64>();
            let mut last_sync = Instant::now();
            set_state(&health.capture_state, "writing");
            loop {
                let record = match rx.try_recv() {
                    Ok(r) => r,
                    Err(mpsc::error::TryRecvError::Empty) => {
                        if last_sync.elapsed() >= Duration::from_secs(1) {
                            if let Some((_, _, path, segment)) = &mut current {
                                segment.manifest(path, false, &health)?;
                            }
                            last_sync = Instant::now();
                        }
                        std::thread::park_timeout(Duration::from_millis(20));
                        continue;
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => break,
                };
                in_progress = true;
                if record.boot_id.is_empty()
                    || record.boot_id.len() > 64
                    || !record.boot_id.bytes().all(|b| b.is_ascii_alphanumeric())
                {
                    return Err(io::Error::other("invalid capture boot identity"));
                }
                let day = record.wall_ms / 86_400_000;
                let hour = record.wall_ms / 3_600_000;
                if fs2::available_space(&root)? < cfg.min_free_bytes {
                    inc(&health.capture_dropped);
                    set_state(&health.capture_state, "stopped_disk");
                    break;
                }
                if current
                    .as_ref()
                    .is_none_or(|(d, h, _, _)| *d != day || *h != hour)
                {
                    if let Some((_, _, path, mut segment)) = current.take() {
                        segment.manifest(&path, true, &health)?;
                    }
                    total = prune(&root, day, &cfg)?;
                    if total + 5 + MANIFEST_RESERVE >= cfg.max_bytes {
                        inc(&health.capture_dropped);
                        set_state(&health.capture_state, "stopped_cap");
                        break;
                    }
                    let dir = root.join(format!("day-{day}"));
                    private_dir(&dir)?;
                    let path = dir.join(format!("boot-{}-{}.ocwcap", record.boot_id, record.seq));
                    current = Some((day, hour, path.clone(), Segment::create(&path)?));
                    total += 5;
                }
                let compressed = record.encoded()?;
                let bytes = 36 + compressed.len() as u64;
                if total + bytes + MANIFEST_RESERVE > cfg.max_bytes {
                    inc(&health.capture_dropped);
                    set_state(&health.capture_state, "stopped_cap");
                    break;
                }
                if let Some((_, _, _, segment)) = &mut current {
                    segment.append_encoded(&record, &compressed)?;
                }
                total += bytes;
                set(&health.capture_bytes, total);
                inc(&health.capture_written);
                in_progress = false;
                if last_sync.elapsed() >= Duration::from_secs(1) {
                    if let Some((_, _, path, segment)) = &mut current {
                        segment.manifest(path, false, &health)?;
                    }
                    last_sync = Instant::now();
                }
            }
            while rx.try_recv().is_ok() {
                inc(&health.capture_dropped);
            }
            if health.snapshot()["capture"]["state"] == "writing" {
                set_state(&health.capture_state, "sealed");
            }
            if let Some((_, _, path, mut segment)) = current {
                segment.manifest(&path, true, &health)?;
            }
            Ok(())
        })();
        if result.is_err() {
            if in_progress {
                inc(&health.capture_dropped);
            }
            while rx.try_recv().is_ok() {
                inc(&health.capture_dropped);
            }
            set_state(&health.capture_state, "error");
        }
    });
    (tap, task)
}

/// One deadline includes both the final loss marker enqueue and the disk worker join.
/// A timed-out blocking writer continues to own its process lock until its I/O completes.
pub(crate) async fn shutdown(
    tap: Tap,
    writer: tokio::task::JoinHandle<()>,
    health: &Health,
    deadline: Duration,
) -> Result<(), &'static str> {
    set_state(&health.capture_shutdown, "draining");
    let joined = tokio::time::timeout(deadline, async move {
        tap.finish().await;
        writer.await
    })
    .await;
    match joined {
        Ok(Ok(())) if health.snapshot()["capture"]["state"] != "error" => {
            set_state(&health.capture_shutdown, "complete");
            Ok(())
        }
        Ok(_) => {
            set_state(&health.capture_state, "error");
            set_state(&health.capture_shutdown, "failed");
            Err("capture writer failed; shutdown incomplete")
        }
        Err(_) => {
            set_state(&health.capture_shutdown, "pending");
            Err("capture shutdown deadline exceeded; disk work may remain pending")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn shutdown_deadline_includes_a_full_queue_with_a_pending_loss_marker() {
        let health = Arc::new(Health::default());
        let (mut tap, rx) = channel(1, health.clone());
        for seq in 1..=2 {
            tap.record(Record::frame(seq, "test", 0, seq, seq, Bytes::new()));
        }
        // Keep the receiver open but unable to drain, as with a stalled disk writer.
        let writer = tokio::spawn(std::future::pending());
        let abort = writer.abort_handle();
        let outcome = tokio::time::timeout(
            Duration::from_secs(1),
            shutdown(tap, writer, &health, Duration::from_millis(20)),
        )
        .await;
        abort.abort();
        drop(rx);
        assert_eq!(
            outcome.unwrap(),
            Err("capture shutdown deadline exceeded; disk work may remain pending")
        );
        assert_eq!(health.snapshot()["capture"]["shutdown"], "pending");
        assert_eq!(health.snapshot()["capture"]["dropped_records"], 1);
    }

    #[tokio::test]
    async fn shutdown_reports_a_writer_panic_as_failure() {
        let health = Arc::new(Health::default());
        let (tap, rx) = channel(1, health.clone());
        let writer = tokio::spawn(async move {
            drop(rx);
            panic!("isolated test worker failure");
        });
        assert_eq!(
            shutdown(tap, writer, &health, Duration::from_secs(1)).await,
            Err("capture writer failed; shutdown incomplete")
        );
        assert_eq!(health.snapshot()["capture"]["shutdown"], "failed");
        assert_eq!(health.snapshot()["capture"]["state"], "error");
    }

    #[tokio::test]
    async fn cancelled_writer_join_retains_ownership_until_capture_finishes() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let ownership = Arc::new(ProcessLock::acquire(&data).unwrap());
        let health = Arc::new(Health::default());
        let (tap, mut writer) = spawn_owned(
            data.join("capture"),
            CaptureConfig {
                min_free_bytes: 0,
                ..CaptureConfig::default()
            },
            health.clone(),
            ownership.clone(),
        );
        drop(ownership);
        tokio::time::timeout(Duration::from_secs(2), async {
            while health.snapshot()["capture"]["state"] != "writing" {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        // The real blocking worker is started and awaits more capture records.
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut writer)
                .await
                .is_err()
        );
        writer.abort();
        let held = ProcessLock::acquire(&data).is_err();
        drop(tap);
        tokio::time::timeout(Duration::from_secs(2), writer)
            .await
            .unwrap()
            .unwrap();
        assert!(held, "pending capture work released collector ownership");
        assert!(ProcessLock::acquire(&data).is_ok());
    }

    #[tokio::test]
    async fn shutdown_seals_real_capture_and_reports_flush_complete() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("capture");
        let health = Arc::new(Health::default());
        let (mut tap, writer) = spawn(
            root.clone(),
            CaptureConfig {
                min_free_bytes: 0,
                ..CaptureConfig::default()
            },
            health.clone(),
        );
        let record = Record::frame(1, "test", 0, 1, 2, Bytes::from_static(b"exact bytes"));
        tap.record(record.clone());
        shutdown(tap, writer, &health, Duration::from_secs(2))
            .await
            .unwrap();
        assert_eq!(health.snapshot()["capture"]["shutdown"], "complete");
        assert_eq!(health.snapshot()["capture"]["state"], "sealed");
        let path = root.join("day-0/boot-test-1.ocwcap");
        let records = read_segment(&path).unwrap();
        assert_eq!(records.records, vec![record]);
        assert!(!records.torn_tail);
        let manifest: serde_json::Value =
            serde_json::from_slice(&fs::read(path.with_extension("json")).unwrap()).unwrap();
        assert_eq!(manifest["closed"], true);
    }

    #[test]
    fn a_real_io_error_is_not_a_torn_tail_that_recovery_may_truncate() {
        struct Broken {
            prefix: std::io::Cursor<Vec<u8>>,
        }
        impl Read for Broken {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if self.prefix.position() as usize >= self.prefix.get_ref().len() {
                    return Err(io::Error::other("simulated disk read failure"));
                }
                self.prefix.read(buf)
            }
        }
        let mut prefix = MAGIC.to_vec();
        prefix.push(1);
        assert!(
            scan_reader(
                Broken {
                    prefix: std::io::Cursor::new(prefix)
                },
                |_| Ok(())
            )
            .is_err()
        );
    }
}
