use bytes::Bytes;
use ocw_core::{
    capture::{Record, Segment, read_segment, recover_segment},
    config::CaptureConfig,
};

#[test]
fn capture_round_trips_exact_bytes_and_detects_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sample.ocwcap");
    let record = Record::frame(
        1,
        "test",
        0,
        10,
        20,
        Bytes::from_static(b"{\"raw\": \"unchanged\"}\n"),
    );
    let mut segment = Segment::create(&path).unwrap();
    segment.append(&record).unwrap();
    segment.sync().unwrap();
    drop(segment);
    let read = read_segment(&path).unwrap();
    assert_eq!(read.records, vec![record]);
    assert!(!read.torn_tail);
    let mut bytes = std::fs::read(&path).unwrap();
    let n = bytes.len();
    bytes[n - 1] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    assert!(read_segment(&path).is_err());
}

#[test]
fn torn_tail_recovery_preserves_every_prior_record() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sample.ocwcap");
    let mut segment = Segment::create(&path).unwrap();
    segment
        .append(&Record::frame(
            1,
            "test",
            0,
            1,
            2,
            Bytes::from_static(b"first"),
        ))
        .unwrap();
    segment.sync().unwrap();
    let good = std::fs::metadata(&path).unwrap().len();
    segment
        .append(&Record::frame(
            2,
            "test",
            0,
            2,
            3,
            Bytes::from_static(b"second"),
        ))
        .unwrap();
    drop(segment);
    let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
    f.set_len(f.metadata().unwrap().len() - 2).unwrap();
    assert!(read_segment(&path).unwrap().torn_tail);
    assert!(recover_segment(&path).unwrap() > 0);
    assert_eq!(std::fs::metadata(&path).unwrap().len(), good);
    assert_eq!(read_segment(&path).unwrap().records.len(), 1);
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(path.with_extension("json")).unwrap()).unwrap();
    use sha2::{Digest, Sha256};
    assert_eq!(
        manifest["sha256"],
        format!("{:x}", Sha256::digest(std::fs::read(&path).unwrap()))
    );
    assert_eq!(manifest["recovered"], true);
}

#[tokio::test]
async fn saturated_capture_is_nonblocking_and_loss_is_explicit() {
    let health = std::sync::Arc::new(ocw_core::health::Health::default());
    let (mut tap, mut rx) = ocw_core::capture::channel(1, health.clone());
    tap.record(Record::frame(1, "test", 0, 1, 2, Bytes::from_static(b"a")));
    tap.record(Record::frame(2, "test", 0, 2, 3, Bytes::from_static(b"b")));
    assert_eq!(health.snapshot()["capture"]["dropped_records"], 1);
    assert_eq!(rx.recv().await.unwrap().seq, 1);
    tap.record(Record::frame(3, "test", 0, 3, 4, Bytes::from_static(b"c")));
    let gap = rx.recv().await.unwrap();
    assert_eq!(gap.kind, "capture_gap");
    assert_eq!(health.snapshot()["capture"]["dropped_records"], 2);
    drop(tap);
}

#[tokio::test]
async fn recorder_stops_at_cap_and_does_not_delete_unowned_files() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().join("capture");
    std::fs::create_dir(&root).unwrap();
    std::fs::write(root.join("keep.txt"), b"operator file").unwrap();
    let health = std::sync::Arc::new(ocw_core::health::Health::default());
    let cfg = CaptureConfig {
        max_bytes: 100,
        min_free_bytes: 0,
        ..CaptureConfig::default()
    };
    let (mut tap, task) = ocw_core::capture::spawn(root.clone(), cfg, health.clone());
    tap.record(Record::frame(
        1,
        "test",
        0,
        1,
        2,
        Bytes::from_static(b"record exceeds cap"),
    ));
    drop(tap);
    task.await.unwrap();
    assert_eq!(health.snapshot()["capture"]["state"], "stopped_cap");
    assert_eq!(health.snapshot()["capture"]["dropped_records"], 1);
    assert_eq!(
        std::fs::read(root.join("keep.txt")).unwrap(),
        b"operator file"
    );
}

#[test]
fn decompression_is_bounded_even_with_a_valid_checksum() {
    use sha2::{Digest, Sha256};
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bomb.ocwcap");
    let compressed = zstd::bulk::compress(&vec![0; 100_000], 3).unwrap();
    let mut file = b"OCWC\x01".to_vec();
    file.extend_from_slice(&(compressed.len() as u32).to_le_bytes());
    file.extend_from_slice(&Sha256::digest(&compressed));
    file.extend(compressed);
    std::fs::write(&path, file).unwrap();
    let err = match read_segment(&path) {
        Ok(_) => panic!("oversized record accepted"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("size"));
}
