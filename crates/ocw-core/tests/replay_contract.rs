use bytes::Bytes;
use ocw_core::{
    capture::{Record, Segment},
    config::FeedConfig,
    replay,
};
use std::path::{Path, PathBuf};

fn recorded_creates(dir: &Path) -> PathBuf {
    let path = dir.join("recorded.ocwcap");
    let mut segment = Segment::create(&path).unwrap();
    // These payloads were observed on the real free provider stream, not generated.
    for (i, line) in include_str!("../../../tests/fixtures/pumpportal-create-observed.jsonl")
        .lines()
        .enumerate()
    {
        segment
            .append(&Record::frame(
                i as u64 + 1,
                "recorded",
                1,
                i as u64 * 1_000_000_000,
                0,
                Bytes::copy_from_slice(line.as_bytes()),
            ))
            .unwrap();
    }
    segment.sync().unwrap();
    path
}

#[test]
fn observed_capture_has_creates_but_no_invented_trades_or_alerts() {
    let dir = tempfile::tempdir().unwrap();
    let path = recorded_creates(dir.path());
    let before = std::fs::read(&path).unwrap();
    let result = replay::run(&path, &FeedConfig::default(), |decision, _| {
        assert!(decision.card(decision.bucket_end_ms).is_none());
        Ok(())
    })
    .unwrap();
    assert_eq!(result["frames"], 4);
    assert_eq!(result["creates"], 2);
    assert_eq!(result["trades"], 0);
    assert_eq!(result["decode_errors"], 0);
    assert_eq!(result["volume"]["fire"], 0);
    assert_eq!(result["alerts_sent"], 0);
    assert_eq!(std::fs::read(&path).unwrap(), before);
}

#[test]
fn corrupt_or_incomplete_capture_is_never_reported_as_a_successful_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = recorded_creates(dir.path());
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.pop();
    std::fs::write(&path, &bytes).unwrap();
    assert!(
        replay::run(&path, &FeedConfig::default(), |_, _| Ok(()))
            .unwrap_err()
            .contains("torn tail")
    );
    bytes[9] ^= 1;
    std::fs::write(&path, &bytes).unwrap();
    assert!(replay::run(&path, &FeedConfig::default(), |_, _| Ok(())).is_err());
}

#[test]
fn mixed_boots_and_clock_rollback_fail_without_echoing_record_values() {
    for (boot, ns) in [("secret-boot", 0), ("first", 0)] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("bad.ocwcap");
        let mut segment = Segment::create(&path).unwrap();
        segment
            .append(&Record::frame(
                1,
                "first",
                1,
                1_000_000,
                0,
                Bytes::from_static(b"{}"),
            ))
            .unwrap();
        segment
            .append(&Record::frame(2, boot, 1, ns, 0, Bytes::from_static(b"{}")))
            .unwrap();
        segment.sync().unwrap();
        let error = replay::run(&path, &FeedConfig::default(), |_, _| Ok(())).unwrap_err();
        assert!(!error.contains("secret-boot"));
    }
}

#[test]
fn a_capture_gap_rewarms_the_engine_and_unknown_fills_remain_unknown() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("gap.ocwcap");
    let mut segment = Segment::create(&path).unwrap();
    segment
        .append(&Record::frame(
            1,
            "first",
            1,
            0,
            0,
            Bytes::from_static(
                b"{\"txType\":\"create\",\"mint\":\"11111111111111111111111111111111\"}",
            ),
        ))
        .unwrap();
    segment
        .append(&Record::control(
            2,
            "first",
            1,
            1_000_000,
            0,
            "capture_gap",
            Bytes::from_static(b"{}"),
        ))
        .unwrap();
    segment.sync().unwrap();
    let result = replay::run(&path, &FeedConfig::default(), |_, _| Ok(())).unwrap();
    assert_eq!(result["volume"]["gaps"], 1);
    assert_eq!(result["trades"], 0);
    assert_eq!(result["volume"]["fire"], 0);
}
