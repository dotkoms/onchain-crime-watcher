use bytes::Bytes;
use ocw_core::{
    capture::{Record, Segment},
    config::Config,
    replay::analysis,
};
use serde_json::json;
fn record(seq: u64, ms: u64, kind: &str, payload: serde_json::Value) -> Record {
    Record::control(
        seq,
        "analysisclock",
        1,
        ms * 1_000_000,
        1000 + ms,
        kind,
        Bytes::from(payload.to_string()),
    )
}
fn capture(dir: &std::path::Path, allow: bool, tick: bool) -> std::path::PathBuf {
    let path = dir.join("steps.ocwcap");
    let mut segment = Segment::create(&path).unwrap();
    segment.append(&record(1,0,"boot",json!({"config":Config::default(),"execution_schema":1,"initial_state":"cold","volume_enabled":true}))).unwrap();
    // Real observed provider payload; only execution timing is constructed in this isolated test.
    let raw = include_str!("../../../tests/fixtures/pumpportal-create-observed.jsonl")
        .lines()
        .find(|raw| serde_json::from_str::<serde_json::Value>(raw).unwrap()["txType"] == "create")
        .unwrap();
    segment
        .append(&Record::frame(
            2,
            "analysisclock",
            1,
            100_000_000,
            1100,
            Bytes::copy_from_slice(raw.as_bytes()),
        ))
        .unwrap();
    if tick {
        segment
            .append(&record(
                3,
                101,
                "frame_apply",
                json!({"input_seq":2,"apply_ms":100,"allow_create":allow}),
            ))
            .unwrap();
        segment
            .append(&record(4, 15_100, "clock_tick", json!({})))
            .unwrap();
    }
    segment.sync().unwrap();
    path
}
#[test]
fn exact_replay_uses_the_recorded_application_clock_and_admission_without_writes() {
    let dir = tempfile::tempdir().unwrap();
    let path = capture(dir.path(), true, true);
    let before = std::fs::read(&path).unwrap();
    let mut ends = Vec::new();
    let result = analysis::run(&path, |decision, now| {
        ends.push((decision.bucket_end_ms, now));
        Ok(())
    })
    .unwrap();
    assert_eq!(ends, vec![(15_100, 15_100)]);
    assert_eq!(result["active_mints"], 1);
    assert_eq!(result["delivery_replayed"], false);
    assert_eq!(result["alerts_sent"], 0);
    let repeated = analysis::run(&path, |_, _| Ok(())).unwrap();
    assert_eq!(result, repeated);
    assert_eq!(std::fs::read(path).unwrap(), before);
    let dir = tempfile::tempdir().unwrap();
    let path = capture(dir.path(), false, true);
    let result = analysis::run(&path, |_, _| {
        panic!("non-admitted create cannot open a volume ring")
    })
    .unwrap();
    assert_eq!(result["active_mints"], 0);
}
#[test]
fn missing_application_and_missing_boot_configuration_fail_instead_of_guessing() {
    let dir = tempfile::tempdir().unwrap();
    let path = capture(dir.path(), true, false);
    assert!(
        analysis::run(&path, |_, _| Ok(()))
            .unwrap_err()
            .contains("incomplete execution tail")
    );
    let path = dir.path().join("without-boot.ocwcap");
    let mut segment = Segment::create(&path).unwrap();
    segment
        .append(&record(1, 0, "clock_tick", json!({})))
        .unwrap();
    segment.sync().unwrap();
    assert!(
        analysis::run(&path, |_, _| Ok(()))
            .unwrap_err()
            .contains("initial boot")
    );
}
