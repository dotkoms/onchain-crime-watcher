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
    assert_eq!(result["watch_controller_replayed"], false);
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

fn controller_capture(dir: &std::path::Path, fault: &str) -> std::path::PathBuf {
    let path = dir.join("controller.ocwcap");
    let mut segment = Segment::create(&path).unwrap();
    let mut config = Config::default();
    config.feed.auto_watch = true;
    let raw = include_str!("../../../tests/fixtures/pumpportal-create-observed.jsonl")
        .lines()
        .find(|raw| serde_json::from_str::<serde_json::Value>(raw).unwrap()["txType"] == "create")
        .unwrap();
    let mint = serde_json::from_str::<serde_json::Value>(raw).unwrap()["mint"]
        .as_str()
        .unwrap()
        .to_owned();
    let key = ocw_core::feed::key::<32>(&mint).unwrap();
    let mut records = vec![
        record(
            1,
            0,
            "boot",
            json!({"config":config,"execution_schema":1,"watch_schema":1,"initial_state":"cold","volume_enabled":true}),
        ),
        record(2, 1, "watch_start", json!({"schema":1})),
        Record::frame(
            3,
            "analysisclock",
            1,
            100_000_000,
            1100,
            Bytes::copy_from_slice(raw.as_bytes()),
        ),
    ];
    if fault != "missing_plan" {
        records.push(record(4,100,"watch_plan",json!({"apply_ms":100,"input_seq":3,"actions":if fault == "wrong_action" { json!([]) } else { json!([{"Subscribe":{"mint":key,"created_ms":100,"partial":false}}]) }})));
    }
    records.push(record(1,100,"watch_apply",json!({"operation":"attach","mint":mint,"observed_create_ms":100,"partial":false,"apply_ms":100})));
    records.push(record(
        1,
        101,
        "frame_apply",
        json!({"input_seq":3,"apply_ms":100,"allow_create":fault != "wrong_admission"}),
    ));
    if fault == "missing_application" {
        records.pop();
    }
    if matches!(fault, "expiry" | "early_expiry") {
        let now = if fault == "expiry" { 300_100 } else { 300_099 };
        records.push(record(
            1,
            now,
            "watch_plan",
            json!({"apply_ms":now,"input_seq":null,"actions":[{"Unsubscribe":key}]}),
        ));
        records.push(record(
            1,
            now,
            "watch_apply",
            json!({"operation":"remove","mint":mint,"apply_ms":now}),
        ));
        records.push(record(1, now, "clock_tick", json!({})));
    }
    for (i, mut item) in records.into_iter().enumerate() {
        item.seq = i as u64 + 1;
        segment.append(&item).unwrap();
    }
    segment.sync().unwrap();
    path
}
#[test]
fn controller_replay_recomputes_actions_and_rejects_missing_or_changed_admission_evidence() {
    let dir = tempfile::tempdir().unwrap();
    let path = controller_capture(dir.path(), "none");
    let before = std::fs::read(&path).unwrap();
    let report = analysis::run(&path, |_, _| Ok(())).unwrap();
    assert_eq!(report["watch_controller_replayed"], true);
    assert_eq!(report["watch_plans"], 1);
    assert_eq!(report["financial_admission_replayed"], false);
    assert_eq!(report, analysis::run(&path, |_, _| Ok(())).unwrap());
    assert_eq!(before, std::fs::read(path).unwrap());
    for fault in [
        "missing_plan",
        "wrong_action",
        "wrong_admission",
        "missing_application",
        "early_expiry",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let path = controller_capture(dir.path(), fault);
        assert!(analysis::run(&path, |_, _| Ok(())).is_err(), "{fault}");
    }
}

#[test]
fn timer_replay_removes_unqualified_mints_at_the_recorded_retention_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let path = controller_capture(dir.path(), "expiry");
    let report = analysis::run(&path, |_, _| Ok(())).unwrap();
    assert_eq!(report["watch_plans"], 2);
    assert_eq!(report["active_mints"], 0);
}

#[test]
fn spend_replay_checks_starting_usage_reservations_and_never_resets_on_clock_rollback() {
    for invalid in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spend.ocwcap");
        let mut segment = Segment::create(&path).unwrap();
        let config = Config::default();
        let block = config.gate.reservation_messages;
        let values = [
            (
                "boot",
                json!({"config":config,"execution_schema":1,"spend_schema":1,"initial_state":"cold","volume_enabled":false}),
            ),
            (
                "spend_start",
                json!({"schema":1,"state":{"day":10,"reserved":block},"credit_day":10,"credits":0}),
            ),
            ("spend_check", json!({"day":9,"credits":0,"credit_day":10})),
            (
                "spend_reservation",
                json!({"day":9,"grant":if invalid {block+1} else {block},"reserved":block*2}),
            ),
        ];
        for (i, (kind, value)) in values.into_iter().enumerate() {
            segment
                .append(&record(i as u64 + 1, i as u64, kind, value))
                .unwrap();
        }
        segment.sync().unwrap();
        let before = std::fs::read(&path).unwrap();
        let result = analysis::run(&path, |_, _| Ok(()));
        if invalid {
            assert!(result.is_err());
        } else {
            let summary = result.unwrap();
            assert_eq!(summary["financial_admission_replayed"], true);
            assert_eq!(summary["spend_reserved_messages"], block * 2);
            assert_eq!(summary["spend_ledger_day"], 10);
        }
        assert_eq!(before, std::fs::read(path).unwrap());
    }
}

#[test]
fn a_route_without_a_recomputed_detector_candidate_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("route.ocwcap");
    let mut segment = Segment::create(&path).unwrap();
    segment.append(&record(1,0,"boot",json!({"config":Config::default(),"execution_schema":1,"route_schema":1,"staging_enabled":true,"initial_state":"cold","volume_enabled":true}))).unwrap();
    segment.append(&record(2,1,"volume_route",json!({"decision":{},"apply_ms":1,"generation":1,"feed_connected":true,"status":"queued"}))).unwrap();
    segment.sync().unwrap();
    assert!(analysis::run(&path, |_, _| Ok(())).is_err());
}
