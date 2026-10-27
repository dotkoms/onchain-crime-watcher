use super::{audit::Audit, http::Outcome, policy::Queue, store::Journal, tests::candidate};
use crate::{config::DeliveryConfig, health::Health, storage::ProcessLock, volume::Verdict};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

#[tokio::test]
async fn recorded_queue_budgets_payloads_and_unknown_outcomes_replay_without_writes() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let guard = Arc::new(ProcessLock::acquire(&data).unwrap());
    let runtime = crate::history::Runtime::start(
        data.join("history"),
        "boot1".into(),
        16 * 1024 * 1024,
        Arc::new(Health::default()),
        guard,
    )
    .await
    .unwrap();
    let config = DeliveryConfig {
        queue_capacity: 2,
        ..DeliveryConfig::default()
    };
    let mut journal = Journal::open(&data.join("delivery.json"), &config).unwrap();
    let origin = Instant::now() - Duration::from_secs(10);
    let audit = Audit::start(
        runtime.handle.clone(),
        origin,
        "boot1",
        &config,
        journal.snapshot(),
    );
    journal.audit(audit.clone());
    let mut queue = Queue::new(2);
    queue.audit(audit.clone());
    for _ in 0..100 {
        assert!(queue.take(Instant::now(), 1).is_none());
    }
    assert!(queue.insert(candidate(Verdict::Near, 1, origin)));
    assert!(queue.insert(candidate(Verdict::Near, 4, origin)));
    assert!(queue.insert(candidate(Verdict::Fire, 2, origin)));
    assert!(!queue.insert(candidate(Verdict::Fire, 2, origin)));
    let mut selected = queue.take(Instant::now(), 1).unwrap();
    assert_eq!(selected.decision.verdict, Verdict::Fire);
    assert!(
        journal
            .reserve(&selected.id, selected.tier, 100_000)
            .unwrap()
    );
    selected.reserved = true;
    let now = Instant::now();
    let payload = selected.payload(now).unwrap();
    selected.attempts += 1;
    audit.attempt(&selected, now, 1, true, &payload);
    audit.response(&selected.id, &Outcome::Unknown, Instant::now());
    journal.finish(&selected.id, None, true).unwrap();
    assert!(queue.insert(candidate(Verdict::Fire, 3, origin)));
    let mut retry = queue.take(Instant::now(), 1).unwrap();
    assert!(journal.reserve(&retry.id, retry.tier, 100_001).unwrap());
    retry.reserved = true;
    let now = Instant::now();
    let payload = retry.payload(now).unwrap();
    retry.attempts += 1;
    audit.attempt(&retry, now, 1, true, &payload);
    let paused = Instant::now();
    audit.response(&retry.id, &Outcome::RateLimited, paused);
    retry.ready = retry.ready.max(paused);
    assert!(queue.insert(retry));
    let mut retry = queue.take(Instant::now(), 1).unwrap();
    assert!(retry.reserved);
    let now = Instant::now();
    let payload = retry.payload(now).unwrap();
    retry.attempts += 1;
    audit.attempt(&retry, now, 1, true, &payload);
    audit.response(&retry.id, &Outcome::Sent("99".into()), Instant::now());
    journal.finish(&retry.id, Some("99"), false).unwrap();
    assert!(queue.insert(candidate(Verdict::Fire, 4, origin)));
    let denied = queue.take(Instant::now(), 1).unwrap();
    assert!(!journal.reserve(&denied.id, denied.tier, 100_002).unwrap());
    assert_eq!(queue.prune(origin + Duration::from_secs(15), 1), 1);
    queue.clear();
    audit.record("end", serde_json::json!({}));
    runtime.shutdown().await.unwrap();
    let path = data.join("history/observations.ocwh");
    let before = std::fs::read(&path).unwrap();
    let report = super::replay::run(&path).unwrap();
    assert_eq!(report["delivery_policy_replayed"], true);
    assert_eq!(report["attempts"], 3);
    assert_eq!(report["observed_sent"], 1);
    assert_eq!(report["observed_unknown"], 1);
    assert_eq!(report["alerts_sent"], 0);
    assert_eq!(report, super::replay::run(&path).unwrap());
    assert_eq!(before, std::fs::read(&path).unwrap());
    let mut records = Vec::new();
    crate::history::read(&path, 16 * 1024 * 1024, |entry| {
        if entry.kind == "delivery_replay" {
            records.push(entry);
        }
        Ok(())
    })
    .unwrap();
    for fault in [
        "queue",
        "budget",
        "payload",
        "response",
        "missing_reservation",
        "missing_response",
        "missing_end",
        "rate_as_rejected",
        "late_attempt",
        "generation",
    ] {
        let copy = tempfile::tempdir().unwrap();
        let root = copy.path().join("history");
        let mut tape = crate::history::Journal::open(&root, 16 * 1024 * 1024, "boot1", 1).unwrap();
        let mut changed = false;
        for entry in &records {
            let mut data = entry.data.clone();
            let op = data["op"].as_str().unwrap();
            if !changed {
                if matches!((fault, op), ("queue", "insert") | ("budget", "reserve")) {
                    data["data"]["accepted"] = false.into();
                    changed = true;
                } else if fault == "rate_as_rejected"
                    && op == "response"
                    && data["data"]["outcome"] == "RateLimited"
                {
                    data["data"]["outcome"] = "Rejected".into();
                    changed = true;
                } else if fault == "late_attempt" && op == "attempt" {
                    data["data"]["now_ns"] = 30_000_000_000u64.into();
                    changed = true;
                } else if fault == "generation" && op == "attempt" {
                    data["data"]["generation"] = 2.into();
                    changed = true;
                } else if fault == "payload" && op == "attempt" {
                    data["data"]["payload_sha256"] = "wrong".into();
                    changed = true;
                } else if fault == "response" && op == "response" {
                    data["data"]["outcome"] = "Rejected".into();
                    changed = true;
                } else if matches!(
                    (fault, op),
                    ("missing_reservation", "reserve")
                        | ("missing_response", "response")
                        | ("missing_end", "end")
                ) {
                    changed = true;
                    continue;
                }
            }
            tape.append(entry.wall_ms, "delivery_replay", data).unwrap();
        }
        assert!(changed);
        tape.sync().unwrap();
        assert!(
            super::replay::run(&root.join("observations.ocwh")).is_err(),
            "{fault}"
        );
    }
}
