use super::{
    policy::{Candidate, Queue},
    store::{Journal, Tier},
};
use crate::{
    config::DeliveryConfig,
    volume::{Decision, Verdict},
};
use std::time::{Duration, Instant};

fn decision(verdict: Verdict, number: u64) -> Decision {
    Decision {
        number,
        mint: bs58::encode([1u8; 32]).into_string(),
        verdict,
        path: "relative",
        regime: "bonding_curve",
        bucket_end_ms: 0,
        completed_buckets: 17,
        volume_lamports: 24_000_000_000,
        baseline_lamports: 6_000_000_000.0,
        floor_lamports: 1_500_000_000.0,
        ratio: Some(3.4),
        cohort_p90: Some(1.0),
        dispersion_threshold_lamports: 9_000_000_000.0,
        trades: 6,
        distinct_takers: 6,
        largest_taker_share: 1.0 / 6.0,
    }
}
pub(super) fn candidate(verdict: Verdict, number: u64, start: Instant) -> Candidate {
    let mut d = decision(verdict, number);
    // Different observed mints represent distinct intents in tests, not application samples.
    d.mint = bs58::encode([number as u8; 32]).into_string();
    Candidate::new(d, "boot1", start, 1).unwrap()
}

#[test]
fn queue_prefers_fire_holds_near_and_never_extends_expiry() {
    let start = Instant::now();
    let mut queue = Queue::new(2);
    assert!(queue.insert(candidate(Verdict::Near, 1, start)));
    assert!(queue.take(start, 1).is_none());
    assert!(queue.insert(candidate(Verdict::Fire, 2, start)));
    assert_eq!(
        queue.take(start, 1).unwrap().decision.verdict,
        Verdict::Fire
    );
    assert!(queue.take(start + Duration::from_millis(4999), 1).is_none());
    assert_eq!(
        queue
            .take(start + Duration::from_millis(5000), 1)
            .unwrap()
            .decision
            .verdict,
        Verdict::Near
    );
    assert!(
        candidate(Verdict::Near, 3, start)
            .payload(start + Duration::from_secs(15))
            .is_none()
    );
    assert!(
        candidate(Verdict::Fire, 3, start)
            .payload(start + Duration::from_secs(30))
            .is_none()
    );
}

#[test]
fn full_queue_replaces_a_near_with_fire_and_deduplicates_intents() {
    let start = Instant::now();
    let mut queue = Queue::new(1);
    assert!(queue.insert(candidate(Verdict::Near, 1, start)));
    assert!(!queue.insert(candidate(Verdict::Near, 1, start)));
    assert!(queue.insert(candidate(Verdict::Fire, 2, start)));
    assert!(!queue.insert(candidate(Verdict::Near, 3, start)));
    assert_eq!(
        queue.take(start, 1).unwrap().decision.verdict,
        Verdict::Fire
    );
}

#[test]
fn staging_payload_is_fresh_safe_bounded_and_does_not_claim_delivery() {
    let start = Instant::now();
    let c = candidate(Verdict::Fire, 1, start);
    let payload = c.payload(start + Duration::from_millis(1000)).unwrap();
    let text = payload["content"].as_str().unwrap();
    assert!(text.contains("STAGING"));
    assert!(!text.contains("Not delivered"));
    assert!(text.contains("age 1.0 s"));
    assert!(text.len() < 2000);
    assert_eq!(payload["allowed_mentions"]["parse"], serde_json::json!([]));
    let mut bad = decision(Verdict::Fire, 1);
    bad.mint = "@everyone".into();
    assert!(Candidate::new(bad, "boot1", start, 1).is_none());
}

#[test]
fn journal_reserves_before_send_persists_ids_and_survives_restart_and_clock_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delivery.json");
    let config = DeliveryConfig::default();
    let mut journal = Journal::open(&path, &config).unwrap();
    assert!(journal.reserve("a", Tier::Fire, 100_000).unwrap());
    journal.finish("a", Some("123"), false).unwrap();
    drop(journal);
    let mut journal = Journal::open(&path, &config).unwrap();
    assert!(!journal.reserve("a", Tier::Fire, 100_001).unwrap());
    assert!(journal.reserve("b", Tier::Fire, 99_000).unwrap());
    assert!(!journal.reserve("c", Tier::Fire, 99_001).unwrap()); // burst two consumed
    assert_eq!(journal.receipt("a"), Some("123"));
    assert!(journal.reserve("c", Tier::Fire, 460_000).unwrap());
}

#[test]
fn budgets_slide_and_near_cannot_use_reserved_fire_capacity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delivery.json");
    let config = DeliveryConfig::default();
    let mut journal = Journal::open(&path, &config).unwrap();
    for i in 0..10 {
        let tier = if i < 3 { Tier::Near } else { Tier::Fire };
        assert!(
            journal
                .reserve(&i.to_string(), tier, 1_000_000 + i * 360_000)
                .unwrap()
        );
    }
    assert!(!journal.reserve("near", Tier::Near, 4_240_001).unwrap());
    assert!(!journal.reserve("over", Tier::Fire, 4_599_999).unwrap());
    assert!(journal.reserve("next", Tier::Fire, 4_600_000).unwrap());
}

#[test]
fn corrupt_journal_blocks_delivery_instead_of_resetting_usage() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delivery.json");
    std::fs::write(&path, b"{broken}").unwrap();
    assert!(Journal::open(&path, &DeliveryConfig::default()).is_err());
}

#[test]
fn rolling_day_cap_is_not_reset_at_midnight_or_by_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delivery.json");
    let config = DeliveryConfig {
        daily_cap: 3,
        ..DeliveryConfig::default()
    };
    let mut journal = Journal::open(&path, &config).unwrap();
    for i in 0..3 {
        assert!(
            journal
                .reserve(&i.to_string(), Tier::Fire, 100_000 + i * 360_000)
                .unwrap()
        );
    }
    drop(journal);
    let mut journal = Journal::open(&path, &config).unwrap();
    assert!(
        !journal
            .reserve("near-midnight", Tier::Fire, 86_499_999)
            .unwrap()
    );
    assert!(
        journal
            .reserve("oldest-expired", Tier::Fire, 86_500_000)
            .unwrap()
    );
}

#[test]
fn payload_format_is_shared_with_readonly_capture_inspection() {
    let start = Instant::now();
    let c = candidate(Verdict::Fire, 1, start);
    assert_eq!(c.payload(start), super::staging_payload(&c.decision, 0));
    assert!(super::staging_payload(&decision(Verdict::Near, 1), 15_000).is_none());
}
