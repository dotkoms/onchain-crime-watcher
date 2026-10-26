use super::*;
fn rows(key: u8, date: &str) -> Value {
    json!([{"chainId":"solana","tokenAddress":bs58::encode([key;32]).into_string(),"claimDate":date}])
}
#[test]
fn date_parser_handles_offsets_leap_days_and_rejects_invalid_calendar_dates() {
    assert_eq!(timestamp("1970-01-01T00:00:00Z"), Some(0));
    assert_eq!(
        timestamp("2024-02-29T01:02:03.123Z"),
        timestamp("2024-02-29T03:02:03.123+02:00")
    );
    for s in [
        "2023-02-29T00:00:00Z",
        "2024-13-01T00:00:00Z",
        "2024-01-01T24:00:00Z",
        "2024-01-01T00:00:00",
        "private-untrusted-value",
    ] {
        assert!(timestamp(s).is_none());
    }
}
#[test]
fn priming_ties_restart_and_old_claims_never_repost_or_invent_dates() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.json");
    let now = timestamp("2026-10-26T12:00:00Z").unwrap();
    let mut store = Store::open(path.clone()).unwrap();
    assert!(
        store
            .observe(&rows(1, "2026-10-26T11:00:00Z"), now, true)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .observe(&rows(2, "2026-10-26T11:00:00Z"), now + 1, false)
            .unwrap()
            .len(),
        1
    );
    assert!(
        store
            .observe(&rows(2, "2026-10-26T11:00:00Z"), now + 2, false)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .observe(&rows(3, "2026-10-26T10:00:00Z"), now + 3, false)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .observe(
                &json!([{"chainId":"solana","tokenAddress":bs58::encode([4;32]).into_string()}]),
                now + 4,
                false
            )
            .unwrap()
            .is_empty()
    );
    drop(store);
    let mut store = Store::open(path).unwrap();
    assert!(
        store
            .observe(&rows(5, "2026-10-26T11:30:00Z"), now + 5, true)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .observe(&rows(5, "2026-10-26T11:30:00Z"), now + 6, false)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .observe(&rows(6, "2026-10-26T11:40:00Z"), now, false)
            .is_err()
    );
}
#[test]
fn corrupted_state_fails_closed_and_claim_wording_never_certifies_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.json");
    std::fs::write(&path, b"{\"schema\":1}").unwrap();
    assert!(Store::open(path).is_err());
    let claim = Claim {
        mint: [1; 32],
        date_ms: 1000,
    };
    let card = claim.card();
    assert!(
        card["content"]
            .as_str()
            .unwrap()
            .contains("does not verify ownership")
    );
    assert!(
        card["content"]
            .as_str()
            .unwrap()
            .contains("Bundle: unavailable")
    );
    assert_eq!(card["allowed_mentions"]["parse"], json!([]));
}

#[test]
fn send_intents_are_durable_and_the_rolling_budget_cannot_reset_on_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state.json");
    let now = timestamp("2026-10-26T12:00:00Z").unwrap();
    let mut store = Store::open(path.clone()).unwrap();
    store.observe(&json!([]), now, true).unwrap();
    let mut claims = Vec::new();
    for key in 1..=3 {
        claims.extend(
            store
                .observe(&rows(key, "2026-10-26T11:00:00Z"), now, false)
                .unwrap(),
        );
    }
    let config = Config {
        daily_cap: 2,
        ..Config::default()
    };
    assert!(store.reserve(&claims[0], now, &config).unwrap());
    store.finish(&claims[0], "unknown", None, 0).unwrap();
    drop(store);
    let mut store = Store::open(path).unwrap();
    assert!(!store.reserve(&claims[0], now, &config).unwrap());
    assert!(store.reserve(&claims[1], now, &config).unwrap());
    assert!(!store.reserve(&claims[2], now + 3_600_000, &config).unwrap());
    assert_eq!(
        store.state.claims[&bs58::encode(claims[0].mint).into_string()].outcome,
        "unknown"
    );
}
