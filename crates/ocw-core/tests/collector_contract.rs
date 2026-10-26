use ocw_core::{
    config::Config,
    feed::{Decoder, Event, SolSource, units},
    spend::Ledger,
};

fn trade(signature: u8, amount: &str, sol: Option<&str>, reserve: &str) -> Vec<u8> {
    let sol = sol
        .map(|x| format!(",\"solAmount\":{x}"))
        .unwrap_or_default();
    format!("{{\"txType\":\"buy\",\"mint\":\"{}\",\"traderPublicKey\":\"{}\",\"signature\":\"{}\",\"tokenAmount\":{amount},\"vSolInBondingCurve\":{reserve}{sol}}}",
        bs58::encode([1u8;32]).into_string(),bs58::encode([2u8;32]).into_string(),bs58::encode([signature;64]).into_string()).into_bytes()
}

#[test]
fn config_rejects_unknown_fields_public_health_and_invalid_budget() {
    assert!(Config::parse("unknown = 1").is_err());
    assert!(Config::parse("health_bind = '0.0.0.0:9898'").is_err());
    assert!(Config::parse("[gate]\ndaily_messages=5\nstop_margin_messages=5").is_err());
    assert!(Config::parse("").unwrap().feed.mint.is_none());
}

#[test]
fn decimal_normalization_is_exact_and_rejects_fractional_raw_units() {
    assert_eq!(units("1.000000001", 9), Some(1_000_000_001));
    assert_eq!(units("1.23e-3", 9), Some(1_230_000));
    assert_eq!(units("1234.567", 6), Some(1_234_567_000));
    for invalid in ["-1", "NaN", "1e100", "0.0000000001"] {
        assert_eq!(units(invalid, 9), None);
    }
}

#[test]
fn duplicate_frames_do_not_move_reserves_and_unresolved_is_not_a_fill() {
    let mut d = Decoder::new(4, 180_000);
    let first = trade(3, "1", Some("2"), "10");
    assert!(matches!(d.decode(&first,0).unwrap(),Event::Trade(t) if t.sol_lamports==2_000_000_000));
    assert_eq!(d.decode(&first, 1).unwrap(), Event::Duplicate);
    assert!(
        matches!(d.decode(&trade(4,"2",None,"13"),2).unwrap(),Event::Trade(t) if t.sol_lamports==3_000_000_000 && t.sol_source==SolSource::ReserveDelta)
    );
    d.reset_session();
    assert_eq!(
        d.decode(&trade(5, "1", None, "14"), 3).unwrap(),
        Event::Unresolved
    );
}

#[test]
fn different_fills_in_one_transaction_are_preserved_and_state_is_bounded() {
    let mut d = Decoder::new(2, 180_000);
    for (n, amount) in [(1, "1"), (1, "2"), (2, "3"), (3, "4")] {
        assert!(matches!(
            d.decode(&trade(n, amount, Some("1"), "10"), n as u64)
                .unwrap(),
            Event::Trade(_)
        ));
    }
    assert!(d.dedup_len() <= 2);
    assert!(
        d.decode(b"{\"txType\":\"buy\",\"mint\":\"not a key\"}", 1)
            .is_err()
    );
}

#[test]
fn spend_reservations_survive_restart_and_clock_rollback() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spend.json");
    assert!(Ledger::open(&path, 10, 20, 2, 3).is_err());
    Ledger::initialize(&path, 10).unwrap();
    let mut ledger = Ledger::open(&path, 10, 20, 2, 3).unwrap();
    assert_eq!(ledger.reserve(10).unwrap(), 3);
    drop(ledger);
    let mut ledger = Ledger::open(&path, 9, 20, 2, 3).unwrap();
    assert_eq!(ledger.reserved(), 3);
    for _ in 0..5 {
        assert_eq!(ledger.reserve(9).unwrap(), 3);
    }
    assert_eq!(ledger.reserve(9).unwrap(), 0);
    assert_eq!(ledger.reserved(), 18);
    assert_eq!(ledger.reserve(11).unwrap(), 3);
    assert_eq!(ledger.reserved(), 3);
    assert!(Ledger::initialize(&path, 11).is_err());
}

#[test]
fn corrupt_ledger_fails_closed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spend.json");
    std::fs::write(&path, b"{\"reserved\":0}").unwrap();
    assert!(Ledger::open(&path, 10, 20, 2, 3).is_err());
}
