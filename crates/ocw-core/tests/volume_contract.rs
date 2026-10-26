use ocw_core::{
    feed::{Event, Side, SolSource, Trade},
    volume::{Engine, Verdict},
};

const SOL: u64 = 1_000_000_000;
fn trade(mint: u8, wallet: u8, lamports: u64) -> Event {
    Event::Trade(Trade {
        mint: [mint; 32],
        trader: [wallet; 32],
        signature: [wallet; 64],
        side: Side::Buy,
        token_amount: 1,
        sol_lamports: lamports,
        sol_source: SolSource::Payload,
    })
}
fn bucket(engine: &mut Engine, index: u64, total: u64) {
    for wallet in 0..6 {
        engine
            .on_event(
                trade(1, wallet, total / 6),
                index * 15_000 + wallet as u64 + 1,
            )
            .unwrap();
    }
}
fn warm(engine: &mut Engine, total: u64) {
    engine.on_event(Event::Create([1; 32]), 0).unwrap();
    for index in 0..17 {
        bucket(engine, index, total);
    }
}

#[test]
fn current_spike_uses_only_completed_history_and_can_fire() {
    let mut engine = Engine::new(4);
    warm(&mut engine, 6 * SOL);
    bucket(&mut engine, 17, 24 * SOL);
    let decisions = engine.advance(270_000).unwrap();
    let decision = decisions.last().unwrap();
    assert_eq!(decision.verdict, Verdict::Fire);
    assert_eq!(decision.baseline_lamports, 6.0 * SOL as f64);
    assert_eq!(decision.completed_buckets, 17);
    assert_eq!(decision.volume_lamports, 24 * SOL);
    assert_eq!(decision.cohort_p90, Some(1.0));
    assert!((decision.ratio.unwrap() - 3.4).abs() < 1e-9);
    assert!(decision.card(299_999).is_some());
    assert!(decision.card(300_000).is_none());
}

#[test]
fn warmup_participation_and_near_expiry_are_enforced() {
    let mut engine = Engine::new(4);
    engine.on_event(Event::Create([1; 32]), 0).unwrap();
    bucket(&mut engine, 0, 60 * SOL);
    assert_eq!(engine.advance(15_000).unwrap()[0].verdict, Verdict::Warmup);
    let mut engine = Engine::new(4);
    warm(&mut engine, 6 * SOL);
    bucket(&mut engine, 17, 15 * SOL);
    let d = engine.advance(270_000).unwrap().pop().unwrap();
    assert_eq!(d.verdict, Verdict::Near);
    assert!(d.card(284_999).is_some());
    assert!(d.card(285_000).is_none());
    for i in 0..6 {
        engine.on_event(trade(1, 0, 10 * SOL), 270_001 + i).unwrap();
    }
    assert_eq!(
        engine.advance(285_000).unwrap()[0].verdict,
        Verdict::Participation
    );
}

#[test]
fn migration_gap_unresolved_and_clock_rollback_cannot_manufacture_a_signal() {
    let mut engine = Engine::new(4);
    warm(&mut engine, 6 * SOL);
    engine.on_event(Event::Unresolved, 255_010).unwrap();
    assert_eq!(
        engine.advance(270_000).unwrap()[0].verdict,
        Verdict::InvalidBucket
    );
    engine.on_event(Event::Migration([1; 32]), 270_000).unwrap();
    for wallet in 0..6 {
        engine
            .on_event(trade(1, wallet, 10 * SOL), 270_001 + wallet as u64)
            .unwrap();
    }
    let d = engine.advance(285_000).unwrap().pop().unwrap();
    assert_eq!(d.verdict, Verdict::Warmup);
    assert_eq!(d.regime, "pool");
    engine.gap(285_000).unwrap();
    assert!(engine.advance(284_999).is_err());
    assert_eq!(engine.advance(300_000).unwrap()[0].completed_buckets, 0);
}

#[test]
fn unknown_mints_and_capacity_are_visible_and_long_silence_is_bounded() {
    let mut engine = Engine::new(1);
    engine.on_event(Event::Create([1; 32]), 0).unwrap();
    engine.on_event(Event::Create([2; 32]), 1).unwrap();
    engine.on_event(trade(2, 1, SOL), 2).unwrap();
    assert_eq!(engine.stats().registry_full, 1);
    assert_eq!(engine.stats().unknown_trades, 1);
    assert_eq!(engine.active_mints(), 1);
    let decisions = engine.advance(86_400_000).unwrap();
    assert!(decisions.len() <= 31);
    assert!(decisions.iter().all(|d| d.card(86_400_000).is_none()));
}

#[test]
fn cold_baseline_is_an_absolute_labelled_path() {
    let mut engine = Engine::new(1);
    warm(&mut engine, 600_000_000);
    bucket(&mut engine, 17, 15 * SOL);
    let d = engine.advance(270_000).unwrap().pop().unwrap();
    assert_eq!(d.verdict, Verdict::Fire);
    assert_eq!(d.path, "cold_baseline");
    assert!(d.ratio.is_none());
    let card = d.card(270_000).unwrap();
    assert_eq!(card["allowed_mentions"]["parse"], serde_json::json!([]));
    assert!(card["content"].as_str().unwrap().contains("Not delivered"));
}

#[test]
fn zero_buckets_count_and_sparse_history_cannot_pass_warmup() {
    let mut engine = Engine::new(1);
    engine.on_event(Event::Create([1; 32]), 0).unwrap();
    bucket(&mut engine, 0, 6 * SOL);
    // Continuous observation with no trades is zero volume, not missing coverage.
    engine.advance(255_000).unwrap();
    bucket(&mut engine, 17, 60 * SOL);
    let d = engine.advance(270_000).unwrap().pop().unwrap();
    assert_eq!(d.completed_buckets, 17);
    assert_eq!(d.verdict, Verdict::InsufficientHistory);
}

#[test]
fn integer_overflow_and_bucket_saturation_are_invalid_not_partial_signals() {
    let mut engine = Engine::new(1);
    warm(&mut engine, 6 * SOL);
    engine.on_event(trade(1, 0, u64::MAX), 255_001).unwrap();
    engine.on_event(trade(1, 1, 1), 255_002).unwrap();
    assert_eq!(
        engine.advance(270_000).unwrap()[0].verdict,
        Verdict::InvalidBucket
    );
    for i in 0..1025 {
        engine
            .on_event(trade(1, (i % 6) as u8, SOL), 270_001 + i)
            .unwrap();
    }
    assert_eq!(
        engine.advance(285_000).unwrap()[0].verdict,
        Verdict::InvalidBucket
    );
}

#[test]
fn cohort_floor_and_statistical_dispersion_use_preceding_even_sample_medians() {
    let mut engine = Engine::new(1);
    engine.on_event(Event::Create([1; 32]), 0).unwrap();
    for index in 0..18 {
        bucket(
            &mut engine,
            index,
            if index % 2 == 0 { 6 * SOL } else { 60 * SOL },
        );
    }
    // Median of [6,60] is 33 SOL, MAD 27; fire dispersion threshold is 193.1208 SOL.
    bucket(&mut engine, 18, 180 * SOL);
    let d = engine.advance(285_000).unwrap().pop().unwrap();
    assert!((d.dispersion_threshold_lamports / SOL as f64 - 193.1208).abs() < 1e-6);
    // Median historical trade size is (1+10)/2=5.5 SOL, hence floor=2.75 SOL.
    assert_eq!(d.floor_lamports, 2.75 * SOL as f64);
    assert_eq!(d.verdict, Verdict::Reject);
}

#[test]
fn multiple_mints_close_in_time_order_without_future_cohort_contamination() {
    let mut engine = Engine::new(2);
    engine.on_event(Event::Create([1; 32]), 0).unwrap();
    engine.on_event(Event::Create([2; 32]), 1_000).unwrap();
    let decisions = engine.advance(61_000).unwrap();
    let times: Vec<_> = decisions.iter().map(|d| d.bucket_end_ms).collect();
    assert_eq!(
        times,
        vec![
            15_000, 16_000, 30_000, 31_000, 45_000, 46_000, 60_000, 61_000
        ]
    );
}
