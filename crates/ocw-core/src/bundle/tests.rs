use super::*;
fn bag() -> Bag {
    Bag::open(100, Some(100), 2_000_000_000).unwrap()
}
#[test]
fn sale_price_cannot_turn_twenty_percent_sold_into_a_fully_sold_bag() {
    let mut bags = Vec::new();
    for _ in 0..6 {
        let mut b = bag();
        b.sell(20, Some(80), 10_000_000_000);
        bags.push(b);
    }
    let report = progress(&bags, Some(20_000), true, true).unwrap();
    assert_eq!(report.sold_bps_lower, 2000);
    assert_eq!(report.tier, Tier::None);
    assert_eq!(report.proceeds_lamports, 60_000_000_000);
}
#[test]
fn later_buys_do_not_refill_original_units_and_current_holdings_withhold_cleared() {
    let mut b = Bag::open(100, Some(100), 2_000_000_000).unwrap();
    b.sell(50, Some(50), 1);
    b.buy(50, Some(100));
    b.sell(90, Some(10), 1);
    assert_eq!(b.original_remaining, Some(5));
    assert_eq!(b.inventory, Some(10));
    let report = progress(&vec![b; 6], Some(20_000), true, true).unwrap();
    assert_eq!(report.sold_bps_lower, 9500);
    assert_eq!(report.tier, Tier::Fire);
}
#[test]
fn balance_mismatch_and_missing_history_never_become_a_sale_or_cleared() {
    let mut b = bag();
    b.sell(30, Some(10), 1);
    assert_eq!(b.original_remaining, Some(100));
    assert!(!b.complete);
    let report = progress(&vec![b; 6], Some(20_000), true, true).unwrap();
    assert_eq!(report.tier, Tier::None);
    assert!(report.partial);
    assert!(
        progress(
            &vec![Bag::open(100, None, 2_000_000_000).unwrap(); 6],
            Some(20_000),
            true,
            true
        )
        .is_none()
    );
}
#[test]
fn cleared_requires_minimums_complete_history_price_context_and_current_holdings() {
    let mut b = bag();
    b.sell(95, Some(5), 1);
    assert_eq!(
        progress(&vec![b.clone(); 6], Some(20_000), true, true)
            .unwrap()
            .tier,
        Tier::Cleared
    );
    assert_eq!(
        progress(&vec![b.clone(); 6], Some(20_000), false, true)
            .unwrap()
            .tier,
        Tier::Fire
    );
    assert_eq!(
        progress(&vec![b.clone(); 4], Some(20_000), true, true)
            .unwrap()
            .tier,
        Tier::None
    );
    assert_eq!(
        progress(&vec![b.clone(); 6], Some(100_000), true, true)
            .unwrap()
            .tier,
        Tier::None
    );
    b.gap();
    assert_eq!(
        progress(&vec![b; 6], Some(20_000), true, true)
            .unwrap()
            .tier,
        Tier::Fire
    );
}
#[test]
fn original_allocation_rounds_down_and_final_sale_removes_dust() {
    let mut b = Bag::open(3, Some(3), 1).unwrap();
    b.buy(2, Some(5));
    b.sell(1, Some(4), 1);
    assert_eq!(b.original_remaining, Some(3));
    b.sell(4, Some(0), 1);
    assert_eq!(b.original_remaining, Some(0));
}

#[test]
fn exact_weighted_boundary_and_large_fraction_comparison_cannot_overflow_into_a_tier() {
    assert_eq!(reaches(&[(1, 2, 3), (1, 14, 15)], 2, 8000), Some(true));
    assert_eq!(reaches(&[(1, 2, 3), (1, 13, 15)], 2, 8000), Some(false));
    assert_eq!(
        compare_ratio(u128::MAX, u128::MAX - 1, 1, 1),
        std::cmp::Ordering::Greater
    );
    for a in 0..16 {
        for b in 1..16 {
            for c in 0..16 {
                for d in 1..16 {
                    assert_eq!(compare_ratio(a, b, c, d), (a * d).cmp(&(b * c)));
                }
            }
        }
    }
}
#[test]
fn membership_is_half_open_and_later_nonmember_buys_do_not_enter_the_launch_bag() {
    use crate::feed::{Side, SolSource, Trade};
    let mut launch = capture::Launch::new([1; 32], "boot", 1000, 2000).unwrap();
    let mut t = Trade {
        mint: [1; 32],
        trader: [2; 32],
        signature: [3; 64],
        side: Side::Buy,
        token_amount: 100,
        sol_lamports: 2_000_000_000,
        sol_source: SolSource::Payload,
    };
    launch.observe(1, 10_999, 11_999, &t, Some(100));
    t.trader = [4; 32];
    launch.observe(2, 11_000, 12_000, &t, Some(100));
    assert_eq!(launch.buyers.len(), 1);
    assert!(launch.closed);
    assert!(launch.validate());
    launch.observe(1, 11_001, 12_001, &t, Some(100));
    assert!(launch.partial);
}
#[test]
fn funders_require_timed_transfers_and_jito_never_admits_a_wallet() {
    use funding::*;
    use std::collections::BTreeSet;
    let transfer = Transfer {
        from: [8; 32],
        to: [1; 32],
        lamports: 10_000_000,
        wall_ms: 50_000,
        signature: bs58::encode([3; 64]).into_string(),
    };
    let rows = vec![
        Evidence {
            wallet: [1; 32],
            buy_wall_ms: 60_000,
            direct: vec![transfer],
            direct_complete: true,
            parent: vec![Transfer {
                from: [9; 32],
                to: [8; 32],
                lamports: 20_000_000,
                wall_ms: 40_000,
                signature: bs58::encode([4; 64]).into_string(),
            }],
            parent_complete: true,
            jito_tip_payer: Some([7; 32]),
        },
        Evidence {
            wallet: [2; 32],
            buy_wall_ms: 60_000,
            direct: vec![],
            direct_complete: true,
            parent: vec![],
            parent_complete: true,
            jito_tip_payer: Some([7; 32]),
        },
    ];
    let buyers = BTreeSet::from([[1; 32], [2; 32]]);
    let result = classify(&rows, &buyers, &BTreeSet::new()).unwrap();
    assert_eq!(result[0].root, Some([9; 32]));
    assert_eq!(result[0].hop, Some(1));
    assert!(result[1].jito_corroborated);
    assert_eq!(result[1].root, None);
    let excluded = classify(&rows, &buyers, &BTreeSet::from([[8; 32]])).unwrap();
    assert!(excluded[0].excluded);
    assert_eq!(excluded[0].root, None);
}
#[test]
fn fifty_one_observed_children_exclude_the_root_as_shared_infrastructure() {
    use funding::*;
    use std::collections::BTreeSet;
    let rows: Vec<_> = (1..=51)
        .map(|key| Evidence {
            wallet: [key; 32],
            buy_wall_ms: 60_000,
            direct: vec![Transfer {
                from: [99; 32],
                to: [key; 32],
                lamports: 10_000_000,
                wall_ms: 50_000,
                signature: bs58::encode([3; 64]).into_string(),
            }],
            direct_complete: true,
            parent: vec![],
            parent_complete: true,
            jito_tip_payer: None,
        })
        .collect();
    let buyers = rows.iter().map(|r| r.wallet).collect();
    let result = classify(&rows, &buyers, &BTreeSet::new()).unwrap();
    assert!(
        result
            .iter()
            .all(|r| r.root.is_none() && r.fanout_capped && r.partial)
    );
}

#[test]
fn impossible_supply_and_missing_balance_cannot_complete_the_history() {
    let mut b = bag();
    b.sell(80, None, 1);
    assert!(!b.complete);
    assert_eq!(b.original_remaining, Some(100));
    let report = progress(&vec![bag(); 6], Some(0), true, true).unwrap();
    assert!(report.partial);
    assert!(!report.minimums_met);
    let report = progress(&vec![bag(); 6], Some(100), true, true).unwrap();
    assert!(report.partial);
    assert!(!report.minimums_met);
}

#[test]
fn incomplete_funding_windows_do_not_admit_an_effective_root() {
    use funding::*;
    use std::collections::BTreeSet;
    let mut row = Evidence {
        wallet: [1; 32],
        buy_wall_ms: 60_000,
        direct: vec![Transfer {
            from: [8; 32],
            to: [1; 32],
            lamports: 10_000_000,
            wall_ms: 50_000,
            signature: bs58::encode([3; 64]).into_string(),
        }],
        direct_complete: false,
        parent: vec![],
        parent_complete: false,
        jito_tip_payer: None,
    };
    let buyers = BTreeSet::from([[1; 32]]);
    let report = classify(&[row.clone()], &buyers, &BTreeSet::new()).unwrap();
    assert!(report[0].partial);
    assert_eq!(report[0].root, None);
    row.direct_complete = true;
    let report = classify(&[row], &buyers, &BTreeSet::new()).unwrap();
    assert!(report[0].partial);
    assert_eq!(report[0].root, None);
}

#[test]
fn minimum_wallet_cost_and_supply_boundaries_are_inclusive_and_exact() {
    let mut b = bag();
    b.sell(80, Some(20), 1);
    let mut bags = vec![b; 5];
    assert_eq!(
        progress(&bags, Some(25_000), false, true).unwrap().tier,
        Tier::Fire
    );
    assert!(
        !progress(&bags, Some(25_001), false, true)
            .unwrap()
            .minimums_met
    );
    bags[0].original_cost_lamports -= 1;
    assert!(
        !progress(&bags, Some(25_000), false, true)
            .unwrap()
            .minimums_met
    );
}
