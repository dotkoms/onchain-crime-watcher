use super::*;

#[tokio::test]
async fn cancelled_reservation_keeps_process_ownership_until_the_disk_worker_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("data");
    let (mut pipeline, _records) = crate::collector::pipeline_for_test(&path);
    let ledger_path = path.join("feed-spend.json");
    let day = wall_ms() / 86_400_000;
    Ledger::initialize(&ledger_path, day).unwrap();
    let ledger = Arc::new(Mutex::new(
        Ledger::open(&ledger_path, day, 1000, 100, 10).unwrap(),
    ));
    let held = ledger.clone();
    let (ready, started) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let blocker = std::thread::spawn(move || {
        let _guard = held.lock().unwrap();
        ready.send(()).unwrap();
        wait.recv().unwrap();
    });
    started.await.unwrap();
    let mut budget = Budget {
        ledger,
        credits: 0,
        credit_day: day,
    };
    assert!(
        tokio::time::timeout(Duration::from_millis(20), budget.refill(&mut pipeline))
            .await
            .is_err()
    );
    drop(budget);
    drop(pipeline);
    assert!(crate::storage::ProcessLock::acquire(&path).is_err());
    release.send(()).unwrap();
    blocker.join().unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if crate::storage::ProcessLock::acquire(&path).is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        Ledger::open(&ledger_path, day, 1000, 100, 10)
            .unwrap()
            .reserved(),
        10
    );
}
#[test]
fn credits_from_yesterday_cannot_cross_into_a_new_daily_allowance() {
    let mut credits = 3;
    let mut day = 10;
    super::advance_day(&mut credits, &mut day, 11);
    assert_eq!(credits, 0);
    assert_eq!(day, 11);
    credits = 2;
    super::advance_day(&mut credits, &mut day, 9);
    assert_eq!(credits, 2);
    assert_eq!(day, 11);
}

#[test]
fn healed_batches_remain_bounded_audit_records_and_never_become_live_trades() {
    let dir = tempfile::tempdir().unwrap();
    let (mut pipeline, mut records) = crate::collector::pipeline_for_test(&dir.path().join("data"));
    let trade = crate::feed::Trade {
        mint: [1; 32],
        trader: [2; 32],
        signature: [3; 64],
        side: crate::feed::Side::Buy,
        token_amount: u64::MAX,
        sol_lamports: u64::MAX,
        sol_source: crate::feed::SolSource::Instruction,
    };
    let report = crate::heal::Report {
        generation: 7,
        sample: crate::probe::Sample {
            mint: [1; 32],
            curve: [2; 32],
            signature: [9; 64],
            observed_ms: 0,
        },
        stop: crate::heal::Stop::Complete,
        calls: 1,
        signatures: 1,
        trades: (0..16)
            .map(|index| crate::heal::Recovered {
                event: crate::chain::ChainTrade {
                    trade: trade.clone(),
                    event_index: index,
                    slot: u64::MAX,
                    block_time_ms: u64::MAX,
                    transaction_fee: u64::MAX,
                    transaction_fee_payer: [2; 32],
                },
                transaction_index: usize::MAX,
            })
            .collect(),
    };
    super::record_heals(&mut pipeline, vec![report]);
    let batch = records.try_recv().unwrap();
    assert_eq!(batch.kind, "healed_chain_batch");
    assert!(batch.payload.len() < 32 * 1024);
    let batch: serde_json::Value = serde_json::from_slice(&batch.payload).unwrap();
    assert_eq!(batch["events"].as_array().unwrap().len(), 16);
    assert_eq!(batch["source_generation"], 7);
    assert_eq!(batch["applied_to_detectors"], false);
    assert_eq!(batch["live_overlap"], "unresolved");
    assert_eq!(records.try_recv().unwrap().kind, "curve_heal_result");
    assert_eq!(crate::health::value(&pipeline.health.trades), 0);
}
