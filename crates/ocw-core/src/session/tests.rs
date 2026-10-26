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
