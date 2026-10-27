use super::*;
use crate::spend::Ledger;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tokio_tungstenite::{WebSocketStream, accept_async, tungstenite::Message};
type Socket = WebSocketStream<TcpStream>;

fn config(dir: &std::path::Path) -> Config {
    let mut config = Config {
        data_dir: dir.join("data"),
        health_bind: "127.0.0.1:0".parse().unwrap(),
        ..Config::default()
    };
    config.capture.min_free_bytes = 0;
    config.recovery.reconnect_attempts = 2;
    config.recovery.initial_backoff_ms = 10;
    config.recovery.max_backoff_ms = 20;
    config
}
async fn methods(socket: &mut Socket, count: usize) -> Vec<String> {
    let mut methods = Vec::new();
    for _ in 0..count {
        let raw = socket.next().await.unwrap().unwrap().into_data();
        methods.push(
            serde_json::from_slice::<Value>(&raw).unwrap()["method"]
                .as_str()
                .unwrap()
                .to_string(),
        );
    }
    methods
}
async fn close(mut socket: Socket) {
    socket.close(None).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_secs(1), socket.next())
        .await
        .unwrap();
}
fn controls(dir: &std::path::Path, kind: &str) -> Vec<Value> {
    let mut records = Vec::new();
    for day in std::fs::read_dir(dir.join("data/capture")).unwrap() {
        for file in std::fs::read_dir(day.unwrap().path()).unwrap() {
            let path = file.unwrap().path();
            if path.extension().is_some_and(|e| e == "ocwcap") {
                crate::capture::scan(&path, |record| {
                    if record.kind == kind {
                        records.push(serde_json::from_slice(&record.payload).unwrap());
                    }
                    Ok(())
                })
                .unwrap();
            }
        }
    }
    records
}

#[tokio::test]
async fn paid_disconnect_recovers_free_only_keeps_dedup_and_clears_reserve_history() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.recovery.reconnect_attempts = 1;
    config.feed.mint = Some(bs58::encode([1u8; 32]).into_string());
    config.gate.reservation_messages = 10;
    crate::storage::private_dir(&config.data_dir).unwrap();
    Ledger::initialize(
        &config.data_dir.join("feed-spend.json"),
        wall_ms() / 86_400_000,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let provider = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        assert_eq!(
            methods(&mut socket, 3).await,
            vec![
                "subscribeNewToken",
                "subscribeMigration",
                "subscribeTokenTrade"
            ]
        );
        let mint = bs58::encode([1u8; 32]).into_string();
        let trader = bs58::encode([2u8; 32]).into_string();
        let create = json!({"txType":"create","mint":mint,"vSolInBondingCurve":30});
        let trade = json!({"txType":"buy","mint":mint,"traderPublicKey":trader,"signature":bs58::encode([1u8;64]).into_string(),"tokenAmount":1,"solAmount":1,"vSolInBondingCurve":31});
        socket
            .send(Message::Text(create.to_string().into()))
            .await
            .unwrap();
        socket
            .send(Message::Text(trade.to_string().into()))
            .await
            .unwrap();
        close(socket).await;
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        assert_eq!(
            methods(&mut socket, 2).await,
            vec!["subscribeNewToken", "subscribeMigration"]
        );
        socket
            .send(Message::Text(trade.to_string().into()))
            .await
            .unwrap();
        socket.send(Message::Text(json!({"txType":"buy","mint":mint,"traderPublicKey":trader,"signature":bs58::encode([2u8;64]).into_string(),"tokenAmount":1,"vSolInBondingCurve":32}).to_string().into())).await.unwrap();
        // Await observable processing before asking the collector to stop.
        tokio::time::sleep(Duration::from_millis(100)).await;
        stop.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), socket.next()).await;
    });
    let secrets = Secrets {
        api_key: Some("unit-test-key-do-not-log".into()),
        ..Secrets::default()
    };
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_inner(
            config,
            Options {
                paid: true,
                preview_volume: true,
                ..Options::default()
            },
            secrets,
            endpoint,
            async {
                let _ = stopped.await;
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["feed"]["generation"], 2);
    assert_eq!(result["feed"]["trades"], 1);
    assert_eq!(result["feed"]["duplicates"], 1);
    assert_eq!(result["feed"]["unresolved"], 1);
    assert_eq!(result["paid"]["state"], "interrupted");
    assert_eq!(result["paid"]["reserved_messages"], 10);
    assert_eq!(result["paid"]["billable_received"], 1);
    assert_eq!(result["recovery"]["reconnects"], 1);
    assert!(!result.to_string().contains("unit-test-key"));
}

#[tokio::test]
async fn repeated_transport_closes_exhaust_a_fixed_retry_budget() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
    let provider = tokio::spawn(async move {
        for _ in 0..3 {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            assert_eq!(
                methods(&mut socket, 2).await,
                vec!["subscribeNewToken", "subscribeMigration"]
            );
            close(socket).await;
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err()
        );
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_inner(
            config,
            Options::default(),
            Secrets::default(),
            endpoint,
            std::future::pending(),
        ),
    )
    .await
    .unwrap();
    assert!(result.unwrap_err().contains("redacted"));
    tokio::time::timeout(Duration::from_secs(5), provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        controls(dir.path(), "recovery_stopped")[0]["reason"],
        "budget_exhausted"
    );
}

#[tokio::test]
async fn authentication_and_rate_limit_handshake_failures_never_restart() {
    for status in [401, 429] {
        let dir = tempfile::tempdir().unwrap();
        let config = config(dir.path());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
        let provider = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = [0u8; 4096];
            let _ = stream.read(&mut bytes).await.unwrap();
            stream.write_all(format!("HTTP/1.1 {status} Rejected\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
            drop(stream);
            assert!(
                tokio::time::timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err()
            );
        });
        assert!(
            run_inner(
                config,
                Options::default(),
                Secrets::default(),
                endpoint,
                std::future::pending()
            )
            .await
            .is_err()
        );
        tokio::time::timeout(Duration::from_secs(5), provider)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            controls(dir.path(), "recovery_stopped")[0]["reason"],
            if status == 401 {
                "authentication"
            } else {
                "provider_rate_limit"
            }
        );
    }
}

#[tokio::test]
async fn data_without_matching_pongs_times_out_but_a_responsive_quiet_socket_stays_open() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.recovery.reconnect_attempts = 1;
    config.recovery.ping_interval_ms = 100;
    config.recovery.pong_timeout_ms = 300;
    config.recovery.transport_idle_ms = 1000;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
    let provider = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        methods(&mut socket, 2).await;
        loop {
            // Deliberately never read the client's ping on this connection.
            if socket
                .send(Message::Text("{\"message\":\"data heartbeat\"}".into()))
                .await
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        drop(socket);
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        methods(&mut socket, 2).await;
        while let Some(Ok(message)) = socket.next().await {
            match message {
                Message::Ping(_) => {
                    if socket.flush().await.is_err() {
                        break;
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
    });
    let result = run_inner(
        config,
        Options {
            run_for: Some(Duration::from_millis(1200)),
            ..Options::default()
        },
        Secrets::default(),
        endpoint,
        std::future::pending(),
    )
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["recovery"]["reconnects"], 1);
    assert_eq!(result["transport"]["timeouts"], 1);
    assert!(result["transport"]["matching_pongs"].as_u64().unwrap() > 0);
    assert_eq!(result["application"]["suspicions"], 0);
}

#[tokio::test]
async fn a_run_limit_cancels_backoff_without_waiting_for_the_next_retry() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.recovery.initial_backoff_ms = 30_000;
    config.recovery.max_backoff_ms = 30_000;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
    let provider = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        methods(&mut socket, 2).await;
        close(socket).await;
    });
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        run_inner(
            config,
            Options {
                run_for: Some(Duration::from_millis(300)),
                ..Options::default()
            },
            Secrets::default(),
            endpoint,
            std::future::pending(),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), provider)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["recovery"]["reconnects"], 0);
    assert_eq!(result["feed"]["state"], "stopped");
    assert_eq!(controls(dir.path(), "reconnect_scheduled").len(), 1);
}

#[test]
fn suspected_application_liveness_suppresses_a_fresh_signal_without_fabricating_a_gap() {
    let dir = tempfile::tempdir().unwrap();
    let (mut pipeline, mut records) = pipeline_for_test(&dir.path().join("data"));
    let health = pipeline.health.clone();
    set_state(&health.feed_state, "connected");
    set_state(&health.application_state, "suspected_unprobed");
    let decision = Decision {
        number: 1,
        mint: bs58::encode([1u8; 32]).into_string(),
        verdict: crate::volume::Verdict::Fire,
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
    };
    pipeline.emit_volume(vec![decision.clone()], 0);
    assert_eq!(health.snapshot()["application"]["suppressed_candidates"], 1);
    assert!(records.try_recv().is_err());
    set_state(&health.application_state, "observed");
    pipeline.emit_volume(vec![decision], 0);
    assert_eq!(records.try_recv().unwrap().kind, "volume_preview");
    assert!(records.try_recv().is_err());
}

#[tokio::test]
async fn automatic_watch_subscribes_on_the_same_socket_and_excludes_inactive_trades() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = config(dir.path());
    config.feed.auto_watch = true;
    config.gate.reservation_messages = 10;
    crate::storage::private_dir(&config.data_dir).unwrap();
    Ledger::initialize(
        &config.data_dir.join("feed-spend.json"),
        wall_ms() / 86_400_000,
    )
    .unwrap();
    config.bundle.enabled = true;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let provider = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        assert_eq!(
            methods(&mut socket, 2).await,
            vec!["subscribeNewToken", "subscribeMigration"]
        );
        let mint = bs58::encode([1u8; 32]).into_string();
        socket
            .send(Message::Text(
                json!({"txType":"create","mint":mint,"vSolInBondingCurve":30})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let command = socket.next().await.unwrap().unwrap().into_data();
        let command: Value = serde_json::from_slice(&command).unwrap();
        assert_eq!(command["method"], "subscribeTokenTrade");
        assert_eq!(command["keys"][0], mint);
        for (key, sig) in [(9u8, 1u8), (1, 2)] {
            socket.send(Message::Text(json!({"txType":"buy","mint":bs58::encode([key;32]).into_string(),"traderPublicKey":bs58::encode([2u8;32]).into_string(),"signature":bs58::encode([sig;64]).into_string(),"tokenAmount":1,"solAmount":1,"newTokenBalance":1,"vSolInBondingCurve":31}).to_string().into())).await.unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        stop.send(()).unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), socket.next()).await;
    });
    let result = tokio::time::timeout(
        Duration::from_secs(5),
        run_inner(
            config,
            Options {
                paid: true,
                preview_volume: true,
                ..Options::default()
            },
            Secrets {
                api_key: Some("unit-test-key-do-not-log".into()),
                ..Secrets::default()
            },
            endpoint,
            async {
                let _ = stopped.await;
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    provider.await.unwrap();
    assert_eq!(result["feed"]["trades"], 1);
    assert_eq!(result["paid"]["billable_received"], 2);
    assert_eq!(result["paid"]["reserved_messages"], 10);
    let launch = crate::bundle::store::read(&dir.path().join("data/launch"), [1; 32])
        .unwrap()
        .unwrap();
    assert_eq!(launch.buyers.len(), 1);
    assert!(launch.partial);
    assert_eq!(
        serde_json::to_value(&launch.buyers.values().next().unwrap().bag).unwrap()["original"],
        1_000_000
    );
    assert!(
        crate::bundle::store::read(&dir.path().join("data/launch"), [9; 32])
            .unwrap()
            .is_none()
    );
    let discarded = controls(dir.path(), "discarded_frame");
    assert_eq!(discarded.len(), 1);
    assert_eq!(discarded[0]["reason"], "inactive_subscription");
    let paths: Vec<_> = std::fs::read_dir(dir.path().join("data/capture"))
        .unwrap()
        .flat_map(|day| std::fs::read_dir(day.unwrap().path()).unwrap())
        .map(|file| file.unwrap().path())
        .filter(|path| path.extension().is_some_and(|e| e == "ocwcap"))
        .collect();
    assert_eq!(paths.len(), 1);
    let replay = crate::replay::analysis::run(&paths[0], |_, _| Ok(())).unwrap();
    assert_eq!(replay["watch_controller_replayed"], true);
    assert_eq!(replay["accepted_trades"], 1);
    assert_eq!(replay["financial_admission_replayed"], true);
    assert_eq!(replay["spend_reserved_messages"], 10);
    assert_eq!(replay["spend_local_credits"], 8);
    assert!(replay["watch_plans"].as_u64().unwrap() >= 3);

    assert_eq!(controls(dir.path(), "watch_admitted").len(), 1);
    let mut replays = Vec::new();
    for day in std::fs::read_dir(dir.path().join("data/capture")).unwrap() {
        for file in std::fs::read_dir(day.unwrap().path()).unwrap() {
            let path = file.unwrap().path();
            if path.extension().is_some_and(|ext| ext == "ocwcap") {
                replays.push(crate::replay::analysis::run(&path, |_, _| Ok(())).unwrap());
            }
        }
    }
    assert_eq!(replays.len(), 1);
    assert_eq!(replays[0]["accepted_trades"], 1);
    assert_eq!(replays[0]["active_mints"], 1);
    assert_eq!(
        replays[0]["volume"]["evaluated"],
        result["volume"]["evaluated_buckets"]
    );
}
