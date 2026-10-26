use super::*;
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::test]
async fn paid_stop_closes_first_socket_before_free_only_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = crate::config::Config {
        data_dir: dir.path().join("data"),
        ..crate::config::Config::default()
    };
    crate::storage::private_dir(&config.data_dir).unwrap();
    config.health_bind = "127.0.0.1:0".parse().unwrap();
    config.capture.min_free_bytes = 0;
    config.feed.mint = Some(bs58::encode([1u8; 32]).into_string());
    config.gate = crate::config::GateConfig {
        daily_messages: 8,
        stop_margin_messages: 2,
        reservation_messages: 2,
    };
    crate::spend::Ledger::initialize(
        &config.data_dir.join("feed-spend.json"),
        wall_ms() / 86_400_000,
    )
    .unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = url::Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
    let provider = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        let mut methods = Vec::new();
        for _ in 0..3 {
            let raw = socket.next().await.unwrap().unwrap().into_data();
            methods.push(
                serde_json::from_slice::<serde_json::Value>(&raw).unwrap()["method"]
                    .as_str()
                    .unwrap()
                    .to_string(),
            );
        }
        assert_eq!(
            methods,
            vec![
                "subscribeNewToken",
                "subscribeMigration",
                "subscribeTokenTrade"
            ]
        );
        socket
            .send(Message::Text(
                serde_json::json!({"txType":"create","mint":bs58::encode([1u8;32]).into_string()})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        for n in 1..=6 {
            let trade = serde_json::json!({"txType":"buy","mint":bs58::encode([1u8;32]).into_string(),"traderPublicKey":bs58::encode([2u8;32]).into_string(),"signature":bs58::encode([n;64]).into_string(),"tokenAmount":1,"solAmount":1});
            socket
                .send(Message::Text(trade.to_string().into()))
                .await
                .unwrap();
        }
        assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
        drop(socket);
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        for expected in ["subscribeNewToken", "subscribeMigration"] {
            let raw = socket.next().await.unwrap().unwrap().into_data();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&raw).unwrap()["method"],
                expected
            );
        }
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), socket.next())
                .await
                .is_err()
        );
        stop_tx.send(()).unwrap();
        // Keep the replacement transport alive until the collector handles shutdown.
        // Dropping it here races the shutdown signal against a transport failure.
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next()).await;
    });
    let secrets = Secrets {
        rpc_url: None,
        api_key: Some("unit-test-key-do-not-log".into()),
        webhook: None,
        staging_webhook: None,
    };
    let options = Options {
        paid: true,
        preview_volume: true,
        ..Options::default()
    };
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        run_inner(config, options, secrets, endpoint, async {
            let _ = stop_rx.await;
        }),
    )
    .await
    .unwrap()
    .unwrap();
    provider.await.unwrap();
    assert_eq!(result["paid"]["reserved_messages"], 6);
    assert_eq!(result["paid"]["billable_received"], 6);
    assert_eq!(result["paid"]["stops"], 1);
    assert_eq!(result["feed"]["trades"], 6);
    assert_eq!(result["feed"]["generation"], 2);
    assert_eq!(result["volume"]["preview_enabled"], true);
    assert_eq!(result["volume"]["active_mints"], 1);
    assert_eq!(result["volume"]["unknown_mint_trades"], 0);
    assert_eq!(result["volume"]["fire_candidates"], 0);
    assert!(!result.to_string().contains("unit-test-key"));
}

#[tokio::test]
async fn health_endpoint_reports_counts_without_credentials() {
    let health = std::sync::Arc::new(crate::health::Health::default());
    crate::health::inc(&health.frames);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, rx) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(crate::health::serve(listener, health, async {
        let _ = rx.await;
    }));
    let body: serde_json::Value = reqwest::get(format!("http://{addr}/health"))
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(body["feed"]["frames"], 1);
    assert_eq!(body["signals_enabled"], false);
    assert!(body.get("api_key").is_none());
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn capture_io_failure_is_not_a_successful_collector_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    crate::storage::private_dir(&data).unwrap();
    // A regular file at the capture directory is a real, reproducible I/O failure.
    std::fs::write(data.join("capture"), b"operator file").unwrap();
    let path = dir.path().join("empty.jsonl");
    std::fs::write(&path, b"").unwrap();
    let config = Config {
        data_dir: data.clone(),
        health_bind: "127.0.0.1:0".parse().unwrap(),
        ..Config::default()
    };
    let result = run_inner(
        config,
        Options {
            fixture: Some(path),
            ..Options::default()
        },
        Secrets::default(),
        Url::parse("ws://127.0.0.1:1").unwrap(),
        std::future::pending(),
    )
    .await;
    assert_eq!(
        result.unwrap_err(),
        "capture writer failed; shutdown incomplete"
    );
    assert_eq!(
        std::fs::read(data.join("capture")).unwrap(),
        b"operator file"
    );
    assert!(ProcessLock::acquire(&data).is_ok());
}

#[tokio::test]
async fn failed_capture_is_visible_while_live_collection_and_health_continue() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    crate::storage::private_dir(&data).unwrap();
    std::fs::write(data.join("capture"), b"operator file").unwrap();
    let reserved = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let health_addr = reserved.local_addr().unwrap();
    drop(reserved);
    let config = Config {
        data_dir: data,
        health_bind: health_addr,
        ..Config::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
    let (connected, ready) = tokio::sync::oneshot::channel();
    let provider = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_async(stream).await.unwrap();
        for _ in 0..2 {
            socket.next().await.unwrap().unwrap();
        }
        connected.send(()).unwrap();
        let _ = socket.next().await;
    });
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let collector = tokio::spawn(run_inner(
        config,
        Options::default(),
        Secrets::default(),
        endpoint,
        async {
            let _ = stopped.await;
        },
    ));
    tokio::time::timeout(Duration::from_secs(2), ready)
        .await
        .unwrap()
        .unwrap();
    let snapshot = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot: Value = reqwest::get(format!("http://{health_addr}/health"))
                .await
                .unwrap()
                .json()
                .await
                .unwrap();
            if snapshot["supervision"]["capture"] == "red" {
                break snapshot;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    stop.send(()).unwrap();
    assert!(collector.await.unwrap().is_err());
    provider.await.unwrap();
    let snapshot = snapshot.expect("capture failure was not observed while live");
    assert_eq!(snapshot["capture"]["state"], "error");
    assert_eq!(snapshot["feed"]["state"], "connected");
}
