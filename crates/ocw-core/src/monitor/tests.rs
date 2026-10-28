use super::*;
use crate::health::{set, set_state};
use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, State},
    http::{Method, Response},
    routing::any,
};
use serde_json::Value;
use std::{collections::VecDeque, sync::Mutex};
type Seen = Arc<Mutex<Vec<(Method, String)>>>;
type TestState = (Replies, Seen);
type Replies = Arc<Mutex<VecDeque<(u16, Value)>>>;
#[tokio::test]
async fn readiness_requires_both_progress_clocks_and_never_claims_production_readiness() {
    let health = Health::default();
    set_state(&health.feed_state, "connected");
    set_state(&health.application_state, "observed");
    set(&health.root_tick_ms, 100);
    set(&health.pipeline_tick_ms, 100);
    set(&health.monitor_stall_ms, 5000);
    assert_eq!(health.operational_at(Some(5099))["operational"], true);
    assert_eq!(health.operational_at(Some(5100))["operational"], false);
    assert_eq!(health.operational_at(Some(100))["production_ready"], false);
    set(&health.root_tick_ms, 5100);
    assert!(
        health.operational_at(Some(5100))["reasons"]
            .as_array()
            .unwrap()
            .contains(&json!("pipeline_stalled"))
    );
    set_state(&health.application_state, "suspected_unprobed");
    assert_eq!(health.operational_at(Some(100))["operational"], false);
}
#[test]
fn deadman_validation_redacts_keys_and_requires_the_supported_https_endpoint() {
    let good = "https://hc-ping.com/00000000-0000-0000-0000-000000000001";
    assert!(deadman_url(good).is_ok());
    for bad in [
        "http://hc-ping.com/private-key",
        "https://hc-ping.com.evil.test/private-key",
        "https://hc-ping.com/private-key",
        "https://user:private-key@hc-ping.com/x",
    ] {
        let error = deadman_url(bad).unwrap_err();
        assert!(!error.contains("private-key"));
    }
}
async fn server(replies: Vec<(u16, Value)>) -> (Url, Seen, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = Url::parse(&format!(
        "http://{}/api/webhooks/1/token",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let state = (Arc::new(Mutex::new(VecDeque::from(replies))), seen.clone());
    let app = Router::new()
        .fallback(any(
            |State((replies, seen)): State<TestState>,
             method: Method,
             OriginalUri(uri): OriginalUri| async move {
                seen.lock().unwrap().push((method, uri.to_string()));
                let (status, data) = replies
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or((500, Value::Null));
                Response::builder()
                    .status(status)
                    .header("content-type", "application/json")
                    .body(Body::from(data.to_string()))
                    .unwrap()
            },
        ))
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (url, seen, task)
}
fn verify() -> (u16, Value) {
    (200, json!({"id":"1","channel_id":"22","type":1}))
}
fn sent() -> (u16, Value) {
    (200, json!({"id":"42","channel_id":"22"}))
}
#[tokio::test]
async fn status_card_posts_once_and_edits_its_durable_identity_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("status.json");
    let data = dir.path().join("data");
    let guard = Arc::new(ProcessLock::acquire(&data).unwrap());
    let history = crate::history::Runtime::start(
        data.join("history"),
        "statusboot".into(),
        16 * 1024 * 1024,
        Arc::new(Health::default()),
        guard,
    )
    .await
    .unwrap();
    let evidence = crate::notifications::Evidence {
        boot: "statusboot".into(),
        history: Some(history.handle.clone()),
    };
    let (url, seen, task) = server(vec![verify(), sent(), verify(), sent()]).await;
    let client = discord::client(Duration::from_secs(1)).unwrap();
    let mut card = card::Card::open(
        &path,
        url.clone(),
        "22".into(),
        client.clone(),
        Arc::new(Pause::default()),
    )
    .await
    .unwrap();
    card.trace(&evidence, Duration::from_secs(1));
    card.update("real process status").await.unwrap();
    drop(card);
    let mut card = card::Card::open(&path, url, "22".into(), client, Arc::new(Pause::default()))
        .await
        .unwrap();
    card.trace(&evidence, Duration::from_secs(1));
    card.update("real process status after restart")
        .await
        .unwrap();
    drop(card);
    history.shutdown().await.unwrap();
    let receipts = crate::notifications::run(&data.join("history/observations.ocwh")).unwrap();
    assert_eq!(receipts["requests"], 2);
    assert_eq!(receipts["lanes"], json!(["status"]));
    let methods = seen.lock().unwrap().clone();
    task.abort();
    assert_eq!(methods.iter().filter(|(m, _)| m == Method::POST).count(), 1);
    assert_eq!(
        methods
            .iter()
            .filter(|(m, p)| m == Method::PATCH && p.ends_with("/messages/42"))
            .count(),
        1
    );
    assert!(!std::fs::read_to_string(path).unwrap().contains("token"));
}
#[tokio::test]
async fn ambiguous_initial_status_post_never_creates_a_second_card() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("status.json");
    let (url, seen, task) = server(vec![verify(), (500, Value::Null), verify()]).await;
    let client = discord::client(Duration::from_secs(1)).unwrap();
    let mut card = card::Card::open(
        &path,
        url.clone(),
        "22".into(),
        client.clone(),
        Arc::new(Pause::default()),
    )
    .await
    .unwrap();
    assert!(card.update("status").await.is_err());
    assert!(card.update("status").await.is_err());
    drop(card);
    let mut card = card::Card::open(&path, url, "22".into(), client, Arc::new(Pause::default()))
        .await
        .unwrap();
    assert!(card.update("status after restart").await.is_err());
    task.abort();
    assert_eq!(
        seen.lock()
            .unwrap()
            .iter()
            .filter(|(m, _)| m == Method::POST)
            .count(),
        1
    );
}
#[tokio::test]
async fn deadman_uses_fail_for_unhealthy_state_without_retrying_failed_requests() {
    let (url, seen, task) = server(vec![
        (200, json!("OK")),
        (200, json!("OK")),
        (500, Value::Null),
    ])
    .await;
    let client = discord::client(Duration::from_secs(1)).unwrap();
    assert!(ping(&client, url.clone(), true).await);
    assert!(ping(&client, url.clone(), false).await);
    assert!(!ping(&client, url, false).await);
    task.abort();
    let requests = seen.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].1.ends_with("/fail"));
}

#[tokio::test]
async fn global_rate_limit_pauses_status_without_reposting_an_accepted_card() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("status.json");
    let (url, seen, task) = server(vec![
        verify(),
        (429, json!({"retry_after":60,"global":true})),
    ])
    .await;
    let pause = Arc::new(Pause::default());
    let mut card = card::Card::open(
        &path,
        url,
        "22".into(),
        discord::client(Duration::from_secs(1)).unwrap(),
        pause.clone(),
    )
    .await
    .unwrap();
    assert!(card.update("status").await.is_err());
    assert!(!pause.remaining().is_zero());
    assert!(card.update("status").await.is_err());
    task.abort();
    assert_eq!(seen.lock().unwrap().len(), 2);
}
#[tokio::test]
async fn monitor_loop_cancels_and_reports_stopping_as_failure_to_external_check() {
    let (url, seen, task) = server(vec![(200, json!("OK"))]).await;
    let health = Arc::new(Health::default());
    let (stop, rx) = watch::channel(false);
    stop.send(true).unwrap();
    let worker = run(
        MonitorConfig::default(),
        None,
        Some(url),
        discord::client(Duration::from_secs(1)).unwrap(),
        health.clone(),
        rx,
        "unitboot".into(),
    );
    tokio::time::timeout(Duration::from_secs(2), worker)
        .await
        .unwrap();
    task.abort();
    assert_eq!(seen.lock().unwrap().len(), 1);
    assert!(seen.lock().unwrap()[0].1.ends_with("/fail"));
    assert_eq!(health.snapshot()["monitor"]["state"], "stopped");
}
#[test]
fn monitor_config_requires_opt_in_and_a_valid_status_channel() {
    for cfg in [
        "[monitor]\ndeadman=true",
        "[monitor]\nenabled=true\ndiscord_status=true",
        "[monitor]\nheartbeat_s=0",
        "[monitor]\ndeadman_s=1",
    ] {
        assert!(crate::config::Config::parse(cfg).is_err());
    }
    assert!(crate::config::Config::parse("[monitor]\nenabled=true").is_ok());
}

#[test]
fn a_responsive_collector_does_not_hide_a_stalled_disk_worker() {
    let health = Health::default();
    set_state(&health.feed_state, "connected");
    set_state(&health.application_state, "observed");
    set(&health.root_tick_ms, 5100);
    set(&health.pipeline_tick_ms, 5100);
    set(&health.monitor_stall_ms, 5000);
    set_state(&health.history_state, "recording");
    set(&health.history_tick_ms, 100);
    assert!(
        health.operational_at(Some(5100))["reasons"]
            .as_array()
            .unwrap()
            .contains(&json!("history_stalled"))
    );
    set(&health.history_tick_ms, 5100);
    set_state(&health.capture_state, "writing");
    set(&health.capture_tick_ms, 100);
    assert!(
        health.operational_at(Some(5100))["reasons"]
            .as_array()
            .unwrap()
            .contains(&json!("capture_stalled"))
    );
    set(&health.capture_tick_ms, 5100);
    assert_eq!(health.operational_at(Some(5100))["operational"], true);
}
