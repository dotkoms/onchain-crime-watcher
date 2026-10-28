use super::{
    http::{Http, Outcome},
    policy::Queue,
    store::Journal,
    tests::candidate,
    worker,
};
use crate::{
    config::DeliveryConfig,
    discord::Pause,
    health::{Health, set, set_state},
    volume::Verdict,
};
use axum::{
    Router,
    body::{Body, Bytes},
    extract::{OriginalUri, State},
    http::{Method, Response},
    routing::any,
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
};

struct Reply {
    status: u16,
    data: Value,
    headers: Vec<(&'static str, &'static str)>,
    delay: Duration,
}
fn reply(status: u16, data: Value) -> Reply {
    Reply {
        status,
        data,
        headers: Vec::new(),
        delay: Duration::ZERO,
    }
}
type TestState = (
    Arc<Mutex<VecDeque<Reply>>>,
    Arc<Mutex<Vec<(String, Value)>>>,
);
struct Server {
    url: url::Url,
    seen: Arc<Mutex<Vec<(String, Value)>>>,
    task: JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(replies: Vec<Reply>) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = url::Url::parse(&format!(
        "http://{}/api/webhooks/1/token?wait=true",
        listener.local_addr().unwrap()
    ))
    .unwrap();
    let seen = Arc::new(Mutex::new(Vec::new()));
    let state = (Arc::new(Mutex::new(VecDeque::from(replies))), seen.clone());
    let app = Router::new()
        .route(
            "/api/webhooks/1/token",
            any(
                |State((replies, seen)): State<TestState>,
                 method: Method,
                 OriginalUri(uri): OriginalUri,
                 body: Bytes| async move {
                    seen.lock().unwrap().push((
                        format!("{method} {uri}"),
                        serde_json::from_slice(&body).unwrap_or(Value::Null),
                    ));
                    let r = replies
                        .lock()
                        .unwrap()
                        .pop_front()
                        .unwrap_or_else(|| reply(500, json!({})));
                    tokio::time::sleep(r.delay).await;
                    let mut response = Response::builder().status(r.status);
                    for (name, value) in r.headers {
                        response = response.header(name, value);
                    }
                    response.body(Body::from(r.data.to_string())).unwrap()
                },
            ),
        )
        .with_state(state);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    Server { url, seen, task }
}
fn sent() -> Reply {
    reply(200, json!({"id":"99","channel_id":"22"}))
}
async fn until(mut ready: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while !ready() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn readiness_verifies_channel_identity_with_get_and_does_not_post() {
    let endpoint = server(vec![reply(
        200,
        json!({"id":"1","type":1,"channel_id":"22"}),
    )])
    .await;
    let http = Http::local(endpoint.url.clone(), Arc::new(Pause::default()));
    http.verify().await.unwrap();
    {
        let seen = endpoint.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].0, "GET /api/webhooks/1/token");
    }
    let wrong = server(vec![reply(
        200,
        json!({"id":"1","type":1,"channel_id":"23"}),
    )])
    .await;
    assert!(
        Http::local(wrong.url.clone(), Arc::new(Pause::default()))
            .verify()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn http_classifies_failures_without_retrying_or_following_redirects() {
    for (response, expected) in [
        (reply(500, json!({})), "unknown"),
        (reply(401, json!({"code":50027})), "broken"),
        (reply(403, json!({})), "broken"),
        (reply(404, json!({"code":10015})), "broken"),
        (reply(400, json!({"code":50035})), "rejected"),
        (reply(200, json!({})), "unknown"),
        (
            Reply {
                headers: vec![("location", "http://127.0.0.1:9/never")],
                ..reply(302, json!({}))
            },
            "rejected",
        ),
        (sent(), "sent"),
    ] {
        let server = server(vec![response]).await;
        let http = Http::local(server.url.clone(), Arc::new(Pause::default()));
        let report = http
            .post(
                &candidate(Verdict::Fire, 1, Instant::now())
                    .payload(Instant::now())
                    .unwrap(),
                Instant::now() + Duration::from_secs(2),
            )
            .await;
        let actual = match report.outcome {
            Outcome::Unknown => "unknown",
            Outcome::Broken => "broken",
            Outcome::Rejected => "rejected",
            Outcome::Sent(_) => "sent",
            Outcome::RateLimited => "limited",
            Outcome::WrongTarget => "wrong_target",
        };
        assert_eq!(actual, expected);
        let seen = server.seen.lock().unwrap();
        assert_eq!(seen.len(), 1);
        assert!(seen[0].0.contains("wait=true"));
        assert_eq!(seen[0].1["allowed_mentions"]["parse"], json!([]));
    }
}

#[tokio::test]
async fn rate_limit_uses_the_longest_delay_and_shares_global_pause_with_status() {
    let mut response = reply(429, json!({"retry_after":1,"global":true}));
    response.headers = vec![
        ("retry-after", "2"),
        ("x-ratelimit-remaining", "0"),
        ("x-ratelimit-reset-after", "60"),
    ];
    let server = server(vec![response]).await;
    let global = Arc::new(Pause::default());
    let http = Http::local(server.url.clone(), global.clone());
    let report = http
        .post(&json!({}), Instant::now() + Duration::from_secs(2))
        .await;
    assert!(matches!(report.outcome, Outcome::RateLimited));
    assert_eq!(report.pause, Duration::from_secs(60));
    assert!(!global.remaining().is_zero());
    let health = Health::default();
    crate::outage::notify(
        Some("https://discord.com/api/webhooks/1/private-token"),
        &health,
        &global,
        &crate::notifications::Evidence {
            boot: "unit".into(),
            history: None,
        },
    )
    .await;
    assert_eq!(health.snapshot()["outage"]["unknown_or_failed"], 1);
    assert_eq!(server.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn expired_request_does_not_start_and_malformed_rate_limit_is_not_retried() {
    let server = server(vec![reply(429, json!({"retry_after":86401}))]).await;
    let http = Http::local(server.url.clone(), Arc::new(Pause::default()));
    let report = http
        .post(&json!({}), Instant::now() - Duration::from_secs(1))
        .await;
    assert!(matches!(report.outcome, Outcome::Unknown));
    assert_eq!(server.seen.lock().unwrap().len(), 0);
    let report = http
        .post(&json!({}), Instant::now() + Duration::from_secs(2))
        .await;
    assert!(matches!(report.outcome, Outcome::Broken));
}

struct Running {
    health: Arc<Health>,
    queue: Arc<Mutex<Queue>>,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}
fn run_worker(http: Http, journal: Journal, global: Arc<Pause>) -> Running {
    let queue = Arc::new(Mutex::new(Queue::new(4)));
    queue
        .lock()
        .unwrap()
        .insert(candidate(Verdict::Fire, 1, Instant::now()));
    let health = Arc::new(Health::default());
    set(&health.generation, 1);
    set_state(&health.feed_state, "connected");
    let (stop, rx) = watch::channel(false);
    let task = tokio::spawn(worker::run(
        http,
        journal,
        queue.clone(),
        Arc::new(Notify::new()),
        rx,
        health.clone(),
        global,
    ));
    Running {
        health,
        queue,
        stop,
        task,
    }
}

#[tokio::test]
async fn worker_retries_only_explicit_429_and_persists_one_message_receipt() {
    let server = server(vec![reply(429, json!({"retry_after":0.01})), sent()]).await;
    let global = Arc::new(Pause::default());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delivery.json");
    let journal = Journal::open(&path, &DeliveryConfig::default()).unwrap();
    let worker = run_worker(
        Http::local(server.url.clone(), global.clone()),
        journal,
        global,
    );
    until(|| worker.health.snapshot()["staging_delivery"]["sent"] == 1).await;
    worker.stop.send(true).unwrap();
    worker.task.await.unwrap();
    assert_eq!(worker.health.snapshot()["staging_delivery"]["attempted"], 2);
    assert_eq!(
        worker.health.snapshot()["staging_delivery"]["rate_limited"],
        1
    );
    assert_eq!(server.seen.lock().unwrap().len(), 2);
    let bytes: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(bytes["state"]["entries"].as_array().unwrap().len(), 1);
    assert_eq!(bytes["state"]["entries"][0]["message_id"], "99");
}

#[tokio::test]
async fn unknown_post_is_not_retried_even_if_the_same_intent_is_resubmitted() {
    let server = server(vec![reply(500, json!({}))]).await;
    let global = Arc::new(Pause::default());
    let dir = tempfile::tempdir().unwrap();
    let journal = Journal::open(
        &dir.path().join("delivery.json"),
        &DeliveryConfig::default(),
    )
    .unwrap();
    let origin = Instant::now();
    let worker = run_worker(
        Http::local(server.url.clone(), global.clone()),
        journal,
        global,
    );
    until(|| worker.health.snapshot()["staging_delivery"]["unknown"] == 1).await;
    worker
        .queue
        .lock()
        .unwrap()
        .insert(candidate(Verdict::Fire, 1, origin));
    until(|| worker.health.snapshot()["staging_delivery"]["duplicates"] == 1).await;
    worker.stop.send(true).unwrap();
    worker.task.await.unwrap();
    assert_eq!(server.seen.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn rate_pause_cannot_extend_expiry_or_carry_a_candidate_into_a_new_generation() {
    for change_generation in [false, true] {
        let wait = if change_generation { 0.5 } else { 60.0 };
        let server = server(vec![reply(429, json!({"retry_after":wait}))]).await;
        let global = Arc::new(Pause::default());
        let dir = tempfile::tempdir().unwrap();
        let journal = Journal::open(
            &dir.path().join("delivery.json"),
            &DeliveryConfig::default(),
        )
        .unwrap();
        let worker = run_worker(
            Http::local(server.url.clone(), global.clone()),
            journal,
            global,
        );
        until(|| worker.health.snapshot()["staging_delivery"]["rate_limited"] == 1).await;
        if change_generation {
            set(&worker.health.generation, 2);
        }
        until(|| {
            worker.health.snapshot()["staging_delivery"]["expired_or_generation_changed"] == 1
        })
        .await;
        worker.stop.send(true).unwrap();
        worker.task.await.unwrap();
        assert_eq!(server.seen.lock().unwrap().len(), 1);
    }
}

#[tokio::test]
async fn shutdown_during_an_inflight_post_keeps_the_intent_consumed_and_unknown() {
    let server = server(vec![Reply {
        delay: Duration::from_secs(10),
        ..sent()
    }])
    .await;
    let global = Arc::new(Pause::default());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("delivery.json");
    let journal = Journal::open(&path, &DeliveryConfig::default()).unwrap();
    let worker = run_worker(
        Http::local(server.url.clone(), global.clone()),
        journal,
        global,
    );
    until(|| server.seen.lock().unwrap().len() == 1).await;
    worker.stop.send(true).unwrap();
    worker.task.await.unwrap();
    assert_eq!(worker.health.snapshot()["staging_delivery"]["unknown"], 1);
    let bytes: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert_eq!(bytes["state"]["entries"][0]["outcome"], "unknown");
}

#[tokio::test]
async fn configured_runtime_checks_identity_uses_worker_and_retains_process_ownership() {
    let endpoint = server(vec![
        reply(200, json!({"id":"1","type":1,"channel_id":"22"})),
        sent(),
    ])
    .await;
    let dir = tempfile::tempdir().unwrap();
    let data_dir = dir.path().join("data");
    let guard = Arc::new(crate::storage::ProcessLock::acquire(&data_dir).unwrap());
    let health = Arc::new(Health::default());
    set(&health.generation, 1);
    set_state(&health.feed_state, "connected");
    let global = Arc::new(Pause::default());
    let history = crate::history::Runtime::start(
        data_dir.join("history"),
        "boot1".into(),
        16 * 1024 * 1024,
        health.clone(),
        guard.clone(),
    )
    .await
    .unwrap();
    let capture = super::tests_linked::capture(dir.path());
    let origin = capture.origin;
    let context = super::Context {
        data_dir: data_dir.clone(),
        health: health.clone(),
        global: global.clone(),
        guard: guard.clone(),
        boot: "boot1".into(),
        origin,
        history: Some(history.handle.clone()),
    };
    let config = DeliveryConfig {
        staging_channel_id: Some("22".into()),
        ..DeliveryConfig::default()
    };
    let runtime =
        super::Runtime::with_http(config, Http::local(endpoint.url.clone(), global), context)
            .await
            .unwrap();
    drop(guard);
    assert!(crate::storage::ProcessLock::acquire(&data_dir).is_err());
    let status = runtime.handle.submit(capture.decision.clone(), 1);
    let capture_path = capture.finish(status);
    until(|| health.snapshot()["staging_delivery"]["sent"] == 1).await;
    runtime.shutdown().await.unwrap();
    history.shutdown().await.unwrap();
    let replay = super::replay::run(&data_dir.join("history/observations.ocwh")).unwrap();
    assert_eq!(replay["observed_sent"], 1);
    let linked =
        crate::replay::linked::run(&capture_path, &data_dir.join("history/observations.ocwh"))
            .unwrap();
    assert_eq!(linked["volume_delivery_link_replayed"], true);
    assert_eq!(linked["linked_candidates"], 1);
    let repeat =
        crate::replay::linked::run(&capture_path, &data_dir.join("history/observations.ocwh"))
            .unwrap();
    assert_eq!(linked, repeat);
    let accumulated = tempfile::tempdir().unwrap();
    let root = accumulated.path().join("history");
    let mut journal = crate::history::Journal::open(&root, 16 * 1024 * 1024, "boot1", 1).unwrap();
    let mut last = 1;
    crate::history::read(
        &data_dir.join("history/observations.ocwh"),
        16 * 1024 * 1024,
        |entry| {
            last = entry.wall_ms;
            if entry.kind != "boot" {
                journal.append(entry.wall_ms, &entry.kind, entry.data)?;
            }
            Ok(())
        },
    )
    .unwrap();
    journal.sync().unwrap();
    drop(journal);
    let mut journal =
        crate::history::Journal::open(&root, 16 * 1024 * 1024, "boot2", last).unwrap();
    for data in [
        json!({"op":"start","data":{"schema":1,"boot":"boot2","config":DeliveryConfig::default(),"state":{"clock":0,"refilled":0,"tokens":2,"entries":[]}}}),
        json!({"op":"clear","data":{"removed":0}}),
        json!({"op":"end","data":{}}),
    ] {
        journal.append(last, "delivery_replay", data).unwrap();
    }
    journal.sync().unwrap();
    let paired =
        crate::replay::linked::run(&capture_path, &root.join("observations.ocwh")).unwrap();
    assert_eq!(paired["linked_candidates"], 1);
    assert_eq!(paired["delivery"]["sessions"], 1);
    assert_eq!(paired["delivery"]["selected_boot"], "boot1");

    for status in ["queue_rejected", "busy"] {
        let altered = tempfile::tempdir().unwrap();
        let source = super::tests_linked::capture(altered.path()).finish(status);
        assert!(
            crate::replay::linked::run(&source, &data_dir.join("history/observations.ocwh"))
                .is_err()
        );
    }
    let forged = tempfile::tempdir().unwrap();
    let root = forged.path().join("history");
    let mut journal = crate::history::Journal::open(&root, 16 * 1024 * 1024, "boot1", 1).unwrap();
    let mut duplicated = false;
    crate::history::read(
        &data_dir.join("history/observations.ocwh"),
        16 * 1024 * 1024,
        |entry| {
            if entry.kind == "boot" {
                return Ok(());
            }
            journal.append(entry.wall_ms, &entry.kind, entry.data.clone())?;
            if !duplicated && entry.kind == "delivery_replay" && entry.data["op"] == "insert" {
                let mut duplicate = entry.data;
                duplicate["data"]["accepted"] = false.into();
                journal.append(entry.wall_ms, "delivery_replay", duplicate)?;
                duplicated = true;
            }
            Ok(())
        },
    )
    .unwrap();
    journal.sync().unwrap();
    assert!(duplicated);
    assert!(super::replay::run(&root.join("observations.ocwh")).is_ok());
    assert!(crate::replay::linked::run(&capture_path, &root.join("observations.ocwh")).is_err());

    assert_eq!(replay["attempts"], 1);
    assert!(crate::storage::ProcessLock::acquire(&data_dir).is_ok());
    assert_eq!(endpoint.seen.lock().unwrap().len(), 2); // readiness GET, then one POST
}

#[tokio::test]
async fn failed_durable_reservation_prevents_any_post() {
    let endpoint = server(vec![sent()]).await;
    let dir = tempfile::tempdir().unwrap();
    let parent = dir.path().join("journal");
    std::fs::create_dir(&parent).unwrap();
    let journal = Journal::open(&parent.join("delivery.json"), &DeliveryConfig::default()).unwrap();
    std::fs::remove_dir(&parent).unwrap();
    let global = Arc::new(Pause::default());
    let worker = run_worker(
        Http::local(endpoint.url.clone(), global.clone()),
        journal,
        global,
    );
    worker.task.await.unwrap();
    assert_eq!(
        worker.health.delivery_state.lock().unwrap().to_owned(),
        "journal_error"
    );
    assert_eq!(endpoint.seen.lock().unwrap().len(), 0);
}

#[tokio::test]
async fn oversized_global_rate_limit_body_still_pauses_status_delivery() {
    let endpoint = server(vec![Reply {
        headers: vec![("x-ratelimit-scope", "global"), ("retry-after", "60")],
        ..reply(429, json!({"message":"x".repeat(9000)}))
    }])
    .await;
    let global = Arc::new(Pause::default());
    let http = Http::local(endpoint.url.clone(), global.clone());
    let report = http
        .post(&json!({}), Instant::now() + Duration::from_secs(2))
        .await;
    assert!(matches!(report.outcome, Outcome::Broken));
    assert!(!global.remaining().is_zero());
}

#[tokio::test]
async fn a_changed_response_channel_blocks_further_posts_and_marks_the_outcome_unknown() {
    let endpoint = server(vec![reply(200, json!({"id":"99","channel_id":"23"}))]).await;
    let dir = tempfile::tempdir().unwrap();
    let global = Arc::new(Pause::default());
    let journal = Journal::open(
        &dir.path().join("delivery.json"),
        &DeliveryConfig::default(),
    )
    .unwrap();
    let worker = run_worker(
        Http::local(endpoint.url.clone(), global.clone()),
        journal,
        global,
    );
    worker
        .queue
        .lock()
        .unwrap()
        .insert(candidate(Verdict::Fire, 2, Instant::now()));
    worker.task.await.unwrap();
    assert_eq!(
        *worker.health.delivery_state.lock().unwrap(),
        "target_changed"
    );
    assert_eq!(worker.health.snapshot()["staging_delivery"]["unknown"], 1);
    assert_eq!(worker.health.snapshot()["staging_delivery"]["sent"], 0);
    assert_eq!(endpoint.seen.lock().unwrap().len(), 1);
}

async fn idle_runtime(data_dir: &std::path::Path) -> (super::Runtime, Server) {
    let endpoint = server(vec![reply(
        200,
        json!({"id":"1","type":1,"channel_id":"22"}),
    )])
    .await;
    let health = Arc::new(Health::default());
    let global = Arc::new(Pause::default());
    let context = super::Context {
        data_dir: data_dir.to_path_buf(),
        health,
        global: global.clone(),
        guard: Arc::new(crate::storage::ProcessLock::acquire(data_dir).unwrap()),
        boot: "idle".into(),
        origin: Instant::now(),
        history: None,
    };
    let runtime = super::Runtime::with_http(
        DeliveryConfig {
            staging_channel_id: Some("22".into()),
            ..DeliveryConfig::default()
        },
        Http::local(endpoint.url.clone(), global),
        context,
    )
    .await
    .unwrap();
    (runtime, endpoint)
}
#[tokio::test]
async fn dropping_delivery_runtime_stops_worker_and_releases_directory_ownership() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let (runtime, endpoint) = idle_runtime(&data).await;
    assert!(crate::storage::ProcessLock::acquire(&data).is_err());
    drop(runtime);
    until(|| crate::storage::ProcessLock::acquire(&data).is_ok()).await;
    assert_eq!(endpoint.seen.lock().unwrap().len(), 1); // Readiness only, no notification.
}
#[tokio::test]
async fn delivery_fatal_exit_is_observed_and_cannot_report_clean_shutdown() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let (mut runtime, endpoint) = idle_runtime(&data).await;
    let queue = runtime.handle.queue.clone();
    let _ = std::thread::spawn(move || {
        let _held = queue.lock().unwrap();
        panic!("isolated poisoned queue fault");
    })
    .join();
    tokio::time::timeout(Duration::from_secs(2), runtime.completed())
        .await
        .unwrap();
    assert_eq!(
        runtime.handle.health.snapshot()["staging_delivery"]["state"],
        "queue_error"
    );
    assert_eq!(
        runtime.shutdown().await,
        Err("staging delivery worker stopped unexpectedly")
    );
    assert!(crate::storage::ProcessLock::acquire(&data).is_ok());
    assert_eq!(endpoint.seen.lock().unwrap().len(), 1);
}
