use super::*;
#[tokio::test]
async fn endpoint_validation_is_read_only_and_never_echoes_secrets() {
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let guard = Arc::new(ProcessLock::acquire(&data).unwrap());
    for url in [
        "http://secret.test/private-key",
        "https://user:private-key@example.test/",
        "invalid-private-key",
    ] {
        let error = Client::open(url, &data, guard.clone(), 10, 10)
            .await
            .err()
            .unwrap();
        assert_eq!(error, Failure::Invalid);
        assert!(!format!("{error:?}").contains("private-key"));
    }
    assert!(!data.join("rpc").exists());
}

#[test]
fn minute_limits_pause_and_clock_rollback_survive_client_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rate.json");
    let mut rate = Rate::open(path.clone(), 2).unwrap();
    rate.reserve(100).unwrap();
    rate.reserve(101).unwrap();
    drop(rate);
    let mut rate = Rate::open(path.clone(), 2).unwrap();
    assert_eq!(rate.reserve(102), Err(Failure::Budget));
    assert_eq!(rate.reserve(99), Err(Failure::Budget));
    rate.reserve(60_100).unwrap();
    rate.pause(60_100, 60).unwrap();
    drop(rate);
    let mut rate = Rate::open(path, 2).unwrap();
    assert_eq!(rate.reserve(60_101), Err(Failure::RateLimited));
    rate.reserve(120_100).unwrap();
}
#[tokio::test]
async fn durable_daily_reservations_bound_actual_http_requests_across_restart() {
    use axum::{
        Router,
        extract::{Json, State},
        routing::post,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let requests = Arc::new(Mutex::new(Vec::<Value>::new()));
    let app=Router::new().route("/",post(|State(requests):State<Arc<Mutex<Vec<Value>>>>,Json(body):Json<Value>|async move {
        requests.lock().unwrap().push(body.clone());Json(json!({"jsonrpc":"2.0","id":body["id"],"result":[]}))
    })).with_state(requests.clone());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let guard = Arc::new(ProcessLock::acquire(&data).unwrap());
    let mut client = Client::open("https://example.invalid/", &data, guard.clone(), 2, 600)
        .await
        .unwrap();
    client.url = endpoint.clone();
    assert!(
        Client::open("https://example.invalid/", &data, guard.clone(), 2, 600)
            .await
            .is_err()
    );
    for _ in 0..2 {
        client
            .signatures(&bs58::encode([1; 32]).into_string(), None, 10)
            .await
            .unwrap();
    }
    drop(client);
    let mut client = Client::open("https://example.invalid/", &data, guard, 2, 600)
        .await
        .unwrap();
    client.url = endpoint;
    assert_eq!(
        client
            .signatures(&bs58::encode([1; 32]).into_string(), None, 10)
            .await,
        Err(Failure::Budget)
    );
    server.abort();
    assert_eq!(requests.lock().unwrap().len(), 2);
    assert_eq!(
        requests.lock().unwrap()[0]["method"],
        "getSignaturesForAddress"
    );
}
#[tokio::test]
async fn rpc_id_mismatch_and_http_failure_are_not_retried() {
    use axum::{Router, extract::Json, routing::post};
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
    let app = Router::new().route(
        "/",
        post(|| async { Json(json!({"jsonrpc":"2.0","id":9999,"result":[]})) }),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let dir = tempfile::tempdir().unwrap();
    let data = dir.path().join("data");
    let guard = Arc::new(ProcessLock::acquire(&data).unwrap());
    let mut client = Client::open("https://example.invalid/", &data, guard, 1, 600)
        .await
        .unwrap();
    client.url = endpoint;
    assert_eq!(
        client.call(Method::Signatures, json!([])).await,
        Err(Failure::Invalid)
    );
    assert_eq!(
        client.call(Method::Signatures, json!([])).await,
        Err(Failure::Budget)
    );
    server.abort();
}
