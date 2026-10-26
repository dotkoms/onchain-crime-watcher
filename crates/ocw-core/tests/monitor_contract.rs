use ocw_core::health::Health;
use std::sync::Arc;
#[tokio::test]
async fn readiness_is_unavailable_before_collection_starts() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(ocw_core::health::serve(
        listener,
        Arc::new(Health::default()),
        async {
            let _ = stopped.await;
        },
    ));
    let response = reqwest::get(format!("http://{address}/ready"))
        .await
        .unwrap();
    stop.send(()).unwrap();
    task.await.unwrap().unwrap();
    assert_eq!(response.status().as_u16(), 503);
}
