use crate::health::{Health, inc};
use serde_json::json;
use std::time::Duration;
use url::Url;

pub(crate) fn webhook_url(raw: &str) -> Result<Url, String> {
    crate::discord::webhook_url(raw)
}

pub(crate) async fn notify(
    raw: Option<&str>,
    health: &Health,
    pause: &crate::discord::Pause,
    evidence: &crate::notifications::Evidence,
) {
    let Some(raw) = raw else { return };
    if !pause.remaining().is_zero() {
        inc(&health.outage_unknown);
        return;
    }
    let Ok(url) = webhook_url(raw) else {
        inc(&health.outage_unknown);
        return;
    };
    let Ok(client) = crate::discord::client(Duration::from_secs(5)) else {
        inc(&health.outage_unknown);
        return;
    };
    let tape = crate::notifications::Trace::new(
        evidence,
        "outage",
        &crate::notifications::target(url.as_str()),
        None,
        false,
    );
    send(&client, url, health, tape).await;
}

async fn send(
    client: &reqwest::Client,
    url: Url,
    health: &Health,
    tape: Option<crate::notifications::Trace>,
) {
    let payload = json!({"content":"OCW trade stream unavailable. Collection stopped; data incomplete. Signal delivery is paused.","allowed_mentions":{"parse":[]}});
    let attempt = tape.as_ref().map(|t| {
        t.begin(
            "POST",
            "trade_stream",
            None,
            &payload,
            Duration::from_secs(5),
        )
    });
    let result = async {
        let mut response = client
            .post(url)
            .json(&payload)
            .send()
            .await
            .map_err(|_| ())?;
        if !response.status().is_success() {
            return Err(());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| ())? {
            if body.len() + chunk.len() > 8192 {
                return Err(());
            }
            body.extend(chunk);
        }
        let value: serde_json::Value = serde_json::from_slice(&body).map_err(|_| ())?;
        if value["id"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            .is_none_or(|id| id == 0)
        {
            return Err(());
        }
        Ok(value["id"].as_str().ok_or(())?.to_owned())
    }
    .await;
    if result.is_ok() {
        inc(&health.outage_sent);
    } else {
        inc(&health.outage_unknown);
    }
    if let Some(attempt) = attempt {
        let outcome = match result {
            Ok(id) => crate::delivery::http::Outcome::Sent(id),
            Err(_) => crate::delivery::http::Outcome::Unknown,
        };
        attempt.finish(&outcome, Duration::ZERO);
    }
    // POST outcome can be ambiguous. M1a never retries it automatically.
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    #[test]
    fn webhook_validation_rejects_redirect_hosts_and_credentials_without_echoing_them() {
        let raw = "https://discord.com.evil.invalid/api/webhooks/123/private-token";
        let error = webhook_url(raw).unwrap_err();
        assert!(!error.contains("private-token"));
        assert!(webhook_url("http://discord.com/api/webhooks/123/token").is_err());
        assert!(
            webhook_url("https://discord.com/api/webhooks/123/token")
                .unwrap()
                .query()
                .unwrap()
                .contains("wait=true")
        );
    }
    #[tokio::test]
    async fn failed_post_is_not_retried() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut bytes = vec![0; 8192];
            let n = stream.read(&mut bytes).await.unwrap();
            assert!(String::from_utf8_lossy(&bytes[..n]).contains("allowed_mentions"));
            stream.write_all(b"HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
        });
        let health = Health::default();
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(2))
            .build()
            .unwrap();
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let guard = std::sync::Arc::new(crate::storage::ProcessLock::acquire(&data).unwrap());
        let history = crate::history::Runtime::start(
            data.join("history"),
            "outageboot".into(),
            16 * 1024 * 1024,
            std::sync::Arc::new(Health::default()),
            guard,
        )
        .await
        .unwrap();
        let tape = crate::notifications::Trace::new(
            &crate::notifications::Evidence {
                boot: "outageboot".into(),
                history: Some(history.handle.clone()),
            },
            "outage",
            &"a".repeat(64),
            None,
            false,
        );
        send(
            &client,
            Url::parse(&format!("http://{addr}/?wait=true")).unwrap(),
            &health,
            tape,
        )
        .await;
        server.await.unwrap();
        history.shutdown().await.unwrap();
        let receipts = crate::notifications::run(&data.join("history/observations.ocwh")).unwrap();
        assert_eq!(receipts["lanes"], serde_json::json!(["outage"]));
        assert_eq!(receipts["observed_unknown"], 1);
        assert_eq!(health.snapshot()["outage"]["unknown_or_failed"], 1);
        assert_eq!(health.snapshot()["outage"]["sent"], 0);
    }
}
