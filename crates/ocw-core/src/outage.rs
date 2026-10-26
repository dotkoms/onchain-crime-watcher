use crate::health::{Health, inc};
use serde_json::json;
use std::time::Duration;
use url::Url;

pub(crate) fn webhook_url(raw: &str) -> Result<Url, String> {
    let mut url = Url::parse(raw).map_err(|_| "invalid OCW_DISCORD_STATUS_URL")?;
    let segments = url
        .path_segments()
        .map(|s| s.collect::<Vec<_>>())
        .unwrap_or_default();
    if url.scheme() != "https"
        || url.host_str() != Some("discord.com")
        || url.port().is_some_and(|p| p != 443)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || segments.len() != 4
        || segments[..2] != ["api", "webhooks"]
        || segments[2].parse::<u64>().is_err()
        || segments[3].is_empty()
    {
        return Err("OCW_DISCORD_STATUS_URL must be a Discord HTTPS webhook URL".into());
    }
    url.query_pairs_mut().append_pair("wait", "true");
    Ok(url)
}

pub(crate) async fn notify(raw: Option<&str>, health: &Health) {
    let Some(raw) = raw else { return };
    let Ok(url) = webhook_url(raw) else {
        inc(&health.outage_unknown);
        return;
    };
    let Ok(client) = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(5))
        .build()
    else {
        inc(&health.outage_unknown);
        return;
    };
    send(&client, url, health).await;
}

async fn send(client: &reqwest::Client, url: Url, health: &Health) {
    let payload = json!({"content":"OCW trade stream unavailable. M1a collector; single-source, unverified. No signal alerts are enabled.","allowed_mentions":{"parse":[]}});
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
        Ok(())
    }
    .await;
    if result.is_ok() {
        inc(&health.outage_sent);
    } else {
        inc(&health.outage_unknown);
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
        send(
            &client,
            Url::parse(&format!("http://{addr}/?wait=true")).unwrap(),
            &health,
        )
        .await;
        server.await.unwrap();
        assert_eq!(health.snapshot()["outage"]["unknown_or_failed"], 1);
        assert_eq!(health.snapshot()["outage"]["sent"], 0);
    }
}
