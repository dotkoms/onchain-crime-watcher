use crate::discord::{self, Pause};
use reqwest::header::HeaderMap;
use serde_json::Value;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use url::Url;

pub(crate) enum Outcome {
    Sent(String),
    RateLimited,
    Rejected,
    Broken,
    Unknown,
    WrongTarget,
}
pub(crate) struct Report {
    pub outcome: Outcome,
    pub pause: Duration,
}
pub(crate) struct Http {
    client: reqwest::Client,
    url: Url,
    channel: String,
    global: Arc<Pause>,
}
impl Http {
    pub fn new(
        raw: &str,
        channel: &str,
        timeout: Duration,
        global: Arc<Pause>,
    ) -> Result<Self, String> {
        Ok(Self {
            client: discord::client(timeout)?,
            url: discord::webhook_url(raw)?,
            channel: channel.into(),
            global,
        })
    }
    #[cfg(test)]
    pub fn local(url: Url, global: Arc<Pause>) -> Self {
        Self {
            client: discord::client(Duration::from_secs(2)).unwrap(),
            url,
            channel: "22".into(),
            global,
        }
    }
    pub async fn verify(&self) -> Result<(), String> {
        let mut url = self.url.clone();
        url.set_query(None);
        let result = async {
            let response = self.client.get(url).send().await.map_err(|_| ())?;
            if !response.status().is_success() {
                return Err(());
            }
            let data: Value =
                serde_json::from_slice(&discord::body(response).await?).map_err(|_| ())?;
            let expected = self.url.path_segments().and_then(|mut p| p.nth(2));
            if data["type"] != 1
                || data["channel_id"].as_str() != Some(self.channel.as_str())
                || data["id"].as_str() != expected
            {
                return Err(());
            }
            Ok(())
        }
        .await;
        result.map_err(|_| {
            "staging webhook readiness failed (endpoint and credentials redacted)".into()
        })
    }
    pub async fn post(&self, payload: &Value, expires: Instant) -> Report {
        if Instant::now() >= expires {
            return Report {
                outcome: Outcome::Unknown,
                pause: Duration::ZERO,
            };
        }
        let result = tokio::time::timeout_at(tokio::time::Instant::from_std(expires), async {
            let response = self
                .client
                .post(self.url.clone())
                .json(payload)
                .send()
                .await
                .map_err(|_| ())?;
            let status = response.status();
            let headers = response.headers().clone();
            let mut pause = Duration::ZERO;
            if headers
                .get("x-ratelimit-remaining")
                .and_then(|v| v.to_str().ok())
                == Some("0")
            {
                pause = header_delay(&headers, "x-ratelimit-reset-after").unwrap_or_else(|| {
                    if headers.contains_key("x-ratelimit-reset-after") {
                        Duration::from_secs(86400)
                    } else {
                        Duration::from_secs(1)
                    }
                });
            }
            let bytes = match discord::body(response).await {
                Ok(bytes) => bytes,
                Err(_) => {
                    if status.as_u16() == 429 {
                        pause = pause.max(
                            header_delay(&headers, "retry-after")
                                .unwrap_or(Duration::from_secs(86400)),
                        );
                        if headers
                            .get("x-ratelimit-scope")
                            .is_some_and(|s| s == "global")
                            || headers
                                .get("x-ratelimit-global")
                                .is_some_and(|s| s == "true")
                        {
                            self.global.extend(pause);
                        }
                        return Ok(Report {
                            outcome: Outcome::Broken,
                            pause,
                        });
                    }
                    return Ok(Report {
                        outcome: Outcome::Unknown,
                        pause,
                    });
                }
            };
            let data: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            let outcome = if status.as_u16() == 429 {
                let body_delay = data["retry_after"].as_f64().and_then(delay);
                let retry = header_delay(&headers, "retry-after");
                let invalid = (!data["retry_after"].is_null() && body_delay.is_none())
                    || (headers.contains_key("retry-after") && retry.is_none());
                let is_global = data["global"] == true
                    || headers
                        .get("x-ratelimit-scope")
                        .is_some_and(|s| s == "global")
                    || headers
                        .get("x-ratelimit-global")
                        .is_some_and(|s| s == "true");
                if invalid {
                    if is_global {
                        self.global.extend(Duration::from_secs(86400));
                    }
                    return Ok(Report {
                        outcome: Outcome::Broken,
                        pause: Duration::from_secs(86400),
                    });
                }
                match body_delay.into_iter().chain(retry).max() {
                    Some(wait) => {
                        pause = pause.max(wait);
                        if is_global {
                            self.global.extend(pause);
                        }
                        Outcome::RateLimited
                    }
                    None => {
                        if is_global {
                            pause = Duration::from_secs(86400);
                            self.global.extend(pause);
                        }
                        Outcome::Broken
                    }
                }
            } else if status.is_success() {
                match data["id"]
                    .as_str()
                    .filter(|id| id.parse::<u64>().ok().is_some_and(|id| id > 0))
                {
                    Some(id) if data["channel_id"].as_str() == Some(self.channel.as_str()) => {
                        Outcome::Sent(id.into())
                    }
                    _ if data["channel_id"]
                        .as_str()
                        .is_some_and(|id| id != self.channel) =>
                    {
                        Outcome::WrongTarget
                    }
                    _ => Outcome::Unknown,
                }
            } else if matches!(status.as_u16(), 401 | 403 | 404) {
                Outcome::Broken
            } else if status.is_server_error() {
                Outcome::Unknown
            } else {
                Outcome::Rejected
            };
            Ok::<_, ()>(Report { outcome, pause })
        })
        .await;
        match result {
            Ok(Ok(report)) => report,
            _ => Report {
                outcome: Outcome::Unknown,
                pause: Duration::ZERO,
            },
        }
    }
}
fn delay(seconds: f64) -> Option<Duration> {
    if !seconds.is_finite() || !(0.0..=86_400.0).contains(&seconds) {
        return None;
    }
    Some(Duration::from_millis(
        (seconds * 1000.0).ceil().max(1.0) as u64
    ))
}
fn header_delay(headers: &HeaderMap, name: &str) -> Option<Duration> {
    headers
        .get(name)?
        .to_str()
        .ok()?
        .parse()
        .ok()
        .and_then(delay)
}
