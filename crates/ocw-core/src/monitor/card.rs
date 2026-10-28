//! One durable status-card identity. Unknown initial POST outcomes never create a second card.
use crate::{
    discord::{self, Pause},
    storage::atomic_write,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use url::Url;
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    schema: u8,
    target: String,
    message: Option<String>,
    pending: bool,
}
pub(super) struct Card {
    tape: Option<(crate::notifications::Trace, Duration)>,
    path: PathBuf,
    url: Url,
    channel: String,
    client: reqwest::Client,
    state: State,
    global: Arc<Pause>,
    paused: Instant,
    blocked: bool,
}
impl Card {
    pub async fn open(
        path: &Path,
        mut url: Url,
        channel: String,
        client: reqwest::Client,
        global: Arc<Pause>,
    ) -> Result<Self, ()> {
        url.set_query(None);
        let response = client.get(url.clone()).send().await.map_err(|_| ())?;
        if !response.status().is_success() {
            return Err(());
        }
        let data: Value =
            serde_json::from_slice(&discord::body(response).await?).map_err(|_| ())?;
        let expected = url.path_segments().and_then(|mut parts| parts.nth(2));
        if data["type"] != 1
            || data["channel_id"].as_str() != Some(channel.as_str())
            || data["id"].as_str() != expected
        {
            return Err(());
        }
        let target = format!("{:x}", Sha256::digest(url.as_str().as_bytes()));
        let state = if path.exists() {
            if path.is_symlink() || std::fs::metadata(path).map_err(|_| ())?.len() > 2048 {
                return Err(());
            }
            let value: State =
                serde_json::from_slice(&std::fs::read(path).map_err(|_| ())?).map_err(|_| ())?;
            if value.schema != 1
                || value.target != target
                || value
                    .message
                    .as_ref()
                    .is_some_and(|id| id.parse::<u64>().ok().filter(|n| *n > 0).is_none())
            {
                return Err(());
            }
            value
        } else {
            State {
                schema: 1,
                target,
                message: None,
                pending: false,
            }
        };
        Ok(Self {
            tape: None,
            path: path.into(),
            url,
            channel,
            client,
            state,
            global,
            paused: Instant::now(),
            blocked: false,
        })
    }
    pub fn trace(&mut self, evidence: &crate::notifications::Evidence, timeout: Duration) {
        self.tape = crate::notifications::Trace::new(
            evidence,
            "status",
            &self.state.target,
            self.state.message.clone(),
            self.state.pending,
        )
        .map(|t| (t, timeout));
    }
    fn save(&self) -> Result<(), ()> {
        atomic_write(
            &self.path,
            &serde_json::to_vec(&self.state).map_err(|_| ())?,
        )
        .map_err(|_| ())
    }
    pub async fn update(&mut self, content: &str) -> Result<(), ()> {
        if self.blocked
            || self.state.pending
            || !self.global.remaining().is_zero()
            || Instant::now() < self.paused
        {
            return Err(());
        }
        let payload = json!({"content":content,"allowed_mentions":{"parse":[]}});
        let initial = self.state.message.is_none();
        let request = if let Some(id) = &self.state.message {
            let mut url = self.url.clone();
            url.set_path(&format!("{}/messages/{id}", url.path()));
            self.client.patch(url)
        } else {
            self.state.pending = true;
            if self.save().is_err() {
                self.blocked = true;
                return Err(());
            }
            let mut url = self.url.clone();
            url.set_query(Some("wait=true"));
            self.client.post(url)
        };
        let attempt = self.tape.as_ref().map(|(t, ttl)| {
            t.begin(
                if initial { "POST" } else { "PATCH" },
                self.state.message.as_deref().unwrap_or("status"),
                self.state.message.as_deref(),
                &payload,
                *ttl,
            )
        });
        let response = request.json(&payload).send().await.map_err(|_| ())?;
        let code = response.status().as_u16();
        let headers = response.headers().clone();
        let bytes = discord::body(response).await;
        let header_delay = |name: &str| -> Duration {
            headers
                .get(name)
                .map(|value| {
                    value
                        .to_str()
                        .ok()
                        .and_then(|v| v.parse::<f64>().ok())
                        .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 86400.0)
                        .map(Duration::from_secs_f64)
                        .unwrap_or(Duration::from_secs(86400))
                })
                .unwrap_or_default()
        };
        let mut limit_delay = header_delay("retry-after");
        if headers
            .get("x-ratelimit-remaining")
            .is_some_and(|v| v == "0")
        {
            limit_delay = limit_delay
                .max(header_delay("x-ratelimit-reset-after"))
                .max(Duration::from_secs(1));
        }
        if code == 429 {
            let data: Value = bytes
                .as_ref()
                .ok()
                .and_then(|b| serde_json::from_slice(b).ok())
                .unwrap_or(Value::Null);
            let delay = data["retry_after"]
                .as_f64()
                .filter(|v| v.is_finite() && *v > 0.0 && *v <= 86400.0)
                .map(Duration::from_secs_f64)
                .unwrap_or(Duration::from_secs(86400));
            let delay = delay.max(limit_delay);
            self.paused = Instant::now() + delay;
            if data["global"] == true
                || headers
                    .get("x-ratelimit-scope")
                    .is_some_and(|v| v == "global")
                || headers
                    .get("x-ratelimit-global")
                    .is_some_and(|v| v == "true")
            {
                self.global.extend(delay);
            }
            if initial {
                self.state.pending = false;
                self.save()?;
            }
            if let Some(attempt) = attempt {
                attempt.finish(&crate::delivery::http::Outcome::RateLimited, delay);
            }
            return Err(());
        }
        if !limit_delay.is_zero() {
            self.paused = Instant::now() + limit_delay;
        }
        if !(200..300).contains(&code) {
            if (400..500).contains(&code) {
                self.blocked = true;
                if initial {
                    self.state.pending = false;
                    self.save()?;
                }
            }
            if let Some(attempt) = attempt {
                attempt.finish(
                    &if (400..500).contains(&code) {
                        crate::delivery::http::Outcome::Broken
                    } else {
                        crate::delivery::http::Outcome::Unknown
                    },
                    limit_delay,
                );
            }
            return Err(());
        }
        let data: Value = serde_json::from_slice(&bytes?).map_err(|_| ())?;
        let id = data["id"]
            .as_str()
            .filter(|id| id.parse::<u64>().ok().is_some_and(|n| n > 0))
            .ok_or(())?;
        if data["channel_id"].as_str() != Some(self.channel.as_str())
            || self
                .state
                .message
                .as_ref()
                .is_some_and(|expected| expected != id)
        {
            self.blocked = true;
            if let Some(attempt) = attempt {
                attempt.finish(&crate::delivery::http::Outcome::WrongTarget, limit_delay);
            }
            return Err(());
        }
        self.state.message = Some(id.into());
        self.state.pending = false;
        if self.save().is_err() {
            self.blocked = true;
            return Err(());
        }
        if let Some(attempt) = attempt {
            attempt.finish(
                &crate::delivery::http::Outcome::Sent(id.into()),
                limit_delay,
            );
        }
        Ok(())
    }
}
