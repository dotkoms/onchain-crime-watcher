//! Shared credential-safe Discord URL/client and process-local global pause.
use std::{
    sync::Mutex,
    time::{Duration, Instant},
};
use url::Url;

#[derive(Default)]
pub(crate) struct Pause(Mutex<Option<Instant>>);
impl Pause {
    pub fn extend(&self, duration: Duration) {
        if let Some(until) = Instant::now().checked_add(duration)
            && let Ok(mut current) = self.0.lock()
        {
            *current = Some(current.map_or(until, |old| old.max(until)));
        }
    }
    pub fn remaining(&self) -> Duration {
        match self.0.lock() {
            Ok(until) => until
                .and_then(|until| until.checked_duration_since(Instant::now()))
                .unwrap_or_default(),
            Err(_) => Duration::from_secs(86400),
        }
    }
}
pub(crate) fn webhook_url(raw: &str) -> Result<Url, String> {
    let invalid = || "invalid Discord webhook URL (value redacted)".to_string();
    if raw.len() > 2048 {
        return Err(invalid());
    }
    let mut url = Url::parse(raw).map_err(|_| invalid())?;
    let parts: Vec<_> = url.path_segments().ok_or_else(invalid)?.collect();
    if url.scheme() != "https"
        || url.host_str() != Some("discord.com")
        || url.port().is_some_and(|p| p != 443)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || parts.len() != 4
        || parts[..2] != ["api", "webhooks"]
        || parts[2].parse::<u64>().ok().filter(|n| *n > 0).is_none()
        || parts[3].is_empty()
        || !parts[3]
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'_' | b'-' | b'.'))
    {
        return Err(invalid());
    }
    url.set_query(Some("wait=true"));
    Ok(url)
}
pub(crate) fn client(timeout: Duration) -> Result<reqwest::Client, String> {
    reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .timeout(timeout)
        .build()
        .map_err(|_| "cannot build Discord client".into())
}
pub(crate) async fn body(mut response: reqwest::Response) -> Result<Vec<u8>, ()> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| ())? {
        if bytes.len() + chunk.len() > 8192 {
            return Err(());
        }
        bytes.extend(chunk);
    }
    Ok(bytes)
}
