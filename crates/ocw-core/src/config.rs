use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub data_dir: PathBuf,
    pub health_bind: SocketAddr,
    pub feed: FeedConfig,
    pub gate: GateConfig,
    pub capture: CaptureConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeedConfig {
    pub mint: Option<String>,
    pub max_frame_bytes: usize,
    pub dedup_capacity: usize,
    pub dedup_window_ms: u64,
    pub connect_timeout_ms: u64,
    pub paid_window_s: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct GateConfig {
    pub daily_messages: u64,
    pub stop_margin_messages: u64,
    pub reservation_messages: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CaptureConfig {
    pub queue_capacity: usize,
    pub max_bytes: u64,
    pub retention_days: u64,
    pub min_free_bytes: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: "data".into(),
            health_bind: "127.0.0.1:9898".parse().expect("literal address"),
            feed: FeedConfig::default(),
            gate: GateConfig::default(),
            capture: CaptureConfig::default(),
        }
    }
}
impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            mint: None,
            max_frame_bytes: 65_536,
            dedup_capacity: 4096,
            dedup_window_ms: 180_000,
            connect_timeout_ms: 10_000,
            paid_window_s: 10,
        }
    }
}
impl Default for GateConfig {
    fn default() -> Self {
        Self {
            daily_messages: 500_000,
            stop_margin_messages: 10_000,
            reservation_messages: 1000,
        }
    }
}
impl Default for CaptureConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 4096,
            max_bytes: 2 * 1024 * 1024 * 1024,
            retention_days: 14,
            min_free_bytes: 2 * 1024 * 1024 * 1024,
        }
    }
}
impl Config {
    pub fn parse(text: &str) -> Result<Self, String> {
        // TOML errors can echo source values. Only expose a field-independent classification.
        let config: Self = toml::from_str(text)
            .map_err(|_| "invalid configuration (unknown key, type or TOML syntax)".to_string())?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), String> {
        let f = &self.feed;
        let c = &self.capture;
        let g = &self.gate;
        if !self.health_bind.ip().is_loopback() {
            return Err("health_bind must be loopback in M1a".into());
        }
        if self.data_dir.as_os_str().is_empty() {
            return Err("data_dir is required".into());
        }
        if let Some(mint) = &f.mint {
            crate::feed::key::<32>(mint)
                .map_err(|_| "feed.mint must be a 32-byte base58 address")?;
        }
        if !(1024..=65_536).contains(&f.max_frame_bytes)
            || !(1..=405_000).contains(&f.dedup_capacity)
            || f.dedup_window_ms == 0
        {
            return Err("feed frame/dedup limits are out of range".into());
        }
        if !(1..=30_000).contains(&f.connect_timeout_ms) || !(1..=300).contains(&f.paid_window_s) {
            return Err("feed timeouts are out of range".into());
        }
        if g.daily_messages <= g.stop_margin_messages
            || g.reservation_messages == 0
            || g.reservation_messages > g.daily_messages - g.stop_margin_messages
        {
            return Err("gate reservation and stop margin must fit the daily allowance".into());
        }
        if !(1..=4096).contains(&c.queue_capacity)
            || c.max_bytes < 1024
            || c.retention_days == 0
            || c.retention_days > 365
        {
            return Err("capture limits are out of range".into());
        }
        Ok(())
    }
}
