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
    pub delivery: DeliveryConfig,
    pub recovery: RecoveryConfig,
    pub monitor: MonitorConfig,
    pub history: HistoryConfig,
    pub watch: crate::watch::Config,
    pub probe: crate::probe::Config,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RecoveryConfig {
    pub reconnect_attempts: u8,
    pub initial_backoff_ms: u64,
    pub max_backoff_ms: u64,
    pub healthy_reset_ms: u64,
    pub ping_interval_ms: u64,
    pub pong_timeout_ms: u64,
    pub transport_idle_ms: u64,
}
impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            reconnect_attempts: 0,
            initial_backoff_ms: 1000,
            max_backoff_ms: 30_000,
            healthy_reset_ms: 60_000,
            ping_interval_ms: 20_000,
            pong_timeout_ms: 10_000,
            transport_idle_ms: 60_000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct DeliveryConfig {
    pub staging_channel_id: Option<String>,
    pub queue_capacity: usize,
    pub hourly_cap: u64,
    pub daily_cap: u64,
    pub burst: u64,
    pub request_timeout_ms: u64,
}
impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            staging_channel_id: None,
            queue_capacity: 64,
            hourly_cap: 10,
            daily_cap: 40,
            burst: 2,
            request_timeout_ms: 2000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct FeedConfig {
    pub mint: Option<String>,
    pub auto_watch: bool,
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
            delivery: DeliveryConfig::default(),
            recovery: RecoveryConfig::default(),
            monitor: MonitorConfig::default(),
            history: HistoryConfig::default(),
            watch: crate::watch::Config::default(),
            probe: crate::probe::Config::default(),
        }
    }
}
impl Default for FeedConfig {
    fn default() -> Self {
        Self {
            mint: None,
            auto_watch: false,
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
        if !(16 * 1024 * 1024..=512 * 1024 * 1024).contains(&self.history.max_bytes) {
            return Err("history capacity out of range".into());
        }
        let m = &self.monitor;
        if !(60..=900).contains(&m.heartbeat_s)
            || !(30..=300).contains(&m.deadman_s)
            || !(3000..=30_000).contains(&m.stall_ms)
            || !(100..=2000).contains(&m.timeout_ms)
            || (m.discord_status
                && m.status_channel_id
                    .as_ref()
                    .is_none_or(|id| id.parse::<u64>().ok().filter(|n| *n > 0).is_none()))
            || (!m.enabled && (m.discord_status || m.deadman))
        {
            return Err("monitor limits or status channel are invalid".into());
        }
        if self.probe.heal_enabled && (!self.probe.enabled || !self.history.enabled) {
            return Err("healing requires enabled independent probing and durable history".into());
        }
        self.probe.validate().map_err(str::to_string)?;
        self.watch.validate().map_err(str::to_string)?;
        if self.feed.auto_watch && self.feed.mint.is_some() {
            return Err("choose automatic watching or one manual mint".into());
        }
        let f = &self.feed;
        let c = &self.capture;
        let g = &self.gate;
        let d = &self.delivery;
        let r = &self.recovery;
        if r.reconnect_attempts > 5
            || !(10..=30_000).contains(&r.initial_backoff_ms)
            || r.max_backoff_ms < r.initial_backoff_ms
            || r.max_backoff_ms > 60_000
            || !(1000..=600_000).contains(&r.healthy_reset_ms)
            || !(100..=60_000).contains(&r.ping_interval_ms)
            || !(100..=30_000).contains(&r.pong_timeout_ms)
            || r.transport_idle_ms < r.ping_interval_ms + r.pong_timeout_ms
            || r.transport_idle_ms > 600_000
        {
            return Err("recovery limits are out of range".into());
        }
        if !(1..=64).contains(&d.queue_capacity)
            || !(2..=10).contains(&d.hourly_cap)
            || !(1..=40).contains(&d.daily_cap)
            || !(1..=2).contains(&d.burst)
            || !(100..=2000).contains(&d.request_timeout_ms)
            || d.staging_channel_id
                .as_ref()
                .is_some_and(|id| id.parse::<u64>().ok().filter(|v| *v > 0).is_none())
        {
            return Err("delivery limits or staging channel are invalid".into());
        }
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

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MonitorConfig {
    pub enabled: bool,
    pub discord_status: bool,
    pub deadman: bool,
    pub status_channel_id: Option<String>,
    pub heartbeat_s: u64,
    pub deadman_s: u64,
    pub stall_ms: u64,
    pub timeout_ms: u64,
}
impl Default for MonitorConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            discord_status: false,
            deadman: false,
            status_channel_id: None,
            heartbeat_s: 300,
            deadman_s: 60,
            stall_ms: 5000,
            timeout_ms: 2000,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct HistoryConfig {
    pub enabled: bool,
    pub max_bytes: u64,
}
impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_bytes: 512 * 1024 * 1024,
        }
    }
}
