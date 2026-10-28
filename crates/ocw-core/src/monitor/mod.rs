//! Opt-in status reporting. No process can detect its own disappearance: missing pings are
//! evaluated by an independently hosted Healthchecks-compatible check.
mod card;
use crate::{
    config::MonitorConfig,
    discord::{self, Pause},
    health::{Health, inc, set_state},
    storage::ProcessLock,
};
use serde_json::json;
use std::{
    path::Path,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinHandle};
use url::Url;

pub(crate) struct Runtime {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl Runtime {
    pub async fn start(
        config: MonitorConfig,
        status: Option<&str>,
        data_dir: &Path,
        health: Arc<Health>,
        pause: Arc<Pause>,
        guard: Arc<ProcessLock>,
        evidence: crate::notifications::Evidence,
    ) -> Result<Self, String> {
        let crate::notifications::Evidence { boot, history } = evidence;
        let deadman = if config.deadman {
            Some(deadman_url(
                &std::env::var("OCW_DEADMAN_URL").map_err(|_| "OCW_DEADMAN_URL is required")?,
            )?)
        } else {
            None
        };
        let status = if config.discord_status {
            Some((
                discord::webhook_url(
                    status.ok_or("OCW_DISCORD_STATUS_URL is required for status heartbeat")?,
                )?,
                config
                    .status_channel_id
                    .clone()
                    .ok_or("monitor.status_channel_id required")?,
            ))
        } else {
            None
        };
        let path = data_dir.join("status-card.json");
        let client = discord::client(Duration::from_millis(config.timeout_ms))?;
        let (stop, rx) = watch::channel(false);
        let (ready, started) = tokio::sync::oneshot::channel();
        let runtime = tokio::runtime::Handle::current();
        let task = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            runtime.block_on(async move {
                let card = match status {
                    Some((url, channel)) => {
                        match card::Card::open(&path, url, channel, client.clone(), pause).await {
                            Ok(mut card) => {
                                card.trace(
                                    &crate::notifications::Evidence {
                                        boot: boot.clone(),
                                        history: history.clone(),
                                    },
                                    Duration::from_millis(config.timeout_ms),
                                );
                                Some(card)
                            }
                            Err(_) => {
                                set_state(&health.monitor_worker_state, "status_unavailable");
                                let _ = ready.send(Err(()));
                                return;
                            }
                        }
                    }
                    None => None,
                };
                let _ = ready.send(Ok(()));
                run(config, card, deadman, client, health, rx, boot).await;
            });
        });
        let worker = Self {
            stop,
            task: Some(task),
        };
        match tokio::time::timeout(Duration::from_secs(5), started).await {
            Ok(Ok(Ok(()))) => Ok(worker),
            _ => Err("monitor startup validation failed (credentials redacted)".into()),
        }
    }
    pub async fn completed(&mut self) {
        let Some(task) = &mut self.task else {
            return std::future::pending().await;
        };
        let _ = task.await;
        self.task = None;
    }
    pub async fn shutdown(mut self) -> Result<(), &'static str> {
        let _ = self.stop.send(true);
        let Some(task) = &mut self.task else {
            return Err("monitor worker ended unexpectedly");
        };
        match tokio::time::timeout(Duration::from_secs(6), task).await {
            Ok(Ok(())) => Ok(()),
            _ => Err("monitor shutdown incomplete"),
        }
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
    }
}

pub(crate) fn deadman_url(raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw).map_err(|_| "invalid dead-man URL (redacted)")?;
    let id = url.path().trim_start_matches('/');
    if raw.len() > 2048
        || url.scheme() != "https"
        || url.host_str() != Some("hc-ping.com")
        || url.port().is_some_and(|n| n != 443)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || id.len() != 36
        || !id.bytes().enumerate().all(|(i, b)| {
            if [8, 13, 18, 23].contains(&i) {
                b == b'-'
            } else {
                b.is_ascii_hexdigit()
            }
        })
    {
        return Err("invalid dead-man URL (redacted)".into());
    }
    Ok(url)
}
async fn ping(client: &reqwest::Client, mut url: Url, healthy: bool) -> bool {
    if !healthy {
        url.set_path(&format!("{}/fail", url.path()));
    }
    match client.get(url).send().await {
        Ok(response) => response.status().as_u16() == 200 && discord::body(response).await.is_ok(),
        Err(_) => false,
    }
}
async fn run(
    config: MonitorConfig,
    mut card: Option<card::Card>,
    deadman: Option<Url>,
    client: reqwest::Client,
    health: Arc<Health>,
    mut stop: watch::Receiver<bool>,
    boot: String,
) {
    let origin = *health.monitor_origin.get_or_init(Instant::now);
    let mut heartbeat = tokio::time::interval(Duration::from_secs(config.heartbeat_s));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut deadman_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(config.deadman_s),
        Duration::from_secs(config.deadman_s),
    );
    deadman_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut progress = tokio::time::interval(Duration::from_secs(1));
    progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    health.pulse(&health.monitor_tick_ms);
    set_state(&health.monitor_worker_state, "running");
    loop {
        if *stop.borrow() {
            break;
        }
        tokio::select! { biased;
            _=stop.changed()=>break,
            _=progress.tick()=>health.pulse(&health.monitor_tick_ms),
            _=heartbeat.tick()=> {
                let report=health.operational();
                println!("{}",json!({"event":"heartbeat","entry_point":"live","boot_id":boot,"uptime_s":origin.elapsed().as_secs(),"readiness":report,"health":health.snapshot()}));
                let content=format!("OCW HEARTBEAT | {} | feed single-source, unverified | generation {} | frames {} | trades {} | volume warmup {} | sent {} | uptime {}s | production disabled",
                    if report["operational"]==true {"RUNNING"}else{"UNHEALTHY / UNVERIFIED"},health.snapshot()["feed"]["generation"],health.snapshot()["feed"]["frames"],health.snapshot()["feed"]["trades"],health.snapshot()["volume"]["warmup_buckets"],health.snapshot()["staging_delivery"]["sent"],origin.elapsed().as_secs());
                if let Some(card)=&mut card {
                    if card.update(&content).await.is_err() {inc(&health.heartbeat_errors);set_state(&health.monitor_worker_state,"degraded");}
                    else {inc(&health.heartbeat_updates);set_state(&health.monitor_worker_state,"running");}
                } else {inc(&health.heartbeat_updates);}
            },
            _=deadman_tick.tick(), if deadman.is_some()=> {
                let healthy=health.operational()["operational"]==true;
                if ping(&client,deadman.as_ref().expect("configured").clone(),healthy).await {
                    inc(&health.deadman_pings); if !healthy {inc(&health.deadman_failures);}
                }else{inc(&health.deadman_errors);}
            }
        }
    }
    if let Some(url) = deadman {
        let _ = ping(&client, url, false).await;
    }
    set_state(&health.monitor_worker_state, "stopped");
}

#[cfg(test)]
mod tests;
