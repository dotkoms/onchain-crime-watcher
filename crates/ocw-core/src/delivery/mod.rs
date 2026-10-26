//! Explicitly enabled staging delivery; never used for replay or generated inputs.
mod http;
mod policy;
mod store;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_http;
mod worker;

use crate::{
    config::DeliveryConfig,
    discord::Pause,
    health::{Health, inc, set, set_state, value},
    storage::ProcessLock,
    volume::Decision,
};
use policy::{Candidate, Queue};
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{Notify, watch},
    task::JoinHandle,
};

#[derive(Clone)]
pub(crate) struct Handle {
    queue: Arc<Mutex<Queue>>,
    notify: Arc<Notify>,
    active: Arc<AtomicBool>,
    health: Arc<Health>,
    boot: String,
    origin: Instant,
}
impl Handle {
    pub fn submit(&self, decision: Decision, generation: u64) {
        if !self.active.load(Ordering::Relaxed) {
            inc(&self.health.delivery_dropped);
            return;
        }
        let Some(c) = Candidate::new(decision, &self.boot, self.origin, generation) else {
            inc(&self.health.delivery_rejected);
            return;
        };
        let Ok(mut queue) = self.queue.try_lock() else {
            inc(&self.health.delivery_dropped);
            return;
        };
        let expired = queue.prune(Instant::now(), value(&self.health.generation));
        self.health
            .delivery_expired
            .fetch_add(expired as u64, Ordering::Relaxed);
        let replaced = queue.replaced;
        if queue.insert(c) {
            inc(&self.health.delivery_queued);
            self.health
                .delivery_dropped
                .fetch_add(queue.replaced - replaced, Ordering::Relaxed);
            self.notify.notify_one();
        } else {
            inc(&self.health.delivery_dropped);
        }
    }
}
pub(crate) struct Runtime {
    pub handle: Handle,
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}
pub(crate) struct Context {
    pub data_dir: PathBuf,
    pub health: Arc<Health>,
    pub global: Arc<Pause>,
    pub guard: Arc<ProcessLock>,
    pub boot: String,
    pub origin: Instant,
}
struct ActiveGuard(Arc<AtomicBool>);
impl Drop for ActiveGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

/// Exact staging envelope for a real decision; pure formatting, never an HTTP operation.
pub fn staging_payload(decision: &Decision, now_ms: u64) -> Option<serde_json::Value> {
    policy::valid_decision(decision)?;
    let payload = decision.staging_card(now_ms)?;
    (payload["content"].as_str()?.chars().count() <= 2000).then_some(payload)
}
impl Runtime {
    pub async fn start(
        config: DeliveryConfig,
        raw: &str,
        context: Context,
    ) -> Result<Self, String> {
        let channel = config
            .staging_channel_id
            .as_deref()
            .ok_or("delivery.staging_channel_id is required")?;
        let http = http::Http::new(
            raw,
            channel,
            Duration::from_millis(config.request_timeout_ms),
            context.global.clone(),
        )?;
        Self::with_http(config, http, context).await
    }
    async fn with_http(
        config: DeliveryConfig,
        http: http::Http,
        context: Context,
    ) -> Result<Self, String> {
        let Context {
            data_dir,
            health,
            global,
            guard,
            boot,
            origin,
        } = context;
        http.verify().await?;
        let path = data_dir.join("staging-delivery.json");
        let policy = config.clone();
        let journal = tokio::task::spawn_blocking(move || store::Journal::open(&path, &policy))
            .await
            .map_err(|_| "delivery journal worker failed")?
            .map_err(|_| "delivery journal unavailable; refusing staging sends")?;
        set(&health.delivery_enabled, 1);
        set_state(&health.delivery_state, "ready");
        let queue = Arc::new(Mutex::new(Queue::new(config.queue_capacity)));
        let notify = Arc::new(Notify::new());
        let active = Arc::new(AtomicBool::new(true));
        let handle = Handle {
            queue: queue.clone(),
            notify: notify.clone(),
            active: active.clone(),
            health: health.clone(),
            boot,
            origin,
        };
        let (stop, rx) = watch::channel(false);
        let runtime = tokio::runtime::Handle::current();
        // Dedicated blocking worker owns disk I/O and holds the process lock through shutdown.
        // A slow disk cannot block feed decode or release ownership while an old write is pending.
        let task = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let _active = ActiveGuard(active);
            runtime.block_on(worker::run(
                http, journal, queue, notify, rx, health, global,
            ));
        });
        Ok(Self { handle, stop, task })
    }
    pub async fn shutdown(mut self) {
        self.handle.active.store(false, Ordering::Relaxed);
        let _ = self.stop.send(true);
        match tokio::time::timeout(Duration::from_secs(3), &mut self.task).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) => set_state(&self.handle.health.delivery_state, "worker_error"),
            Err(_) => set_state(&self.handle.health.delivery_state, "shutdown_pending"),
        }
    }
}
