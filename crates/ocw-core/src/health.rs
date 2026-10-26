use serde_json::{Value, json};
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering},
};

#[derive(Default)]
pub struct Health {
    pub frames: AtomicU64,
    pub trades: AtomicU64,
    pub creates: AtomicU64,
    pub migrations: AtomicU64,
    pub duplicates: AtomicU64,
    pub unresolved: AtomicU64,
    pub decode_errors: AtomicU64,
    pub unusable_reserves: AtomicU64,
    pub capture_written: AtomicU64,
    pub capture_dropped: AtomicU64,
    pub capture_bytes: AtomicU64,
    pub capture_recovered: AtomicU64,
    pub billable_received: AtomicU64,
    pub budget_reserved: AtomicU64,
    pub budget_stops: AtomicU64,
    pub generation: AtomicU64,
    pub reconnects: AtomicU64,
    pub recovery_exhausted: AtomicU64,
    pub backoff_ms: AtomicU64,
    pub transport_timeouts: AtomicU64,
    pub transport_received_ms: AtomicU64,
    pub matching_pongs: AtomicU64,
    pub wrong_pongs: AtomicU64,
    pub last_pong_ms: AtomicU64,
    pub create_received_ms: AtomicU64,
    pub create_ewma_ms: AtomicU64,
    pub create_threshold_ms: AtomicU64,
    pub app_suspicions: AtomicU64,
    pub app_recoveries: AtomicU64,
    pub app_suppressed: AtomicU64,
    pub last_failure: Mutex<&'static str>,
    pub transport_state: Mutex<&'static str>,
    pub application_state: Mutex<&'static str>,
    pub last_frame_ms: AtomicU64,
    pub outage_sent: AtomicU64,
    pub outage_unknown: AtomicU64,
    pub volume_enabled: AtomicU64,
    pub volume_active: AtomicU64,
    pub volume_evaluated: AtomicU64,
    pub volume_warmup: AtomicU64,
    pub volume_fire: AtomicU64,
    pub volume_near: AtomicU64,
    pub volume_unknown: AtomicU64,
    pub volume_full: AtomicU64,
    pub volume_invalid: AtomicU64,
    pub delivery_enabled: AtomicU64,
    pub delivery_queued: AtomicU64,
    pub delivery_dropped: AtomicU64,
    pub delivery_expired: AtomicU64,
    pub delivery_budget: AtomicU64,
    pub delivery_duplicates: AtomicU64,
    pub delivery_attempted: AtomicU64,
    pub delivery_sent: AtomicU64,
    pub delivery_unknown: AtomicU64,
    pub delivery_rejected: AtomicU64,
    pub delivery_limited: AtomicU64,
    pub delivery_state: Mutex<&'static str>,
    pub feed_state: Mutex<&'static str>,
    pub capture_state: Mutex<&'static str>,
    pub capture_shutdown: Mutex<&'static str>,
    pub paid_state: Mutex<&'static str>,
    pub supervisor_state: Mutex<&'static str>,
    pub pipeline_state: Mutex<&'static str>,
    pub capture_worker_state: Mutex<&'static str>,
    pub delivery_worker_state: Mutex<&'static str>,
    pub health_worker_state: Mutex<&'static str>,
    pub pipeline_panics: AtomicU64,
    pub pipeline_restarts: AtomicU64,
    pub worker_failures: AtomicU64,
}
pub fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}
pub fn value(counter: &AtomicU64) -> u64 {
    counter.load(Ordering::Relaxed)
}
pub fn set(counter: &AtomicU64, n: u64) {
    counter.store(n, Ordering::Relaxed);
}
fn state(lock: &Mutex<&'static str>) -> &'static str {
    lock.lock().map(|s| *s).unwrap_or("error")
}
pub fn set_state(lock: &Mutex<&'static str>, s: &'static str) {
    if let Ok(mut current) = lock.lock() {
        *current = s;
    }
}
impl Health {
    pub(crate) fn feed_connected(&self) -> bool {
        state(&self.feed_state) == "connected"
            && !matches!(
                state(&self.application_state),
                "suspected_unprobed" | "error"
            )
    }
    pub fn snapshot(&self) -> Value {
        json!({"version":env!("CARGO_PKG_VERSION"),"milestone":"M1c-preparation","signals_enabled":value(&self.delivery_enabled)>0,"production_signals_enabled":false,
            "supervision":{"state":state(&self.supervisor_state),"pipeline":state(&self.pipeline_state),"capture":state(&self.capture_worker_state),"delivery":state(&self.delivery_worker_state),"health":state(&self.health_worker_state),"pipeline_panics":value(&self.pipeline_panics),"pipeline_restarts":value(&self.pipeline_restarts),"failed_stages":value(&self.worker_failures)},
            "feed":{"state":state(&self.feed_state),"verification":"single-source, unverified","generation":value(&self.generation),"frames":value(&self.frames),"trades":value(&self.trades),"creates":value(&self.creates),"migrations":value(&self.migrations),"duplicates":value(&self.duplicates),"unresolved":value(&self.unresolved),"decode_errors":value(&self.decode_errors),"unusable_reserves":value(&self.unusable_reserves),"last_frame_ms":value(&self.last_frame_ms)},
            "recovery":{"reconnects":value(&self.reconnects),"exhausted":value(&self.recovery_exhausted),"backoff_ms":value(&self.backoff_ms),"last_failure":state(&self.last_failure)},
            "transport":{"state":state(&self.transport_state),"last_received_ms":value(&self.transport_received_ms),"matching_pongs":value(&self.matching_pongs),"wrong_or_late_pongs":value(&self.wrong_pongs),"last_matching_pong_ms":value(&self.last_pong_ms),"timeouts":value(&self.transport_timeouts)},
            "application":{"state":state(&self.application_state),"last_create_ms":value(&self.create_received_ms),"ewma_gap_ms":value(&self.create_ewma_ms),"threshold_ms":value(&self.create_threshold_ms),"suspicions":value(&self.app_suspicions),"quiet_recoveries":value(&self.app_recoveries),"suppressed_candidates":value(&self.app_suppressed),"probe":"not configured; suspicion never reconnects"},
            "capture":{"state":state(&self.capture_state),"shutdown":state(&self.capture_shutdown),"written_records":value(&self.capture_written),"dropped_records":value(&self.capture_dropped),"bytes":value(&self.capture_bytes),"recovered_bytes":value(&self.capture_recovered),"replay_mode":"decoder-only"},
            "paid":{"state":state(&self.paid_state),"billable_received":value(&self.billable_received),"reserved_messages":value(&self.budget_reserved),"stops":value(&self.budget_stops),"hard_provider_cap":false},
            "volume":{"preview_enabled":value(&self.volume_enabled)>0,"active_mints":value(&self.volume_active),"evaluated_buckets":value(&self.volume_evaluated),"warmup_buckets":value(&self.volume_warmup),"fire_candidates":value(&self.volume_fire),"near_candidates":value(&self.volume_near),"unknown_mint_trades":value(&self.volume_unknown),"registry_full":value(&self.volume_full),"invalid_buckets":value(&self.volume_invalid),"delivery":if value(&self.delivery_enabled)>0 {"staging"}else{"disabled"}},
            "staging_delivery":{"enabled":value(&self.delivery_enabled)>0,"state":if value(&self.delivery_enabled)==0 {"disabled"}else{state(&self.delivery_state)},"queued":value(&self.delivery_queued),"dropped":value(&self.delivery_dropped),"expired_or_generation_changed":value(&self.delivery_expired),"budget_suppressed":value(&self.delivery_budget),"duplicates":value(&self.delivery_duplicates),"attempted":value(&self.delivery_attempted),"sent":value(&self.delivery_sent),"unknown":value(&self.delivery_unknown),"rejected":value(&self.delivery_rejected),"rate_limited":value(&self.delivery_limited)},
            "outage":{"sent":value(&self.outage_sent),"unknown_or_failed":value(&self.outage_unknown)}})
    }
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    health: std::sync::Arc<Health>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    if !listener.local_addr()?.ip().is_loopback() {
        return Err(std::io::Error::other("health listener must be loopback"));
    }
    let app = axum::Router::new().route(
        "/health",
        axum::routing::get(move || {
            let health = health.clone();
            async move { axum::Json(health.snapshot()) }
        }),
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown)
        .await
}
