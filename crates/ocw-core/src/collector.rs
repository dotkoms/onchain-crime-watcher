use crate::{
    capture::{self, Record, Tap},
    config::Config,
    delivery,
    discord::Pause,
    feed::{Decoder, Event},
    health::{self, Health, inc, set, set_state},
    outage,
    storage::ProcessLock,
    supervision::{self, AbortOnDrop},
    volume::{Decision, Engine},
};
use bytes::Bytes;
use serde_json::{Value, json};
use std::{
    future::Future,
    io::{BufRead, BufReader, Read},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use url::Url;

#[derive(Default)]
pub struct Options {
    pub paid: bool,
    pub preview_volume: bool,
    pub staging_delivery: bool,
    pub fixture: Option<PathBuf>,
    pub run_for: Option<Duration>,
}
#[derive(Default)]
pub(crate) struct Secrets {
    pub api_key: Option<String>,
    pub webhook: Option<String>,
    pub staging_webhook: Option<String>,
}
impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "Secrets { api_key: [REDACTED], webhook: [REDACTED], staging_webhook: [REDACTED] }",
        )
    }
}
impl Secrets {
    fn from_env(staging_enabled: bool) -> Result<Self, String> {
        let api_key = std::env::var("OCW_PUMPPORTAL_KEY")
            .ok()
            .filter(|s| !s.is_empty());
        if api_key
            .as_ref()
            .is_some_and(|key| key.len() < 32 || key.len() > 512)
        {
            return Err("OCW_PUMPPORTAL_KEY length is invalid".into());
        }
        let webhook = std::env::var("OCW_DISCORD_STATUS_URL")
            .ok()
            .filter(|s| !s.is_empty());
        if let Some(url) = &webhook {
            outage::webhook_url(url)?;
        }
        let staging_webhook = if staging_enabled {
            Some(
                std::env::var("OCW_DISCORD_STAGING_URL")
                    .ok()
                    .filter(|v| !v.is_empty())
                    .ok_or("OCW_DISCORD_STAGING_URL is required for staging delivery")?,
            )
        } else {
            None
        };
        if let Some(raw) = &staging_webhook {
            crate::discord::webhook_url(raw)?;
        }
        Ok(Self {
            api_key,
            webhook,
            staging_webhook,
        })
    }
}
pub fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

pub(crate) struct Pipeline {
    pub decoder: Decoder,
    pub health: Arc<Health>,
    pub generation: u64,
    pub(crate) ownership: Arc<ProcessLock>,
    tap: Tap,
    seq: u64,
    boot: String,
    start: Instant,
    sensitive: Option<String>,
    volume: Option<Engine>,
    delivery: Option<delivery::Handle>,
}
impl Pipeline {
    fn record(&mut self, kind: &str, payload: Bytes) {
        self.seq += 1;
        self.tap.record(Record::control(
            self.seq,
            &self.boot,
            self.generation,
            self.start
                .elapsed()
                .as_nanos()
                .try_into()
                .unwrap_or(u64::MAX),
            wall_ms(),
            kind,
            payload,
        ));
    }
    pub(crate) fn control(&mut self, kind: &str, payload: Value) {
        self.record(kind, Bytes::from(payload.to_string()));
    }
    pub(crate) fn ingest(&mut self, bytes: Bytes) {
        inc(&self.health.frames);
        set(&self.health.last_frame_ms, wall_ms());
        if self
            .sensitive
            .as_ref()
            .is_some_and(|key| bytes.windows(key.len()).any(|part| part == key.as_bytes()))
        {
            inc(&self.health.capture_dropped);
            inc(&self.health.decode_errors);
            if let Some(engine) = &mut self.volume {
                engine.invalidate();
            }
            self.control("redacted_frame", json!({"reason":"credential_echo"}));
            return;
        }
        self.record("frame", bytes.clone());
        let now = self
            .start
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        match self.decoder.decode(&bytes, now) {
            Ok(event) => {
                match &event {
                    Event::Trade(_) => inc(&self.health.trades),
                    Event::Create(_) => inc(&self.health.creates),
                    Event::Migration(_) => inc(&self.health.migrations),
                    Event::Duplicate => inc(&self.health.duplicates),
                    Event::Unresolved => inc(&self.health.unresolved),
                    Event::Other => {}
                }
                if let Some(engine) = &mut self.volume {
                    let decisions = engine
                        .on_event(event, now)
                        .expect("monotonic receive clock");
                    self.emit_volume(decisions, now);
                }
            }
            Err(_) => {
                inc(&self.health.decode_errors);
                if let Some(engine) = &mut self.volume {
                    engine.invalidate();
                }
            }
        }
        self.volume_health();
        set(
            &self.health.unusable_reserves,
            self.decoder.unusable_reserves(),
        );
    }
    fn volume_health(&self) {
        if let Some(engine) = &self.volume {
            let stats = engine.stats();
            set(&self.health.volume_active, engine.active_mints() as u64);
            set(&self.health.volume_evaluated, stats.evaluated);
            set(&self.health.volume_warmup, stats.warmup);
            set(&self.health.volume_fire, stats.fire);
            set(&self.health.volume_near, stats.near);
            set(&self.health.volume_unknown, stats.unknown_trades);
            set(&self.health.volume_full, stats.registry_full);
            set(&self.health.volume_invalid, stats.invalidated_buckets);
        }
    }
    fn emit_volume(&mut self, decisions: Vec<Decision>, now: u64) {
        for decision in decisions {
            if let Some(preview) = decision.card(now) {
                if !self.health.feed_connected() {
                    inc(&self.health.app_suppressed);
                    continue;
                }
                if let Some(sender) = &self.delivery {
                    sender.submit(decision.clone(), self.generation);
                }
                let event = json!({"event":"volume_preview","source":"live_feed","decision":decision,"preview":preview,"sent":false});
                self.control("volume_preview", event.clone());
                println!("{event}");
            }
        }
    }
    pub(crate) fn tick_volume(&mut self) {
        let now = self
            .start
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        if let Some(engine) = &mut self.volume {
            let decisions = engine.advance(now).expect("monotonic receive clock");
            self.emit_volume(decisions, now);
            self.volume_health();
        }
    }
    pub(crate) fn reset_volume(&mut self) {
        let now = self
            .start
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX);
        if let Some(engine) = &mut self.volume {
            engine.gap(now).expect("monotonic receive clock");
        }
    }
}

pub async fn run(
    config: Config,
    options: Options,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<Value, String> {
    let secrets = if options.fixture.is_some() {
        Secrets::default()
    } else {
        Secrets::from_env(options.staging_delivery)?
    };
    let endpoint =
        Url::parse("wss://pumpportal.fun/api/data").map_err(|_| "invalid built-in endpoint")?;
    run_inner(config, options, secrets, endpoint, shutdown).await
}

async fn run_inner(
    config: Config,
    options: Options,
    secrets: Secrets,
    endpoint: Url,
    shutdown: impl Future<Output = ()> + Send,
) -> Result<Value, String> {
    config.validate()?;
    if options.staging_delivery
        && secrets
            .webhook
            .as_deref()
            .zip(secrets.staging_webhook.as_deref())
            .is_some_and(|(status, staging)| {
                crate::discord::webhook_url(status).ok()
                    == crate::discord::webhook_url(staging).ok()
            })
    {
        return Err("status and staging delivery require distinct webhooks".into());
    }
    if options.staging_delivery && config.delivery.staging_channel_id.is_none() {
        return Err("delivery.staging_channel_id is required for staging delivery".into());
    }
    if options.staging_delivery && (!options.preview_volume || options.fixture.is_some()) {
        return Err("staging delivery requires real live volume processing".into());
    }
    if options.preview_volume && options.fixture.is_some() {
        return Err("volume previews require live mode or recorded capture replay".into());
    }
    if options.paid
        && (options.fixture.is_some() || config.feed.mint.is_none() || secrets.api_key.is_none())
    {
        return Err("paid mode requires feed.mint, OCW_PUMPPORTAL_KEY, and live mode".into());
    }
    let lock = Arc::new(
        ProcessLock::acquire(&config.data_dir)
            .map_err(|_| "cannot acquire private data directory/collector lock")?,
    );
    let listener = tokio::net::TcpListener::bind(config.health_bind)
        .await
        .map_err(|_| "cannot bind loopback health endpoint")?;
    let address = listener
        .local_addr()
        .map_err(|_| "cannot read health address")?;
    let health = Arc::new(Health::default());
    set_state(&health.paid_state, "off");
    set_state(&health.transport_state, "not_started");
    set_state(&health.application_state, "not_started");
    set_state(&health.last_failure, "none");
    set_state(&health.feed_state, "starting");
    set_state(&health.capture_shutdown, "not_requested");
    set_state(&health.supervisor_state, "watching");
    set_state(&health.pipeline_state, "starting");
    set_state(&health.capture_worker_state, "running");
    set_state(
        &health.delivery_worker_state,
        if options.staging_delivery {
            "running"
        } else {
            "disabled"
        },
    );
    set_state(&health.health_worker_state, "running");
    set(&health.volume_enabled, u64::from(options.preview_volume));
    let global_pause = Arc::new(Pause::default());
    let boot = format!("{:x}{:x}", wall_ms(), std::process::id());
    let origin = Instant::now();
    let mut delivery_runtime = if options.staging_delivery {
        Some(
            delivery::Runtime::start(
                config.delivery.clone(),
                secrets
                    .staging_webhook
                    .as_deref()
                    .ok_or("OCW_DISCORD_STAGING_URL is required")?,
                delivery::Context {
                    data_dir: config.data_dir.clone(),
                    health: health.clone(),
                    global: global_pause.clone(),
                    guard: lock.clone(),
                    boot: boot.clone(),
                    origin,
                },
            )
            .await?,
        )
    } else {
        None
    };
    let delivery = delivery_runtime
        .as_ref()
        .map(|runtime| runtime.handle.clone());
    let (stop_server, mut stop_rx) = tokio::sync::watch::channel(false);
    let mut server = AbortOnDrop(tokio::spawn(health::serve(
        listener,
        health.clone(),
        async move {
            while !*stop_rx.borrow() {
                if stop_rx.changed().await.is_err() {
                    break;
                }
            }
        },
    )));
    let (tap, mut writer) = capture::spawn_owned(
        config.data_dir.join("capture"),
        config.capture.clone(),
        health.clone(),
        lock.clone(),
    );
    let mut pipeline = Pipeline {
        decoder: Decoder::new(config.feed.dedup_capacity, config.feed.dedup_window_ms),
        ownership: lock,
        health: health.clone(),
        generation: 0,
        tap,
        seq: 0,
        boot,
        start: origin,
        sensitive: secrets.api_key.clone(),
        volume: options.preview_volume.then(|| Engine::new(256)),
        delivery,
    };
    pipeline.control("boot",json!({"config":config,"mode":if options.fixture.is_some(){"fixture"}else{"live"},"initial_state":"cold","decoder_schema":1}));
    println!(
        "{}",
        json!({"event":"collector_boot","health":format!("http://{address}/health"),"mode":if options.fixture.is_some(){"fixture"}else{"live"},"paid_requested":options.paid,"staging_delivery":options.staging_delivery,"signals_enabled":options.staging_delivery,"production_signals_enabled":false})
    );
    let timer = async {
        if let Some(duration) = options.run_for {
            tokio::time::sleep(duration).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let mut source = supervised::Input {
        config: &config,
        secrets: &secrets,
        endpoint,
        fixture: options.fixture.as_deref(),
        retry: crate::recovery::RetryPolicy::new(&config.recovery, wall_ms()),
    };
    let mut writer_outcome = None;
    let (result, server_done) = {
        let operation = supervised::run(&mut pipeline, &mut source, &config, options.paid);
        tokio::pin!(operation, shutdown, timer);
        loop {
            tokio::select! {
                result=&mut operation=>break (result,false),
                _=&mut shutdown=>break (Ok(()),false),
                _=&mut timer=>break (Ok(()),false),
                _=&mut server=>{
                    supervision::red(&health,"health");
                    break (Err("health server stopped unexpectedly".into()),true);
                },
                outcome=&mut writer, if writer_outcome.is_none()=>{
                    if outcome.is_err() || health.snapshot()["capture"]["state"] == "error" {
                        set_state(&health.capture_state,"error");
                        supervision::red(&health,"capture");
                    } else {
                        set_state(&health.capture_worker_state,"stopped");
                    }
                    writer_outcome=Some(outcome);
                },
                _=async {
                    match &mut delivery_runtime {
                        Some(runtime)=>runtime.completed().await,
                        None=>std::future::pending().await,
                    }
                }=>supervision::red(&health,"delivery"),
            }
        }
    };
    set_state(
        &health.feed_state,
        if result.is_err() {
            "failed"
        } else {
            "stopping"
        },
    );
    set_state(&health.transport_state, "stopped");
    set(&health.backoff_ms, 0);
    let delivery_result = match delivery_runtime {
        Some(sender) => sender.shutdown().await,
        None => Ok(()),
    };
    if result.is_err() {
        set_state(&health.feed_state, "failed");
        pipeline.control("outage", json!({"class":"collector_failure"}));
        if options.fixture.is_none() {
            outage::notify(secrets.webhook.as_deref(), &health, &global_pause).await;
        }
    } else {
        set_state(
            &health.feed_state,
            if options.fixture.is_some() {
                "fixture_complete"
            } else {
                "stopped"
            },
        );
    }
    pipeline.control(
        "shutdown",
        json!({"operation_clean":result.is_ok(),"capture_flush":"pending"}),
    );
    let capture_result = capture::shutdown(
        pipeline.tap,
        async move {
            match writer_outcome {
                Some(outcome) => outcome,
                None => writer.await,
            }
        },
        &health,
        Duration::from_secs(5),
    )
    .await;
    let _ = stop_server.send(true);
    if !server_done {
        let _ = tokio::time::timeout(Duration::from_secs(2), &mut server).await;
    }
    if capture_result.is_err() {
        supervision::red(&health, "capture");
    } else if health.snapshot()["supervision"]["capture"] != "red" {
        set_state(&health.capture_worker_state, "stopped");
    }
    if delivery_result.is_err() {
        supervision::red(&health, "delivery");
    } else if options.staging_delivery {
        set_state(&health.delivery_worker_state, "stopped");
    }
    if health.snapshot()["supervision"]["pipeline"] != "red" {
        set_state(
            &health.pipeline_state,
            if result.is_err() { "failed" } else { "stopped" },
        );
    }
    if !server_done {
        set_state(&health.health_worker_state, "stopped");
    }
    result?;
    capture_result?;
    delivery_result?;
    if health.snapshot()["supervision"]["state"] == "red" {
        return Err("supervised stage remains red; explicit restart required".into());
    }
    set_state(&health.supervisor_state, "stopped");
    Ok(health.snapshot())
}

async fn fixture(
    path: &std::path::Path,
    config: &Config,
    pipeline: &mut Pipeline,
) -> Result<(), String> {
    let file = std::fs::File::open(path).map_err(|_| "cannot open fixture")?;
    let mut reader = BufReader::new(file);
    set_state(&pipeline.health.feed_state, "fixture");
    loop {
        let mut bytes = Vec::new();
        let n = reader
            .by_ref()
            .take(config.feed.max_frame_bytes as u64 + 1)
            .read_until(b'\n', &mut bytes)
            .map_err(|_| "cannot read fixture")?;
        if n == 0 {
            break;
        }
        if n > config.feed.max_frame_bytes {
            return Err("fixture record exceeds feed.max_frame_bytes".into());
        }
        pipeline.ingest(Bytes::from(bytes));
        tokio::task::yield_now().await;
    }
    Ok(())
}

#[cfg(test)]
pub(crate) fn pipeline_for_test(
    data_dir: &std::path::Path,
) -> (Pipeline, tokio::sync::mpsc::Receiver<Record>) {
    let ownership = Arc::new(ProcessLock::acquire(data_dir).unwrap());
    let health = Arc::new(Health::default());
    let (tap, records) = capture::channel(8, health.clone());
    (
        Pipeline {
            decoder: Decoder::new(8, 180_000),
            health,
            generation: 1,
            ownership,
            tap,
            seq: 0,
            boot: "test".into(),
            start: Instant::now(),
            sensitive: None,
            volume: None,
            delivery: None,
        },
        records,
    )
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_recovery;

mod supervised;
