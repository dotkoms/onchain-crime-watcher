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
    pub rpc_url: Option<String>,
}
impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(
            "Secrets { api_key: [REDACTED], webhook: [REDACTED], staging_webhook: [REDACTED], rpc_url: [REDACTED] }",
        )
    }
}
impl Secrets {
    fn from_env(staging_enabled: bool, probe_enabled: bool) -> Result<Self, String> {
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
            rpc_url: if probe_enabled {
                Some(
                    std::env::var("SOLANA_RPC_URL")
                        .ok()
                        .filter(|s| !s.is_empty())
                        .ok_or("SOLANA_RPC_URL is required for enabled probing")?,
                )
            } else {
                None
            },
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

#[derive(Clone, Copy)]
pub(crate) struct FrameStamp {
    pub now_ms: u64,
    pub input_seq: u64,
    pub wall_ms: u64,
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
    history: Option<crate::history::Handle>,
    bundle: Option<crate::bundle::runtime::Handle>,
    pub last_trade: Option<crate::feed::Trade>,
    pub rpc: Option<crate::rpc::Client>,
}
impl Pipeline {
    fn record(&mut self, kind: &str, payload: Bytes) -> FrameStamp {
        self.seq += 1;
        let ns: u64 = self
            .start
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX);
        let wall = wall_ms();
        self.tap.record(Record::control(
            self.seq,
            &self.boot,
            self.generation,
            ns,
            wall,
            kind,
            payload,
        ));
        FrameStamp {
            now_ms: ns / 1_000_000,
            input_seq: self.seq,
            wall_ms: wall,
        }
    }
    fn step(&mut self, kind: &str, payload: Value) -> FrameStamp {
        if let Some(history) = &self.history {
            history.record(kind, payload.clone());
        }
        self.record(kind, Bytes::from(payload.to_string()))
    }
    pub(crate) fn control(&mut self, kind: &str, payload: Value) {
        self.step(kind, payload);
    }
    pub(crate) fn ingest(&mut self, bytes: Bytes) {
        if let Some(stamp) = self.receive(&bytes) {
            self.ingest_received(bytes, stamp, true);
        }
    }
    pub(crate) fn receive(&mut self, bytes: &Bytes) -> Option<FrameStamp> {
        self.last_trade = None;
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
            if let Some(bundle) = &self.bundle {
                bundle.submit(crate::bundle::runtime::Input::Gap(None));
            }
            self.control("redacted_frame", json!({"reason":"credential_echo"}));
            return None;
        }
        Some(self.record("frame", bytes.clone()))
    }
    pub(crate) fn ingest_received(&mut self, bytes: Bytes, stamp: FrameStamp, allow_create: bool) {
        let now = stamp.now_ms;
        self.control(
            "frame_apply",
            json!({"input_seq":stamp.input_seq,"apply_ms":now,"allow_create":allow_create}),
        );
        match self.decoder.decode(&bytes, now) {
            Ok(event) => {
                self.bundle_event(&event, &bytes, stamp, allow_create);
                if let Event::Trade(trade) = &event {
                    self.last_trade = Some(trade.clone());
                }
                if let Some(history) = &self.history {
                    let data = match &event {
                        Event::Create(mint) => {
                            let raw: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                            json!({"type":"create","mint":bs58::encode(mint).into_string(),"bonding_curve":raw.get("bondingCurveKey").and_then(Value::as_str).filter(|v|crate::feed::key::<32>(v).is_ok()),"creator":raw.get("traderPublicKey").and_then(Value::as_str).filter(|v|crate::feed::key::<32>(v).is_ok())})
                        }
                        Event::Trade(trade) => {
                            json!({"type":"trade","mint":bs58::encode(trade.mint).into_string(),"trader":bs58::encode(trade.trader).into_string(),"signature":bs58::encode(trade.signature).into_string(),"side":if trade.side==crate::feed::Side::Buy {"buy"}else{"sell"},"tokens_raw":trade.token_amount.to_string(),"sol_lamports":trade.sol_lamports.to_string(),"sol_source":match trade.sol_source {crate::feed::SolSource::Payload=>"payload",crate::feed::SolSource::ReserveDelta=>"reserve_delta",crate::feed::SolSource::Instruction=>"instruction"}})
                        }
                        Event::Migration(mint) => {
                            json!({"type":"migration","mint":bs58::encode(mint).into_string()})
                        }
                        Event::Duplicate => json!({"type":"duplicate"}),
                        Event::Unresolved => json!({"type":"unresolved"}),
                        Event::Other => json!({"type":"other"}),
                    };
                    history.record("observation",json!({"input_seq":stamp.input_seq,"received_wall_ms":stamp.wall_ms,"mono_ms":now,"source":"pumpportal","generation":self.generation,"event":data}));
                }
                match &event {
                    Event::Trade(_) => inc(&self.health.trades),
                    Event::Create(_) => inc(&self.health.creates),
                    Event::Migration(_) => inc(&self.health.migrations),
                    Event::Duplicate => inc(&self.health.duplicates),
                    Event::Unresolved => inc(&self.health.unresolved),
                    Event::Other => {}
                }
                if let Some(engine) = &mut self.volume {
                    let decisions = if matches!(event, Event::Create(_)) && !allow_create {
                        engine.advance(now)
                    } else {
                        engine.on_event(event, now)
                    }
                    .expect("monotonic receive clock");
                    self.emit_volume(decisions, now);
                }
            }
            Err(_) => {
                if let Some(bundle) = &self.bundle {
                    bundle.submit(crate::bundle::runtime::Input::Gap(None));
                }
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
    fn bundle_event(&self, event: &Event, bytes: &Bytes, stamp: FrameStamp, allow_create: bool) {
        use crate::bundle::{capture::Launch, runtime::Input};
        let Some(bundle) = &self.bundle else {
            return;
        };
        match event {
            Event::Create(mint)
                if allow_create
                    && self
                        .health
                        .paid_state
                        .lock()
                        .is_ok_and(|state| *state == "observing_auto") =>
            {
                if let Some(mut launch) =
                    Launch::new(*mint, &self.boot, stamp.now_ms, stamp.wall_ms)
                {
                    let raw: Value = serde_json::from_slice(bytes).unwrap_or(Value::Null);
                    launch.creator = raw["traderPublicKey"]
                        .as_str()
                        .and_then(|s| crate::feed::key::<32>(s).ok());
                    launch.curve = raw["bondingCurveKey"]
                        .as_str()
                        .and_then(|s| crate::feed::key::<32>(s).ok());
                    bundle.submit(Input::Create(Box::new(launch)));
                }
            }
            Event::Trade(trade) => {
                let raw: Value = serde_json::from_slice(bytes).unwrap_or(Value::Null);
                let balance = raw
                    .get("newTokenBalance")
                    .and_then(|v| crate::feed::units(v.as_str().unwrap_or(&v.to_string()), 6));
                bundle.submit(Input::Trade {
                    seq: stamp.input_seq,
                    mono: stamp.now_ms,
                    wall: stamp.wall_ms,
                    trade: trade.clone(),
                    balance,
                });
            }
            Event::Migration(mint) => bundle.submit(Input::Gap(Some(*mint))),
            Event::Unresolved => bundle.submit(Input::Gap(None)),
            _ => {}
        }
    }
    pub(crate) fn discard_received(&mut self, stamp: FrameStamp, reason: &str) {
        self.control(
            "discarded_frame",
            json!({"input_seq":stamp.input_seq,"apply_ms":stamp.now_ms,"reason":reason}),
        );
        if let Some(engine) = &mut self.volume {
            let decisions = engine
                .advance(stamp.now_ms)
                .expect("monotonic receive clock");
            self.emit_volume(decisions, stamp.now_ms);
        }
    }
    pub(crate) fn forget_mint(&mut self, mint: &[u8; 32], now: u64) {
        if let Some(bundle) = &self.bundle {
            bundle.submit(crate::bundle::runtime::Input::Forget(*mint));
        }
        self.control(
            "watch_apply",
            json!({"operation":"remove","mint":bs58::encode(mint).into_string(),"apply_ms":now}),
        );
        if let Some(engine) = &mut self.volume {
            engine.forget(mint);
        }
    }
    pub(crate) fn attach_mint(
        &mut self,
        mint: [u8; 32],
        created: Option<u64>,
        now: u64,
        partial: bool,
    ) {
        self.control("watch_apply",json!({"operation":"attach","mint":bs58::encode(mint).into_string(),"observed_create_ms":created,"apply_ms":now,"partial":partial}));
        if let (Some(engine), Some(created)) = (&mut self.volume, created) {
            engine.attach_observed(mint, created, now, partial);
        }
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
    pub(crate) fn now_ms(&self) -> u64 {
        self.start
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }
    pub(crate) fn tick_volume(&mut self) {
        if let Some(bundle) = &self.bundle {
            bundle.submit(crate::bundle::runtime::Input::Clock(self.now_ms()));
        }
        let now = self.step("clock_tick", json!({})).now_ms;
        if let Some(engine) = &mut self.volume {
            let decisions = engine.advance(now).expect("monotonic receive clock");
            self.emit_volume(decisions, now);
            self.volume_health();
        }
    }
    pub(crate) fn reset_volume(&mut self) {
        if let Some(bundle) = &self.bundle {
            bundle.submit(crate::bundle::runtime::Input::Gap(None));
        }
        let now = self.step("volume_reset", json!({})).now_ms;
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
        Secrets::from_env(
            options.staging_delivery,
            config.probe.enabled || config.bundle.funding,
        )?
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
        && (options.fixture.is_some()
            || (config.feed.mint.is_none() && !config.feed.auto_watch)
            || secrets.api_key.is_none())
    {
        return Err(
            "paid mode requires feed.mint or auto_watch, OCW_PUMPPORTAL_KEY, and live mode".into(),
        );
    }
    if options.fixture.is_some() && config.monitor.enabled {
        return Err("monitoring requires explicit live collection".into());
    }
    if options.fixture.is_some() && config.history.enabled {
        return Err("durable history requires live operation".into());
    }
    if config.probe.enabled && options.fixture.is_some() {
        return Err("chain probing requires live collection".into());
    }
    if options.fixture.is_none() && crate::storage::restore_review_required(&config.data_dir) {
        return Err(
            "restored storage requires spend and delivery reconciliation before live startup"
                .into(),
        );
    }
    if config.cto.enabled && options.fixture.is_some() {
        return Err("CTO polling requires live mode".into());
    }
    if config.cto.staging_delivery && !options.staging_delivery {
        return Err("CTO staging delivery requires --enable-staging-delivery".into());
    }
    if config.cto.staging_delivery {
        let raw =
            std::env::var("OCW_DISCORD_CTO_URL").map_err(|_| "OCW_DISCORD_CTO_URL is required")?;
        let url = crate::discord::webhook_url(&raw)?;
        if [
            secrets.webhook.as_deref(),
            secrets.staging_webhook.as_deref(),
        ]
        .into_iter()
        .flatten()
        .any(|raw| crate::discord::webhook_url(raw).ok().as_ref() == Some(&url))
        {
            return Err("CTO delivery requires a distinct webhook".into());
        }
    }
    if config.bundle.enabled
        && (options.fixture.is_some() || !options.paid || !config.feed.auto_watch)
    {
        return Err("bundle capture requires live paid automatic watching".into());
    }
    let lock = Arc::new(
        ProcessLock::acquire(&config.data_dir)
            .map_err(|_| "cannot acquire private data directory/collector lock")?,
    );
    let rpc = if config.probe.enabled || config.bundle.funding {
        Some(
            crate::rpc::Client::open(
                secrets
                    .rpc_url
                    .as_deref()
                    .ok_or("SOLANA_RPC_URL is required")?,
                &config.data_dir,
                lock.clone(),
                config.probe.calls_per_day,
                config.probe.calls_per_minute,
            )
            .await
            .map_err(|_| "cannot initialize chain RPC (credentials redacted)")?,
        )
    } else {
        None
    };
    let listener = tokio::net::TcpListener::bind(config.health_bind)
        .await
        .map_err(|_| "cannot bind loopback health endpoint")?;
    let address = listener
        .local_addr()
        .map_err(|_| "cannot read health address")?;
    let health = Arc::new(Health::default());
    set(&health.paid_requested, u64::from(options.paid));
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
    let _ = health.monitor_origin.set(origin);
    health.pulse(&health.root_tick_ms);
    set(&health.monitor_stall_ms, config.monitor.stall_ms);
    set(&health.monitor_enabled, u64::from(config.monitor.enabled));
    set(
        &health.heartbeat_discord_enabled,
        u64::from(config.monitor.discord_status),
    );
    set(&health.deadman_enabled, u64::from(config.monitor.deadman));
    set_state(
        &health.monitor_worker_state,
        if config.monitor.enabled {
            "starting"
        } else {
            "disabled"
        },
    );
    set(&health.bundle_enabled, u64::from(config.bundle.enabled));
    set_state(
        &health.bundle_analysis_state,
        if config.bundle.funding {
            "idle"
        } else {
            "disabled"
        },
    );
    set_state(
        &health.bundle_state,
        if config.bundle.enabled {
            "starting"
        } else {
            "disabled"
        },
    );
    let mut bundle_runtime = if config.bundle.enabled {
        Some(
            crate::bundle::runtime::Runtime::start(
                config.data_dir.join("launch"),
                config.bundle.clone(),
                lock.clone(),
                health.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    let bundle = bundle_runtime
        .as_ref()
        .map(|runtime| runtime.handle.clone());
    set(&health.cto_enabled, u64::from(config.cto.enabled));
    set_state(
        &health.cto_worker_state,
        if config.cto.enabled {
            "starting"
        } else {
            "disabled"
        },
    );
    set_state(
        &health.cto_state,
        if config.cto.enabled {
            "starting"
        } else {
            "disabled"
        },
    );
    let mut cto_runtime = if config.cto.enabled {
        Some(
            crate::cto::runtime::Runtime::start(
                config.cto.clone(),
                config.data_dir.clone(),
                lock.clone(),
                global_pause.clone(),
                health.clone(),
                config
                    .bundle
                    .enabled
                    .then(|| crate::bundle::integration::Integration {
                        root: config.data_dir.join("launch"),
                        rpc: if config.bundle.funding {
                            rpc.clone()
                        } else {
                            None
                        },
                        config: config.bundle.clone(),
                        boot: boot.clone(),
                        lock: Arc::new(std::sync::Mutex::new(())),
                        reads: Arc::new(tokio::sync::Semaphore::new(2)),
                    }),
            )
            .await?,
        )
    } else {
        None
    };
    let mut monitor_runtime = if config.monitor.enabled {
        Some(
            crate::monitor::Runtime::start(
                config.monitor.clone(),
                secrets.webhook.as_deref(),
                &config.data_dir,
                health.clone(),
                global_pause.clone(),
                lock.clone(),
                boot.clone(),
            )
            .await?,
        )
    } else {
        None
    };
    let mut history_runtime = if config.history.enabled {
        Some(
            crate::history::Runtime::start(
                config.data_dir.join("history"),
                boot.clone(),
                config.history.max_bytes,
                health.clone(),
                lock.clone(),
            )
            .await?,
        )
    } else {
        set_state(&health.history_state, "disabled");
        None
    };
    let history = history_runtime
        .as_ref()
        .map(|runtime| runtime.handle.clone());
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
                    history: history.clone(),
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
        history: history.clone(),
        bundle,
        last_trade: None,
        rpc,
    };
    pipeline.control("boot",json!({"config":config,"mode":if options.fixture.is_some(){"fixture"}else{"live"},"initial_state":"cold","decoder_schema":1,"execution_schema":1,"watch_schema":1,"spend_schema":1,"volume_enabled":options.preview_volume}));
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
    let mut scheduler_tick = tokio::time::interval(Duration::from_secs(1));
    scheduler_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let (result, server_done) = {
        let operation = supervised::run(&mut pipeline, &mut source, &config, options.paid);
        tokio::pin!(operation, shutdown, timer);
        loop {
            tokio::select! {
                result=&mut operation=>break (result,false),
                _=scheduler_tick.tick()=>{
                    health.pulse(&health.root_tick_ms);
                    if let Some(history)=&history {history.record("coverage_tick",json!({"coverage":"observed_subset_unverified"}));}
                },
                _=async {match &mut bundle_runtime {Some(runtime)=>runtime.completed().await,None=>std::future::pending().await}}=>supervision::red(&health,"bundle"),
                _=async {match &mut cto_runtime {Some(runtime)=>runtime.completed().await,None=>std::future::pending().await}}=>supervision::red(&health,"cto"),
                _=async {match &mut history_runtime {Some(runtime)=>runtime.completed().await,None=>std::future::pending().await}}=>supervision::red(&health,"history"),
                _=async {
                    match &mut monitor_runtime {
                        Some(runtime)=>runtime.completed().await,
                        None=>std::future::pending().await,
                    }
                }=>supervision::red(&health,"monitor"),
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
    let cto_result = match cto_runtime {
        Some(runtime) => runtime.shutdown().await,
        None => Ok(()),
    };
    pipeline.bundle = None;
    let bundle_result = match bundle_runtime {
        Some(runtime) => runtime.shutdown().await,
        None => Ok(()),
    };
    pipeline.history = None;
    let monitor_result = match monitor_runtime {
        Some(runtime) => runtime.shutdown().await,
        None => Ok(()),
    };
    let delivery_result = match delivery_runtime {
        Some(sender) => sender.shutdown().await,
        None => Ok(()),
    };
    // Delivery's final receipt and queue-clear evidence must precede history sealing.
    let history_result = match history_runtime {
        Some(runtime) => runtime.shutdown().await,
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
    bundle_result.map_err(str::to_string)?;
    cto_result.map_err(str::to_string)?;
    result?;
    monitor_result?;
    history_result?;
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
            history: None,
            bundle: None,
            last_trade: None,
            rpc: None,
        },
        records,
    )
}

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_recovery;

mod supervised;
