use crate::{
    capture::{self, Record, Tap},
    config::Config,
    feed::{Decoder, Event},
    health::{self, Health, inc, set, set_state},
    outage,
    storage::ProcessLock,
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
    pub fixture: Option<PathBuf>,
    pub run_for: Option<Duration>,
}
#[derive(Default)]
pub(crate) struct Secrets {
    pub api_key: Option<String>,
    pub webhook: Option<String>,
}
impl std::fmt::Debug for Secrets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Secrets { api_key: [REDACTED], webhook: [REDACTED] }")
    }
}
impl Secrets {
    fn from_env() -> Result<Self, String> {
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
        Ok(Self { api_key, webhook })
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
    tap: Tap,
    seq: u64,
    boot: String,
    start: Instant,
    sensitive: Option<String>,
    volume: Option<Engine>,
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
        Secrets::from_env()?
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
    if options.preview_volume && options.fixture.is_some() {
        return Err("volume previews require live mode or recorded capture replay".into());
    }
    if options.paid
        && (options.fixture.is_some() || config.feed.mint.is_none() || secrets.api_key.is_none())
    {
        return Err("paid mode requires feed.mint, OCW_PUMPPORTAL_KEY, and live mode".into());
    }
    let _lock = ProcessLock::acquire(&config.data_dir)
        .map_err(|_| "cannot acquire private data directory/collector lock")?;
    let listener = tokio::net::TcpListener::bind(config.health_bind)
        .await
        .map_err(|_| "cannot bind loopback health endpoint")?;
    let address = listener
        .local_addr()
        .map_err(|_| "cannot read health address")?;
    let health = Arc::new(Health::default());
    set_state(&health.paid_state, "off");
    set_state(&health.feed_state, "starting");
    set(&health.volume_enabled, u64::from(options.preview_volume));
    let (stop_server, mut stop_rx) = tokio::sync::watch::channel(false);
    let mut server = tokio::spawn(health::serve(listener, health.clone(), async move {
        while !*stop_rx.borrow() {
            if stop_rx.changed().await.is_err() {
                break;
            }
        }
    }));
    let (tap, writer) = capture::spawn(
        config.data_dir.join("capture"),
        config.capture.clone(),
        health.clone(),
    );
    let boot = format!("{:x}{:x}", wall_ms(), std::process::id());
    let mut pipeline = Pipeline {
        decoder: Decoder::new(config.feed.dedup_capacity, config.feed.dedup_window_ms),
        health: health.clone(),
        generation: 0,
        tap,
        seq: 0,
        boot,
        start: Instant::now(),
        sensitive: secrets.api_key.clone(),
        volume: options.preview_volume.then(|| Engine::new(256)),
    };
    pipeline.control("boot",json!({"config":config,"mode":if options.fixture.is_some(){"fixture"}else{"live"},"initial_state":"cold","decoder_schema":1}));
    println!(
        "{}",
        json!({"event":"collector_boot","health":format!("http://{address}/health"),"mode":if options.fixture.is_some(){"fixture"}else{"live"},"paid_requested":options.paid,"signals_enabled":false})
    );
    let timer = async {
        if let Some(duration) = options.run_for {
            tokio::time::sleep(duration).await;
        } else {
            std::future::pending::<()>().await;
        }
    };
    let operation = async {
        if let Some(path) = &options.fixture {
            fixture(path, &config, &mut pipeline).await
        } else {
            crate::session::live(&config, &mut pipeline, &secrets, endpoint, options.paid).await
        }
    };
    let (result, server_done) = tokio::select! {
        result=operation=>(result,false),
        _=shutdown=>(Ok(()),false),
        _=timer=>(Ok(()),false),
        _=&mut server=>(Err("health server stopped unexpectedly".into()),true),
    };
    if result.is_err() {
        set_state(&health.feed_state, "failed");
        pipeline.control("outage", json!({"class":"collector_failure"}));
        if options.fixture.is_none() {
            outage::notify(secrets.webhook.as_deref(), &health).await;
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
    pipeline.control("shutdown", json!({"clean":result.is_ok()}));
    pipeline.tap.finish().await;
    let writer_result = tokio::time::timeout(Duration::from_secs(5), writer).await;
    if !matches!(writer_result, Ok(Ok(()))) {
        set_state(&health.capture_state, "error");
    }
    let _ = stop_server.send(true);
    if !server_done {
        let _ = tokio::time::timeout(Duration::from_secs(2), server).await;
    }
    result?;
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
mod tests {
    use super::*;
    use futures_util::{SinkExt, StreamExt};
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[tokio::test]
    async fn paid_stop_closes_first_socket_before_free_only_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = crate::config::Config {
            data_dir: dir.path().join("data"),
            ..crate::config::Config::default()
        };
        crate::storage::private_dir(&config.data_dir).unwrap();
        config.health_bind = "127.0.0.1:0".parse().unwrap();
        config.capture.min_free_bytes = 0;
        config.feed.mint = Some(bs58::encode([1u8; 32]).into_string());
        config.gate = crate::config::GateConfig {
            daily_messages: 8,
            stop_margin_messages: 2,
            reservation_messages: 2,
        };
        crate::spend::Ledger::initialize(
            &config.data_dir.join("feed-spend.json"),
            wall_ms() / 86_400_000,
        )
        .unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            url::Url::parse(&format!("ws://{}", listener.local_addr().unwrap())).unwrap();
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let provider = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            let mut methods = Vec::new();
            for _ in 0..3 {
                let raw = socket.next().await.unwrap().unwrap().into_data();
                methods.push(
                    serde_json::from_slice::<serde_json::Value>(&raw).unwrap()["method"]
                        .as_str()
                        .unwrap()
                        .to_string(),
                );
            }
            assert_eq!(
                methods,
                vec![
                    "subscribeNewToken",
                    "subscribeMigration",
                    "subscribeTokenTrade"
                ]
            );
            socket.send(Message::Text(serde_json::json!({"txType":"create","mint":bs58::encode([1u8;32]).into_string()}).to_string().into())).await.unwrap();
            for n in 1..=6 {
                let trade = serde_json::json!({"txType":"buy","mint":bs58::encode([1u8;32]).into_string(),"traderPublicKey":bs58::encode([2u8;32]).into_string(),"signature":bs58::encode([n;64]).into_string(),"tokenAmount":1,"solAmount":1});
                socket
                    .send(Message::Text(trade.to_string().into()))
                    .await
                    .unwrap();
            }
            assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
            drop(socket);
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = accept_async(stream).await.unwrap();
            for expected in ["subscribeNewToken", "subscribeMigration"] {
                let raw = socket.next().await.unwrap().unwrap().into_data();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&raw).unwrap()["method"],
                    expected
                );
            }
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), socket.next())
                    .await
                    .is_err()
            );
            stop_tx.send(()).unwrap();
            // Keep the replacement transport alive until the collector handles shutdown.
            // Dropping it here races the shutdown signal against a transport failure.
            let _ = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next()).await;
        });
        let secrets = Secrets {
            api_key: Some("unit-test-key-do-not-log".into()),
            webhook: None,
        };
        let options = Options {
            paid: true,
            preview_volume: true,
            ..Options::default()
        };
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            run_inner(config, options, secrets, endpoint, async {
                let _ = stop_rx.await;
            }),
        )
        .await
        .unwrap()
        .unwrap();
        provider.await.unwrap();
        assert_eq!(result["paid"]["reserved_messages"], 6);
        assert_eq!(result["paid"]["billable_received"], 6);
        assert_eq!(result["paid"]["stops"], 1);
        assert_eq!(result["feed"]["trades"], 6);
        assert_eq!(result["feed"]["generation"], 2);
        assert_eq!(result["volume"]["preview_enabled"], true);
        assert_eq!(result["volume"]["active_mints"], 1);
        assert_eq!(result["volume"]["unknown_mint_trades"], 0);
        assert_eq!(result["volume"]["fire_candidates"], 0);
        assert!(!result.to_string().contains("unit-test-key"));
    }

    #[tokio::test]
    async fn health_endpoint_reports_counts_without_credentials() {
        let health = std::sync::Arc::new(crate::health::Health::default());
        crate::health::inc(&health.frames);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(crate::health::serve(listener, health, async {
            let _ = rx.await;
        }));
        let body: serde_json::Value = reqwest::get(format!("http://{addr}/health"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(body["feed"]["frames"], 1);
        assert_eq!(body["signals_enabled"], false);
        assert!(body.get("api_key").is_none());
        stop.send(()).unwrap();
        task.await.unwrap().unwrap();
    }
}
