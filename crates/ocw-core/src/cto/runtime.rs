use super::{Claim, Config, Store};
use crate::{
    collector::wall_ms,
    delivery::http::{Http, Outcome},
    discord::{self, Pause},
    health::{Health, inc, set, set_state},
    storage::ProcessLock,
};
use serde_json::Value;
use std::{
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{sync::watch, task::JoinHandle};
const ENDPOINT: &str = "https://api.dexscreener.com/community-takeovers/latest/v1";
type Shared = Arc<Mutex<Store>>;
pub(super) async fn edit<T: Send + 'static>(
    store: Shared,
    guard: Arc<ProcessLock>,
    f: impl FnOnce(&mut Store) -> Result<T, &'static str> + Send + 'static,
) -> Result<T, &'static str> {
    tokio::task::spawn_blocking(move || {
        let _guard = guard;
        let mut state = store.lock().map_err(|_| "CTO lock failed")?;
        f(&mut state)
    })
    .await
    .map_err(|_| "CTO storage task failed")?
}
pub(crate) struct Runtime {
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<Result<(), &'static str>>>,
}
impl Drop for Runtime {
    fn drop(&mut self) {
        let _ = self.stop.send(true);
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}
impl Runtime {
    pub async fn start(
        config: Config,
        data: std::path::PathBuf,
        guard: Arc<ProcessLock>,
        global: Arc<Pause>,
        health: Arc<Health>,
        integration: Option<crate::bundle::integration::Integration>,
        evidence: crate::notifications::Evidence,
    ) -> Result<Self, String> {
        let sender = if config.staging_delivery {
            let raw = std::env::var("OCW_DISCORD_CTO_URL")
                .map_err(|_| "OCW_DISCORD_CTO_URL is required")?;
            let mut http = Http::new(
                &raw,
                config.channel_id.as_deref().ok_or("CTO channel required")?,
                Duration::from_secs(2),
                global.clone(),
            )?;
            http.verify().await?;
            http.trace(&evidence, "cto");
            Some(http)
        } else {
            None
        };
        let keep = guard.clone();
        let store = tokio::task::spawn_blocking(move || {
            let _keep = keep;
            let dir = data.join("cto");
            crate::storage::private_dir(&dir).map_err(|_| "CTO directory unavailable")?;
            Store::open(dir.join("state.json"))
        })
        .await
        .map_err(|_| "CTO storage startup failed")??;
        let (stop, rx) = watch::channel(false);
        let state = Arc::new(Mutex::new(store));
        let task = tokio::spawn(async move {
            set_state(&health.cto_worker_state, "running");
            let result = run(
                Inputs {
                    config,
                    store: state,
                    guard,
                    global,
                    health: health.clone(),
                    sender,
                    integration,
                },
                rx,
                ENDPOINT,
            )
            .await;
            set_state(
                &health.cto_worker_state,
                if result.is_ok() { "stopped" } else { "error" },
            );
            result
        });
        Ok(Self {
            stop,
            task: Some(task),
        })
    }
    pub async fn completed(&mut self) {
        if let Some(task) = &mut self.task {
            let _ = task.await;
            self.task = None;
        } else {
            std::future::pending::<()>().await;
        }
    }
    pub async fn shutdown(mut self) -> Result<(), &'static str> {
        let _ = self.stop.send(true);
        let task = self.task.as_mut().ok_or("CTO worker ended unexpectedly")?;
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .map_err(|_| "CTO shutdown timed out")?
            .map_err(|_| "CTO worker failed")?
    }
}
#[derive(Clone)]
pub(super) struct Inputs {
    pub(super) config: Config,
    pub(super) store: Shared,
    pub(super) guard: Arc<ProcessLock>,
    pub(super) global: Arc<Pause>,
    pub(super) health: Arc<Health>,
    pub(super) sender: Option<Http>,
    pub(super) integration: Option<crate::bundle::integration::Integration>,
}
async fn run(
    context: Inputs,
    mut stop: watch::Receiver<bool>,
    endpoint: &str,
) -> Result<(), &'static str> {
    let client = discord::client(Duration::from_secs(2)).map_err(|_| "CTO HTTP startup failed")?;
    let mut poll = tokio::time::interval(Duration::from_secs(context.config.poll_s));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut beat = tokio::time::interval(Duration::from_secs(1));
    beat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut prime = true;
    let mut paused_until = edit(context.store.clone(), context.guard.clone(), |s| {
        Ok(s.state.source_paused_until)
    })
    .await?;
    type JobResult = (Option<[u8; 32]>, Result<(), &'static str>);
    let mut jobs: tokio::task::JoinSet<JobResult> = tokio::task::JoinSet::new();
    let mut refreshing = false;
    let mut inflight = std::collections::BTreeSet::new();
    let mut last_refresh = Instant::now();
    let result = 'worker: loop {
        tokio::select! {biased;
            _=stop.changed()=>break 'worker Ok(()),
            result=jobs.join_next(),if !jobs.is_empty()=>{
                if let Some(Ok((mint,result)))=result {if let Some(mint)=mint {inflight.remove(&mint);}else{refreshing=false;}
                if result.is_err(){inc(&context.health.bundle_analysis_errors);}}
                else {inc(&context.health.bundle_analysis_errors);}
            },
            _=beat.tick()=>{
                context.health.pulse(&context.health.cto_tick_ms);
                if last_refresh.elapsed()>=Duration::from_secs(45) && jobs.len()<2 && !refreshing && context.integration.is_some() {
                    last_refresh=Instant::now();refreshing=true;let context=context.clone();let skip=inflight.clone();
                    jobs.spawn(async move{(None,super::bundle_jobs::sweep(context,skip).await)});
                }
            },
            _=poll.tick()=>{
                if wall_ms()<paused_until {continue;}
                let operation=async {
                    let response=match client.get(endpoint).send().await {Ok(response)=>response,Err(_)=>{inc(&context.health.cto_errors);set_state(&context.health.cto_state,"fetch_failed");return Ok(());}};
                    if response.status().as_u16()==429 {
                        let delay=response.headers().get("retry-after").and_then(|v|v.to_str().ok()).and_then(|v|v.parse::<u64>().ok()).filter(|n|*n<=86400).unwrap_or(60).max(60);
                        paused_until=wall_ms().saturating_add(delay*1000);
                        let until=paused_until;edit(context.store.clone(),context.guard.clone(),move|s|{let mut state=s.state.clone();state.source_paused_until=until;s.save(state)}).await?;
                        set_state(&context.health.cto_state,"source_rate_limited");inc(&context.health.cto_errors);return Ok(());
                    }
                    if !response.status().is_success(){inc(&context.health.cto_errors);set_state(&context.health.cto_state,"fetch_failed");return Ok(());}
                    let mut response=response;let mut bytes=Vec::new();
                    while let Some(chunk)=response.chunk().await.map_err(|_|"CTO body unreadable")? {
                        if bytes.len()+chunk.len()>1024*1024 {return Err("CTO response exceeds limit");}bytes.extend(chunk);
                    }
                    let value:Value=serde_json::from_slice(&bytes).map_err(|_|"invalid CTO source response")?;
                    let now=wall_ms();let initialized=prime;
                    let (claims,suppressed,unknown_dates)=edit(context.store.clone(),context.guard.clone(),move|store|{let claims=store.observe(&value,wall_ms(),initialized)?;Ok((claims,store.state.suppressed,store.state.unknown_dates))}).await?;
                    set(&context.health.cto_suppressed,suppressed);set(&context.health.cto_unknown_dates,unknown_dates);
                    prime=false;set(&context.health.cto_last_success_ms,now);set_state(&context.health.cto_state,"ready");inc(&context.health.cto_polls);
                    let admitted_at=Instant::now();
                    for claim in claims {context.health.pulse(&context.health.cto_tick_ms);
                        if let Some(job)=deliver(&context,claim,admitted_at).await? {
                            if jobs.len()<2 && !inflight.contains(&job.claim.mint) {let mint=job.claim.mint;inflight.insert(mint);let context=context.clone();jobs.spawn(async move{(Some(mint),super::bundle_jobs::run(context,job).await)});}else{inc(&context.health.bundle_analysis_dropped);}
                        }
                    }
                    Ok::<_,&'static str>(())
                };
                tokio::select! {biased;_=stop.changed()=>break 'worker Ok(()),result=operation=>if let Err(error)=result {break 'worker Err(error)}}
            }
        }
    };
    jobs.shutdown().await;
    result
}
async fn deliver(
    context: &Inputs,
    claim: Claim,
    at: Instant,
) -> Result<Option<super::bundle_jobs::Job>, &'static str> {
    let lookup = if let Some(integration) = &context.integration {
        integration
            .launch(claim.mint, context.guard.clone(), &context.health)
            .await
    } else {
        Ok(None)
    };
    let unavailable = lookup.is_err();
    let launch = lookup.ok().flatten();
    let payload = if context.integration.is_some() {
        crate::bundle::integration::card(&claim, launch.as_ref(), None, unavailable)
    } else {
        claim.card()
    };
    inc(&context.health.cto_observed);
    let key = claim.clone();
    let Some(sender) = &context.sender else {
        edit(context.store.clone(), context.guard.clone(), move |s| {
            s.finish(&key, "preview", None, 0)
        })
        .await?;
        println!(
            "{}",
            serde_json::json!({"event":"cto_listing_preview","source":"dexscreener_curated_claim","claim":claim,"card":payload,"sent":false})
        );
        return Ok(
            if context
                .integration
                .as_ref()
                .is_some_and(|i| i.rpc.is_some())
            {
                launch.map(|launch| super::bundle_jobs::Job {
                    claim,
                    message: None,
                    launch,
                    refresh_only: false,
                })
            } else {
                None
            },
        );
    };
    if at.elapsed() >= Duration::from_secs(120) || !context.global.remaining().is_zero() {
        edit(context.store.clone(), context.guard.clone(), move |s| {
            s.finish(&key, "expired_or_paused", None, 0)
        })
        .await?;
        return Ok(None);
    }
    let policy = context.config.clone();
    let allowed = edit(context.store.clone(), context.guard.clone(), move |s| {
        s.reserve(&key, wall_ms(), &policy)
    })
    .await?;
    if !allowed {
        inc(&context.health.cto_budget_dropped);
        return Ok(None);
    }
    let report = sender
        .post_named(
            &bs58::encode(claim.mint).into_string(),
            &payload,
            at + Duration::from_secs(120),
        )
        .await;
    let (outcome, id) = match report.outcome {
        Outcome::Sent(id) => {
            inc(&context.health.cto_sent);
            ("sent", Some(id))
        }
        Outcome::Unknown => {
            inc(&context.health.cto_unknown);
            ("unknown", None)
        }
        Outcome::RateLimited => ("rate_limited", None),
        Outcome::Rejected => ("rejected", None),
        Outcome::Broken | Outcome::WrongTarget => {
            edit(context.store.clone(), context.guard.clone(), move |s| {
                s.finish(&claim, "delivery_blocked", None, 0)
            })
            .await?;
            return Err("CTO webhook blocked or changed target");
        }
    };
    let sent_id = id.clone();
    let retained = claim.clone();
    let until = wall_ms().saturating_add(report.pause.as_millis().try_into().unwrap_or(u64::MAX));
    edit(context.store.clone(), context.guard.clone(), move |s| {
        s.finish(&claim, outcome, id, until)
    })
    .await?;
    if let (Some(id), Some(integration)) = (&sent_id, &context.integration) {
        let target = context.sender.as_ref().expect("checked sender").identity();
        if integration
            .edit_meta(
                retained.mint,
                Some(crate::bundle::integration::Edit {
                    schema: 1,
                    message: id.clone(),
                    tier: 0,
                    wall_ms: wall_ms(),
                    pending: false,
                    enriched: false,
                    target_sha256: target,
                }),
                context.guard.clone(),
            )
            .await
            .is_err()
        {
            inc(&context.health.bundle_analysis_errors);
            return Ok(None);
        }
    }
    Ok(
        if sent_id.is_some()
            && context
                .integration
                .as_ref()
                .is_some_and(|i| i.rpc.is_some())
        {
            launch.map(|launch| super::bundle_jobs::Job {
                claim: retained,
                message: sent_id,
                launch,
                refresh_only: false,
            })
        } else {
            None
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Router,
        extract::{Json, State},
        http::StatusCode,
        routing::get,
    };
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    #[tokio::test]
    async fn cto_sends_independently_of_trade_health_and_never_reposts_unknown_outcomes() {
        let count = Arc::new(AtomicU64::new(0));
        let unknown = Arc::new(AtomicBool::new(false));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = url::Url::parse(&format!(
            "http://{}/api/webhooks/1/unit",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let app = Router::new()
            .route(
                "/api/webhooks/1/unit",
                get(|| async { Json(json!({"id":"1","type":1,"channel_id":"22"})) }).post(
                    |State((count, unknown)): State<(Arc<AtomicU64>, Arc<AtomicBool>)>,
                     Json(body): Json<Value>| async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(body["allowed_mentions"]["parse"], json!([]));
                        if unknown.load(Ordering::SeqCst) {
                            (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({})))
                        } else {
                            (StatusCode::OK, Json(json!({"id":"99","channel_id":"22"})))
                        }
                    },
                ),
            )
            .with_state((count.clone(), unknown.clone()));
        let server = crate::supervision::AbortOnDrop(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let guard = Arc::new(ProcessLock::acquire(&data).unwrap());
        let path = data.join("cto-state.json");
        let mut store = Store::open(path.clone()).unwrap();
        let now = wall_ms();
        store.observe(&json!([]), now, true).unwrap();
        let rows = |key| json!([{"chainId":"solana","tokenAddress":bs58::encode([key;32]).into_string(),"claimDate":"1970-01-01T00:00:01Z"}]);
        let first = store.observe(&rows(1), now, false).unwrap().pop().unwrap();
        let second = store.observe(&rows(2), now, false).unwrap().pop().unwrap();
        let health = Arc::new(Health::default());
        set_state(&health.feed_state, "failed");
        let global = Arc::new(Pause::default());
        let history = crate::history::Runtime::start(
            data.join("history"),
            "ctotest".into(),
            16 * 1024 * 1024,
            health.clone(),
            guard.clone(),
        )
        .await
        .unwrap();
        let mut http = Http::local(endpoint, global.clone());
        http.trace(
            &crate::notifications::Evidence {
                boot: "ctotest".into(),
                history: Some(history.handle.clone()),
            },
            "cto",
        );
        http.verify().await.unwrap();
        let context = Inputs {
            config: Config::default(),
            store: Arc::new(Mutex::new(store)),
            guard,
            global,
            health: health.clone(),
            sender: Some(http),
            integration: None,
        };
        deliver(&context, first.clone(), Instant::now())
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(health.cto_sent.load(Ordering::Relaxed), 1);
        unknown.store(true, Ordering::SeqCst);
        deliver(&context, second.clone(), Instant::now())
            .await
            .unwrap();
        assert_eq!(health.cto_unknown.load(Ordering::Relaxed), 1);
        deliver(&context, first, Instant::now()).await.unwrap();
        deliver(&context, second, Instant::now()).await.unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 2);
        drop(context);
        history.shutdown().await.unwrap();
        let receipts = crate::notifications::run(&data.join("history/observations.ocwh")).unwrap();
        assert_eq!(receipts["requests"], 2);
        assert_eq!(receipts["observed_unknown"], 1);
        let restored = Store::open(path).unwrap();
        assert_eq!(
            restored.state.claims[&bs58::encode([2; 32]).into_string()].outcome,
            "unknown"
        );
        assert_eq!(
            restored.state.claims[&bs58::encode([1; 32]).into_string()]
                .message_id
                .as_deref(),
            Some("99")
        );
        drop(server);
    }
}

#[cfg(test)]
mod poll_tests {
    use super::*;
    use axum::{
        Router,
        extract::{Json, State},
        routing::get,
    };
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};
    #[tokio::test]
    async fn source_poll_primes_then_records_real_response_claims_without_trade_feed_dependency() {
        let seen = Arc::new(AtomicU64::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let app=Router::new().route("/",get(|State(seen):State<Arc<AtomicU64>>|async move {
            let number=seen.fetch_add(1,Ordering::SeqCst);
            Json(if number==0 {json!([])}else{json!([{"chainId":"solana","tokenAddress":bs58::encode([1;32]).into_string(),"claimDate":"1970-01-01T00:00:01Z","description":"ignored".repeat(5000)}])})
        })).with_state(seen.clone());
        let server = crate::supervision::AbortOnDrop(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let guard = Arc::new(ProcessLock::acquire(&data).unwrap());
        let path = data.join("state.json");
        let health = Arc::new(Health::default());
        set_state(&health.feed_state, "failed");
        let (stop, rx) = watch::channel(false);
        // A shorter interval is confined to this isolated worker test; public config requires >=60s.
        let context = Inputs {
            config: Config {
                poll_s: 1,
                ..Config::default()
            },
            store: Arc::new(Mutex::new(Store::open(path.clone()).unwrap())),
            guard,
            global: Arc::new(Pause::default()),
            health: health.clone(),
            sender: None,
            integration: None,
        };
        let worker = tokio::spawn(async move { run(context, rx, &endpoint).await });
        tokio::time::timeout(Duration::from_secs(4), async {
            while !Store::open(path.clone())
                .is_ok_and(|s| s.state.claims.values().any(|row| row.outcome == "preview"))
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        stop.send(true).unwrap();
        worker.await.unwrap().unwrap();
        let state = Store::open(path).unwrap();
        assert_eq!(state.state.claims.len(), 1);
        assert_eq!(
            state.state.claims.values().next().unwrap().outcome,
            "preview"
        );
        assert_eq!(health.cto_observed.load(Ordering::Relaxed), 1);
        assert_eq!(health.cto_sent.load(Ordering::Relaxed), 0);
        assert_eq!(health.cto_polls.load(Ordering::Relaxed), 2);
        drop(server);
    }
}
