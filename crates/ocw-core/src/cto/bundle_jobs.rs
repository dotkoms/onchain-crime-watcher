use super::{
    Claim,
    runtime::{Inputs, edit},
};
use crate::{
    bundle::{
        Tier,
        capture::Launch,
        integration::{Edit, card},
        rpc::Report,
    },
    collector::wall_ms,
    delivery::http::Outcome,
    health::{inc, set_state},
};
use std::{
    collections::BTreeSet,
    time::{Duration, Instant},
};
#[derive(Clone)]
pub(super) struct Job {
    pub claim: Claim,
    pub message: Option<String>,
    pub launch: Launch,
    pub refresh_only: bool,
}
pub(super) async fn run(context: Inputs, mut job: Job) -> Result<(), &'static str> {
    let integration = context
        .integration
        .as_ref()
        .ok_or("bundle integration disabled")?;
    if !job.launch.closed {
        let until = Instant::now() + Duration::from_secs(15);
        while !job.launch.closed && Instant::now() < until {
            tokio::time::sleep(Duration::from_millis(500)).await;
            job.launch = integration
                .launch(job.claim.mint, context.guard.clone(), &context.health)
                .await?
                .ok_or("launch disappeared")?;
        }
        if !job.launch.closed {
            return Err("launch window has not closed");
        }
    }
    let cached = integration
        .cached(job.claim.mint, context.guard.clone())
        .await;
    let report =
        if let Some(report) = cached.and_then(|r| crate::bundle::rpc::refresh(&job.launch, &r)) {
            report
        } else {
            if job.refresh_only {
                return Ok(());
            }
            let Some(rpc) = &integration.rpc else {
                return Ok(());
            };
            let exclusions = integration
                .config
                .exclusions
                .iter()
                .filter_map(|s| crate::feed::key::<32>(s).ok())
                .collect::<BTreeSet<_>>();
            crate::bundle::rpc::analyze(rpc, &job.launch, &exclusions).await
        };
    integration
        .cache(report.clone(), context.guard.clone())
        .await?;
    if job.message.is_none() {
        println!(
            "{}",
            serde_json::json!({"event":"bundle_analysis_preview","report":report,"card":card(&job.claim,None,Some(&report),false),"sent":false})
        );
        return Ok(());
    }
    apply(
        &context,
        &job.claim,
        job.message.as_deref().expect("checked message"),
        report,
    )
    .await
}
async fn apply(
    context: &Inputs,
    claim: &Claim,
    message: &str,
    mut report: Report,
) -> Result<(), &'static str> {
    let integration = context
        .integration
        .as_ref()
        .ok_or("bundle integration disabled")?;
    let Some(sender) = &context.sender else {
        return Ok(());
    };
    let previous = integration
        .edit_meta(claim.mint, None, context.guard.clone())
        .await?;
    if previous.is_none() {
        return Err("bundle message target was not recorded");
    }
    if let Some(meta) = &previous {
        if meta.schema != 1
            || meta.message != message
            || meta.pending
            || meta.target_sha256 != sender.identity()
        {
            return Ok(());
        }
        if meta.enriched {
            let Some(current) = integration
                .launch(claim.mint, context.guard.clone(), &context.health)
                .await?
            else {
                return Ok(());
            };
            let current = crate::bundle::rpc::refresh(&current, &report)
                .ok_or("launch changed during funding analysis")?;
            let tier = current
                .clusters
                .iter()
                .filter_map(|c| c.progress.as_ref())
                .filter(|p| p.minimums_met)
                .map(|p| if p.tier >= Tier::Fire { 2 } else { 0 })
                .max()
                .unwrap_or(0);
            if tier <= meta.tier {
                return Ok(());
            }
        }
        let remaining = 45_000u64.saturating_sub(wall_ms().saturating_sub(meta.wall_ms));
        if remaining > 0 {
            tokio::time::sleep(Duration::from_millis(remaining)).await;
        }
    } else {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    if !context.global.remaining().is_zero() {
        return Ok(());
    }
    // Re-read current bags after funding lookups; never paint a stale RPC snapshot as current holdings.
    let Some(launch) = integration
        .launch(claim.mint, context.guard.clone(), &context.health)
        .await?
    else {
        return Ok(());
    };
    report = crate::bundle::rpc::refresh(&launch, &report)
        .ok_or("launch changed during funding analysis")?;
    let tier = report
        .clusters
        .iter()
        .filter_map(|c| c.progress.as_ref())
        .filter(|p| p.minimums_met)
        .map(|p| if p.tier >= Tier::Fire { 2 } else { 0 })
        .max()
        .unwrap_or(0);
    if previous
        .as_ref()
        .is_some_and(|meta| meta.enriched && tier <= meta.tier)
    {
        return Ok(());
    }
    let key = claim.clone();
    let id = message.to_owned();
    let policy = context.config.clone();
    let now = wall_ms();
    if !edit(context.store.clone(), context.guard.clone(), move |s| {
        s.reserve_edit(&key, &id, wall_ms(), &policy)
    })
    .await?
    {
        return Ok(());
    }
    let mut intent = Edit {
        schema: 1,
        message: message.into(),
        tier,
        wall_ms: now,
        pending: true,
        enriched: true,
        target_sha256: sender.identity(),
    };
    integration
        .edit_meta(claim.mint, Some(intent.clone()), context.guard.clone())
        .await?;
    let response = sender
        .patch(
            message,
            &card(claim, None, Some(&report), false),
            Instant::now() + Duration::from_secs(120),
        )
        .await;
    let pause = wall_ms().saturating_add(response.pause.as_millis().try_into().unwrap_or(u64::MAX));
    let key = claim.clone();
    let id = message.to_owned();
    match response.outcome {
        Outcome::Sent(_) => {
            edit(context.store.clone(), context.guard.clone(), move |s| {
                s.finish(&key, "sent", Some(id), pause)
            })
            .await?;
            intent.pending = false;
            intent.wall_ms = wall_ms();
            integration
                .edit_meta(claim.mint, Some(intent), context.guard.clone())
                .await?;
            inc(&context.health.bundle_edits);
        }
        _ => {
            inc(&context.health.cto_unknown);
            edit(context.store.clone(), context.guard.clone(), move |s| {
                s.finish(&key, "bundle_unknown", Some(id), pause)
            })
            .await?;
            set_state(&context.health.bundle_analysis_state, "edit_unknown");
        }
    }
    Ok(())
}

pub(super) async fn sweep(context: Inputs, skip: BTreeSet<[u8; 32]>) -> Result<(), &'static str> {
    let Some(integration) = &context.integration else {
        return Ok(());
    };
    let until = Instant::now() + Duration::from_secs(120);
    let targets = edit(context.store.clone(), context.guard.clone(), |s| {
        Ok(s.targets())
    })
    .await?;
    for (claim, message) in targets {
        if Instant::now() >= until {
            break;
        }
        if skip.contains(&claim.mint) {
            continue;
        }
        let Some(cached) = integration.cached(claim.mint, context.guard.clone()).await else {
            continue;
        };
        let Ok(Some(launch)) = integration
            .launch(claim.mint, context.guard.clone(), &context.health)
            .await
        else {
            continue;
        };
        let Some(report) = crate::bundle::rpc::refresh(&launch, &cached) else {
            continue;
        };
        integration
            .cache(report.clone(), context.guard.clone())
            .await?;
        apply(&context, &claim, &message, report).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        bundle::{
            capture::Launch, funding::Attribution, integration::Integration,
            runtime::Config as BundleConfig,
        },
        cto::{Config, Store},
        delivery::http::Http,
        discord::Pause,
        health::Health,
        storage::ProcessLock,
    };
    use axum::{
        Router,
        extract::{Json, State},
        http::StatusCode,
        routing::patch,
    };
    use serde_json::{Value, json};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, Ordering},
    };
    #[tokio::test]
    async fn existing_cards_are_patched_once_and_unknown_patch_outcomes_remain_reserved() {
        type TestState = (
            Arc<AtomicU64>,
            Arc<AtomicBool>,
            Arc<Mutex<Option<std::path::PathBuf>>>,
        );
        let count = Arc::new(AtomicU64::new(0));
        let unknown = Arc::new(AtomicBool::new(false));
        let intent_path = Arc::new(Mutex::new(None::<std::path::PathBuf>));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = url::Url::parse(&format!(
            "http://{}/api/webhooks/1/unit",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let app =
            Router::new()
                .route(
                    "/api/webhooks/1/unit/messages/99",
                    patch(
                        |State((count, unknown, path)): State<TestState>,
                         Json(body): Json<Value>| async move {
                            let path = path.lock().unwrap().clone().unwrap();
                            let saved: Value =
                                serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
                            assert_eq!(saved["value"]["pending"], true);
                            assert!(body["content"].as_str().unwrap().contains("partial"));
                            assert!(!body["content"].as_str().unwrap().contains("CLEARED"));
                            count.fetch_add(1, Ordering::SeqCst);
                            if unknown.load(Ordering::SeqCst) {
                                (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({})))
                            } else {
                                (StatusCode::OK, Json(json!({"id":"99","channel_id":"22"})))
                            }
                        },
                    ),
                )
                .with_state((count.clone(), unknown.clone(), intent_path.clone()));
        let server = crate::supervision::AbortOnDrop(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        for bad in [false, true] {
            unknown.store(bad, Ordering::SeqCst);
            let dir = tempfile::tempdir().unwrap();
            let data = dir.path().join("data");
            let guard = Arc::new(ProcessLock::acquire(&data).unwrap());
            let root = data.join("launch");
            let mut launch = Launch::new([1; 32], "boot", 0, 1000).unwrap();
            for id in 2..=7 {
                let t = crate::feed::Trade {
                    mint: [1; 32],
                    trader: [id; 32],
                    signature: [id; 64],
                    side: crate::feed::Side::Buy,
                    token_amount: 100,
                    sol_lamports: 2_000_000_000,
                    sol_source: crate::feed::SolSource::Payload,
                };
                launch.observe(id as u64, id as u64, 1000 + id as u64, &t, Some(100));
            }
            for id in 2..=7 {
                let t = crate::feed::Trade {
                    mint: [1; 32],
                    trader: [id; 32],
                    signature: [id + 50; 64],
                    side: crate::feed::Side::Sell,
                    token_amount: 80,
                    sol_lamports: 1,
                    sol_source: crate::feed::SolSource::Payload,
                };
                launch.observe(
                    100 + id as u64,
                    100 + id as u64,
                    1100 + id as u64,
                    &t,
                    Some(20),
                );
            }
            launch.tick(10_000);
            let mut storage =
                crate::bundle::store::Store::open(root.clone(), 32 * 1024 * 1024, 45).unwrap();
            storage.write_protected(&launch, &BTreeSet::new()).unwrap();
            let now = wall_ms();
            let mut store = Store::open(data.join("cto.json")).unwrap();
            store.observe(&json!([]), now - 60_000, true).unwrap();
            let claim=store.observe(&json!([{"chainId":"solana","tokenAddress":bs58::encode([1;32]).into_string(),"claimDate":"1970-01-01T00:00:01Z"}]),now-60_000,false).unwrap().pop().unwrap();
            assert!(
                store
                    .reserve(&claim, now - 60_000, &Config::default())
                    .unwrap()
            );
            store.finish(&claim, "sent", Some("99".into()), 0).unwrap();
            let health = Arc::new(Health::default());
            crate::health::set_state(&health.bundle_state, "recording");
            health.pulse(&health.bundle_tick_ms);
            let global = Arc::new(Pause::default());
            let http = Http::local(endpoint.clone(), global.clone());
            let integration = Integration {
                root: root.clone(),
                rpc: None,
                config: BundleConfig::default(),
                boot: "boot".into(),
                lock: Arc::new(Mutex::new(())),
                reads: Arc::new(tokio::sync::Semaphore::new(2)),
            };
            integration
                .edit_meta(
                    [1; 32],
                    Some(Edit {
                        schema: 1,
                        message: "99".into(),
                        tier: 0,
                        wall_ms: now - 60_000,
                        pending: false,
                        enriched: false,
                        target_sha256: http.identity(),
                    }),
                    guard.clone(),
                )
                .await
                .unwrap();
            *intent_path.lock().unwrap() = Some(
                data.join("bundle-analysis")
                    .join(format!("edit-{}.json", bs58::encode([1; 32]).into_string())),
            );
            let report = Report {
                schema: 1,
                mint: bs58::encode([1; 32]).into_string(),
                calls: 0,
                anchor_sha256: crate::bundle::rpc::anchor_hash(&launch),
                exclusions_sha256: crate::bundle::rpc::exclusions_hash(&BTreeSet::new()),
                verified_buyers: (2..=7).map(|id| [id; 32]).collect(),
                observed_wall_ms: launch.updated_wall_ms,
                supply_raw: Some(25_000),
                attributions: (2..=7)
                    .map(|id| Attribution {
                        wallet: [id; 32],
                        root: Some([8; 32]),
                        hop: Some(0),
                        excluded: false,
                        fanout_capped: false,
                        partial: false,
                        jito_corroborated: false,
                    })
                    .collect(),
                clusters: Vec::new(),
                partial: true,
                coverage: "test boundary".into(),
                stop: None,
            };
            let report = crate::bundle::rpc::refresh(&launch, &report).unwrap();
            integration
                .cache(report.clone(), guard.clone())
                .await
                .unwrap();
            assert!(integration.cached([1; 32], guard.clone()).await.is_some());
            let mut changed_policy = integration.clone();
            changed_policy
                .config
                .exclusions
                .push(bs58::encode([8; 32]).into_string());
            assert!(
                changed_policy
                    .cached([1; 32], guard.clone())
                    .await
                    .is_none()
            );

            let context = Inputs {
                config: Config::default(),
                store: Arc::new(Mutex::new(store)),
                guard: guard.clone(),
                global,
                health,
                sender: Some(http),
                integration: Some(integration.clone()),
            };
            let mut original = integration
                .edit_meta([1; 32], None, guard.clone())
                .await
                .unwrap()
                .unwrap();
            let mut changed = original.clone();
            changed.target_sha256 = "different-target".into();
            integration
                .edit_meta([1; 32], Some(changed), guard.clone())
                .await
                .unwrap();
            let before = count.load(Ordering::SeqCst);
            apply(&context, &claim, "99", report.clone()).await.unwrap();
            assert_eq!(count.load(Ordering::SeqCst), before);
            original.pending = false;
            integration
                .edit_meta([1; 32], Some(original), guard.clone())
                .await
                .unwrap();
            apply(&context, &claim, "99", report.clone()).await.unwrap();
            let after = count.load(Ordering::SeqCst);
            apply(&context, &claim, "99", report).await.unwrap();
            assert_eq!(count.load(Ordering::SeqCst), after);
            let meta = integration
                .edit_meta([1; 32], None, guard)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(meta.pending, bad);
            assert!(meta.enriched);
            assert_eq!(meta.tier, 2);
        }
        assert_eq!(count.load(Ordering::SeqCst), 2);
        drop(server);
    }
}
