use super::*;
use crate::{
    recovery::RetryPolicy,
    supervision::{self, RestartBudget},
};
use futures_util::FutureExt;
use std::panic::AssertUnwindSafe;
pub(super) trait Source {
    fn attempt(
        &mut self,
        pipeline: &mut Pipeline,
        paid: bool,
    ) -> impl Future<Output = Result<(), String>> + Send;
}
pub(super) struct Input<'a> {
    pub config: &'a Config,
    pub secrets: &'a Secrets,
    pub endpoint: Url,
    pub fixture: Option<&'a std::path::Path>,
    pub retry: RetryPolicy,
}
impl Source for Input<'_> {
    async fn attempt(&mut self, pipeline: &mut Pipeline, paid: bool) -> Result<(), String> {
        if let Some(path) = self.fixture {
            fixture(path, self.config, pipeline).await
        } else {
            crate::session::live(
                self.config,
                pipeline,
                self.secrets,
                self.endpoint.clone(),
                paid,
                &mut self.retry,
            )
            .await
        }
    }
}
pub(super) async fn run(
    pipeline: &mut Pipeline,
    source: &mut impl Source,
    config: &Config,
    mut paid: bool,
) -> Result<(), String> {
    let mut budget = RestartBudget::new(wall_ms() ^ u64::from(std::process::id()));
    let origin = Instant::now();
    loop {
        set_state(&pipeline.health.pipeline_state, "running");
        let started = Instant::now();
        match AssertUnwindSafe(source.attempt(pipeline, paid))
            .catch_unwind()
            .await
        {
            Ok(Err(_)) if config.cto.enabled => {
                set_state(&pipeline.health.feed_state, "failed");
                supervision::red(&pipeline.health, "pipeline");
                pipeline.control("trade_feed_stopped",json!({"reason":"recovery_policy_stopped","cto":"independent_worker_continues"}));
                return std::future::pending().await;
            }
            Ok(result) => return result,
            Err(_) => {
                inc(&pipeline.health.pipeline_panics);
                set_state(&pipeline.health.feed_state, "panic_recovery");
                set_state(&pipeline.health.transport_state, "stopped");
                set(&pipeline.health.backoff_ms, 0);
                set_state(&pipeline.health.application_state, "not_started");
                if paid {
                    paid = false;
                    set_state(&pipeline.health.paid_state, "interrupted");
                }
                // The unwound attempt has dropped its socket and local spend credits.
                // No possibly mutated decoder or detector state is reused after a panic.
                pipeline.decoder =
                    Decoder::new(config.feed.dedup_capacity, config.feed.dedup_window_ms);
                if pipeline.volume.is_some() {
                    pipeline.volume = Some(Engine::new(256));
                }
                pipeline.volume_health();
                pipeline.control("pipeline_panic", json!({"history":"discarded_cold","subscription":"free-only","payload":"redacted"}));
                let delay = budget.panic(
                    origin.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
                );
                let Some(delay) = delay else {
                    set_state(&pipeline.health.feed_state, "red");
                    supervision::red(&pipeline.health, "pipeline");
                    // Leave health and the caller's shutdown/run timer alive; never re-arm red.
                    return std::future::pending().await;
                };
                set_state(&pipeline.health.pipeline_state, "backoff");
                tokio::time::sleep(delay).await;
                inc(&pipeline.health.pipeline_restarts);
            }
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::{feed::Event, health::value};
    struct PanicOnce {
        attempts: usize,
        retry: RetryPolicy,
    }
    impl Source for PanicOnce {
        async fn attempt(&mut self, pipeline: &mut Pipeline, paid: bool) -> Result<(), String> {
            self.attempts += 1;
            let mint = bs58::encode([1u8; 32]).into_string();
            let trade = json!({"txType":"buy","mint":mint,"traderPublicKey":mint,"signature":bs58::encode([2u8;64]).into_string(),"tokenAmount":1,"solAmount":1});
            if self.attempts == 1 {
                assert!(paid);
                set(&pipeline.health.backoff_ms, 99);
                assert!(self.retry.next(0).is_some());
                pipeline.ingest(Bytes::from(
                    json!({"txType":"create","mint":mint}).to_string(),
                ));
                pipeline.ingest(Bytes::from(trade.to_string()));
                assert_eq!(pipeline.volume.as_ref().unwrap().active_mints(), 1);
                panic!("isolated pipeline fault");
            }
            assert!(!paid, "paid observation resumed after panic");
            assert_eq!(value(&pipeline.health.backoff_ms), 0);
            assert!(
                self.retry.next(0).is_none(),
                "transport credits were replenished"
            );
            assert_eq!(pipeline.volume.as_ref().unwrap().active_mints(), 0);
            assert!(
                matches!(
                    pipeline
                        .decoder
                        .decode(trade.to_string().as_bytes(), 1)
                        .unwrap(),
                    Event::Trade(_)
                ),
                "possibly mutated decoder/dedup state survived panic"
            );
            Ok(())
        }
    }
    #[tokio::test]
    async fn panic_restarts_cold_free_only_without_refilling_transport_credits() {
        let dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _records) = pipeline_for_test(&dir.path().join("data"));
        pipeline.volume = Some(Engine::new(256));
        let mut config = Config::default();
        config.recovery.reconnect_attempts = 1;
        let mut source = PanicOnce {
            attempts: 0,
            retry: RetryPolicy::new(&config.recovery, 1),
        };
        tokio::time::timeout(
            Duration::from_secs(3),
            run(&mut pipeline, &mut source, &config, true),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(source.attempts, 2);
        assert_eq!(value(&pipeline.health.pipeline_panics), 1);
        assert_eq!(value(&pipeline.health.pipeline_restarts), 1);
        assert_eq!(pipeline.health.snapshot()["paid"]["state"], "interrupted");
    }
    struct AlwaysPanic;
    impl Source for AlwaysPanic {
        async fn attempt(&mut self, _pipeline: &mut Pipeline, _paid: bool) -> Result<(), String> {
            panic!("isolated repeated stage fault");
        }
    }
    #[tokio::test]
    async fn repeated_panics_latch_red_while_health_answers_until_cancelled() {
        let dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _records) = pipeline_for_test(&dir.path().join("data"));
        let health = pipeline.health.clone();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(crate::health::serve(listener, health.clone(), async {
            let _ = stopped.await;
        }));
        let config = Config::default();
        let mut source = AlwaysPanic;
        let operation = run(&mut pipeline, &mut source, &config, false);
        tokio::pin!(operation);
        tokio::select! {
            _ = &mut operation => panic!("red stage returned instead of waiting for shutdown"),
            result = tokio::time::timeout(Duration::from_secs(4), async {
                loop {
                    if health.snapshot()["supervision"]["pipeline"] == "red" { break; }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }) => result.unwrap(),
        }
        let snapshot: Value = reqwest::get(format!("http://{address}/health"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(snapshot["supervision"]["state"], "red");
        assert_eq!(snapshot["supervision"]["pipeline_panics"], 3);
        assert_eq!(snapshot["supervision"]["pipeline_restarts"], 2);
        assert!(!health.feed_connected());
        assert!(
            tokio::time::timeout(Duration::from_millis(20), &mut operation)
                .await
                .is_err()
        );
        stop.send(()).unwrap();
        server.await.unwrap().unwrap();
    }
}

#[cfg(test)]
mod family_isolation_tests {
    use super::*;
    struct FailedFeed(usize);
    impl Source for FailedFeed {
        async fn attempt(&mut self, _: &mut Pipeline, _: bool) -> Result<(), String> {
            self.0 += 1;
            Err("feed stopped".into())
        }
    }
    #[tokio::test]
    async fn exhausted_trade_recovery_does_not_terminate_the_process_when_cto_is_enabled() {
        let dir = tempfile::tempdir().unwrap();
        let (mut pipeline, _) = pipeline_for_test(&dir.path().join("data"));
        let mut config = Config::default();
        config.cto.enabled = true;
        let mut source = FailedFeed(0);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(20),
                run(&mut pipeline, &mut source, &config, false)
            )
            .await
            .is_err()
        );
        assert_eq!(source.0, 1);
        assert_eq!(pipeline.health.snapshot()["feed"]["state"], "failed");
        assert_eq!(pipeline.health.snapshot()["supervision"]["pipeline"], "red");
    }
}
