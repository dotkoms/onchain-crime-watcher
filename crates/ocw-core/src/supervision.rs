//! Current-stage supervision. Panic payloads and external error values never enter diagnostics.
use crate::health::{Health, inc, set_state};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tokio::task::JoinHandle;
const WINDOW_MS: u64 = 600_000;

pub(crate) struct RestartBudget {
    restarts: VecDeque<u64>,
    step: u32,
    early_panics: u8,
    seed: u64,
    red: bool,
}
impl RestartBudget {
    pub fn new(seed: u64) -> Self {
        Self {
            restarts: VecDeque::new(),
            step: 0,
            early_panics: 0,
            seed,
            red: false,
        }
    }
    pub fn panic(&mut self, now_ms: u64, running_ms: u64) -> Option<Duration> {
        if self.red {
            return None;
        }
        while self
            .restarts
            .front()
            .is_some_and(|at| now_ms.saturating_sub(*at) >= WINDOW_MS)
        {
            self.restarts.pop_front();
        }
        if running_ms >= WINDOW_MS {
            self.step = 0;
        }
        self.early_panics = if running_ms < 5_000 {
            self.early_panics.saturating_add(1)
        } else {
            0
        };
        if self.early_panics >= 3 || self.restarts.len() >= 5 {
            self.red = true;
            return None;
        }
        let base = 500u64.saturating_mul(1 << self.step.min(20)).min(60_000);
        self.seed = self.seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let radius = base / 5;
        let delay = (base - radius + self.seed % (2 * radius + 1)).min(60_000);
        self.step = self.step.saturating_add(1);
        self.restarts.push_back(now_ms);
        Some(Duration::from_millis(delay))
    }
}
pub(crate) fn red(health: &Health, stage: &'static str) {
    if !matches!(stage, "cto" | "bundle") {
        set_state(&health.supervisor_state, "red");
    }
    let state = match stage {
        "pipeline" => &health.pipeline_state,
        "capture" => &health.capture_worker_state,
        "delivery" => &health.delivery_worker_state,
        "health" => &health.health_worker_state,
        "monitor" => &health.monitor_worker_state,
        "history" => &health.history_state,
        "cto" => &health.cto_worker_state,
        "bundle" => &health.bundle_state,
        _ => unreachable!("fixed internal stage"),
    };
    if let Ok(mut current) = state.lock() {
        if *current == "red" {
            return;
        }
        *current = "red";
    }
    inc(&health.worker_failures);
    use std::io::Write;
    let _ = writeln!(
        std::io::stderr().lock(),
        "{{\"event\":\"stage_red\",\"stage\":\"{stage}\"}}"
    );
}
/// Install before reading configuration. Payloads and locations can contain secret material.
/// Library hosts choose their own global hook; the OCW executable always installs this hook.
pub fn install_panic_hook() {
    std::panic::set_hook(Box::new(|_| {
        use std::io::Write;
        let _ = writeln!(
            std::io::stderr().lock(),
            "ocw: task panicked; payload and location redacted"
        );
    }));
}
/// Async services must not detach when an enclosing collector future is cancelled.
pub(crate) struct AbortOnDrop<T>(pub JoinHandle<T>);
impl<T> Future for AbortOnDrop<T> {
    type Output = Result<T, tokio::task::JoinError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx)
    }
}
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn restart_window_fast_failure_and_red_latch_are_bounded() {
        let mut budget = RestartBudget::new(7);
        for (i, now) in [6_000, 12_000, 18_000, 24_000, 30_000]
            .into_iter()
            .enumerate()
        {
            let delay = budget.panic(now, 6_000).unwrap().as_millis();
            let base = 500u128 << i;
            assert!((base * 8 / 10..=base * 12 / 10).contains(&delay));
        }
        assert!(budget.panic(36_000, 6_000).is_none());
        assert!(budget.panic(1_000_000, WINDOW_MS).is_none());
        let mut fast = RestartBudget::new(1);
        assert!(fast.panic(1, 1).is_some());
        assert!(fast.panic(2, 1).is_some());
        assert!(fast.panic(3, 1).is_none());
        assert!(fast.panic(WINDOW_MS, WINDOW_MS).is_none());
        let mut window = RestartBudget::new(2);
        for n in 0..5 {
            assert!(window.panic(n * 6_000, 6_000).is_some());
        }
        assert!(window.panic(WINDOW_MS, 6_000).is_some());
    }
    #[test]
    fn sustained_health_resets_backoff_and_backoff_never_exceeds_cap() {
        let mut budget = RestartBudget::new(9);
        for n in 0..20 {
            assert!(budget.panic(n * WINDOW_MS, 6_000).unwrap() <= Duration::from_secs(60));
        }
        assert!(
            (400..=600).contains(&budget.panic(21 * WINDOW_MS, WINDOW_MS).unwrap().as_millis())
        );
    }
    #[test]
    fn executable_panic_hook_redacts_payload_and_location() {
        const CHILD: &str = "OCW_TEST_PANIC_HOOK_CHILD";
        if std::env::var_os(CHILD).is_some() {
            install_panic_hook();
            panic!("private-test-credential-never-print");
        }
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "supervision::tests::executable_panic_hook_redacts_payload_and_location",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(!result.status.success());
        let stderr = String::from_utf8(result.stderr).unwrap();
        assert!(stderr.contains("payload and location redacted"));
        assert!(!stderr.contains("private-test-credential"));
        assert!(!stderr.contains("supervision.rs:"));
    }
}
#[cfg(test)]
mod cleanup_tests {
    use super::*;
    #[tokio::test]
    async fn dropping_health_service_handle_releases_its_listener() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = AbortOnDrop(tokio::spawn(crate::health::serve(
            listener,
            std::sync::Arc::new(Health::default()),
            std::future::pending(),
        )));
        drop(task);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if tokio::net::TcpListener::bind(address).await.is_ok() {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }
    #[test]
    fn stage_failure_is_latched_and_counted_once() {
        let health = Health::default();
        red(&health, "capture");
        red(&health, "capture");
        assert_eq!(health.snapshot()["supervision"]["failed_stages"], 1);
        assert_eq!(health.snapshot()["supervision"]["state"], "red");
    }
}
