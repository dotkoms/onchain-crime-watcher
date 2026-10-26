//! Bounded transport recovery policy; application quiet is never proof of missing data.
use crate::config::RecoveryConfig;

pub(crate) struct RetryPolicy {
    config: RecoveryConfig,
    used: u8,
    step: u32,
    seed: u64,
}
impl RetryPolicy {
    pub fn new(config: &RecoveryConfig, seed: u64) -> Self {
        Self {
            config: config.clone(),
            used: 0,
            step: 0,
            seed,
        }
    }
    pub fn next(&mut self, healthy_ms: u64) -> Option<u64> {
        if self.used >= self.config.reconnect_attempts {
            return None;
        }
        if healthy_ms >= self.config.healthy_reset_ms {
            self.step = 0;
        }
        let base = self
            .config
            .initial_backoff_ms
            .saturating_mul(1u64 << self.step.min(31))
            .min(self.config.max_backoff_ms);
        self.seed = self.seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let jitter = self.seed % (base / 4 + 1);
        self.used += 1;
        self.step += 1;
        Some(base.saturating_add(jitter).min(self.config.max_backoff_ms))
    }
}

#[derive(Default)]
pub(crate) struct Heartbeat {
    sequence: u64,
    pending: Option<(u64, u64)>,
}
impl Heartbeat {
    pub fn sent(&mut self, now: u64, timeout: u64) -> [u8; 8] {
        self.sequence = self.sequence.wrapping_add(1);
        self.pending = Some((self.sequence, now.saturating_add(timeout)));
        self.sequence.to_le_bytes()
    }
    pub fn deadline(&self) -> Option<u64> {
        self.pending.map(|(_, deadline)| deadline)
    }
    pub fn pong(&mut self, bytes: &[u8], now: u64) -> bool {
        if let Some((sequence, deadline)) = self.pending
            && now < deadline
            && bytes == sequence.to_le_bytes()
        {
            self.pending = None;
            return true;
        }
        false
    }
}

#[derive(Default)]
pub(crate) struct CreateWatchdog {
    last: Option<u64>,
    ewma: Option<u64>,
    suspected: bool,
}
impl CreateWatchdog {
    pub fn observe(&mut self, now: u64) -> bool {
        if let Some(last) = self.last {
            let gap = now.saturating_sub(last);
            if gap > 0 && gap < 60_000 {
                self.ewma = Some(self.ewma.map_or(gap, |old| (old * 4 + gap) / 5));
            }
        }
        self.last = Some(now);
        let was = self.suspected;
        self.suspected = false;
        was
    }
    pub fn threshold(&self) -> u64 {
        30_000.max(self.ewma.unwrap_or(0) * 6)
    }
    pub fn ewma(&self) -> u64 {
        self.ewma.unwrap_or(0)
    }
    pub fn seen(&self) -> bool {
        self.last.is_some()
    }
    pub fn check(&mut self, now: u64) -> bool {
        self.suspected = now.saturating_sub(self.last.unwrap_or(0)) >= self.threshold();
        self.suspected
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::RecoveryConfig;

    #[test]
    fn invalid_recovery_configuration_cannot_create_zero_timers_or_unbounded_retries() {
        for policy in [
            "reconnect_attempts=6",
            "initial_backoff_ms=0",
            "initial_backoff_ms=2000\nmax_backoff_ms=1000",
            "healthy_reset_ms=0",
            "ping_interval_ms=0",
            "pong_timeout_ms=0",
            "transport_idle_ms=1",
        ] {
            assert!(crate::config::Config::parse(&format!("[recovery]\n{policy}\n")).is_err());
        }
    }

    #[test]
    fn retry_cap_survives_healthy_backoff_resets_and_jitter_stays_capped() {
        let config = RecoveryConfig {
            reconnect_attempts: 3,
            initial_backoff_ms: 100,
            max_backoff_ms: 500,
            ..RecoveryConfig::default()
        };
        let mut policy = RetryPolicy::new(&config, 7);
        let a = policy.next(0).unwrap();
        let b = policy.next(0).unwrap();
        assert!((100..=125).contains(&a));
        assert!((200..=250).contains(&b));
        let c = policy.next(config.healthy_reset_ms).unwrap();
        assert!((100..=125).contains(&c));
        assert!(policy.next(config.healthy_reset_ms).is_none());
        assert!(
            RetryPolicy::new(&RecoveryConfig::default(), 7)
                .next(0)
                .is_none()
        );
        let mut policy = RetryPolicy::new(
            &RecoveryConfig {
                reconnect_attempts: 5,
                max_backoff_ms: 1000,
                ..RecoveryConfig::default()
            },
            7,
        );
        while let Some(delay) = policy.next(0) {
            assert!(delay <= 1000);
        }
    }

    #[test]
    fn data_and_wrong_pongs_do_not_extend_a_required_matching_pong_deadline() {
        let mut heartbeat = Heartbeat::default();
        let ping = heartbeat.sent(1_000, 10_000);
        assert_eq!(heartbeat.deadline(), Some(11_000));
        assert!(!heartbeat.pong(b"unrelated", 2_000));
        assert_eq!(heartbeat.deadline(), Some(11_000));
        assert!(!heartbeat.pong(&ping, 11_000));
        let ping = heartbeat.sent(12_000, 10_000);
        assert!(heartbeat.pong(&ping, 12_001));
        assert!(heartbeat.deadline().is_none());
    }

    #[test]
    fn application_watchdog_is_adaptive_and_does_not_learn_long_stalls_as_normal() {
        let mut app = CreateWatchdog::default();
        assert!(!app.check(29_999));
        assert!(app.check(30_000));
        assert!(app.observe(30_001));
        assert!(!app.check(30_002));
        app.observe(40_001); // gap 10 s -> threshold 60 s
        assert_eq!(app.threshold(), 60_000);
        assert!(!app.check(100_000));
        assert!(app.check(100_001));
        assert!(app.observe(200_000)); // ignore long gap when learning EWMA
        assert_eq!(app.threshold(), 60_000);
        app.observe(201_000); // EWMA (4*10,000+1,000)/5=8,200 ms
        assert_eq!(app.threshold(), 49_200);
    }
}
