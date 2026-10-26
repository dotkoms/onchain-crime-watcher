use super::store::Tier;
use crate::{
    feed,
    volume::{Decision, Verdict},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::{Duration, Instant};

pub(super) struct Candidate {
    pub decision: Decision,
    pub id: String,
    pub tier: Tier,
    pub created: Instant,
    pub expires: Instant,
    pub ready: Instant,
    pub generation: u64,
    pub reserved: bool,
    pub attempts: u8,
}
impl Candidate {
    pub fn new(decision: Decision, boot: &str, origin: Instant, generation: u64) -> Option<Self> {
        valid_decision(&decision)?;
        if boot.is_empty() || boot.len() > 64 || !boot.bytes().all(|c| c.is_ascii_alphanumeric()) {
            return None;
        }
        let (tier, ttl, hold) = match decision.verdict {
            Verdict::Fire => (Tier::Fire, 30, 0),
            Verdict::Near => (Tier::Near, 15, 5),
            _ => return None,
        };
        let created = origin.checked_add(Duration::from_millis(decision.bucket_end_ms))?;
        let id = format!(
            "{:x}",
            Sha256::digest(format!(
                "{boot}:{}:{}:{tier:?}",
                decision.mint, decision.bucket_end_ms
            ))
        );
        Some(Self {
            decision,
            id,
            tier,
            created,
            expires: created.checked_add(Duration::from_secs(ttl))?,
            ready: created.checked_add(Duration::from_secs(hold))?,
            generation,
            reserved: false,
            attempts: 0,
        })
    }
    pub fn payload(&self, now: Instant) -> Option<Value> {
        if now >= self.expires {
            return None;
        }
        let age = now.checked_duration_since(self.created)?;
        let at = self
            .decision
            .bucket_end_ms
            .checked_add(age.as_millis().try_into().ok()?)?;
        super::staging_payload(&self.decision, at)
    }
}

pub(super) struct Queue {
    pending: Vec<Candidate>,
    capacity: usize,
    pub replaced: u64,
}
impl Queue {
    pub fn new(capacity: usize) -> Self {
        Self {
            pending: Vec::new(),
            capacity: capacity.clamp(1, 64),
            replaced: 0,
        }
    }
    pub fn insert(&mut self, c: Candidate) -> bool {
        if self.pending.iter().any(|p| p.id == c.id) {
            return false;
        }
        if self.pending.len() == self.capacity {
            if c.tier != Tier::Fire {
                return false;
            }
            let Some(i) = self.pending.iter().rposition(|p| p.tier == Tier::Near) else {
                return false;
            };
            self.pending.remove(i);
            self.replaced += 1;
        }
        self.pending.push(c);
        true
    }
    pub fn prune(&mut self, now: Instant, generation: u64) -> usize {
        let before = self.pending.len();
        self.pending
            .retain(|c| now < c.expires && c.generation == generation);
        before - self.pending.len()
    }
    pub fn take(&mut self, now: Instant, generation: u64) -> Option<Candidate> {
        self.prune(now, generation);
        let (index, _) = self
            .pending
            .iter()
            .enumerate()
            .filter(|(_, c)| now >= c.ready)
            .min_by_key(|(_, c)| (if c.tier == Tier::Fire { 0 } else { 1 }, c.created))?;
        Some(self.pending.remove(index))
    }
    pub fn clear(&mut self) -> usize {
        let count = self.pending.len();
        self.pending.clear();
        count
    }
}

pub(super) fn valid_decision(decision: &Decision) -> Option<()> {
    feed::key::<32>(&decision.mint).ok()?;
    if !matches!(decision.regime, "pool" | "bonding_curve")
        || !matches!(decision.path, "relative" | "cold_baseline")
        || [
            decision.baseline_lamports,
            decision.floor_lamports,
            decision.dispersion_threshold_lamports,
            decision.largest_taker_share,
        ]
        .iter()
        .any(|v| !v.is_finite() || *v < 0.0)
        || decision.floor_lamports == 0.0
        || decision.largest_taker_share > 1.0
        || decision.ratio.is_some_and(|v| !v.is_finite() || v < 0.0)
        || decision
            .cohort_p90
            .is_some_and(|v| !v.is_finite() || v < 0.0)
    {
        return None;
    }
    Some(())
}
