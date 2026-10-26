//! Volume prototype for normalized real events. Totals are exact lamports; statistics use f64.
//! Thresholds are scope §2A priors, not validated predictions or trading recommendations.
use crate::feed::{Event, Trade};
use serde::Serialize;
use serde_json::{Value, json};
use std::collections::{BTreeMap, VecDeque};

const BUCKET_MS: u64 = 15_000;
const RING: usize = 30;
const SAMPLE: usize = 1024;
const TAKERS: usize = 256;
const SOL: f64 = 1_000_000_000.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    Warmup,
    InsufficientHistory,
    InsufficientTrades,
    Participation,
    InvalidBucket,
    CohortCold,
    Reject,
    Near,
    Fire,
}

#[derive(Clone, Debug, Serialize)]
pub struct Decision {
    pub number: u64,
    pub mint: String,
    pub verdict: Verdict,
    pub path: &'static str,
    pub regime: &'static str,
    pub bucket_end_ms: u64,
    pub completed_buckets: usize,
    pub volume_lamports: u64,
    pub baseline_lamports: f64,
    pub floor_lamports: f64,
    pub ratio: Option<f64>,
    pub cohort_p90: Option<f64>,
    pub dispersion_threshold_lamports: f64,
    pub trades: usize,
    pub distinct_takers: usize,
    pub largest_taker_share: f64,
}
impl Decision {
    /// Pure preview: never sends HTTP, changes a budget, or claims delivery.
    pub fn card(&self, now_ms: u64) -> Option<Value> {
        self.formatted_card(now_ms, false)
    }
    pub(crate) fn staging_card(&self, now_ms: u64) -> Option<Value> {
        self.formatted_card(now_ms, true)
    }
    fn formatted_card(&self, now_ms: u64, staging: bool) -> Option<Value> {
        let label = if staging { "STAGING" } else { "PREVIEW" };
        let footer = if staging {
            "Thresholds are priors. Staging only."
        } else {
            "Thresholds are priors. Not delivered."
        };
        let ttl = match self.verdict {
            Verdict::Fire => 30_000,
            Verdict::Near => 15_000,
            _ => return None,
        };
        let age = now_ms.checked_sub(self.bucket_end_ms)?;
        if age >= ttl {
            return None;
        }
        let tier = if self.verdict == Verdict::Fire {
            "FIRE"
        } else {
            "NEAR"
        };
        let comparison = match self.ratio {
            Some(r) => format!("Ratio {r:.2}x | cohort p90 {:.2}x", self.cohort_p90?),
            None => "cold_baseline | absolute threshold, ratio unavailable".into(),
        };
        Some(json!({
            "content": format!(
                "#{} {tier} | Volume anomaly | {label}\nMint: {}\n15 s volume {:.3} SOL | baseline {:.3} SOL | floor {:.3} SOL\n{comparison}\n{} trades | {} takers | largest {:.1}%\nRegime: {} | age {:.1} s\nWatched subset only; coverage unverified. {footer}",
                self.number, self.mint, self.volume_lamports as f64 / SOL, self.baseline_lamports / SOL,
                self.floor_lamports / SOL, self.trades, self.distinct_takers,
                self.largest_taker_share * 100.0, self.regime, age as f64 / 1000.0),
            "allowed_mentions": {"parse": []}
        }))
    }
}

#[derive(Default, Debug, Serialize)]
pub struct Stats {
    pub registry_full: u64,
    pub unknown_trades: u64,
    pub invalidated_buckets: u64,
    pub evaluated: u64,
    pub warmup: u64,
    pub fire: u64,
    pub near: u64,
    pub gaps: u64,
}

#[derive(Default)]
struct Bucket {
    volume: u64,
    trades: usize,
    takers: BTreeMap<[u8; 32], u64>,
    sizes: Vec<u64>,
    invalid: bool,
}
impl Bucket {
    fn add(&mut self, trade: Trade) {
        if self.invalid {
            return;
        }
        if self.trades >= SAMPLE
            || (self.takers.len() >= TAKERS && !self.takers.contains_key(&trade.trader))
        {
            self.invalid = true;
            return;
        }
        let Some(volume) = self.volume.checked_add(trade.sol_lamports) else {
            self.invalid = true;
            return;
        };
        self.volume = volume;
        self.trades += 1;
        // A taker's sum cannot exceed the already checked total.
        *self.takers.entry(trade.trader).or_default() += trade.sol_lamports;
        self.sizes.push(trade.sol_lamports);
    }
}
struct Mint {
    born: u64,
    start: u64,
    pool: bool,
    ring: VecDeque<u64>,
    current: Bucket,
}
#[derive(Default)]
struct Cohort {
    ratios: VecDeque<f64>,
    sizes: VecDeque<f64>,
}
fn push(sample: &mut VecDeque<f64>, value: f64) {
    if sample.len() == SAMPLE {
        sample.pop_front();
    }
    sample.push_back(value);
}
fn quantile(
    values: impl Iterator<Item = f64>,
    numerator: usize,
    denominator: usize,
) -> Option<f64> {
    let mut sorted: Vec<_> = values.collect();
    sorted.sort_by(f64::total_cmp);
    // Nearest-rank percentile, using only the preceding bounded sample.
    let rank = (sorted.len() * numerator).div_ceil(denominator).max(1);
    sorted.get(rank - 1).copied()
}
fn median(values: impl Iterator<Item = f64>) -> Option<f64> {
    let mut sorted: Vec<_> = values.collect();
    sorted.sort_by(f64::total_cmp);
    if sorted.is_empty() {
        return None;
    }
    let middle = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    })
}
fn band(age: u64, pool: bool) -> usize {
    let age_band = match age {
        0..300_000 => 0,
        300_000..1_800_000 => 1,
        1_800_000..7_200_000 => 2,
        7_200_000..21_600_000 => 3,
        _ => 4,
    };
    age_band + if pool { 5 } else { 0 }
}

pub struct Engine {
    mints: BTreeMap<[u8; 32], Mint>,
    capacity: usize,
    cohorts: [Cohort; 10],
    now: u64,
    stats: Stats,
}
impl Engine {
    pub fn new(capacity: usize) -> Self {
        Self {
            mints: BTreeMap::new(),
            capacity: capacity.clamp(1, 256),
            cohorts: std::array::from_fn(|_| Cohort::default()),
            now: 0,
            stats: Stats::default(),
        }
    }
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
    pub(crate) fn forget(&mut self, key: &[u8; 32]) {
        self.mints.remove(key);
    }
    pub(crate) fn attach_observed(&mut self, key: [u8; 32], born: u64, now: u64, partial: bool) {
        if self.mints.contains_key(&key) || self.mints.len() >= self.capacity || born > now {
            return;
        }
        let start = born + ((now - born) / BUCKET_MS) * BUCKET_MS;
        let bucket = Bucket {
            invalid: partial || born < now,
            ..Bucket::default()
        };
        self.mints.insert(
            key,
            Mint {
                born,
                start,
                pool: false,
                ring: VecDeque::new(),
                current: bucket,
            },
        );
    }
    pub fn active_mints(&self) -> usize {
        self.mints.len()
    }
    pub fn on_event(&mut self, event: Event, now: u64) -> Result<Vec<Decision>, &'static str> {
        let decisions = self.advance(now)?;
        match event {
            Event::Create(key) => {
                if !self.mints.contains_key(&key) {
                    if self.mints.len() >= self.capacity {
                        self.stats.registry_full += 1;
                    } else {
                        self.mints.insert(
                            key,
                            Mint {
                                born: now,
                                start: now,
                                pool: false,
                                ring: VecDeque::new(),
                                current: Bucket::default(),
                            },
                        );
                    }
                }
            }
            Event::Migration(key) => {
                if let Some(mint) = self.mints.get_mut(&key)
                    && !mint.pool
                {
                    mint.pool = true;
                    mint.start = now;
                    mint.ring.clear();
                    mint.current = Bucket::default();
                }
            }
            Event::Trade(trade) => {
                if let Some(mint) = self.mints.get_mut(&trade.mint) {
                    mint.current.add(trade);
                } else {
                    self.stats.unknown_trades += 1;
                }
            }
            // The decoder cannot identify the mint of an unresolved SOL leg. Conservatively
            // invalidate every open bucket instead of manufacturing zero-volume observations.
            Event::Unresolved => self.invalidate(),
            _ => {}
        }
        Ok(decisions)
    }
    pub fn invalidate(&mut self) {
        for mint in self.mints.values_mut() {
            mint.current.invalid = true;
        }
    }
    pub fn gap(&mut self, now: u64) -> Result<(), &'static str> {
        if now < self.now {
            return Err("receive clock moved backwards");
        }
        self.now = now;
        for mint in self.mints.values_mut() {
            mint.start = now;
            mint.ring.clear();
            mint.current = Bucket::default();
        }
        self.cohorts = std::array::from_fn(|_| Cohort::default());
        self.stats.gaps += 1;
        Ok(())
    }
    pub fn advance(&mut self, now: u64) -> Result<Vec<Decision>, &'static str> {
        if now < self.now {
            return Err("receive clock moved backwards");
        }
        self.now = now;
        let mut decisions = Vec::new();
        for mint in self.mints.values_mut() {
            let elapsed = (now - mint.start) / BUCKET_MS;
            // A long continuous idle interval needs only the last ring plus its open bucket.
            // Discard old partial data; never turn an ancient spike into a fresh candidate.
            if elapsed > (RING + 1) as u64 {
                mint.start += (elapsed - (RING + 1) as u64) * BUCKET_MS;
                mint.ring.clear();
                mint.current = Bucket::default();
            }
        }
        // Process simultaneous mint histories in global bucket-end order, never feeding a
        // future bucket from another mint into an earlier cohort comparison.
        while let Some(key) = self
            .mints
            .iter()
            .filter(|(_, mint)| now - mint.start >= BUCKET_MS)
            .min_by_key(|(key, mint)| (mint.start, **key))
            .map(|(key, _)| *key)
        {
            let mint = self.mints.get_mut(&key).expect("selected registered mint");
            let end = mint.start + BUCKET_MS;
            let cohort = &mut self.cohorts[band(end - mint.born, mint.pool)];
            let mut decision = evaluate(&key, mint, cohort, end);
            self.stats.evaluated += 1;
            decision.number = self.stats.evaluated;
            match decision.verdict {
                Verdict::Warmup => self.stats.warmup += 1,
                Verdict::InvalidBucket => self.stats.invalidated_buckets += 1,
                Verdict::Fire => self.stats.fire += 1,
                Verdict::Near => self.stats.near += 1,
                _ => {}
            }
            if mint.current.invalid {
                mint.ring.clear();
            } else {
                if let Some(ratio) = decision.ratio {
                    push(&mut cohort.ratios, ratio);
                }
                for size in &mint.current.sizes {
                    push(&mut cohort.sizes, *size as f64);
                }
                if mint.ring.len() == RING {
                    mint.ring.pop_front();
                }
                mint.ring.push_back(mint.current.volume);
            }
            mint.current = Bucket::default();
            mint.start = end;
            decisions.push(decision);
        }
        Ok(decisions)
    }
}

fn evaluate(key: &[u8; 32], mint: &Mint, cohort: &Cohort, end: u64) -> Decision {
    let mut history: Vec<_> = mint.ring.iter().map(|v| *v as f64).collect();
    history.sort_by(f64::total_cmp);
    let trimmed = &history[..history.len().saturating_sub(3)];
    let baseline = if trimmed.is_empty() {
        0.0
    } else {
        trimmed.iter().sum::<f64>() / trimmed.len() as f64
    };
    let location = median(history.iter().copied()).unwrap_or(0.0);
    let mad = median(history.iter().map(|v| (v - location).abs())).unwrap_or(0.0);
    let floor = (1.5 * SOL).max(0.5 * median(cohort.sizes.iter().copied()).unwrap_or(0.0));
    let cold = baseline < floor;
    let enough = history.len() >= 16 && mint.ring.iter().filter(|v| **v > 0).count() >= 8;
    let ratio = if enough && !cold && !mint.current.invalid {
        Some((mint.current.volume as f64 + floor) / (baseline + floor))
    } else {
        None
    };
    let p90 = quantile(cohort.ratios.iter().copied(), 9, 10);
    let largest = mint.current.takers.values().max().copied().unwrap_or(0);
    let share = if mint.current.volume == 0 {
        0.0
    } else {
        largest as f64 / mint.current.volume as f64
    };
    let threshold = location + (4.0 * 1.4826 * mad).max(0.5 * location);
    let volume = mint.current.volume as f64;
    let verdict = if mint.current.invalid {
        Verdict::InvalidBucket
    } else if history.len() < 16 {
        Verdict::Warmup
    } else if !enough {
        Verdict::InsufficientHistory
    } else if mint.current.trades < 6 {
        Verdict::InsufficientTrades
    } else if mint.current.takers.len() < 5
        || (largest as u128) * 100 > (mint.current.volume as u128) * 60
    {
        Verdict::Participation
    } else if cold {
        if volume >= 10.0 * floor {
            Verdict::Fire
        } else {
            Verdict::Reject
        }
    } else if ratio.unwrap_or(0.0) < 2.0 {
        Verdict::Reject
    } else if p90.is_none() {
        Verdict::CohortCold
    } else if ratio.unwrap_or(0.0) >= 3.0 && ratio >= p90 && volume >= threshold {
        Verdict::Fire
    } else if ratio.unwrap_or(0.0) < 3.0 {
        Verdict::Near
    } else {
        Verdict::Reject
    };
    Decision {
        number: 0,
        mint: bs58::encode(key).into_string(),
        verdict,
        path: if cold { "cold_baseline" } else { "relative" },
        regime: if mint.pool { "pool" } else { "bonding_curve" },
        bucket_end_ms: end,
        completed_buckets: history.len(),
        volume_lamports: mint.current.volume,
        baseline_lamports: baseline,
        floor_lamports: floor,
        ratio,
        cohort_p90: p90,
        dispersion_threshold_lamports: threshold,
        trades: mint.current.trades,
        distinct_takers: mint.current.takers.len(),
        largest_taker_share: share,
    }
}
