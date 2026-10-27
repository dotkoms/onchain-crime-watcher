//! Deterministic bounded desired mint subscriptions. Financial admission remains in the
//! session's shared durable spend ledger; this planner never opens a socket.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub max_subs: usize,
    pub launch_slots: usize,
    pub launch_s: u64,
    pub retain_window_s: u64,
    pub retain_min_lamports: u64,
    pub ttl_s: u64,
    pub keepalive_lamports: u64,
    pub evict_margin_bps: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            max_subs: 20,
            launch_slots: 4,
            launch_s: 10,
            retain_window_s: 300,
            retain_min_lamports: 3_000_000_000,
            ttl_s: 1800,
            keepalive_lamports: 500_000_000,
            evict_margin_bps: 15_000,
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(2..=20).contains(&self.max_subs)
            || self.launch_slots == 0
            || self.launch_slots >= self.max_subs
            || !(5..=30).contains(&self.launch_s)
            || self.retain_window_s < self.launch_s
            || self.retain_window_s > self.ttl_s
            || !(300..=7200).contains(&self.ttl_s)
            || !(10_000..=100_000).contains(&self.evict_margin_bps)
        {
            return Err("invalid watch limits");
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Action {
    Subscribe {
        mint: [u8; 32],
        created_ms: Option<u64>,
        partial: bool,
    },
    Unsubscribe([u8; 32]),
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Priority {
    Normal,
    Wallet,
    Cto,
}
struct Mint {
    created: u64,
    launch: bool,
    qualified: bool,
    volume: u128,
    recent: VecDeque<(u64, u128)>,
    expires: u64,
    priority: Priority,
}
pub struct Planner {
    config: Config,
    mints: BTreeMap<[u8; 32], Mint>,
    seen: BTreeSet<[u8; 32]>,
    waiting: VecDeque<([u8; 32], u64)>,
    pub skipped: u64,
    pub evicted: u64,
    pub exhausted: bool,
    now: u64,
}
impl Planner {
    pub fn new(config: Config) -> Result<Self, &'static str> {
        config.validate()?;
        Ok(Self {
            config,
            mints: BTreeMap::new(),
            seen: BTreeSet::new(),
            waiting: VecDeque::new(),
            skipped: 0,
            evicted: 0,
            exhausted: false,
            now: 0,
        })
    }
    fn tick(&mut self, now: u64) -> bool {
        if self.exhausted || now < self.now {
            self.exhausted = true;
            return false;
        }
        self.now = now;
        true
    }
    pub fn contains(&self, mint: &[u8; 32]) -> bool {
        self.mints.contains_key(mint)
    }
    pub fn keys(&self) -> impl Iterator<Item = &[u8; 32]> {
        self.mints.keys()
    }
    pub fn create(&mut self, mint: [u8; 32], now: u64) -> Vec<Action> {
        if !self.tick(now) {
            return Vec::new();
        }
        if self.seen.contains(&mint) || self.exhausted {
            return Vec::new();
        }
        if self.seen.len() >= 50_000 {
            self.exhausted = true;
            return Vec::new();
        }
        self.seen.insert(mint);
        if self.mints.contains_key(&mint) {
            return Vec::new();
        }
        if self.mints.len() < self.config.max_subs && self.launches() < self.config.launch_slots {
            self.insert(mint, now, true, Priority::Normal);
            return vec![Action::Subscribe {
                mint,
                created_ms: Some(now),
                partial: false,
            }];
        }
        if self.waiting.len() < 32 {
            self.waiting.push_back((mint, now));
        } else {
            self.skipped += 1;
        }
        Vec::new()
    }
    fn launches(&self) -> usize {
        self.mints.values().filter(|mint| mint.launch).count()
    }
    fn insert(&mut self, key: [u8; 32], now: u64, launch: bool, priority: Priority) {
        self.mints.insert(
            key,
            Mint {
                created: now,
                launch,
                qualified: priority != Priority::Normal,
                volume: 0,
                recent: VecDeque::new(),
                expires: now.saturating_add(self.config.ttl_s * 1000),
                priority,
            },
        );
    }
    pub fn trade(&mut self, mint: &[u8; 32], lamports: u64, now: u64) {
        if !self.tick(now) {
            return;
        }
        if let Some(row) = self.mints.get_mut(mint) {
            row.volume = row.volume.saturating_add(lamports as u128);
            let second = now / 1000;
            if let Some((last, total)) = row.recent.back_mut().filter(|(at, _)| *at == second) {
                let _ = last;
                *total = total.saturating_add(lamports as u128);
            } else {
                row.recent.push_back((second, lamports as u128));
            }
            while row
                .recent
                .front()
                .is_some_and(|(at, _)| second.saturating_sub(*at) >= 60)
            {
                row.recent.pop_front();
            }
            if row.recent.iter().map(|(_, n)| *n).sum::<u128>()
                >= self.config.keepalive_lamports as u128
            {
                row.expires = now.saturating_add(self.config.ttl_s * 1000);
            }
        }
    }
    fn victim(&self, priority: Priority, score: u128) -> Option<[u8; 32]> {
        let (key, row) = self
            .mints
            .iter()
            .filter(|(_, row)| {
                !row.launch && row.priority < Priority::Cto && row.priority <= priority
            })
            .min_by_key(|(key, row)| (row.priority, row.volume, **key))?;
        (priority > row.priority
            || score.saturating_mul(10_000)
                >= row
                    .volume
                    .saturating_mul(self.config.evict_margin_bps as u128))
        .then_some(*key)
    }
    /// CTO/watch-set requests have no observed launch time. Their subscription cannot create
    /// launch history or a volume baseline; callers must retain that unknown status.
    pub fn request(&mut self, mint: [u8; 32], priority: Priority, now: u64) -> Vec<Action> {
        if !self.tick(now) {
            return Vec::new();
        }
        if let Some(row) = self.mints.get_mut(&mint) {
            row.priority = row.priority.max(priority);
            return Vec::new();
        }
        let mut actions = Vec::new();
        if self.mints.values().filter(|row| !row.launch).count()
            >= self.config.max_subs - self.config.launch_slots
        {
            if let Some(victim) = self.victim(priority, 0) {
                self.mints.remove(&victim);
                self.evicted += 1;
                actions.push(Action::Unsubscribe(victim));
            } else {
                self.skipped += 1;
                return actions;
            }
        }
        self.insert(mint, now, false, priority);
        actions.push(Action::Subscribe {
            mint,
            created_ms: None,
            partial: true,
        });
        actions
    }
    pub fn reconcile(&mut self, now: u64) -> Vec<Action> {
        if !self.tick(now) {
            return Vec::new();
        }
        let mut actions = Vec::new();
        let keys: Vec<_> = self.mints.keys().copied().collect();
        for key in keys {
            let Some(row) = self.mints.get(&key) else {
                continue;
            };
            if row.launch && now.saturating_sub(row.created) >= self.config.launch_s * 1000 {
                let score = row.volume;
                let priority = row.priority;
                if self.mints.values().filter(|row| !row.launch).count()
                    >= self.config.max_subs - self.config.launch_slots
                {
                    if let Some(victim) = self.victim(priority, score) {
                        self.mints.remove(&victim);
                        self.evicted += 1;
                        actions.push(Action::Unsubscribe(victim));
                    } else {
                        self.mints.remove(&key);
                        actions.push(Action::Unsubscribe(key));
                        self.skipped += 1;
                        continue;
                    }
                }
                self.mints.get_mut(&key).expect("retained candidate").launch = false;
            }
            if let Some(row) = self.mints.get_mut(&key)
                && !row.qualified
                && now.saturating_sub(row.created) >= self.config.retain_window_s * 1000
            {
                if row.priority > Priority::Normal
                    || row.volume >= self.config.retain_min_lamports as u128
                {
                    row.qualified = true;
                } else {
                    self.mints.remove(&key);
                    actions.push(Action::Unsubscribe(key));
                    continue;
                }
            }
            if self.mints.get(&key).is_some_and(|row| now >= row.expires) {
                self.mints.remove(&key);
                actions.push(Action::Unsubscribe(key));
            }
        }
        while let Some((key, created)) = self.waiting.front().copied() {
            if now.saturating_sub(created) >= 2000 {
                self.waiting.pop_front();
                self.skipped += 1;
                continue;
            }
            if self.mints.len() >= self.config.max_subs
                || self.launches() >= self.config.launch_slots
            {
                break;
            }
            self.waiting.pop_front();
            self.insert(key, created, true, Priority::Normal);
            actions.push(Action::Subscribe {
                mint: key,
                created_ms: Some(created),
                partial: true,
            });
        }
        actions
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn launch_wait_promotion_retention_and_expiry_have_exact_boundaries() {
        let mut p = Planner::new(Config::default()).unwrap();
        for key in 1..=4 {
            assert_eq!(p.create([key; 32], 0).len(), 1);
        }
        assert!(p.create([5; 32], 0).is_empty());
        assert!(p.reconcile(1999).is_empty());
        p.reconcile(2000);
        assert_eq!(p.skipped, 1);
        assert!(p.reconcile(9999).is_empty());
        assert!(p.reconcile(10_000).is_empty());
        p.trade(&[1; 32], 3_000_000_000, 11_000);
        let removed = p.reconcile(300_000);
        assert_eq!(removed.len(), 3);
        assert!(p.contains(&[1; 32]));
        assert_eq!(p.reconcile(1_811_000), vec![Action::Unsubscribe([1; 32])]);
        assert!(p.create([1; 32], 1_811_001).is_empty());
    }
    #[test]
    fn priority_eviction_never_exceeds_caps_and_does_not_evict_cto_for_normal_launch() {
        let mut p = Planner::new(Config {
            max_subs: 3,
            launch_slots: 1,
            ..Config::default()
        })
        .unwrap();
        p.request([1; 32], Priority::Cto, 0);
        p.request([2; 32], Priority::Cto, 0);
        assert!(p.request([3; 32], Priority::Cto, 0).is_empty());
        p.create([4; 32], 0);
        assert_eq!(p.reconcile(10_000), vec![Action::Unsubscribe([4; 32])]);
        assert_eq!(p.keys().count(), 2);
    }
}
