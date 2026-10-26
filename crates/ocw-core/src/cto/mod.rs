//! Curated Solana CTO listings. No ownership inference and no invented launch history.
use crate::{feed, storage::atomic_write};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    path::PathBuf,
};
pub(crate) mod runtime;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub staging_delivery: bool,
    pub channel_id: Option<String>,
    pub poll_s: u64,
    pub hourly_cap: u64,
    pub daily_cap: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            staging_delivery: false,
            channel_id: None,
            poll_s: 60,
            hourly_cap: 20,
            daily_cap: 120,
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(60..=300).contains(&self.poll_s)
            || !(1..=20).contains(&self.hourly_cap)
            || !(1..=120).contains(&self.daily_cap)
            || self.staging_delivery
                && (!self.enabled
                    || self
                        .channel_id
                        .as_ref()
                        .is_none_or(|v| v.parse::<u64>().ok().filter(|n| *n > 0).is_none()))
        {
            return Err("invalid CTO configuration");
        }
        Ok(())
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Claim {
    pub mint: [u8; 32],
    pub date_ms: u64,
}
impl Claim {
    pub fn card(&self) -> Value {
        let mint = bs58::encode(self.mint).into_string();
        json!({"content":format!("**Community takeover listing — curated claim**\nMint: `{mint}`\nDexScreener lists a takeover claim for this token. This does not verify ownership, control, or who holds tokens.\nVendor claim date: <t:{}:f>\nBundle: unavailable — verified launch history not loaded\nPrice: unavailable\nhttps://dexscreener.com/solana/{mint}",self.date_ms/1000),"allowed_mentions":{"parse":[]}})
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Posted {
    date_ms: u64,
    outcome: String,
    message_id: Option<String>,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    schema: u8,
    water: u64,
    wall: u64,
    claims: BTreeMap<String, Posted>,
    attempts: VecDeque<u64>,
    refilled: u64,
    tokens: u64,
    paused_until: u64,
    source_paused_until: u64,
    suppressed: u64,
    unknown_dates: u64,
    overflow: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    state: State,
    sha256: String,
}
pub(crate) struct Store {
    path: PathBuf,
    state: State,
}
impl Store {
    fn open(path: PathBuf) -> Result<Self, &'static str> {
        let state = if path.exists() || path.is_symlink() {
            if path.is_symlink()
                || !path.is_file()
                || std::fs::metadata(&path)
                    .map_err(|_| "CTO state unavailable")?
                    .len()
                    > 4 * 1024 * 1024
            {
                return Err("invalid CTO state");
            }
            let envelope: Envelope =
                serde_json::from_slice(&std::fs::read(&path).map_err(|_| "CTO state unavailable")?)
                    .map_err(|_| "invalid CTO state")?;
            let s = &envelope.state;
            if envelope.sha256 != hash(s)?
                || s.schema != 1
                || s.claims.len() > 20_000
                || s.attempts.len() > 120
                || s.tokens > 2
                || s.refilled > s.wall
                || s.attempts.iter().any(|at| *at > s.wall)
                || !s
                    .attempts
                    .iter()
                    .zip(s.attempts.iter().skip(1))
                    .all(|(a, b)| a <= b)
                || s.claims.iter().any(|(key, v)| {
                    feed::key::<32>(key).is_err()
                        || v.date_ms > s.water
                        || v.message_id
                            .as_ref()
                            .is_some_and(|id| id.parse::<u64>().ok().filter(|n| *n > 0).is_none())
                })
            {
                return Err("invalid CTO state");
            }
            envelope.state
        } else {
            State {
                schema: 1,
                water: 0,
                wall: 0,
                claims: BTreeMap::new(),
                attempts: VecDeque::new(),
                refilled: 0,
                tokens: 2,
                paused_until: 0,
                source_paused_until: 0,
                suppressed: 0,
                unknown_dates: 0,
                overflow: 0,
            }
        };
        Ok(Self { path, state })
    }
    fn save(&mut self, state: State) -> Result<(), &'static str> {
        let bytes = serde_json::to_vec(&Envelope {
            sha256: hash(&state)?,
            state: state.clone(),
        })
        .map_err(|_| "CTO serialization failed")?;
        if bytes.len() > 4 * 1024 * 1024 {
            return Err("CTO state capacity reached");
        }
        atomic_write(&self.path, &bytes).map_err(|_| "CTO persistence failed")?;
        self.state = state;
        Ok(())
    }
    fn observe(
        &mut self,
        value: &Value,
        now: u64,
        prime: bool,
    ) -> Result<Vec<Claim>, &'static str> {
        if now < self.state.wall {
            return Err("CTO clock rollback");
        }
        let rows = value
            .as_array()
            .filter(|rows| rows.len() <= 1000)
            .ok_or("invalid CTO response")?;
        let mut next = self.state.clone();
        next.wall = now;
        let prior = next.water;
        let mut candidates = Vec::new();
        for row in rows {
            if row["chainId"] != "solana" {
                continue;
            }
            let Some(mint) = row["tokenAddress"]
                .as_str()
                .and_then(|s| feed::key::<32>(s).ok())
            else {
                continue;
            };
            let Some(date_ms) = row["claimDate"]
                .as_str()
                .and_then(timestamp)
                .filter(|date| *date <= now.saturating_add(300_000))
            else {
                next.unknown_dates = next.unknown_dates.saturating_add(1);
                continue;
            };
            next.water = next.water.max(date_ms);
            let key = bs58::encode(mint).into_string();
            if next.claims.contains_key(&key) {
                continue;
            }
            if next.claims.len() >= 20_000 {
                return Err("CTO seen-set capacity reached");
            }
            let outcome = if prime {
                "primed"
            } else if date_ms < prior {
                next.suppressed = next.suppressed.saturating_add(1);
                "suppressed_by_high_water"
            } else if candidates.len() >= 64 {
                next.overflow = next.overflow.saturating_add(1);
                "overflow"
            } else {
                candidates.push(Claim { mint, date_ms });
                "observed"
            };
            next.claims.insert(
                key,
                Posted {
                    date_ms,
                    outcome: outcome.into(),
                    message_id: None,
                },
            );
        }
        candidates.sort_by_key(|c| (c.date_ms, c.mint));
        self.save(next)?;
        Ok(candidates)
    }
    fn reserve(&mut self, claim: &Claim, now: u64, config: &Config) -> Result<bool, &'static str> {
        if now < self.state.wall {
            return Err("CTO clock rollback");
        }
        let mut s = self.state.clone();
        s.wall = now;
        let key = bs58::encode(claim.mint).into_string();
        if s.claims.get(&key).is_none_or(|v| v.outcome != "observed") {
            return Ok(false);
        }
        while s.attempts.front().is_some_and(|at| now - *at >= 86_400_000) {
            s.attempts.pop_front();
        }
        let interval = 3_600_000 / config.hourly_cap;
        if s.refilled == 0 {
            s.refilled = now;
        }
        let credits = (now - s.refilled) / interval;
        s.tokens = (s.tokens.saturating_add(credits)).min(2);
        s.refilled += credits * interval;
        let admitted = s.paused_until <= now
            && s.tokens > 0
            && s.attempts.len() < (config.daily_cap as usize)
            && s.attempts
                .iter()
                .filter(|at| now - **at < 3_600_000)
                .count()
                < (config.hourly_cap as usize);
        if admitted {
            s.tokens -= 1;
            s.attempts.push_back(now);
        }
        s.claims.get_mut(&key).ok_or("missing CTO intent")?.outcome =
            if admitted { "reserved" } else { "budget" }.into();
        self.save(s)?;
        Ok(admitted)
    }
    fn finish(
        &mut self,
        claim: &Claim,
        outcome: &str,
        id: Option<String>,
        pause_until: u64,
    ) -> Result<(), &'static str> {
        let mut s = self.state.clone();
        let key = bs58::encode(claim.mint).into_string();
        let row = s.claims.get_mut(&key).ok_or("missing CTO intent")?;
        row.outcome = outcome.into();
        row.message_id = id;
        s.paused_until = s.paused_until.max(pause_until);
        self.save(s)
    }
}
fn hash(state: &State) -> Result<String, &'static str> {
    Ok(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(state).map_err(|_| "CTO serialization failed")?)
    ))
}
/// Strict bounded RFC3339 calendar parsing; absent dates never become observation time.
fn timestamp(s: &str) -> Option<u64> {
    if !s.is_ascii() || !(20..=35).contains(&s.len()) {
        return None;
    }
    let b = s.as_bytes();
    if b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let n = |a, b| {
        let value = s.get(a..b)?;
        if !value.bytes().all(|c| c.is_ascii_digit()) {
            return None;
        }
        value.parse::<i64>().ok()
    };
    let (year, month, day, hour, min, sec) = (
        n(0, 4)?,
        n(5, 7)?,
        n(8, 10)?,
        n(11, 13)?,
        n(14, 16)?,
        n(17, 19)?,
    );
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if leap {
                29
            } else {
                28
            }
        }
        _ => return None,
    };
    if !(1970..=9999).contains(&year)
        || !(1..=days).contains(&day)
        || hour > 23
        || min > 59
        || sec > 59
    {
        return None;
    }
    let mut at = 19;
    let mut millis = 0;
    if b[at] == b'.' {
        at += 1;
        let first = at;
        while at < b.len() && b[at].is_ascii_digit() {
            at += 1;
        }
        let count = at - first;
        if count == 0 || count > 9 {
            return None;
        }
        let fraction = &s[first..at];
        millis = fraction[..count.min(3)].parse::<i64>().ok()? * 10i64.pow(3 - count.min(3) as u32);
    }
    let offset = if s.get(at..) == Some("Z") {
        0
    } else {
        if b.len() != at + 6 || !matches!(b[at], b'+' | b'-') || b[at + 3] != b':' {
            return None;
        }
        let (h, m) = (n(at + 1, at + 3)?, n(at + 4, at + 6)?);
        if h > 23 || m > 59 {
            return None;
        }
        (h * 3600 + m * 60) * if b[at] == b'+' { 1 } else { -1 }
    };
    let y = year - i64::from(month <= 2);
    let era = y / 400;
    let yoe = y - era * 400;
    let m = month + if month > 2 { -3 } else { 9 };
    let doy = (153 * m + 2) / 5 + day - 1;
    let total_days = era * 146097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719468;
    u64::try_from(((total_days * 24 + hour) * 3600 + min * 60 + sec - offset) * 1000 + millis).ok()
}
#[cfg(test)]
mod tests;
