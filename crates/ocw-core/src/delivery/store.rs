use crate::{config::DeliveryConfig, storage::atomic_write};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(super) enum Tier {
    Fire,
    Near,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Entry {
    id: String,
    tier: Tier,
    at: u64,
    message_id: Option<String>,
    outcome: String,
}
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct State {
    clock: u64,
    refilled: u64,
    tokens: u64,
    entries: Vec<Entry>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    version: u8,
    checksum: String,
    state: State,
}
pub(super) struct Journal {
    path: PathBuf,
    state: State,
    config: DeliveryConfig,
}
impl Journal {
    pub fn open(path: &Path, config: &DeliveryConfig) -> io::Result<Self> {
        let state = if path.exists() || path.is_symlink() {
            if path.is_symlink() || fs::metadata(path)?.len() > 65536 {
                return Err(io::Error::other("invalid delivery journal"));
            }
            let envelope: Envelope = serde_json::from_slice(&fs::read(path)?)
                .map_err(|_| io::Error::other("corrupt delivery journal"))?;
            let checksum = format!("{:x}", Sha256::digest(serde_json::to_vec(&envelope.state)?));
            if envelope.version != 1
                || checksum != envelope.checksum
                || envelope.state.entries.len() > 40
                || envelope.state.tokens > 2
                || envelope.state.refilled > envelope.state.clock
                || envelope.state.entries.iter().any(|e| {
                    e.at > envelope.state.clock
                        || e.id.len() > 128
                        || e.message_id
                            .as_ref()
                            .is_some_and(|id| id.parse::<u64>().ok().filter(|n| *n > 0).is_none())
                })
            {
                return Err(io::Error::other("corrupt delivery journal"));
            }
            envelope.state
        } else {
            State {
                clock: 0,
                refilled: 0,
                tokens: config.burst,
                entries: Vec::new(),
            }
        };
        Ok(Self {
            path: path.into(),
            state,
            config: config.clone(),
        })
    }
    fn save(&mut self, state: State) -> io::Result<()> {
        let checksum = format!("{:x}", Sha256::digest(serde_json::to_vec(&state)?));
        let bytes = serde_json::to_vec(&Envelope {
            version: 1,
            checksum,
            state: state.clone(),
        })?;
        atomic_write(&self.path, &bytes)?;
        self.state = state;
        Ok(())
    }
    pub fn contains(&self, id: &str) -> bool {
        self.state.entries.iter().any(|e| e.id == id)
    }
    pub fn reserve(&mut self, id: &str, tier: Tier, now: u64) -> io::Result<bool> {
        if id.is_empty() || id.len() > 128 || self.contains(id) {
            return Ok(false);
        }
        let mut state = self.state.clone();
        let now = now.max(state.clock); // Clock rollback cannot reset a rolling budget.
        if state.entries.is_empty() && state.clock == 0 {
            state.refilled = now;
        }
        state.clock = now;
        state.entries.retain(|e| now - e.at < 86_400_000);
        let hour: Vec<_> = state
            .entries
            .iter()
            .filter(|e| now - e.at < 3_600_000)
            .collect();
        let near_hour = hour.iter().filter(|e| e.tier == Tier::Near).count() as u64;
        let near_day = state
            .entries
            .iter()
            .filter(|e| e.tier == Tier::Near)
            .count() as u64;
        if hour.len() as u64 >= self.config.hourly_cap
            || state.entries.len() as u64 >= self.config.daily_cap
            || (tier == Tier::Near
                && (near_hour >= self.config.hourly_cap * 30 / 100
                    || near_day >= self.config.daily_cap * 30 / 100
                    || hour.len() as u64 >= self.config.hourly_cap - 2))
        {
            return Ok(false);
        }
        let interval = 3_600_000 / self.config.hourly_cap;
        let credits = (now - state.refilled) / interval;
        state.tokens = (state.tokens.saturating_add(credits)).min(self.config.burst);
        state.refilled += credits * interval;
        if state.tokens == 0 {
            return Ok(false);
        }
        state.tokens -= 1;
        state.entries.push(Entry {
            id: id.into(),
            tier,
            at: now,
            message_id: None,
            outcome: "reserved".into(),
        });
        self.save(state)?;
        Ok(true)
    }
    pub fn finish(&mut self, id: &str, message_id: Option<&str>, unknown: bool) -> io::Result<()> {
        let mut state = self.state.clone();
        let entry = state
            .entries
            .iter_mut()
            .find(|e| e.id == id)
            .ok_or_else(|| io::Error::other("missing delivery reservation"))?;
        entry.message_id = message_id.map(str::to_owned);
        entry.outcome = if message_id.is_some() {
            "sent"
        } else if unknown {
            "unknown"
        } else {
            "not_sent"
        }
        .into();
        self.save(state)
    }
    #[cfg(test)]
    pub fn receipt(&self, id: &str) -> Option<&str> {
        self.state
            .entries
            .iter()
            .find(|e| e.id == id)?
            .message_id
            .as_deref()
    }
}
