//! Offline staging-volume policy replay. Reads normalized history, never credentials or HTTP.
use super::{
    audit,
    http::Outcome,
    policy::{Candidate, Queue},
    store::{Journal, Tier},
};
mod decision;
use crate::{
    config::{Config, DeliveryConfig},
    history,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    io,
    path::Path,
    time::{Duration, Instant},
};

struct Model {
    queue: Queue,
    journal: Journal,
    boot: String,
    origin: Instant,
    selected: Option<Candidate>,
    request: bool,
    retryable: bool,
    receipt: Option<(String, Option<String>, bool)>,
    paused: Instant,
    blocked: bool,
    cleared: bool,
    ended: bool,
}
impl Model {
    fn start(data: &Value, boot: &str) -> Result<Self, String> {
        if data["schema"] != 1
            || data["boot"] != boot
            || boot.is_empty()
            || boot.len() > 64
            || !boot.bytes().all(|b| b.is_ascii_alphanumeric())
        {
            return Err("invalid delivery replay start".into());
        }
        let config: DeliveryConfig = serde_json::from_value(data["config"].clone())
            .map_err(|_| "invalid recorded delivery configuration")?;
        Config {
            delivery: config.clone(),
            ..Config::default()
        }
        .validate()
        .map_err(|_| "invalid recorded delivery configuration")?;
        let origin = Instant::now();
        Ok(Self {
            queue: Queue::new(config.queue_capacity),
            journal: Journal::memory(&config, data["state"].clone())
                .map_err(|_| "invalid recorded delivery state")?,
            boot: boot.into(),
            origin,
            selected: None,
            request: false,
            retryable: false,
            receipt: None,
            paused: origin,
            blocked: false,
            cleared: false,
            ended: false,
        })
    }
    fn at(&self, value: &Value, key: &str) -> Result<Instant, String> {
        self.origin
            .checked_add(Duration::from_nanos(number(value, key)?))
            .ok_or_else(|| "delivery replay clock overflow".into())
    }
    fn candidate(&self, value: &Value) -> Result<Candidate, String> {
        let decision = decision::parse(value["decision"].clone())?;
        let mut c = Candidate::new(
            decision,
            &self.boot,
            self.origin,
            number(value, "generation")?,
        )
        .ok_or("invalid recorded candidate")?;
        if value["id"] != c.id {
            return Err("delivery candidate identity mismatch".into());
        }
        let ready = self.at(value, "ready_ns")?;
        if ready < c.ready || ready >= c.expires {
            return Err("invalid candidate ready clock".into());
        }
        c.ready = ready;
        c.reserved = boolean(value, "reserved")?;
        c.attempts = number(value, "attempts")?
            .try_into()
            .map_err(|_| "invalid candidate attempt count")?;
        Ok(c)
    }
    fn step(&mut self, op: &str, data: &Value) -> Result<Value, String> {
        if self.ended {
            return Err("delivery replay operation after shutdown".into());
        }
        match op {
            "insert" => {
                let c = self.candidate(&data["candidate"])?;
                if c.reserved || c.attempts > 0 {
                    let selected = self
                        .selected
                        .as_ref()
                        .ok_or("retry has no selected candidate")?;
                    if c.attempts >= 3
                        || !self.retryable
                        || self.request
                        || self
                            .receipt
                            .as_ref()
                            .is_none_or(|(_, message, unknown)| message.is_some() || *unknown)
                        || audit::candidate(selected, self.origin) != data["candidate"]
                    {
                        return Err("delivery retry lacks a matching explicit rate limit".into());
                    }
                    self.selected = None;
                    self.receipt = None;
                }
                let accepted = self.queue.insert(c);
                if data["accepted"] != accepted || data["replaced"] != self.queue.replaced {
                    return Err("replayed queue admission differs from history".into());
                }
                self.cleared = false;
                Ok(json!([accepted, self.queue.replaced]))
            }
            "prune" => {
                let count = self
                    .queue
                    .prune(self.at(data, "now_ns")?, number(data, "generation")?);
                if data["pruned"] != count {
                    return Err("replayed queue expiry differs from history".into());
                }
                Ok(json!(count))
            }
            "take" => {
                if self.request || self.receipt.is_some() {
                    return Err("delivery selection interrupts an unfinished request".into());
                }
                let c = self
                    .queue
                    .take(self.at(data, "now_ns")?, number(data, "generation")?);
                if json!(c.as_ref().map(|c| &c.id)) != data["id"] {
                    return Err("replayed queue priority differs from history".into());
                }
                let result = json!(c.as_ref().map(|c| &c.id));
                self.selected = c;
                Ok(result)
            }
            "reserve" => {
                let id = text(data, "id")?;
                let tier: Tier = serde_json::from_value(data["tier"].clone())
                    .map_err(|_| "invalid delivery tier")?;
                let c = self
                    .selected
                    .as_mut()
                    .filter(|c| c.id == id && c.tier == tier)
                    .ok_or("reservation has no selected candidate")?;
                let accepted = self
                    .journal
                    .reserve(id, tier, number(data, "wall_ms")?)
                    .map_err(|_| "recorded delivery reservation invalid")?;
                if data["accepted"] != accepted
                    || data["state_sha256"] != audit::hash(&self.journal.snapshot())
                {
                    return Err("replayed delivery budget differs from history".into());
                }
                if accepted {
                    c.reserved = true;
                } else {
                    self.selected = None;
                }
                Ok(json!([accepted, audit::hash(&self.journal.snapshot())]))
            }
            "attempt" => {
                let now = self.at(data, "now_ns")?;
                let c = self
                    .selected
                    .as_mut()
                    .ok_or("delivery attempt has no selected candidate")?;
                if self.request
                    || self.blocked
                    || !c.reserved
                    || !self.journal.contains(&c.id)
                    || data["id"] != c.id
                    || !boolean(data, "connected")?
                    || data["generation"] != c.generation
                    || now < c.ready
                    || now >= c.expires
                    || now < self.paused
                    || c.attempts >= 3
                {
                    return Err("delivery attempt violates reservation or freshness policy".into());
                }
                c.attempts += 1;
                let payload = c
                    .payload(now)
                    .ok_or("recorded payload is no longer eligible")?;
                if data["attempts"] != c.attempts || data["payload_sha256"] != audit::hash(&payload)
                {
                    return Err("replayed delivery payload differs from history".into());
                }
                self.request = true;
                Ok(json!([c.id, c.attempts, audit::hash(&payload)]))
            }
            "response" => {
                let paused = self.at(data, "paused_ns")?;
                let c = self
                    .selected
                    .as_mut()
                    .ok_or("delivery response has no selected candidate")?;
                if !self.request || data["id"] != c.id {
                    return Err("delivery response has no request".into());
                }
                let outcome: Outcome = serde_json::from_value(data["outcome"].clone())
                    .map_err(|_| "invalid normalized delivery response")?;
                self.request = false;
                self.retryable = matches!(outcome, Outcome::RateLimited);
                self.paused = paused;
                let (message, unknown) = match &outcome {
                    Outcome::Sent(id) => {
                        if id.parse::<u64>().ok().filter(|n| *n > 0).is_none() {
                            return Err("invalid delivery receipt identity".into());
                        }
                        (Some(id.clone()), false)
                    }
                    Outcome::Unknown => (None, true),
                    Outcome::WrongTarget => {
                        self.blocked = true;
                        (None, true)
                    }
                    Outcome::Broken => {
                        self.blocked = true;
                        (None, false)
                    }
                    Outcome::RateLimited => {
                        c.ready = c.ready.max(paused);
                        (None, false)
                    }
                    Outcome::Rejected => (None, false),
                };
                self.receipt = Some((c.id.clone(), message, unknown));
                Ok(data["outcome"].clone())
            }
            "finish" => {
                let id = text(data, "id")?;
                let message = if data["message_id"].is_null() {
                    None
                } else {
                    Some(text(data, "message_id")?.to_owned())
                };
                let unknown = boolean(data, "unknown")?;
                if self.selected.as_ref().is_none_or(|c| c.id != id)
                    || self.request
                    || self
                        .receipt
                        .as_ref()
                        .is_some_and(|r| r != &(id.to_owned(), message.clone(), unknown))
                    || self.receipt.is_none() && (message.is_some() || unknown)
                {
                    return Err("delivery receipt differs from recorded response".into());
                }
                self.journal
                    .finish(id, message.as_deref(), unknown)
                    .map_err(|_| "invalid recorded delivery receipt")?;
                if data["state_sha256"] != audit::hash(&self.journal.snapshot()) {
                    return Err("replayed receipt state differs from history".into());
                }
                self.receipt = None;
                self.selected = None;
                Ok(json!(audit::hash(&self.journal.snapshot())))
            }
            "clear" => {
                let count = self.queue.clear();
                if data["removed"] != count {
                    return Err("delivery clear count mismatch".into());
                }
                self.cleared = true;
                Ok(json!(count))
            }
            "end" => {
                if !self.cleared || self.request || self.receipt.is_some() {
                    return Err("delivery replay has an incomplete shutdown".into());
                }
                self.ended = true;
                Ok(json!("ended"))
            }
            _ => Err("unsupported or failed delivery replay operation".into()),
        }
    }
}
fn number(value: &Value, key: &str) -> Result<u64, String> {
    value[key]
        .as_u64()
        .ok_or_else(|| "recorded delivery integer missing".into())
}
fn boolean(value: &Value, key: &str) -> Result<bool, String> {
    value[key]
        .as_bool()
        .ok_or_else(|| "recorded delivery boolean missing".into())
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value[key]
        .as_str()
        .ok_or_else(|| "recorded delivery text missing".into())
}

/// Replays the staging-volume lane only. Other lanes and raw HTTP classification remain separate.
pub fn run(path: &Path) -> Result<Value, String> {
    let mut model: Option<Model> = None;
    let mut sessions = 0u64;
    let mut steps = 0u64;
    let mut records = 0u64;
    let mut attempts = 0u64;
    let mut sent = 0u64;
    let mut unknown = 0u64;
    let mut input = Sha256::new();
    let mut decisions = Sha256::new();
    let mut failure = None;
    history::read(path, 512 * 1024 * 1024, |entry| {
        let result = (|| -> Result<(), String> {
            records += 1;
            if records > 1_000_000 {
                return Err("delivery replay record cap exceeded".into());
            }
            let bytes = serde_json::to_vec(&entry).map_err(|_| "invalid replay input")?;
            input.update((bytes.len() as u64).to_le_bytes());
            input.update(bytes);
            if entry.kind == "coverage_gap"
                && matches!(
                    entry.data["reason"].as_str(),
                    Some("queue_loss" | "writer_failure")
                )
                || entry.kind == "boot"
                    && entry.data["recovered_tail_bytes"]
                        .as_u64()
                        .is_some_and(|n| n > 0)
            {
                return Err("delivery replay evidence contains history loss".into());
            }
            if entry.kind != "delivery_replay" {
                return Ok(());
            }
            let op = text(&entry.data, "op")?;
            let data = &entry.data["data"];
            let result = if op == "start" {
                if model.as_ref().is_some_and(|m| !m.ended) {
                    return Err("delivery replay session is incomplete".into());
                }
                model = Some(Model::start(data, &entry.boot)?);
                sessions += 1;
                json!("started")
            } else {
                let model = model
                    .as_mut()
                    .filter(|m| m.boot == entry.boot)
                    .ok_or("delivery operation lacks matching boot state")?;
                let output = model.step(op, data)?;
                if op == "attempt" {
                    attempts += 1;
                }
                if op == "response" {
                    match serde_json::from_value::<Outcome>(data["outcome"].clone())
                        .map_err(|_| "invalid recorded response")?
                    {
                        Outcome::Sent(_) => sent += 1,
                        Outcome::Unknown | Outcome::WrongTarget => unknown += 1,
                        _ => {}
                    }
                }
                output
            };
            steps += 1;
            let bytes = serde_json::to_vec(&json!([op, result]))
                .map_err(|_| "invalid replayed policy decision")?;
            decisions.update((bytes.len() as u64).to_le_bytes());
            decisions.update(bytes);
            Ok(())
        })();
        if let Err(error) = result {
            failure = Some(error);
            return Err(io::Error::other("delivery replay stopped"));
        }
        Ok(())
    })
    .map_err(|_| {
        failure.unwrap_or_else(|| "delivery history corrupt, torn or unreadable".into())
    })?;
    if model.as_ref().is_none_or(|m| !m.ended) {
        return Err("delivery replay needs complete start and shutdown evidence".into());
    }
    Ok(
        json!({"event":"delivery_replay_complete","lane":"staging_volume","delivery_policy_replayed":true,"sessions":sessions,"steps":steps,"attempts":attempts,"observed_sent":sent,"observed_unknown":unknown,"alerts_sent":0,"network":"disabled","input_sha256":format!("{:x}",input.finalize()),"decision_sha256":format!("{:x}",decisions.finalize()),"http_classification_replayed":false,"other_lanes_replayed":false,"coverage":"recorded queue and budget decisions with normalized observed outcomes; provider and Discord acceptance unverified"}),
    )
}
