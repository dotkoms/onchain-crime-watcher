//! Recompute desired watch membership; no sockets, credits or provider responses.
use crate::{
    capture::Record,
    config::Config,
    feed::{self, Trade},
    watch::{Action, Planner},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::VecDeque;

#[derive(Default)]
pub(super) struct Controller {
    schema: bool,
    planner: Option<Planner>,
    generation: u64,
    planned_frame: Option<u64>,
    expected: VecDeque<(Action, u64)>,
    pub plans: u64,
    starts: u64,
    hash: Sha256,
}
impl Controller {
    pub fn configure(&mut self, boot: &Value) -> Result<(), String> {
        self.schema = match boot.get("watch_schema") {
            None => false,
            Some(v) if v == 1 => true,
            _ => return Err("unsupported watch replay schema".into()),
        };
        Ok(())
    }
    pub fn start(&mut self, config: &Config, record: &Record, step: &Value) -> Result<(), String> {
        if !self.schema || !config.feed.auto_watch || step["schema"] != 1 || self.planner.is_some()
        {
            return Err("invalid watch controller start".into());
        }
        self.planner = Some(Planner::new(config.watch.clone()).map_err(str::to_owned)?);
        self.generation = record.generation;
        self.starts += 1;
        Ok(())
    }
    pub fn plan(
        &mut self,
        record: &Record,
        step: &Value,
        pending: Option<&Record>,
    ) -> Result<(), String> {
        if !self.expected.is_empty()
            || self.planned_frame.is_some()
            || record.generation != self.generation
        {
            return Err("watch plan interrupts unapplied actions".into());
        }
        let planner = self
            .planner
            .as_mut()
            .ok_or("watch plan has no controller")?;
        let now = step["apply_ms"]
            .as_u64()
            .ok_or("watch plan clock missing")?;
        if now > record.mono_ns / 1_000_000 {
            return Err("watch plan clock is in the future".into());
        }
        let mut actions = planner.reconcile(now);
        if let Some(raw) = pending {
            if step["input_seq"] != raw.seq
                || now != raw.mono_ns / 1_000_000
                || raw.generation != self.generation
            {
                return Err("watch plan frame identity mismatch".into());
            }
            let parsed: Option<Value> = serde_json::from_slice(&raw.payload).ok();
            if let Some(value) = parsed
                && value["txType"] == "create"
                && let Some(mint) = value["mint"].as_str().and_then(|s| feed::key::<32>(s).ok())
            {
                actions.extend(planner.create(mint, now));
            }
            self.planned_frame = Some(raw.seq);
        } else if !step["input_seq"].is_null() {
            return Err("watch plan frame missing".into());
        }
        let recorded = step["actions"]
            .as_array()
            .filter(|a| a.len() <= 40)
            .ok_or("watch actions invalid")?;
        let recorded: Vec<Action> = serde_json::from_value(Value::Array(recorded.clone()))
            .map_err(|_| "watch actions invalid")?;
        if recorded != actions {
            return Err("replayed watch actions differ from capture".into());
        }
        let bytes =
            serde_json::to_vec(&json!([record.generation, now, step["input_seq"], actions]))
                .map_err(|_| "watch digest invalid")?;
        self.hash.update((bytes.len() as u64).to_le_bytes());
        self.hash.update(bytes);
        self.expected
            .extend(actions.into_iter().map(|action| (action, now)));
        self.plans += 1;
        Ok(())
    }
    pub fn apply(&mut self, record: &Record, step: &Value) -> Result<(), String> {
        if !self.schema {
            return Ok(());
        }
        let mint = feed::key::<32>(step["mint"].as_str().ok_or("watch mint missing")?)
            .map_err(|_| "invalid watch mint")?;
        let action = match step["operation"].as_str() {
            Some("attach") => Action::Subscribe {
                mint,
                created_ms: if step["observed_create_ms"].is_null() {
                    None
                } else {
                    Some(
                        step["observed_create_ms"]
                            .as_u64()
                            .ok_or("watch creation clock invalid")?,
                    )
                },
                partial: step["partial"]
                    .as_bool()
                    .ok_or("watch partial flag missing")?,
            },
            Some("remove") => Action::Unsubscribe(mint),
            _ => return Err("invalid watch action".into()),
        };
        let now = step["apply_ms"].as_u64().ok_or("watch clock missing")?;
        if record.generation != self.generation || self.expected.pop_front() != Some((action, now))
        {
            return Err("watch application differs from controller".into());
        }
        Ok(())
    }
    pub fn frame(&mut self, raw: &Record, step: &Value, discarded: bool) -> Result<(), String> {
        let Some(planner) = &self.planner else {
            return Ok(());
        };
        if self.planned_frame.take() != Some(raw.seq)
            || !self.expected.is_empty()
            || raw.generation != self.generation
        {
            return Err("watch frame lacks completed planning evidence".into());
        }
        let parsed: Option<Value> = serde_json::from_slice(&raw.payload).ok();
        let mint = parsed
            .as_ref()
            .and_then(|v| v["mint"].as_str())
            .and_then(|s| feed::key::<32>(s).ok());
        let active = mint.is_some_and(|key| planner.contains(&key));
        let inactive_trade = parsed
            .as_ref()
            .is_some_and(|v| matches!(v["txType"].as_str(), Some("buy" | "sell")))
            && !active;
        if discarded != inactive_trade
            || (!discarded && step["allow_create"] != active)
            || (discarded && step["reason"] != "inactive_subscription")
        {
            return Err("recorded frame admission differs from controller".into());
        }
        Ok(())
    }
    pub fn trade(&mut self, trade: &Trade, now: u64) {
        if let Some(planner) = &mut self.planner {
            planner.trade(&trade.mint, trade.sol_lamports, now);
        }
    }
    pub fn boundary(&mut self) -> Result<(), String> {
        self.complete()?;
        self.planner = None;
        Ok(())
    }
    pub fn complete(&self) -> Result<(), String> {
        if !self.expected.is_empty() || self.planned_frame.is_some() {
            return Err("incomplete watch controller execution tail".into());
        }
        Ok(())
    }
    pub fn summary(self) -> Value {
        json!({"watch_controller_replayed":self.schema && self.starts > 0,"watch_plans":self.plans,"watch_sessions":self.starts,"watch_decision_sha256":format!("{:x}",self.hash.finalize()),"financial_admission_replayed":false,"subscription_transport_replayed":false})
    }
}
