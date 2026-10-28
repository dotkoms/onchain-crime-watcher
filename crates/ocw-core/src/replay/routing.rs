//! Bind recorded routing to detector outputs, including suppressed candidates.
use crate::volume::Decision;
use serde_json::{Value, json};
use std::collections::VecDeque;
#[derive(Default)]
pub(super) struct Routing {
    enabled: bool,
    staging: bool,
    pending: VecDeque<(Decision, u64, u64)>,
    count: u64,
}
impl Routing {
    pub fn configure(&mut self, boot: &Value) -> Result<(), String> {
        self.enabled = match boot.get("route_schema") {
            None => false,
            Some(v) if v == 1 => true,
            _ => return Err("unsupported volume route schema".into()),
        };
        if self.enabled {
            self.staging = boot["staging_enabled"]
                .as_bool()
                .ok_or("staging mode missing from routing configuration")?;
        }
        Ok(())
    }
    pub fn produced(
        &mut self,
        decision: &Decision,
        now: u64,
        generation: u64,
    ) -> Result<(), String> {
        if self.enabled && decision.card(now).is_some() {
            if self.pending.len() >= 4096 {
                return Err("volume routing pending cap exceeded".into());
            }
            self.pending.push_back((decision.clone(), now, generation));
        }
        Ok(())
    }
    pub fn apply(&mut self, value: &Value, generation: u64) -> Result<(Decision, String), String> {
        if !self.enabled {
            return Err("volume route lacks a declared schema".into());
        }
        let (decision, now, expected_generation) = self
            .pending
            .pop_front()
            .ok_or("volume route has no detector candidate")?;
        if serde_json::to_value(&decision).map_err(|_| "invalid route decision")?
            != value["decision"]
            || value["apply_ms"] != now
            || value["generation"] != generation
            || generation != expected_generation
        {
            return Err("volume route differs from recomputed decision".into());
        }
        let connected = value["feed_connected"]
            .as_bool()
            .ok_or("route feed state missing")?;
        let status = value["status"]
            .as_str()
            .ok_or("route admission result missing")?;
        let valid = if !connected {
            status == "suppressed"
        } else if !self.staging {
            status == "disabled"
        } else {
            matches!(
                status,
                "queued" | "queue_rejected" | "inactive" | "busy" | "invalid"
            )
        };
        if !valid {
            return Err("volume route conflicts with its recorded mode or health".into());
        }
        self.count += 1;
        Ok((decision, status.into()))
    }
    pub fn complete(&self) -> Result<(), String> {
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err("analysis has an incomplete volume routing tail".into())
        }
    }
    pub fn summary(self) -> Value {
        json!({"routing_replayed":self.enabled,"routing_staging_enabled":self.staging,"volume_routes":self.count})
    }
}
