//! Verify durable grants and local credits without opening or changing a ledger.
use crate::{config::Config, spend::Snapshot};
use serde_json::{Value, json};
#[derive(Default)]
pub(super) struct Spend {
    schema: bool,
    state: Option<Snapshot>,
    credits: u64,
    credit_day: u64,
    due: Option<u64>,
    active: bool,
    reservations: u64,
}
impl Spend {
    pub fn configure(&mut self, boot: &Value) -> Result<(), String> {
        self.schema = match boot.get("spend_schema") {
            None => false,
            Some(v) if v == 1 => true,
            _ => return Err("unsupported spend replay schema".into()),
        };
        Ok(())
    }
    pub fn step(&mut self, kind: &str, value: &Value, config: &Config) -> Result<(), String> {
        match kind {
            "spend_start" => {
                if !self.schema
                    || self.state.is_some()
                    || value["schema"] != 1
                    || value["credits"] != 0
                {
                    return Err("invalid spend replay start".into());
                }
                self.state = Some(
                    serde_json::from_value(value["state"].clone())
                        .map_err(|_| "invalid starting spend ledger")?,
                );
                self.credit_day = value["credit_day"]
                    .as_u64()
                    .ok_or("spend credit day missing")?;
                self.active = true;
            }
            "spend_check" if self.schema => {
                if !self.active
                    || self.due.is_some()
                    || value["credits"] != self.credits
                    || value["credit_day"] != self.credit_day
                {
                    return Err("spend credit check mismatch".into());
                }
                let day = value["day"].as_u64().ok_or("spend day missing")?;
                if day > self.credit_day {
                    self.credits = 0;
                    self.credit_day = day;
                }
                if self.credits == 0 {
                    self.due = Some(day);
                }
            }
            "spend_reservation" if self.schema => {
                let day = self
                    .due
                    .take()
                    .ok_or("spend reservation has no credit check")?;
                let state = self.state.as_ref().ok_or("spend starting state missing")?;
                let (next, grant) = state.reserve(
                    day,
                    config.gate.daily_messages - config.gate.stop_margin_messages,
                    config.gate.reservation_messages,
                );
                if value["day"] != day
                    || value["grant"] != grant
                    || value["reserved"] != next.reserved
                {
                    return Err("replayed spend reservation differs from capture".into());
                }
                self.state = Some(next);
                self.credits = grant;
                self.active = grant > 0;
                self.reservations += 1;
            }
            "paid_stream_stopped" | "pipeline_panic" => {
                self.complete()?;
                self.active = false;
            }
            "redacted_frame" if self.active => {
                return Err("spend replay cannot classify a redacted billable frame".into());
            }
            _ => {}
        }
        Ok(())
    }
    pub fn frame(&mut self, bytes: &[u8]) -> Result<(), String> {
        if self.active && crate::session::billable(bytes) {
            if self.due.is_some() || self.credits == 0 {
                return Err("billable frame lacks reserved spend credits".into());
            }
            self.credits -= 1;
        }
        Ok(())
    }
    pub fn complete(&self) -> Result<(), String> {
        if self.due.is_some() {
            return Err("spend replay has an incomplete reservation".into());
        }
        Ok(())
    }
    pub fn summary(self) -> Value {
        json!({"financial_admission_replayed":self.schema && self.state.is_some(),"spend_reservations":self.reservations,"spend_reserved_messages":self.state.as_ref().map(|s|s.reserved),"spend_ledger_day":self.state.as_ref().map(|s|s.day),"spend_local_credits":self.credits,"provider_billing_verified":false})
    }
}
