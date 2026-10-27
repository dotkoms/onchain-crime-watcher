//! Best-effort replay evidence. Durable send reservations remain the delivery safety barrier.
use super::{http::Outcome, policy::Candidate};
use crate::{config::DeliveryConfig, history};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::time::Instant;
#[derive(Clone)]
pub(super) struct Audit {
    history: history::Handle,
    origin: Instant,
}
impl Audit {
    pub fn start(
        history: history::Handle,
        origin: Instant,
        boot: &str,
        config: &DeliveryConfig,
        state: Value,
    ) -> Self {
        let this = Self { history, origin };
        this.record(
            "start",
            json!({"schema":1,"boot":boot,"config":config,"state":state}),
        );
        this
    }
    pub fn ns(&self, at: Instant) -> u64 {
        at.duration_since(self.origin)
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX)
    }
    pub fn record(&self, op: &str, data: Value) {
        self.history
            .record("delivery_replay", json!({"op":op,"data":data}));
    }
    pub fn candidate(&self, c: &Candidate) -> Value {
        candidate(c, self.origin)
    }
    pub fn attempt(
        &self,
        c: &Candidate,
        now: Instant,
        generation: u64,
        connected: bool,
        payload: &Value,
    ) {
        self.record("attempt",json!({"id":c.id,"now_ns":self.ns(now),"generation":generation,"connected":connected,"attempts":c.attempts,"payload_sha256":hash(payload)}));
    }
    pub fn response(&self, id: &str, outcome: &Outcome, paused: Instant) {
        self.record(
            "response",
            json!({"id":id,"outcome":outcome,"paused_ns":self.ns(paused)}),
        );
    }
}
pub(super) fn hash(value: &Value) -> String {
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).expect("JSON value serialization"))
    )
}

pub(super) fn candidate(c: &Candidate, origin: Instant) -> Value {
    let ready: u64 = c
        .ready
        .duration_since(origin)
        .as_nanos()
        .try_into()
        .unwrap_or(u64::MAX);
    json!({"decision":c.decision,"id":c.id,"generation":c.generation,"ready_ns":ready,"reserved":c.reserved,"attempts":c.attempts})
}
