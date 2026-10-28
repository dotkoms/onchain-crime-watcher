//! Recorded transport receipts for CTO, bundle, status and minimal-outage notifications.
//! This evidence never authorizes a live request or replaces durable admission state.
mod replay;
use crate::{delivery::http::Outcome, history};
pub use replay::run;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
pub(crate) struct Evidence {
    pub boot: String,
    pub history: Option<history::Handle>,
}
#[derive(Clone)]
pub(crate) struct Trace(Arc<Inner>);
struct Inner {
    history: history::Handle,
    session: u64,
    origin: Instant,
    sequence: Mutex<u64>,
}
static SESSION: AtomicU64 = AtomicU64::new(1);
impl Inner {
    fn record(&self, op: &str, data: Value) {
        self.history.record(
            "notification_replay",
            json!({"session":self.session,"op":op,"data":data}),
        );
    }
    fn ns(&self) -> u64 {
        self.origin
            .elapsed()
            .as_nanos()
            .try_into()
            .unwrap_or(u64::MAX)
    }
}
impl Drop for Inner {
    fn drop(&mut self) {
        let requests = self.sequence.get_mut().map(|n| *n).unwrap_or(u64::MAX);
        self.record(
            if std::thread::panicking() {
                "error"
            } else {
                "end"
            },
            json!({"requests":requests}),
        );
    }
}
pub(crate) fn hash(value: &Value) -> String {
    format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).expect("JSON value serialization"))
    )
}
pub(crate) fn target(raw: &str) -> String {
    format!("{:x}", Sha256::digest(raw))
}
impl Trace {
    pub fn new(
        evidence: &Evidence,
        family: &str,
        target: &str,
        message: Option<String>,
        pending: bool,
    ) -> Option<Self> {
        let history = evidence.history.as_ref()?.clone();
        let trace = Self(Arc::new(Inner {
            history,
            session: SESSION.fetch_add(1, Ordering::Relaxed),
            origin: Instant::now(),
            sequence: Mutex::new(0),
        }));
        trace.0.record("start",json!({"schema":1,"boot":evidence.boot,"family":family,"target":target,"message":message,"pending":pending}));
        Some(trace)
    }
    pub fn begin(
        &self,
        method: &str,
        key: &str,
        message: Option<&str>,
        payload: &Value,
        ttl: Duration,
    ) -> Attempt {
        let mut sequence = self.0.sequence.lock().unwrap_or_else(|p| p.into_inner());
        *sequence = sequence.saturating_add(1);
        let id = *sequence;
        let ttl: u64 = ttl.as_nanos().try_into().unwrap_or(u64::MAX);
        self.0.record("request",json!({"id":id,"at_ns":self.0.ns(),"ttl_ns":ttl,"method":method,"key":key,"message":message,"payload":payload,"payload_sha256":hash(payload)}));
        Attempt {
            trace: self.clone(),
            id,
            done: false,
        }
    }
}
pub(crate) struct Attempt {
    trace: Trace,
    id: u64,
    done: bool,
}
impl Attempt {
    pub fn finish(mut self, outcome: &Outcome, pause: Duration) {
        self.record(outcome, pause);
        self.done = true;
    }
    fn record(&self, outcome: &Outcome, pause: Duration) {
        let pause: u64 = pause.as_nanos().try_into().unwrap_or(u64::MAX);
        self.trace.0.record(
            "result",
            json!({"id":self.id,"at_ns":self.trace.0.ns(),"outcome":outcome,"pause_ns":pause}),
        );
    }
}
impl Drop for Attempt {
    fn drop(&mut self) {
        if !self.done {
            self.record(&Outcome::Unknown, Duration::ZERO);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn all_implemented_lanes_replay_and_cancelled_attempts_remain_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let guard = Arc::new(crate::storage::ProcessLock::acquire(&data).unwrap());
        let runtime = history::Runtime::start(
            data.join("history"),
            "boot1".into(),
            16 * 1024 * 1024,
            Arc::new(crate::health::Health::default()),
            guard,
        )
        .await
        .unwrap();
        let e = Evidence {
            boot: "boot1".into(),
            history: Some(runtime.handle.clone()),
        };
        let payload = json!({"content":"isolated test","allowed_mentions":{"parse":[]}});
        let cto = Trace::new(&e, "cto", &"a".repeat(64), None, false).unwrap();
        cto.begin("POST", "mint", None, &payload, Duration::from_secs(120))
            .finish(&Outcome::Sent("99".into()), Duration::ZERO);
        cto.begin(
            "PATCH",
            "99",
            Some("99"),
            &payload,
            Duration::from_secs(120),
        )
        .finish(&Outcome::Sent("99".into()), Duration::ZERO);
        drop(cto);
        let status = Trace::new(&e, "status", &"b".repeat(64), Some("77".into()), false).unwrap();
        status
            .begin("PATCH", "77", Some("77"), &payload, Duration::from_secs(2))
            .finish(&Outcome::Sent("77".into()), Duration::ZERO);
        drop(status);
        let outage = Trace::new(&e, "outage", &"c".repeat(64), None, false).unwrap();
        drop(outage.begin(
            "POST",
            "trade_stream",
            None,
            &payload,
            Duration::from_secs(5),
        ));
        drop(outage);
        runtime.shutdown().await.unwrap();
        let path = data.join("history/observations.ocwh");
        let before = std::fs::read(&path).unwrap();
        let report = run(&path).unwrap();
        assert_eq!(
            report["lanes"],
            json!(["bundle", "cto", "outage", "status"])
        );
        assert_eq!(report["observed_unknown"], 1);
        assert_eq!(report["alerts_sent"], 0);
        assert_eq!(report, run(&path).unwrap());
        assert_eq!(before, std::fs::read(&path).unwrap());
        let mut entries = Vec::new();
        history::read(&path, 16 * 1024 * 1024, |e| {
            if e.kind == "notification_replay" {
                entries.push(e);
            }
            Ok(())
        })
        .unwrap();
        let cto = entries
            .iter()
            .find(|e| e.data["op"] == "start" && e.data["data"]["family"] == "cto")
            .unwrap()
            .data["session"]
            .clone();
        // A restarted process may reuse its local session counter; outage intent scope is per boot.
        let restart = tempfile::tempdir().unwrap();
        let root = restart.path().join("history");
        let mut copy = history::Journal::open(&root, 16 * 1024 * 1024, "boot1", 1).unwrap();
        for e in &entries {
            copy.append(e.wall_ms, "notification_replay", e.data.clone())
                .unwrap();
        }
        copy.sync().unwrap();
        drop(copy);
        let at = entries.last().unwrap().wall_ms;
        let outage = entries
            .iter()
            .find(|e| e.data["op"] == "start" && e.data["data"]["family"] == "outage")
            .unwrap()
            .data["session"]
            .clone();
        let mut copy = history::Journal::open(&root, 16 * 1024 * 1024, "boot2", at).unwrap();
        for e in entries.iter().filter(|e| e.data["session"] == outage) {
            let mut data = e.data.clone();
            if data["op"] == "start" {
                data["data"]["boot"] = "boot2".into();
            }
            copy.append(at, "notification_replay", data).unwrap();
        }
        copy.sync().unwrap();
        let restarted = run(&root.join("observations.ocwh")).unwrap();
        assert_eq!(restarted["requests"], 5);
        assert_eq!(restarted["observed_unknown"], 2);
        let patch = entries
            .iter()
            .find(|e| {
                e.data["session"] == cto && e.data["op"] == "request" && e.data["data"]["id"] == 2
            })
            .unwrap()
            .data
            .clone();
        for fault in [
            "wrong_receipt",
            "unsafe_mentions",
            "explicit_users",
            "missing_result",
            "missing_end",
            "edit_after_unknown",
            "post_after_unknown",
            "pending_status",
            "bad_count",
        ] {
            let copy = tempfile::tempdir().unwrap();
            let root = copy.path().join("history");
            let mut journal = history::Journal::open(&root, 16 * 1024 * 1024, "boot1", 1).unwrap();
            let mut changed = false;
            for entry in &entries {
                let mut v = entry.data.clone();
                let op = v["op"].as_str().unwrap().to_owned();
                let is_cto = v["session"] == cto;
                if fault == "edit_after_unknown" && is_cto && op == "end" {
                    v["data"]["requests"] = 3.into();
                }
                if !changed {
                    if fault == "wrong_receipt" && is_cto && op == "result" && v["data"]["id"] == 2
                    {
                        v["data"]["outcome"] = json!({"Sent":"100"});
                        changed = true;
                    } else if fault == "unsafe_mentions" && op == "request" {
                        v["data"]["payload"]["allowed_mentions"]["parse"] = json!(["everyone"]);
                        v["data"]["payload_sha256"] = hash(&v["data"]["payload"]).into();
                        changed = true;
                    } else if fault == "explicit_users" && op == "request" {
                        v["data"]["payload"]["allowed_mentions"]["users"] = json!(["123"]);
                        v["data"]["payload_sha256"] = hash(&v["data"]["payload"]).into();
                        changed = true;
                    } else if fault == "missing_result" && op == "result"
                        || fault == "missing_end" && op == "end"
                    {
                        changed = true;
                        continue;
                    } else if fault == "pending_status"
                        && op == "start"
                        && v["data"]["family"] == "status"
                    {
                        v["data"]["pending"] = true.into();
                        changed = true;
                    } else if fault == "bad_count" && op == "end" {
                        v["data"]["requests"] = 100.into();
                        changed = true;
                    } else if matches!(fault, "edit_after_unknown" | "post_after_unknown")
                        && is_cto
                        && op == "result"
                        && v["data"]["id"] == if fault == "edit_after_unknown" { 2 } else { 1 }
                    {
                        v["data"]["outcome"] = "Unknown".into();
                        changed = true;
                    }
                }
                journal
                    .append(entry.wall_ms, "notification_replay", v.clone())
                    .unwrap();
                if fault == "edit_after_unknown" && is_cto && op == "result" && v["data"]["id"] == 2
                {
                    let mut retry = patch.clone();
                    retry["data"]["id"] = 3.into();
                    retry["data"]["at_ns"] = v["data"]["at_ns"].clone();
                    journal
                        .append(entry.wall_ms, "notification_replay", retry)
                        .unwrap();
                    v["data"]["id"] = 3.into();
                    journal
                        .append(entry.wall_ms, "notification_replay", v.clone())
                        .unwrap();
                }
                if fault == "post_after_unknown" && is_cto && op == "result" && v["data"]["id"] == 1
                {
                    let mut retry = patch.clone();
                    retry["data"]["method"] = "POST".into();
                    retry["data"]["key"] = "mint".into();
                    retry["data"]["message"] = Value::Null;
                    journal
                        .append(entry.wall_ms, "notification_replay", retry)
                        .unwrap();
                }
            }
            assert!(changed);
            journal.sync().unwrap();
            assert!(run(&root.join("observations.ocwh")).is_err(), "{fault}");
        }
    }
}
