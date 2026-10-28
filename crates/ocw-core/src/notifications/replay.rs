use crate::{delivery::http::Outcome, history};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io,
    path::Path,
};
struct Request {
    at: u64,
    method: String,
    key: String,
    message: Option<String>,
    lane: String,
}
struct Session {
    boot: String,
    family: String,
    target: String,
    message: Option<String>,
    pending: bool,
    next: u64,
    open: BTreeMap<u64, Request>,
    ended: bool,
    blocked: bool,
}
fn text<'a>(value: &'a Value, key: &str) -> Result<&'a str, String> {
    value[key]
        .as_str()
        .ok_or_else(|| "notification text missing".into())
}
fn num(value: &Value, key: &str) -> Result<u64, String> {
    value[key]
        .as_u64()
        .ok_or_else(|| "notification integer missing".into())
}
fn message(value: &Value, key: &str) -> Result<Option<String>, String> {
    if value[key].is_null() {
        Ok(None)
    } else {
        let s = text(value, key)?;
        if s.parse::<u64>().ok().is_none_or(|n| n == 0) {
            return Err("invalid notification message identity".into());
        }
        Ok(Some(s.into()))
    }
}
fn hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
/// Verifies normalized receipt handling and request identities, not upstream decision/budget logic.
pub fn run(path: &Path) -> Result<Value, String> {
    let mut sessions = BTreeMap::<(String, u64), Session>::new();
    let mut posts = BTreeSet::new();
    let mut blocked_edits = BTreeSet::new();
    let mut lanes = BTreeSet::new();
    let mut records = 0u64;
    let mut requests = 0u64;
    let mut sent = 0u64;
    let mut unknown = 0u64;
    let mut source = Sha256::new();
    let mut outputs = Sha256::new();
    let mut failure = None;
    history::read(path, 512 * 1024 * 1024, |entry| {
        let result = (|| -> Result<(), String> {
            records += 1;
            if records > 1_000_000 {
                return Err("notification replay record cap exceeded".into());
            }
            let bytes = serde_json::to_vec(&entry).map_err(|_| "invalid notification input")?;
            source.update((bytes.len() as u64).to_le_bytes());
            source.update(bytes);
            if entry.kind == "coverage_gap"
                && matches!(
                    entry.data["reason"].as_str(),
                    Some("queue_loss" | "writer_failure")
                )
                || entry.kind == "boot"
                    && entry.data["recovered_tail_bytes"]
                        .as_u64()
                        .is_some_and(|v| v > 0)
            {
                return Err("notification evidence contains history loss".into());
            }
            if entry.kind != "notification_replay" {
                return Ok(());
            }
            let id = num(&entry.data, "session")?;
            let op = text(&entry.data, "op")?;
            let d = &entry.data["data"];
            if op == "start" {
                if id == 0
                    || sessions.contains_key(&(entry.boot.clone(), id))
                    || sessions.len() >= 4096
                    || d["schema"] != 1
                    || d["boot"] != entry.boot
                {
                    return Err("invalid notification session start".into());
                }
                let family = text(d, "family")?;
                let target = text(d, "target")?;
                if !matches!(family, "cto" | "status" | "outage") || !hex(target) {
                    return Err("invalid notification lane or target digest".into());
                }
                let pending = d["pending"]
                    .as_bool()
                    .ok_or("notification checkpoint pending flag missing")?;
                sessions.insert(
                    (entry.boot.clone(), id),
                    Session {
                        boot: entry.boot.clone(),
                        family: family.into(),
                        target: target.into(),
                        message: message(d, "message")?,
                        pending,
                        next: 1,
                        open: BTreeMap::new(),
                        ended: false,
                        blocked: false,
                    },
                );
            } else {
                let s = sessions
                    .get_mut(&(entry.boot.clone(), id))
                    .filter(|s| s.boot == entry.boot && !s.ended)
                    .ok_or("notification operation has no active session")?;
                match op {
                    "request" => {
                        let req = num(d, "id")?;
                        let method = text(d, "method")?;
                        let key = text(d, "key")?;
                        let msg = message(d, "message")?;
                        let at = num(d, "at_ns")?;
                        let ttl = num(d, "ttl_ns")?;
                        if req != s.next
                            || s.open.len() >= 64
                            || ttl == 0
                            || ttl > 120_000_000_000
                            || key.is_empty()
                            || key.len() > 128
                            || !matches!(method, "POST" | "PATCH")
                            || (method == "POST") != msg.is_none()
                        {
                            return Err("invalid notification request identity or deadline".into());
                        }
                        let payload = &d["payload"];
                        if payload.as_object().is_none_or(|p| p.len() != 2)
                            || payload["allowed_mentions"]
                                .as_object()
                                .is_none_or(|m| m.len() != 1)
                            || payload["content"]
                                .as_str()
                                .is_none_or(|c| c.chars().count() > 2000)
                            || payload["allowed_mentions"]["parse"] != json!([])
                            || d["payload_sha256"] != super::hash(payload)
                        {
                            return Err("unsafe or changed recorded notification payload".into());
                        }
                        let lane = if s.family == "cto" && method == "PATCH" {
                            "bundle"
                        } else {
                            &s.family
                        };
                        if s.family == "status" && (s.blocked || !s.open.is_empty())
                            || s.family == "outage" && method != "POST"
                            || s.family == "status"
                                && (s.pending
                                    || (method == "POST" && s.message.is_some())
                                    || method == "PATCH" && msg != s.message)
                        {
                            return Err("notification request contradicts checkpoint state".into());
                        }
                        if method == "POST"
                            && !posts.insert((
                                s.target.clone(),
                                lane.to_owned(),
                                if lane == "outage" {
                                    format!("{}:{key}", s.boot)
                                } else {
                                    key.to_owned()
                                },
                            ))
                        {
                            return Err("notification POST repeats a recorded intent".into());
                        }
                        if lane == "bundle"
                            && blocked_edits.contains(&(
                                s.target.clone(),
                                msg.clone().ok_or("bundle target missing")?,
                            ))
                        {
                            return Err(
                                "bundle edit repeats an unresolved or rejected operation".into()
                            );
                        }
                        lanes.insert(lane.to_owned());
                        s.open.insert(
                            req,
                            Request {
                                at,
                                method: method.into(),
                                key: key.into(),
                                message: msg,
                                lane: lane.into(),
                            },
                        );
                        s.next += 1;
                        requests += 1;
                        if requests > 100_000 {
                            return Err("notification request cap exceeded".into());
                        }
                    }
                    "result" => {
                        let r = s
                            .open
                            .remove(&num(d, "id")?)
                            .ok_or("notification result has no request")?;
                        if num(d, "at_ns")? < r.at || num(d, "pause_ns")? > 86_400_000_000_000 {
                            return Err("notification result clock or pause invalid".into());
                        }
                        let outcome: Outcome = serde_json::from_value(d["outcome"].clone())
                            .map_err(|_| "invalid normalized notification outcome")?;
                        if s.family == "status"
                            && matches!(outcome, Outcome::WrongTarget | Outcome::Broken)
                        {
                            s.blocked = true;
                        }
                        match &outcome {
                            Outcome::Sent(id) => {
                                if id.parse::<u64>().ok().is_none_or(|n| n == 0)
                                    || r.message.as_ref().is_some_and(|m| m != id)
                                {
                                    return Err(
                                        "notification receipt changed message identity".into()
                                    );
                                }
                                if s.family == "status" {
                                    s.message = Some(id.clone());
                                }
                                sent += 1;
                            }
                            Outcome::Unknown | Outcome::WrongTarget => {
                                unknown += 1;
                                if s.family == "status" && r.method == "POST" {
                                    s.pending = true;
                                }
                            }
                            _ => {}
                        }
                        if r.lane == "bundle" && !matches!(outcome, Outcome::Sent(_)) {
                            blocked_edits.insert((
                                s.target.clone(),
                                r.message.ok_or("bundle reference missing")?,
                            ));
                        }
                        // Status initial POST is safely retryable only after an explicit rejection/429.
                        if s.family == "status"
                            && r.method == "POST"
                            && matches!(
                                outcome,
                                Outcome::RateLimited | Outcome::Rejected | Outcome::Broken
                            )
                        {
                            posts.remove(&(s.target.clone(), r.lane, r.key));
                        }
                    }
                    "end" => {
                        if !s.open.is_empty() || num(d, "requests")? != s.next - 1 {
                            return Err("notification session has incomplete outcomes".into());
                        }
                        s.ended = true;
                    }
                    _ => return Err("unsupported or failed notification operation".into()),
                }
            }
            let bytes =
                serde_json::to_vec(&entry.data).map_err(|_| "invalid verified notification")?;
            outputs.update((bytes.len() as u64).to_le_bytes());
            outputs.update(bytes);
            Ok(())
        })();
        if let Err(error) = result {
            failure = Some(error);
            return Err(io::Error::other("notification replay stopped"));
        }
        Ok(())
    })
    .map_err(|_| {
        failure.unwrap_or_else(|| "notification history corrupt, torn or unreadable".into())
    })?;
    if sessions.is_empty() || sessions.values().any(|s| !s.ended) {
        return Err("notification replay requires complete session evidence".into());
    }
    Ok(
        json!({"event":"notification_replay_complete","notification_receipts_replayed":true,"lanes":lanes,"sessions":sessions.len(),"requests":requests,"observed_sent":sent,"observed_unknown":unknown,"alerts_sent":0,"network":"disabled","input_sha256":format!("{:x}",source.finalize()),"decision_sha256":format!("{:x}",outputs.finalize()),"admission_replayed":false,"http_classification_replayed":false,"coverage":"recorded normalized transport receipts; source selection, budgets and external acceptance remain separate"}),
    )
}
