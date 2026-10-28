//! Pair one uninterrupted cold-boot analysis capture with staging delivery evidence.
use crate::{delivery, volume::Decision};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    path::Path,
};
fn key(boot: &str, generation: u64, decision: &Value) -> Result<String, String> {
    Ok(format!(
        "{boot}:{generation}:{:x}",
        Sha256::digest(serde_json::to_vec(decision).map_err(|_| "invalid linked decision")?)
    ))
}
pub fn run(capture: &Path, history: &Path) -> Result<Value, String> {
    let mut expected = BTreeMap::<String, VecDeque<bool>>::new();
    let mut routes = 0u64;
    let analysis = super::analysis::run_with_routes(
        capture,
        |_, _| Ok(()),
        |id, generation, decision: &Decision, status| {
            if matches!(status, "queued" | "queue_rejected") {
                routes += 1;
                if routes > 100_000 {
                    return Err("linked replay candidate cap exceeded".into());
                }
                let value = serde_json::to_value(decision)
                    .map_err(|_| "invalid linked analysis decision")?;
                expected
                    .entry(key(id, generation, &value)?)
                    .or_default()
                    .push_back(status == "queued");
            }
            Ok(())
        },
    )?;
    if analysis["routing_replayed"] != true
        || analysis["routing_staging_enabled"] != true
        || analysis["volume"].is_null()
    {
        return Err(
            "linked replay requires recorded staging-enabled routing and volume analysis".into(),
        );
    }
    let selected_boot = analysis["boot_id"]
        .as_str()
        .ok_or("analysis boot identity missing")?
        .to_owned();
    let mut linked = 0u64;
    let delivery =
        delivery::replay::run_with_inputs(history, Some(&selected_boot), |op, data, id| {
            if id != selected_boot {
                return Err("capture and delivery boot identities differ".into());
            }
            if op == "start" {
                return Ok(());
            }
            let c = &data["candidate"];
            if c["reserved"] == true || c["attempts"].as_u64().is_some_and(|n| n > 0) {
                return Ok(());
            }
            let generation = c["generation"]
                .as_u64()
                .ok_or("delivery candidate generation missing")?;
            let key = key(id, generation, &c["decision"])?;
            let value = expected
                .get_mut(&key)
                .and_then(VecDeque::pop_front)
                .ok_or("delivery candidate has no matching recomputed route")?;
            if data["accepted"] != value {
                return Err("queue admission differs between analysis and delivery tapes".into());
            }
            linked += 1;
            Ok(())
        })?;
    if expected.values().any(|v| !v.is_empty()) {
        return Err("analysis route is missing from delivery history".into());
    }
    // Hashes attest to both complete inputs; the report never sends a recorded candidate.
    let hash = format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&json!([analysis["input_sha256"], delivery["input_sha256"]]))
                .map_err(|_| "invalid paired report")?
        )
    );
    Ok(
        json!({"event":"linked_replay_complete","volume_delivery_link_replayed":true,"linked_candidates":linked,"analysis":analysis,"delivery":delivery,"report_sha256":hash,"alerts_sent":0,"network":"disabled","other_lanes_linked":false,"coverage":"one recorded cold boot; observed feed and normalized outcomes, not provider truth"}),
    )
}
