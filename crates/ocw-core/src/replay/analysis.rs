//! Exact application-order decoder/volume replay, with optional watch-schema-1 verification.
//! Delivery workers and external response tapes are deliberately outside this report's claims.
use crate::{
    capture,
    config::Config,
    feed::{Decoder, Event},
    volume::{Decision, Engine},
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{io, path::Path};

pub fn run(
    path: &Path,
    mut emit: impl FnMut(&Decision, u64) -> Result<(), String>,
) -> Result<Value, String> {
    let mut config: Option<Config> = None;
    let mut decoder = None;
    let mut engine = None;
    let mut pending: Option<capture::Record> = None;
    let mut boot = None;
    let mut last_seq = 0;
    let mut last_ns = 0;
    let mut last_wall = 0;
    let mut records = 0u64;
    let mut frames = 0u64;
    let mut trades = 0u64;
    let mut decisions = 0u64;
    let mut subscriptions = 0u64;
    let mut input_hash = Sha256::new();
    let mut decision_hash = Sha256::new();
    let mut failure = None;
    let mut controller = super::controller::Controller::default();
    let (_, torn) = capture::scan(path, |record| {
        let result = (|| -> Result<(), String> {
            records += 1;
            if records > 1_000_000 {
                return Err("analysis replay record cap exceeded".into());
            }
            if record.seq != last_seq + 1
                || record.mono_ns < last_ns
                || record.wall_ms < last_wall
                || boot.as_ref().is_some_and(|id| id != &record.boot_id)
            {
                return Err("analysis capture sequence or clock discontinuity".into());
            }
            if record.boot_id.is_empty() || record.boot_id.len() > 64 {
                return Err("analysis capture boot identity invalid".into());
            }
            last_seq = record.seq;
            last_ns = record.mono_ns;
            last_wall = record.wall_ms;
            boot = Some(record.boot_id.clone());
            let meta = json!([
                record.seq,
                record.boot_id,
                record.generation,
                record.mono_ns,
                record.wall_ms,
                record.kind
            ]);
            let metadata = serde_json::to_vec(&meta).map_err(|_| "analysis metadata invalid")?;
            input_hash.update((metadata.len() as u64).to_le_bytes());
            input_hash.update(metadata);
            input_hash.update((record.payload.len() as u64).to_le_bytes());
            input_hash.update(&record.payload);
            let now = record.mono_ns / 1_000_000;
            if record.kind == "capture_gap" {
                return Err("analysis replay cannot reconstruct missing execution records".into());
            }
            if config.is_none() {
                if record.kind != "boot" {
                    return Err(
                        "analysis replay requires the initial boot/configuration record".into(),
                    );
                }
                let value: Value = serde_json::from_slice(&record.payload)
                    .map_err(|_| "invalid analysis boot record")?;
                if value["execution_schema"] != 1
                    || value["initial_state"] != "cold"
                    || !value["volume_enabled"].is_boolean()
                {
                    return Err("unsupported analysis execution schema or initial state".into());
                }
                let initial: Config = serde_json::from_value(value["config"].clone())
                    .map_err(|_| "invalid recorded analysis configuration")?;
                initial
                    .validate()
                    .map_err(|_| "invalid recorded analysis configuration")?;
                decoder = Some(Decoder::new(
                    initial.feed.dedup_capacity,
                    initial.feed.dedup_window_ms,
                ));
                engine = if value["volume_enabled"] == true {
                    Some(Engine::new(256))
                } else {
                    None
                };
                controller.configure(&value)?;
                config = Some(initial);
                return Ok(());
            }
            let config = config.as_ref().expect("checked configuration");
            let decoder = decoder.as_mut().expect("initialized decoder");
            let mut produced = Vec::new();
            let mut applied_now = now;
            match record.kind.as_str() {
                "watch_start" | "watch_plan" => {
                    let step: Value = serde_json::from_slice(&record.payload)
                        .map_err(|_| "invalid watch controller record")?;
                    if record.kind == "watch_start" {
                        controller.start(config, &record, &step)?;
                    } else {
                        controller.plan(&record, &step, pending.as_ref())?;
                    }
                }
                "session_close" => controller.boundary()?,
                "boot" => return Err("analysis capture contains a second boot".into()),
                "frame" => {
                    if pending.is_some() || record.payload.len() > config.feed.max_frame_bytes {
                        return Err("missing analysis frame application or oversized frame".into());
                    }
                    pending = Some(record);
                    frames += 1;
                    return Ok(());
                }
                "frame_apply" | "discarded_frame" => {
                    let step: Value = serde_json::from_slice(&record.payload)
                        .map_err(|_| "invalid analysis execution step")?;
                    let raw = pending
                        .take()
                        .ok_or("analysis step has no captured frame")?;
                    applied_now = step["apply_ms"]
                        .as_u64()
                        .ok_or("analysis application clock missing")?;
                    if step["input_seq"] != raw.seq
                        || applied_now != raw.mono_ns / 1_000_000
                        || raw.generation != record.generation
                    {
                        return Err("analysis frame application identity mismatch".into());
                    }
                    controller.frame(&raw, &step, record.kind == "discarded_frame")?;
                    if record.kind == "discarded_frame" {
                        if let Some(engine) = &mut engine {
                            produced = engine.advance(applied_now)?;
                        }
                    } else {
                        let allow = step["allow_create"]
                            .as_bool()
                            .ok_or("analysis admission flag missing")?;
                        match decoder.decode(&raw.payload, applied_now) {
                            Ok(event) => {
                                if let Event::Trade(trade) = &event {
                                    controller.trade(trade, applied_now);
                                    trades += 1;
                                }
                                if let Some(engine) = &mut engine {
                                    produced = if matches!(event, Event::Create(_)) && !allow {
                                        engine.advance(applied_now)?
                                    } else {
                                        engine.on_event(event, applied_now)?
                                    };
                                }
                            }
                            Err(_) => {
                                if let Some(engine) = &mut engine {
                                    engine.invalidate();
                                }
                            }
                        }
                    }
                }
                "watch_apply" => {
                    let step: Value = serde_json::from_slice(&record.payload)
                        .map_err(|_| "invalid analysis watch step")?;
                    controller.apply(&record, &step)?;
                    applied_now = step["apply_ms"]
                        .as_u64()
                        .ok_or("analysis watch clock missing")?;
                    if applied_now > now {
                        return Err("analysis watch clock is in the future".into());
                    }
                    let mint = crate::feed::key::<32>(
                        step["mint"].as_str().ok_or("analysis watch mint missing")?,
                    )
                    .map_err(|_| "invalid analysis watch mint")?;
                    match step["operation"].as_str() {
                        Some("remove") => {
                            if let Some(engine) = &mut engine {
                                engine.forget(&mint);
                            }
                        }
                        Some("attach") => {
                            let partial = step["partial"]
                                .as_bool()
                                .ok_or("analysis watch coverage missing")?;
                            let created = if step["observed_create_ms"].is_null() {
                                None
                            } else {
                                Some(
                                    step["observed_create_ms"]
                                        .as_u64()
                                        .ok_or("invalid observed create clock")?,
                                )
                            };
                            if created.is_some_and(|created| created > applied_now) {
                                return Err("observed create is after subscription".into());
                            }
                            if let (Some(engine), Some(created)) = (&mut engine, created) {
                                engine.attach_observed(mint, created, applied_now, partial);
                            }
                        }
                        _ => return Err("unsupported analysis watch operation".into()),
                    }
                    subscriptions += 1;
                }
                "clock_tick" => {
                    if let Some(engine) = &mut engine {
                        produced = engine.advance(now)?;
                    }
                }
                "volume_reset" => {
                    if pending.is_some() {
                        return Err("reset interrupts an unapplied captured frame".into());
                    }
                    decoder.reset_session();
                    if let Some(engine) = &mut engine {
                        engine.gap(now)?;
                    }
                }
                "pipeline_panic" => {
                    controller.boundary()?;
                    pending = None;
                    *decoder =
                        Decoder::new(config.feed.dedup_capacity, config.feed.dedup_window_ms);
                    if engine.is_some() {
                        engine = Some(Engine::new(256));
                    }
                }
                "redacted_frame" => {
                    if let Some(engine) = &mut engine {
                        engine.invalidate();
                    }
                }
                _ => {}
            }
            for decision in produced {
                decisions += 1;
                if decisions > 1_000_000 {
                    return Err("analysis replay decision cap exceeded".into());
                }
                let bytes = serde_json::to_vec(&json!([applied_now, &decision]))
                    .map_err(|_| "invalid replayed decision")?;
                decision_hash.update((bytes.len() as u64).to_le_bytes());
                decision_hash.update(bytes);
                emit(&decision, applied_now)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            failure = Some(error);
            return Err(io::Error::other("analysis replay stopped"));
        }
        Ok(())
    })
    .map_err(|_| {
        failure.unwrap_or_else(|| "analysis capture corrupt, invalid or unreadable".into())
    })?;
    if torn || pending.is_some() || config.is_none() {
        return Err("analysis capture is torn or has an incomplete execution tail".into());
    }
    controller.complete()?;
    let mut summary = json!({"event":"analysis_replay_complete","execution_schema":1,"records":records,"frames":frames,"accepted_trades":trades,"watch_operations":subscriptions,"decisions":decisions,"input_sha256":format!("{:x}",input_hash.finalize()),"decision_sha256":format!("{:x}",decision_hash.finalize()),"volume":engine.as_ref().map(Engine::stats),"active_mints":engine.as_ref().map(Engine::active_mints),"alerts_sent":0,"network":"disabled","initial_state":"recorded_cold_boot","delivery_replayed":false,"external_responses_replayed":false,"coverage":"only the supplied uninterrupted capture; provider completeness unverified"});
    summary.as_object_mut().expect("summary object").extend(
        controller
            .summary()
            .as_object()
            .expect("controller summary")
            .clone(),
    );
    Ok(summary)
}
