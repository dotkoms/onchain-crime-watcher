//! Stream a recorded OCWC capture through the real decoder and volume engine.
//! Diagnostic receive-timing replay; the analysis module additionally verifies recorded watch choices.
use crate::{
    capture,
    config::FeedConfig,
    feed::{Decoder, Event},
    volume::{Decision, Engine},
};
use serde_json::{Value, json};
use std::{io, path::Path};

pub fn run(
    path: &Path,
    config: &FeedConfig,
    mut emit: impl FnMut(&Decision, u64) -> Result<(), String>,
) -> Result<Value, String> {
    let mut decoder = Decoder::new(config.dedup_capacity, config.dedup_window_ms);
    let mut engine = Engine::new(256);
    let mut boot = None;
    let mut generation = None;
    let mut first = None;
    let mut last = 0;
    let mut records = 0;
    let mut frames = 0;
    let mut trades = 0;
    let mut creates = 0;
    let mut duplicates = 0;
    let mut decode_errors = 0;
    let mut unresolved = 0;
    let mut failure = None;
    let (_, torn) = capture::scan(path, |record| {
        let result = (|| -> Result<(), String> {
            records += 1;
            if records > 30_000 {
                return Err("capture replay record limit exceeded".into());
            }
            if boot.as_ref().is_some_and(|b| b != &record.boot_id) {
                return Err("capture contains mixed boot clocks".into());
            }
            boot = Some(record.boot_id);
            let base = *first.get_or_insert(record.mono_ns);
            let now = record
                .mono_ns
                .checked_sub(base)
                .ok_or("capture clock moved backwards")?
                / 1_000_000;
            if record.mono_ns < last {
                return Err("capture clock moved backwards".into());
            }
            last = record.mono_ns;
            if generation.is_some_and(|g| g != record.generation) {
                engine.gap(now)?;
                decoder.reset_session();
            }
            generation = Some(record.generation);
            let decisions = match record.kind.as_str() {
                "capture_gap" | "session_close" | "paid_stream_stopped" => {
                    engine.gap(now)?;
                    decoder.reset_session();
                    Vec::new()
                }
                "redacted_frame" => {
                    let d = engine.advance(now)?;
                    engine.invalidate();
                    d
                }
                "frame" => {
                    if record.payload.len() > config.max_frame_bytes {
                        return Err("recorded frame exceeds configured limit".into());
                    }
                    frames += 1;
                    match decoder.decode(&record.payload, now) {
                        Ok(event) => {
                            match &event {
                                Event::Trade(_) => trades += 1,
                                Event::Create(_) => creates += 1,
                                Event::Duplicate => duplicates += 1,
                                Event::Unresolved => unresolved += 1,
                                _ => {}
                            }
                            engine.on_event(event, now)?
                        }
                        Err(_) => {
                            decode_errors += 1;
                            let d = engine.advance(now)?;
                            engine.invalidate();
                            d
                        }
                    }
                }
                _ => engine.advance(now)?,
            };
            if engine.stats().evaluated > 20_000 {
                return Err("capture replay decision limit exceeded".into());
            }
            for decision in decisions {
                emit(&decision, now)?;
            }
            Ok(())
        })();
        if let Err(error) = result {
            failure = Some(error);
            return Err(io::Error::other("capture replay stopped"));
        }
        Ok(())
    })
    .map_err(|_| failure.unwrap_or_else(|| "capture invalid, corrupt or unreadable".into()))?;
    if torn {
        return Err("capture has a torn tail; recover it before replay".into());
    }
    Ok(
        json!({"event":"capture_replay_complete","source":"recorded_capture","provider_connected":false,
        "records":records,"frames":frames,"trades":trades,"creates":creates,"duplicates":duplicates,
        "decode_errors":decode_errors,"unresolved":unresolved,"unusable_reserves":decoder.unusable_reserves(),
        "volume":engine.stats(),"active_mints":engine.active_mints(),"alerts_sent":0,
        "coverage":"unverified; only this capture segment","partial_bucket":"not evaluated"}),
    )
}

pub mod analysis;

mod controller;

mod spend;

pub mod linked;
mod routing;
