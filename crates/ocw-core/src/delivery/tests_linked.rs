//! Synthetic inputs are confined to this collector/worker boundary test.
use crate::{
    capture::{Record, Segment},
    config::Config,
    feed::Decoder,
    volume::{Decision, Engine, Verdict},
};
use bytes::Bytes;
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
pub(super) struct Capture {
    pub path: PathBuf,
    pub decision: Decision,
    pub origin: Instant,
    segment: Segment,
    seq: u64,
}
fn append(segment: &mut Segment, seq: &mut u64, ms: u64, kind: &str, data: Value) {
    *seq += 1;
    segment
        .append(&Record::control(
            *seq,
            "boot1",
            1,
            ms * 1_000_000,
            1000 + ms,
            kind,
            Bytes::from(data.to_string()),
        ))
        .unwrap();
}
pub(super) fn capture(root: &Path) -> Capture {
    let path = root.join("analysis.ocwcap");
    let mut segment = Segment::create(&path).unwrap();
    let mut seq = 0;
    append(
        &mut segment,
        &mut seq,
        0,
        "boot",
        json!({"config":Config::default(),"execution_schema":1,"route_schema":1,"staging_enabled":true,"initial_state":"cold","volume_enabled":true}),
    );
    let mut decoder = Decoder::new(4096, 120_000);
    let mut engine = Engine::new(256);
    let mint = bs58::encode([1; 32]).into_string();
    let mut frames = vec![(
        0,
        json!({"txType":"create","mint":mint,"vSolInBondingCurve":30}),
    )];
    for bucket in 0..18 {
        for wallet in 0..6 {
            frames.push((bucket*15_000+wallet+1,json!({"txType":"buy","mint":mint,"traderPublicKey":bs58::encode([wallet as u8+2;32]).into_string(),"signature":bs58::encode([(bucket*6+wallet+1) as u8;64]).into_string(),"tokenAmount":1,"solAmount":if bucket==17{4}else{1},"vSolInBondingCurve":31})));
        }
    }
    for (ms, raw) in frames {
        let bytes = Bytes::from(raw.to_string());
        seq += 1;
        segment
            .append(&Record::frame(
                seq,
                "boot1",
                1,
                ms * 1_000_000,
                1000 + ms,
                bytes.clone(),
            ))
            .unwrap();
        let input = seq;
        append(
            &mut segment,
            &mut seq,
            ms,
            "frame_apply",
            json!({"input_seq":input,"apply_ms":ms,"allow_create":true}),
        );
        let decisions = engine
            .on_event(decoder.decode(&bytes, ms).unwrap(), ms)
            .unwrap();
        assert!(decisions.iter().all(|d| d.card(ms).is_none()));
    }
    append(&mut segment, &mut seq, 270_000, "clock_tick", json!({}));
    let decision = engine.advance(270_000).unwrap().pop().unwrap();
    assert_eq!(decision.verdict, Verdict::Fire);
    Capture {
        path,
        decision,
        origin: Instant::now() - Duration::from_millis(270_000),
        segment,
        seq,
    }
}
impl Capture {
    pub fn finish(mut self, status: &str) -> PathBuf {
        append(
            &mut self.segment,
            &mut self.seq,
            270_000,
            "volume_route",
            json!({"decision":self.decision,"apply_ms":270_000,"generation":1,"feed_connected":true,"status":status}),
        );
        self.segment.sync().unwrap();
        self.path
    }
}
