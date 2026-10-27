//! Owned wire boundary; recorded strings never acquire static lifetime by leaking memory.
use crate::volume::{Decision, Verdict};
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Recorded {
    number: u64,
    mint: String,
    verdict: Verdict,
    path: String,
    regime: String,
    bucket_end_ms: u64,
    completed_buckets: usize,
    volume_lamports: u64,
    baseline_lamports: f64,
    floor_lamports: f64,
    ratio: Option<f64>,
    cohort_p90: Option<f64>,
    dispersion_threshold_lamports: f64,
    trades: usize,
    distinct_takers: usize,
    largest_taker_share: f64,
}
pub(super) fn parse(value: serde_json::Value) -> Result<Decision, String> {
    let d: Recorded =
        serde_json::from_value(value).map_err(|_| "invalid recorded delivery decision")?;
    let path = match d.path.as_str() {
        "relative" => "relative",
        "cold_baseline" => "cold_baseline",
        _ => return Err("invalid recorded decision path".into()),
    };
    let regime = match d.regime.as_str() {
        "pool" => "pool",
        "bonding_curve" => "bonding_curve",
        _ => return Err("invalid recorded decision regime".into()),
    };
    Ok(Decision {
        number: d.number,
        mint: d.mint,
        verdict: d.verdict,
        path,
        regime,
        bucket_end_ms: d.bucket_end_ms,
        completed_buckets: d.completed_buckets,
        volume_lamports: d.volume_lamports,
        baseline_lamports: d.baseline_lamports,
        floor_lamports: d.floor_lamports,
        ratio: d.ratio,
        cohort_p90: d.cohort_p90,
        dispersion_threshold_lamports: d.dispersion_threshold_lamports,
        trades: d.trades,
        distinct_takers: d.distinct_takers,
        largest_taker_share: d.largest_taker_share,
    })
}
