//! Independent confirmed-chain samples of actually subscribed mint curves.
//! A sample can establish observed missing trades; it cannot prove global feed completeness.
use crate::{chain, feed, rpc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub heal_enabled: bool,
    pub calls_per_day: u64,
    pub calls_per_minute: usize,
    pub interval_s: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            heal_enabled: false,
            calls_per_day: 50_000,
            calls_per_minute: 600,
            interval_s: 30,
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(1..=50_000).contains(&self.calls_per_day)
            || !(1..=600).contains(&self.calls_per_minute)
            || !(30..=300).contains(&self.interval_s)
        {
            return Err("invalid chain probe limits");
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct Sample {
    pub mint: [u8; 32],
    pub curve: [u8; 32],
    pub signature: [u8; 64],
    pub observed_ms: u64,
}
struct Row {
    migrated: bool,
    sample: Sample,
    recent: VecDeque<[u8; 64]>,
}
#[derive(Default)]
pub struct Book {
    rows: BTreeMap<[u8; 32], Row>,
}
impl Book {
    pub fn create(&mut self, mint: [u8; 32], curve: [u8; 32], now: u64) {
        // A duplicate create must not replace the observed cursor/history.
        if self.rows.len() < 20 && !self.rows.contains_key(&mint) {
            self.rows.insert(
                mint,
                Row {
                    migrated: false,
                    sample: Sample {
                        mint,
                        curve,
                        signature: [0; 64],
                        observed_ms: now,
                    },
                    recent: VecDeque::new(),
                },
            );
        }
    }
    pub fn retain(&mut self, mut keep: impl FnMut(&[u8; 32]) -> bool) {
        self.rows.retain(|key, _| keep(key));
    }
    fn current(&self, sample: &Sample) -> bool {
        self.rows.get(&sample.mint).is_some_and(|row| {
            !row.migrated
                && row.sample.curve == sample.curve
                && row.recent.contains(&sample.signature)
        })
    }
    pub fn forget(&mut self, mint: &[u8; 32]) {
        self.rows.remove(mint);
    }
    pub fn migrate(&mut self, mint: &[u8; 32]) {
        if let Some(row) = self.rows.get_mut(mint) {
            row.migrated = true;
        }
    }
    pub fn trade(&mut self, trade: &feed::Trade, now: u64) {
        if let Some(row) = self.rows.get_mut(&trade.mint) {
            row.sample.signature = trade.signature;
            row.sample.observed_ms = now;
            row.recent.push_back(trade.signature);
            if row.recent.len() > 256 {
                row.recent.pop_front();
            }
        }
    }
    pub fn cursors(&self) -> Vec<Sample> {
        self.rows
            .values()
            .filter(|row| !row.migrated)
            .map(|row| row.sample.clone())
            .collect()
    }
    pub fn samples(&self, now: u64) -> Vec<Sample> {
        let mut rows: Vec<_> = self
            .rows
            .values()
            .filter(|row| {
                !row.migrated
                    && !row.recent.is_empty()
                    && now.saturating_sub(row.sample.observed_ms) <= 300_000
            })
            .map(|row| row.sample.clone())
            .collect();
        rows.sort_by_key(|row| (std::cmp::Reverse(row.observed_ms), row.mint));
        rows.truncate(3);
        rows
    }
    pub fn missing(&self, sample: &Sample, signature: &[u8; 64]) -> bool {
        self.rows
            .get(&sample.mint)
            .is_some_and(|row| row.sample.curve == sample.curve && !row.recent.contains(signature))
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Evidence {
    Quiet,
    NewTrade([u8; 64]),
    Unavailable,
}
pub struct ResultRow {
    pub sample: Sample,
    pub evidence: Evidence,
}
pub async fn run(client: rpc::Client, samples: Vec<Sample>) -> Vec<ResultRow> {
    if samples.len() != 3 {
        return Vec::new();
    }
    futures_util::future::join_all(samples.into_iter().map(|sample| {
        let client = client.clone();
        async move {
            let evidence =
                tokio::time::timeout(std::time::Duration::from_secs(8), one(&client, &sample))
                    .await
                    .unwrap_or(Evidence::Unavailable);
            ResultRow { sample, evidence }
        }
    }))
    .await
}
async fn one(client: &rpc::Client, sample: &Sample) -> Evidence {
    let address = bs58::encode(sample.curve).into_string();
    let Ok(value) = client.signatures(&address, None, 1).await else {
        return Evidence::Unavailable;
    };
    let Some(rows) = value.as_array().filter(|rows| rows.len() == 1) else {
        return Evidence::Unavailable;
    };
    let row = &rows[0];
    if row.get("err") != Some(&serde_json::Value::Null) {
        return Evidence::Unavailable;
    }
    let Some(signature) = row["signature"].as_str() else {
        return Evidence::Unavailable;
    };
    let Ok(key) = feed::key::<64>(signature) else {
        return Evidence::Unavailable;
    };
    if key == sample.signature {
        return Evidence::Quiet;
    }
    let Ok(tx) = client.transaction(signature).await else {
        return Evidence::Unavailable;
    };
    let Ok(decoded) = chain::decode(&tx, signature) else {
        return Evidence::Unavailable;
    };
    if decoded.unresolved > 0
        || !decoded
            .trades
            .iter()
            .any(|row| row.trade.mint == sample.mint)
    {
        return Evidence::Unavailable;
    }
    // A lagging RPC index can report an older, different newest signature. Compare
    // confirmed slots against the actual observed anchor; same-slot order is unknown.
    let anchor_signature = bs58::encode(sample.signature).into_string();
    let Ok(anchor_tx) = client.transaction(&anchor_signature).await else {
        return Evidence::Unavailable;
    };
    let Ok(anchor) = chain::decode(&anchor_tx, &anchor_signature) else {
        return Evidence::Unavailable;
    };
    let Some(anchor) = anchor
        .trades
        .iter()
        .find(|row| row.trade.mint == sample.mint)
    else {
        return Evidence::Unavailable;
    };
    if decoded
        .trades
        .iter()
        .any(|row| row.trade.mint == sample.mint && row.slot > anchor.slot)
    {
        Evidence::NewTrade(key)
    } else {
        Evidence::Unavailable
    }
}
/// Recheck evidence against the *current* live observations, avoiding a race while RPC runs.
pub fn classify(book: &Book, results: &[ResultRow]) -> &'static str {
    if results.len() != 3 || results.iter().any(|row| !book.current(&row.sample)) {
        return "unavailable";
    }
    let missing = results
        .iter()
        .filter(
            |row| matches!(row.evidence,Evidence::NewTrade(sig) if book.missing(&row.sample,&sig)),
        )
        .count();
    if missing >= 2 {
        return "missing_trades";
    }
    if results.iter().all(|row| {
        matches!(row.evidence, Evidence::Quiet)
            || matches!(row.evidence,Evidence::NewTrade(sig) if !book.missing(&row.sample,&sig))
    }) {
        "sample_quiet"
    } else {
        "unavailable"
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_watched_recent_cursors_are_sampled_and_inflight_live_trades_prevent_false_reconnect() {
        let mut book = Book::default();
        for mint in 1..=3 {
            book.create([mint; 32], [mint + 10; 32], 0);
            book.trade(
                &feed::Trade {
                    mint: [mint; 32],
                    trader: [9; 32],
                    signature: [mint; 64],
                    side: feed::Side::Buy,
                    token_amount: 1,
                    sol_lamports: 1,
                    sol_source: feed::SolSource::Payload,
                },
                100,
            );
        }
        assert_eq!(book.samples(300_100).len(), 3);
        assert!(book.samples(300_101).is_empty());
        let samples = book.samples(100);
        let mut results: Vec<_> = samples
            .into_iter()
            .map(|sample| ResultRow {
                sample,
                evidence: Evidence::NewTrade([99; 64]),
            })
            .collect();
        assert_eq!(classify(&book, &results), "missing_trades");
        for row in &results {
            book.trade(
                &feed::Trade {
                    mint: row.sample.mint,
                    trader: [9; 32],
                    signature: [99; 64],
                    side: feed::Side::Buy,
                    token_amount: 1,
                    sol_lamports: 1,
                    sol_source: feed::SolSource::Payload,
                },
                101,
            );
        }
        assert_eq!(classify(&book, &results), "sample_quiet");
        results[0].evidence = Evidence::Unavailable;
        assert_eq!(classify(&book, &results), "unavailable");
        book.migrate(&results[1].sample.mint);
        assert_eq!(book.samples(101).len(), 2);
        assert_eq!(classify(&book, &results), "unavailable");
        book.forget(&results[1].sample.mint);
        assert_eq!(book.samples(101).len(), 2);
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use axum::{
        Router,
        extract::{Json, State},
        routing::post,
    };
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use serde_json::{Value, json};
    use std::sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    };
    #[tokio::test]
    async fn older_rpc_heads_same_slot_and_unresolved_heads_never_prove_missing_trades() {
        let slot = Arc::new(AtomicU64::new(5));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let app=Router::new().route("/",post(|State(slot):State<Arc<AtomicU64>>,Json(body):Json<Value>|async move {
            let head=bs58::encode([9;64]).into_string();
            let result=if body["method"]=="getSignaturesForAddress" {
                json!([{"signature":head,"err":null,"slot":slot.load(Ordering::SeqCst)}])
            }else{
                let log=format!("Program data: {}",STANDARD.encode(chain::tests::raw(feed::key::<32>(chain::WRAPPED_SOL).unwrap())));
                let (mut tx,_)=chain::tests::tx(vec![format!("Program {} invoke [1]",chain::PUMP_PROGRAM),log,format!("Program {} success",chain::PUMP_PROGRAM)]);
                tx["transaction"]["signatures"]=json!([body["params"][0]]);
                tx["slot"]=json!(if body["params"][0]==head {slot.load(Ordering::SeqCst)} else {6});
                tx
            };
            Json(json!({"jsonrpc":"2.0","id":body["id"],"result":result}))
        })).with_state(slot.clone());
        let server = crate::supervision::AbortOnDrop(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let guard = Arc::new(crate::storage::ProcessLock::acquire(&data).unwrap());
        let mut client = rpc::Client::open("https://example.invalid/", dir.path(), guard, 100, 100)
            .await
            .unwrap();
        client.test_endpoint(endpoint);
        let sample = Sample {
            mint: [1; 32],
            curve: [2; 32],
            signature: [3; 64],
            observed_ms: 0,
        };
        assert_eq!(one(&client, &sample).await, Evidence::Unavailable);
        slot.store(6, Ordering::SeqCst);
        assert_eq!(one(&client, &sample).await, Evidence::Unavailable);
        slot.store(7, Ordering::SeqCst);
        assert_eq!(one(&client, &sample).await, Evidence::NewTrade([9; 64]));
        drop(server);
    }
}
