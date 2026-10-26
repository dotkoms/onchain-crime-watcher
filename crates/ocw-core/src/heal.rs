//! Bounded confirmed curve scans. Recovered deltas are audit history, never live baselines.
use crate::{
    chain, feed,
    probe::Sample,
    rpc::{self, Method},
};
use futures_util::{StreamExt, stream};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Stop {
    Complete,
    NoCursor,
    CursorMissing,
    SignatureCap,
    CallBudget,
    Duration,
    Rpc,
    Invalid,
    UnknownLayout,
    SlotCap,
    EventCap,
}
impl Stop {
    pub fn label(self) -> &'static str {
        match self {
            Self::Complete => "curve_scan_complete",
            Self::NoCursor => "no_cursor",
            Self::CursorMissing => "cursor_missing",
            Self::SignatureCap => "signature_cap",
            Self::CallBudget => "call_budget",
            Self::Duration => "duration",
            Self::Rpc => "rpc_unavailable",
            Self::Invalid => "invalid_chain_response",
            Self::UnknownLayout => "unknown_layout",
            Self::SlotCap => "slot_cap",
            Self::EventCap => "event_cap",
        }
    }
}
pub struct Recovered {
    pub event: chain::ChainTrade,
    pub transaction_index: usize,
}
pub struct Report {
    pub generation: u64,
    pub sample: Sample,
    pub stop: Stop,
    pub calls: usize,
    pub signatures: usize,
    pub trades: Vec<Recovered>,
}
struct Budget {
    calls: AtomicUsize,
    signatures: AtomicUsize,
    events: AtomicUsize,
    deadline: tokio::time::Instant,
}
impl Budget {
    fn reserve(
        &self,
        counter: &AtomicUsize,
        n: usize,
        cap: usize,
        reason: Stop,
    ) -> Result<(), Stop> {
        if tokio::time::Instant::now() >= self.deadline {
            return Err(Stop::Duration);
        }
        counter
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                old.checked_add(n).filter(|n| *n <= cap)
            })
            .map(|_| ())
            .map_err(|_| reason)
    }
    async fn call(
        &self,
        client: &rpc::Client,
        method: Method,
        params: Value,
        calls: &mut usize,
    ) -> Result<Value, Stop> {
        self.reserve(&self.calls, 1, 1200, Stop::CallBudget)?;
        *calls += 1;
        tokio::time::timeout_at(self.deadline, client.call(method, params))
            .await
            .map_err(|_| Stop::Duration)?
            .map_err(|e| match e {
                rpc::Failure::Budget | rpc::Failure::RateLimited => Stop::CallBudget,
                _ => Stop::Rpc,
            })
    }
}
/// One gap: at most two simultaneous curve jobs, 120 seconds, 1,200 calls, 1,000
/// total signatures and 8,192 output events. The RPC client also enforces global spend.
pub async fn run(client: rpc::Client, samples: Vec<Sample>, generation: u64) -> Vec<Report> {
    if samples.len() > 20 {
        return Vec::new();
    }
    let budget = Arc::new(Budget {
        calls: AtomicUsize::new(0),
        signatures: AtomicUsize::new(0),
        events: AtomicUsize::new(0),
        deadline: tokio::time::Instant::now() + Duration::from_secs(120),
    });
    stream::iter(samples.into_iter().map(|sample| {
        let client = client.clone();
        let budget = budget.clone();
        async move {
            let mut report = Report {
                generation,
                sample,
                stop: Stop::Complete,
                calls: 0,
                signatures: 0,
                trades: Vec::new(),
            };
            report.stop = scan(&client, &budget, &mut report)
                .await
                .err()
                .unwrap_or(Stop::Complete);
            // A partial prefix is never reported as a complete interval.
            report
        }
    }))
    .buffered(2)
    .collect()
    .await
}
fn signatures(value: &Value, cursor: [u8; 64]) -> Result<Vec<([u8; 64], u64, bool)>, Stop> {
    let rows = value
        .as_array()
        .filter(|rows| rows.len() <= 1000)
        .ok_or(Stop::Invalid)?;
    let mut seen = BTreeSet::new();
    let mut previous = u64::MAX;
    let mut newer = Vec::new();
    for row in rows {
        let key = feed::key::<64>(row["signature"].as_str().ok_or(Stop::Invalid)?)
            .map_err(|_| Stop::Invalid)?;
        let slot = row["slot"].as_u64().ok_or(Stop::Invalid)?;
        if slot > previous || !seen.insert(key) {
            return Err(Stop::Invalid);
        }
        previous = slot;
        let err = row.get("err").ok_or(Stop::Invalid)?;
        if key == cursor {
            return if err.is_null() {
                Ok(newer)
            } else {
                Err(Stop::Invalid)
            };
        }
        newer.push((key, slot, err.is_null()));
    }
    Err(if rows.len() == 1000 {
        Stop::SignatureCap
    } else {
        Stop::CursorMissing
    })
}
async fn scan(client: &rpc::Client, budget: &Budget, report: &mut Report) -> Result<(), Stop> {
    if report.sample.signature == [0; 64] {
        return Err(Stop::NoCursor);
    }
    let address = bs58::encode(report.sample.curve).into_string();
    let page = budget
        .call(
            client,
            Method::Signatures,
            json!([address,{"commitment":"confirmed","limit":1000}]),
            &mut report.calls,
        )
        .await?;
    let rows = signatures(&page, report.sample.signature)?;
    report.signatures = rows.len();
    budget.reserve(&budget.signatures, rows.len(), 1000, Stop::SignatureCap)?;
    let anchor_text = bs58::encode(report.sample.signature).into_string();
    let anchor_slot = page
        .as_array()
        .and_then(|rows| rows.iter().find(|row| row["signature"] == anchor_text))
        .and_then(|row| row["slot"].as_u64())
        .ok_or(Stop::Invalid)?;
    let mut wanted: BTreeSet<_> = rows.iter().map(|(sig, _, _)| *sig).collect();
    wanted.insert(report.sample.signature);
    let slots: BTreeSet<_> = rows.iter().map(|(_, slot, _)| *slot).collect();
    if slots.len() > 150 {
        return Err(Stop::SlotCap);
    }
    let mut order = BTreeMap::new();
    for slot in slots {
        let block=budget.call(client,Method::Block,json!([slot,{"commitment":"confirmed","transactionDetails":"signatures","rewards":false,"maxSupportedTransactionVersion":0}]),&mut report.calls).await?;
        let list = block["signatures"]
            .as_array()
            .filter(|list| list.len() <= 20_000)
            .ok_or(Stop::Invalid)?;
        let mut local = BTreeSet::new();
        for (index, value) in list.iter().enumerate() {
            let key =
                feed::key::<64>(value.as_str().ok_or(Stop::Invalid)?).map_err(|_| Stop::Invalid)?;
            if !local.insert(key) {
                return Err(Stop::Invalid);
            }
            if wanted.contains(&key) {
                order.insert((slot, key), index);
            }
        }
    }
    let mut ordered = Vec::new();
    for (signature, slot, success) in rows {
        let index = *order.get(&(slot, signature)).ok_or(Stop::Invalid)?;
        if slot == anchor_slot
            && index
                <= *order
                    .get(&(slot, report.sample.signature))
                    .ok_or(Stop::Invalid)?
        {
            return Err(Stop::Invalid);
        }
        ordered.push((slot, index, signature, success));
    }
    ordered.sort_unstable();
    for (slot, index, signature, success) in ordered {
        if !success {
            continue;
        }
        let signature = bs58::encode(signature).into_string();
        let tx=budget.call(client,Method::Transaction,json!([signature,{"commitment":"confirmed","encoding":"jsonParsed","maxSupportedTransactionVersion":0}]),&mut report.calls).await?;
        let decoded = chain::decode(&tx, &signature).map_err(|_| Stop::Invalid)?;
        if decoded.unresolved > 0 {
            return Err(Stop::UnknownLayout);
        }
        for event in decoded.trades {
            if event.slot != slot {
                return Err(Stop::Invalid);
            }
            if event.trade.mint != report.sample.mint {
                continue;
            }
            budget.reserve(&budget.events, 1, 8192, Stop::EventCap)?;
            report.trades.push(Recovered {
                event,
                transaction_index: index,
            });
        }
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cursor_must_be_found_and_signatures_are_never_sorted_lexicographically() {
        let row = |key, slot| json!({"signature":bs58::encode([key;64]).into_string(),"slot":slot,"err":null});
        let value = json!([row(8, 4), row(2, 4), row(1, 3)]);
        assert_eq!(
            signatures(&value, [1; 64]).unwrap(),
            vec![([8; 64], 4, true), ([2; 64], 4, true)]
        );
        assert_eq!(signatures(&value, [9; 64]), Err(Stop::CursorMissing));
        assert_eq!(
            signatures(&json!([row(8, 3), row(2, 4), row(1, 2)]), [1; 64]),
            Err(Stop::Invalid)
        );
        assert_eq!(
            signatures(&json!([row(8, 3), row(8, 3), row(1, 2)]), [1; 64]),
            Err(Stop::Invalid)
        );
    }
    #[tokio::test]
    async fn shared_caps_cannot_overshoot_and_expired_jobs_cannot_reserve() {
        let b = Budget {
            calls: AtomicUsize::new(1199),
            signatures: AtomicUsize::new(0),
            events: AtomicUsize::new(0),
            deadline: tokio::time::Instant::now() + Duration::from_secs(1),
        };
        assert_eq!(b.reserve(&b.calls, 1, 1200, Stop::CallBudget), Ok(()));
        assert_eq!(
            b.reserve(&b.calls, 1, 1200, Stop::CallBudget),
            Err(Stop::CallBudget)
        );
        assert_eq!(b.calls.load(Ordering::Acquire), 1200);
        let b = Budget {
            deadline: tokio::time::Instant::now(),
            ..b
        };
        assert_eq!(
            b.reserve(&b.events, 1, 8192, Stop::EventCap),
            Err(Stop::Duration)
        );
    }
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use axum::{Router, extract::Json, routing::post};
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    #[tokio::test]
    async fn recovered_fills_follow_block_order_and_keep_event_identity_without_live_application() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            url::Url::parse(&format!("http://{}", listener.local_addr().unwrap())).unwrap();
        let app=Router::new().route("/",post(|Json(body):Json<Value>|async move {
            let sig=|key|bs58::encode([key;64]).into_string();
            let result=match body["method"].as_str().unwrap() {
                "getSignaturesForAddress"=>json!([{"signature":sig(8),"slot":4,"err":null},{"signature":sig(2),"slot":4,"err":null},{"signature":sig(1),"slot":4,"err":null}]),
                "getBlock"=>json!({"signatures":[sig(1),sig(2),sig(8)]}),
                "getTransaction"=>{
                    let log=format!("Program data: {}",STANDARD.encode(chain::tests::raw(feed::key::<32>(chain::WRAPPED_SOL).unwrap())));
                    let (mut tx,_)=chain::tests::tx(vec![format!("Program {} invoke [1]",chain::PUMP_PROGRAM),log.clone(),log,format!("Program {} success",chain::PUMP_PROGRAM)]);
                    tx["transaction"]["signatures"]=json!([body["params"][0]]);tx["slot"]=json!(4);tx
                },
                _=>panic!("unexpected read-only method"),
            };
            Json(json!({"jsonrpc":"2.0","id":body["id"],"result":result}))
        }));
        let server = crate::supervision::AbortOnDrop(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let guard = Arc::new(crate::storage::ProcessLock::acquire(&data).unwrap());
        let mut client = rpc::Client::open("https://example.invalid/", &data, guard, 100, 100)
            .await
            .unwrap();
        client.test_endpoint(endpoint);
        let sample = Sample {
            mint: [1; 32],
            curve: [2; 32],
            signature: [1; 64],
            observed_ms: 0,
        };
        let reports = run(client, vec![sample], 7).await;
        assert_eq!(reports[0].stop, Stop::Complete);
        assert_eq!(reports[0].calls, 4);
        assert_eq!(reports[0].trades.len(), 4);
        assert_eq!(reports[0].generation, 7);
        assert_eq!(reports[0].trades[0].event.trade.signature, [2; 64]);
        assert_eq!(reports[0].trades[0].event.event_index, 1);
        assert_eq!(reports[0].trades[1].event.event_index, 2);
        assert_eq!(reports[0].trades[2].event.trade.signature, [8; 64]);
        assert_eq!(reports[0].trades[2].transaction_index, 2);
        drop(server);
    }
}
