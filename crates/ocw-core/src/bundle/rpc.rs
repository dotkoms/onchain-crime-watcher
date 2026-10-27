//! Bounded confirmed funding facts, never inferred from price or sale proceeds.
use super::{
    Progress,
    capture::{Buyer, Launch},
    funding::{Attribution, Evidence, Transfer},
};
use crate::{
    chain, feed,
    rpc::{Client, Method},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    time::Duration,
};
const SYSTEM: &str = "11111111111111111111111111111111";
const TOKEN: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const TOKEN22: &str = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";
struct Budget<'a> {
    client: &'a Client,
    calls: u16,
    deadline: tokio::time::Instant,
}
impl Budget<'_> {
    async fn call(&mut self, method: Method, params: Value) -> Result<Value, &'static str> {
        if self.calls >= 1200 {
            return Err("call_budget");
        }
        self.calls += 1;
        tokio::time::timeout_at(self.deadline, self.client.call(method, params))
            .await
            .map_err(|_| "deadline")?
            .map_err(|_| "rpc_unavailable_or_budget")
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Cluster {
    pub root: String,
    pub wallets: usize,
    pub progress: Option<Progress>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Report {
    pub schema: u8,
    pub mint: String,
    pub calls: u16,
    pub anchor_sha256: String,
    pub exclusions_sha256: String,
    pub verified_buyers: Vec<[u8; 32]>,
    pub observed_wall_ms: u64,
    pub supply_raw: Option<u64>,
    pub attributions: Vec<Attribution>,
    pub clusters: Vec<Cluster>,
    pub partial: bool,
    pub coverage: String,
    pub stop: Option<String>,
}
/// All results remain partial: this scan verifies pre-buy funding, not the complete
/// post-launch wallet/transfer history or the historical price context needed for CLEARED.
pub async fn analyze(client: &Client, launch: &Launch, exclusions: &BTreeSet<[u8; 32]>) -> Report {
    let mut budget = Budget {
        client,
        calls: 0,
        deadline: tokio::time::Instant::now() + Duration::from_secs(120),
    };
    let mint = bs58::encode(launch.mint).into_string();
    let mut report = Report {
        schema: 1,
        mint: mint.clone(),
        calls: 0,
        anchor_sha256: anchor_hash(launch),
        exclusions_sha256: exclusions_hash(exclusions),
        verified_buyers: Vec::new(),
        observed_wall_ms: launch.updated_wall_ms,
        supply_raw: None,
        attributions: Vec::new(),
        clusters: Vec::new(),
        partial: true,
        coverage: "available confirmed funding; post-launch history/price context unverified"
            .into(),
        stop: None,
    };
    if !launch.validate() || !launch.closed {
        report.stop = Some("invalid_or_open_launch_window".into());
        return report;
    }
    let supply = budget
        .call(
            Method::AccountInfo,
            json!([mint,{"encoding":"jsonParsed","commitment":"confirmed"}]),
        )
        .await;
    report.supply_raw = supply.ok().as_ref().and_then(supply_amount);
    let buyers = launch
        .buyers
        .values()
        .map(|b| b.wallet)
        .collect::<BTreeSet<_>>();
    let mut evidence = Vec::new();
    let mut verified = BTreeSet::new();
    for buyer in launch.buyers.values() {
        if tokio::time::Instant::now() >= budget.deadline || budget.calls >= 1200 {
            report.stop = Some("deadline_or_call_budget".into());
            break;
        }
        match funding(&mut budget, launch, buyer, &buyers, exclusions).await {
            Ok((facts, weight_verified)) => {
                if weight_verified {
                    verified.insert(buyer.wallet);
                }
                evidence.push(facts);
            }
            Err(reason) => {
                report.stop = Some(reason.into());
                evidence.push(Evidence {
                    wallet: buyer.wallet,
                    buy_wall_ms: buyer.first_buy_wall_ms,
                    direct: Vec::new(),
                    direct_complete: false,
                    parent: Vec::new(),
                    parent_complete: false,
                    jito_tip_payer: None,
                });
            }
        }
    }
    report.attributions =
        super::funding::classify(&evidence, &buyers, exclusions).unwrap_or_default();
    let mut groups = BTreeMap::<[u8; 32], Vec<super::Bag>>::new();
    for row in &report.attributions {
        if let Some(root) = row.root {
            let Some(buyer) = launch.buyers.get(&bs58::encode(row.wallet).into_string()) else {
                continue;
            };
            let mut bag = buyer.bag.clone();
            if !verified.contains(&row.wallet) {
                continue;
            }
            bag.gap();
            groups.entry(root).or_default().push(bag);
        }
    }
    for (root, bags) in groups {
        report.clusters.push(Cluster {
            root: bs58::encode(root).into_string(),
            wallets: bags.len(),
            progress: super::progress(&bags, report.supply_raw, false, false),
        });
    }
    report.verified_buyers = verified.into_iter().collect();
    report.calls = budget.calls;
    report
}
fn supply_amount(value: &Value) -> Option<u64> {
    let value = &value["value"];
    if value["executable"] != false
        || !matches!(value["owner"].as_str(), Some(TOKEN | TOKEN22))
        || value["data"]["parsed"]["type"] != "mint"
        || value["data"]["parsed"]["info"]["decimals"] != 6
        || value["data"]["parsed"]["info"]["isInitialized"] != true
    {
        return None;
    }
    value["data"]["parsed"]["info"]["supply"]
        .as_str()?
        .parse::<u64>()
        .ok()
        .filter(|n| *n > 0)
}
async fn funding(
    b: &mut Budget<'_>,
    launch: &Launch,
    buyer: &Buyer,
    buyers: &BTreeSet<[u8; 32]>,
    excluded: &BTreeSet<[u8; 32]>,
) -> Result<(Evidence, bool), &'static str> {
    let anchor=b.call(Method::Transaction,json!([buyer.signature,{"encoding":"jsonParsed","commitment":"confirmed","maxSupportedTransactionVersion":0}])).await?;
    let decoded = chain::decode(&anchor, &buyer.signature).map_err(|_| "unverified_launch_buy")?;
    let event = launch
        .events
        .iter()
        .find(|e| e.buy && e.wallet == buyer.wallet && e.signature == buyer.signature)
        .ok_or("opening_event_missing")?;
    let matched: Vec<_> = decoded
        .trades
        .iter()
        .filter(|row| {
            row.trade.mint == launch.mint
                && row.trade.trader == buyer.wallet
                && row.trade.side == feed::Side::Buy
        })
        .collect();
    if matched.len() != 1 {
        return Err("ambiguous_launch_buy");
    }
    let trade = matched[0];
    let balances = anchor["meta"]["postTokenBalances"]
        .as_array()
        .map(|rows| {
            rows.iter()
                .filter(|v| {
                    v["mint"] == bs58::encode(launch.mint).into_string()
                        && v["owner"] == bs58::encode(buyer.wallet).into_string()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let opening_verified = balances.len() == 1
        && balances[0]["uiTokenAmount"]["decimals"] == 6
        && balances[0]["uiTokenAmount"]["amount"]
            .as_str()
            .and_then(|s| s.parse::<u64>().ok())
            == buyer.bag.original;
    let weight_verified = opening_verified
        && trade.trade.sol_lamports == event.lamports
        && trade.trade.token_amount == event.tokens
        && decoded.unresolved == 0;
    let (direct, complete) = window(
        b,
        buyer.wallet,
        trade.block_time_ms,
        trade.slot,
        &buyer.signature,
    )
    .await?;
    let mut result = Evidence {
        wallet: buyer.wallet,
        buy_wall_ms: trade.block_time_ms,
        direct,
        direct_complete: complete,
        parent: Vec::new(),
        parent_complete: false,
        jito_tip_payer: None,
    };
    if !complete {
        return Ok((result, weight_verified));
    }
    let selected = result
        .direct
        .iter()
        .filter(|t| t.to == buyer.wallet && t.lamports >= 5_000_000)
        .max_by_key(|t| (t.lamports, t.wall_ms, t.from));
    if let Some(transfer) = selected.cloned() {
        if buyers.contains(&transfer.from) || excluded.contains(&transfer.from) {
            result.parent_complete = true;
        } else {
            let parent_tx=b.call(Method::Transaction,json!([transfer.signature,{"encoding":"jsonParsed","commitment":"confirmed","maxSupportedTransactionVersion":0}])).await?;
            let slot = parent_tx["slot"].as_u64().ok_or("funding_slot_missing")?;
            let (parent, complete) = window(
                b,
                transfer.from,
                transfer.wall_ms,
                slot,
                &transfer.signature,
            )
            .await?;
            result.parent = parent;
            result.parent_complete = complete;
        }
    } else {
        result.parent_complete = true;
    }
    Ok((result, weight_verified))
}
async fn window(
    b: &mut Budget<'_>,
    wallet: [u8; 32],
    at: u64,
    anchor_slot: u64,
    anchor_sig: &str,
) -> Result<(Vec<Transfer>, bool), &'static str> {
    let mut before = anchor_sig.to_owned();
    let mut seen = BTreeSet::new();
    let mut candidates = Vec::new();
    let lower = at.saturating_sub(60_000);
    let mut closed = false;
    let mut certain = true;
    let mut previous = anchor_slot;
    for _ in 0..2 {
        let value=b.call(Method::Signatures,json!([bs58::encode(wallet).into_string(),{"commitment":"confirmed","before":before,"limit":1000}])).await?;
        let rows = value
            .as_array()
            .filter(|rows| rows.len() <= 1000)
            .ok_or("invalid_signature_page")?;
        for row in rows {
            let signature = row["signature"]
                .as_str()
                .ok_or("missing_funding_signature")?;
            let key = feed::key::<64>(signature).map_err(|_| "invalid_funding_signature")?;
            let slot = row["slot"].as_u64().ok_or("missing_funding_slot")?;
            if slot > previous || !seen.insert(key) {
                return Err("unordered_funding_page");
            }
            previous = slot;
            before = signature.to_owned();
            let Some(time) = row["blockTime"].as_u64().and_then(|n| n.checked_mul(1000)) else {
                certain = false;
                continue;
            };
            if time < lower {
                closed = true;
                break;
            }
            if time > at {
                certain = false;
                continue;
            }
            if row.get("err") == Some(&Value::Null) {
                candidates.push((signature.to_owned(), slot, time));
            } else if row.get("err").is_none() {
                certain = false;
            }
        }
        if closed || rows.len() < 1000 {
            closed = true;
            break;
        }
    }
    if candidates.len() > 4 {
        return Ok((Vec::new(), false));
    }
    let mut transfers = Vec::new();
    for (signature, slot, time) in candidates {
        if slot == anchor_slot {
            let block=b.call(Method::Block,json!([slot,{"commitment":"confirmed","transactionDetails":"signatures","rewards":false,"maxSupportedTransactionVersion":0}])).await?;
            let signatures = block["signatures"]
                .as_array()
                .ok_or("block_order_unavailable")?;
            let from = signatures.iter().position(|v| v == &signature);
            let to = signatures.iter().position(|v| v == anchor_sig);
            if !matches!((from,to),(Some(a),Some(z)) if a<z) {
                return Err("funding_order_unverified");
            }
        }
        let tx=b.call(Method::Transaction,json!([signature,{"encoding":"jsonParsed","commitment":"confirmed","maxSupportedTransactionVersion":0}])).await?;
        if tx["slot"] != slot
            || tx["blockTime"].as_u64().and_then(|n| n.checked_mul(1000)) != Some(time)
        {
            return Err("funding_identity_mismatch");
        }
        transfers.extend(transfers_from(&tx, &signature, wallet)?);
        if transfers.len() > 4 {
            return Ok((Vec::new(), false));
        }
    }
    Ok((transfers, closed && certain))
}
fn transfers_from(
    tx: &Value,
    signature: &str,
    wallet: [u8; 32],
) -> Result<Vec<Transfer>, &'static str> {
    chain::decode(tx, signature).map_err(|_| "funding_execution_unverified")?;
    let mut instructions = Vec::new();
    let outer = tx["transaction"]["message"]["instructions"]
        .as_array()
        .ok_or("funding_instructions_missing")?;
    if outer.len() > 256 {
        return Err("funding_instruction_cap");
    }
    instructions.extend(outer);
    if let Some(inner) = tx["meta"]["innerInstructions"].as_array() {
        for group in inner {
            let rows = group["instructions"]
                .as_array()
                .ok_or("invalid_inner_instructions")?;
            if instructions.len() + rows.len() > 4096 {
                return Err("funding_instruction_cap");
            }
            instructions.extend(rows);
        }
    }
    let system_count = instructions
        .iter()
        .filter(|v| v["programId"] == SYSTEM)
        .count();
    let invokes = tx["meta"]["logMessages"]
        .as_array()
        .ok_or("funding_logs_missing")?
        .iter()
        .filter(|line| {
            line.as_str()
                .is_some_and(|s| s.starts_with("Program 11111111111111111111111111111111 invoke ["))
        })
        .count();
    if system_count != invokes {
        return Err("system_execution_mapping_unverified");
    }
    let time = tx["blockTime"]
        .as_u64()
        .and_then(|n| n.checked_mul(1000))
        .ok_or("funding_time_missing")?;
    let mut output = Vec::new();
    for instruction in instructions {
        if instruction["programId"] != SYSTEM
            || !matches!(
                instruction["parsed"]["type"].as_str(),
                Some("transfer" | "transferWithSeed")
            )
        {
            continue;
        }
        let info = &instruction["parsed"]["info"];
        let from = feed::key::<32>(info["source"].as_str().ok_or("missing_funder")?)
            .map_err(|_| "invalid_funder")?;
        let to = feed::key::<32>(info["destination"].as_str().ok_or("missing_destination")?)
            .map_err(|_| "invalid_destination")?;
        let lamports = info["lamports"].as_u64().ok_or("invalid_funding_amount")?;
        if to == wallet && lamports > 0 {
            output.push(Transfer {
                from,
                to,
                lamports,
                wall_ms: time,
                signature: signature.into(),
            });
        }
    }
    Ok(output)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_successful_bound_system_transfers_become_funding_facts() {
        let signature = bs58::encode([3; 64]).into_string();
        let from = bs58::encode([8; 32]).into_string();
        let to = bs58::encode([1; 32]).into_string();
        let mut tx = json!({"slot":1,"blockTime":10,"transaction":{"signatures":[signature],"message":{"accountKeys":[from],"instructions":[{"programId":SYSTEM,"parsed":{"type":"transfer","info":{"source":from,"destination":to,"lamports":5000000}}}]}},"meta":{"err":null,"fee":5000,"logMessages":[format!("Program {SYSTEM} invoke [1]"),format!("Program {SYSTEM} success")],"innerInstructions":[]}});
        assert_eq!(transfers_from(&tx, &signature, [1; 32]).unwrap().len(), 1);
        tx["meta"]["logMessages"][1] =
            json!(format!("Program {SYSTEM} failed: insufficient funds"));
        assert!(transfers_from(&tx, &signature, [1; 32]).is_err());
        tx["meta"]["logMessages"] = json!([]);
        assert!(transfers_from(&tx, &signature, [1; 32]).is_err());
    }
}

pub fn anchor_hash(launch: &Launch) -> String {
    use sha2::{Digest, Sha256};
    let buyers: Vec<_> = launch
        .buyers
        .values()
        .map(|b| {
            (
                b.wallet,
                &b.signature,
                b.bag.original,
                b.bag.original_cost_lamports,
            )
        })
        .collect();
    format!(
        "{:x}",
        Sha256::digest(
            serde_json::to_vec(&(launch.mint, &launch.boot, launch.t0_mono_ms, buyers))
                .expect("fixed scalar bundle anchor")
        )
    )
}
pub(crate) fn exclusions_hash(exclusions: &BTreeSet<[u8; 32]>) -> String {
    use sha2::Digest;
    let mut hash = sha2::Sha256::new();
    for key in exclusions {
        hash.update(key);
    }
    format!("{:x}", hash.finalize())
}
pub fn refresh(launch: &Launch, previous: &Report) -> Option<Report> {
    if !launch.validate()
        || anchor_hash(launch) != previous.anchor_sha256
        || previous.attributions.len() > 256
        || previous.verified_buyers.len() > 256
    {
        return None;
    }
    if previous.schema != 1
        || previous.mint != bs58::encode(launch.mint).into_string()
        || previous
            .attributions
            .iter()
            .map(|r| r.wallet)
            .collect::<BTreeSet<_>>()
            .len()
            != previous.attributions.len()
        || previous.attributions.iter().any(|r| {
            !launch
                .buyers
                .contains_key(&bs58::encode(r.wallet).into_string())
        })
    {
        return None;
    }
    let mut report = previous.clone();
    report.observed_wall_ms = launch.updated_wall_ms;
    let mut groups = BTreeMap::<[u8; 32], Vec<super::Bag>>::new();
    for row in &previous.attributions {
        if let Some(root) = row.root
            && previous.verified_buyers.contains(&row.wallet)
        {
            let buyer = launch.buyers.get(&bs58::encode(row.wallet).into_string())?;
            let mut bag = buyer.bag.clone();
            bag.gap();
            groups.entry(root).or_default().push(bag);
        }
    }
    report.clusters = groups
        .into_iter()
        .map(|(root, bags)| Cluster {
            root: bs58::encode(root).into_string(),
            wallets: bags.len(),
            progress: super::progress(&bags, report.supply_raw, false, false),
        })
        .collect();
    Some(report)
}

#[cfg(test)]
mod http_tests {
    use super::*;
    use axum::{Router, extract::Json, routing::post};
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use std::sync::Arc;
    #[tokio::test]
    async fn confirmed_funding_and_supply_produce_partial_weighted_progress_without_a_clearance_claim()
     {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint =
            url::Url::parse(&format!("http://{}/", listener.local_addr().unwrap())).unwrap();
        let app=Router::new().route("/",post(|Json(body):Json<Value>|async move {
            let key=|s:&Value,n|feed::key::<32>(s.as_str().unwrap()).unwrap_or([n;32]);
            let result=match body["method"].as_str().unwrap() {
                "getAccountInfo"=>json!({"value":{"owner":TOKEN22,"executable":false,"data":{"parsed":{"type":"mint","info":{"decimals":6,"supply":"25000","isInitialized":true}}}}}),
                "getSignaturesForAddress"=>{let wallet=key(&body["params"][0],0);if wallet==[8;32] {json!([])}else{json!([{"signature":bs58::encode([100+wallet[0];64]).into_string(),"slot":90,"blockTime":950,"err":null}])}},
                "getTransaction"=>{
                    let sig=body["params"][0].as_str().unwrap();let raw_sig=feed::key::<64>(sig).unwrap();let id=raw_sig[0];
                    if id<100 {
                        let mut raw=chain::tests::raw(feed::key::<32>(chain::WRAPPED_SOL).unwrap());raw[40..48].copy_from_slice(&2_000_000_000u64.to_le_bytes());let quote=raw.len()-48;raw[quote..quote+8].copy_from_slice(&2_000_000_000u64.to_le_bytes());raw[48..56].copy_from_slice(&100u64.to_le_bytes());raw[57..89].copy_from_slice(&[id;32]);
                        let (mut tx,_)=chain::tests::tx(vec![format!("Program {} invoke [1]",chain::PUMP_PROGRAM),format!("Program data: {}",STANDARD.encode(raw)),format!("Program {} success",chain::PUMP_PROGRAM)]);
                        tx["slot"]=json!(100);tx["blockTime"]=json!(1000);tx["transaction"]["signatures"]=json!([sig]);
                        tx["meta"]["postTokenBalances"]=json!([{"mint":bs58::encode([1;32]).into_string(),"owner":bs58::encode([id;32]).into_string(),"uiTokenAmount":{"amount":"100","decimals":6}}]);tx
                    }else{
                        let from=bs58::encode([8;32]).into_string();let to=bs58::encode([id-100;32]).into_string();
                        json!({"slot":90,"blockTime":950,"transaction":{"signatures":[sig],"message":{"accountKeys":[from],"instructions":[{"programId":SYSTEM,"parsed":{"type":"transfer","info":{"source":from,"destination":to,"lamports":3000000000u64}}}]}},"meta":{"err":null,"fee":5000,"logMessages":[format!("Program {SYSTEM} invoke [1]"),format!("Program {SYSTEM} success")],"innerInstructions":[]}})
                    }
                },_=>panic!("unexpected RPC method"),
            };Json(json!({"jsonrpc":"2.0","id":body["id"],"result":result}))
        }));
        let server = crate::supervision::AbortOnDrop(tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        }));
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let guard = Arc::new(crate::storage::ProcessLock::acquire(&data).unwrap());
        let mut client = Client::open("https://example.invalid/", &data, guard, 1000, 600)
            .await
            .unwrap();
        client.test_endpoint(endpoint);
        let mut launch = Launch::new([1; 32], "test", 0, 1_000_000).unwrap();
        for id in 2..=7 {
            let trade = feed::Trade {
                mint: [1; 32],
                trader: [id; 32],
                signature: [id; 64],
                side: feed::Side::Buy,
                token_amount: 100,
                sol_lamports: 2_000_000_000,
                sol_source: feed::SolSource::Payload,
            };
            launch.observe(
                id as u64,
                id as u64,
                1_000_000 + id as u64,
                &trade,
                Some(100),
            );
        }
        for id in 2..=7 {
            let trade = feed::Trade {
                mint: [1; 32],
                trader: [id; 32],
                signature: [50 + id; 64],
                side: feed::Side::Sell,
                token_amount: 80,
                sol_lamports: 1,
                sol_source: feed::SolSource::Payload,
            };
            launch.observe(
                100 + id as u64,
                100 + id as u64,
                1_000_100 + id as u64,
                &trade,
                Some(20),
            );
        }
        launch.tick(10_000);
        let report = analyze(&client, &launch, &BTreeSet::new()).await;
        assert_eq!(report.supply_raw, Some(25_000));
        assert_eq!(report.attributions.len(), 6);
        assert_eq!(report.clusters.len(), 1, "{report:?}");
        let progress = report.clusters[0].progress.as_ref().unwrap();
        assert_eq!(progress.tier, super::super::Tier::Fire);
        assert_eq!(progress.sold_bps_lower, 8000);
        assert!(progress.partial);
        assert!(report.calls <= 1200);
        let refreshed = refresh(&launch, &report).unwrap();
        assert_eq!(
            refreshed.clusters[0].progress.as_ref().unwrap().tier,
            super::super::Tier::Fire
        );
        drop(server);
    }
}
