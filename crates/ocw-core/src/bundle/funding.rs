//! Funding facts are inputs, not inferred from same-block timing or Jito tips.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Transfer {
    pub from: [u8; 32],
    pub to: [u8; 32],
    pub lamports: u64,
    pub wall_ms: u64,
    pub signature: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Evidence {
    pub wallet: [u8; 32],
    pub buy_wall_ms: u64,
    pub direct: Vec<Transfer>,
    pub direct_complete: bool,
    pub parent: Vec<Transfer>,
    pub parent_complete: bool,
    pub jito_tip_payer: Option<[u8; 32]>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Attribution {
    pub wallet: [u8; 32],
    pub root: Option<[u8; 32]>,
    pub hop: Option<u8>,
    pub excluded: bool,
    pub fanout_capped: bool,
    pub partial: bool,
    pub jito_corroborated: bool,
}
fn largest(transfers: &[Transfer], recipient: [u8; 32], at: u64) -> Option<&Transfer> {
    transfers
        .iter()
        .filter(|t| {
            t.to == recipient
                && t.from != recipient
                && t.lamports >= 5_000_000
                && t.wall_ms <= at
                && at - t.wall_ms <= 60_000
                && crate::feed::key::<64>(&t.signature).is_ok()
        })
        .max_by_key(|t| (t.lamports, t.wall_ms, t.from))
}
/// Excluded accounts never become roots or intermediates. Missing RPC evidence is
/// partial, not proof of independent funding. At most one ancestor is followed.
pub fn classify(
    rows: &[Evidence],
    launch_buyers: &BTreeSet<[u8; 32]>,
    exclusions: &BTreeSet<[u8; 32]>,
) -> Option<Vec<Attribution>> {
    if rows.len() > 256
        || launch_buyers.len() > 256
        || exclusions.len() > 200
        || rows
            .iter()
            .any(|r| r.direct.len() > 4 || r.parent.len() > 4 || !launch_buyers.contains(&r.wallet))
        || rows.iter().map(|r| r.wallet).collect::<BTreeSet<_>>().len() != rows.len()
    {
        return None;
    }
    let mut results = Vec::new();
    let mut tips = BTreeMap::<[u8; 32], usize>::new();
    for row in rows {
        if let Some(tip) = row.jito_tip_payer {
            *tips.entry(tip).or_default() += 1;
        }
    }
    for row in rows {
        let mut result = Attribution {
            wallet: row.wallet,
            root: None,
            hop: None,
            excluded: false,
            fanout_capped: false,
            partial: !row.direct_complete,
            jito_corroborated: row
                .jito_tip_payer
                .is_some_and(|tip| tips.get(&tip).is_some_and(|n| *n >= 2)),
        };
        if !row.direct_complete {
            results.push(result);
            continue;
        }
        if let Some(direct) = largest(&row.direct, row.wallet, row.buy_wall_ms) {
            if exclusions.contains(&direct.from) {
                result.excluded = true;
            } else {
                result.root = Some(direct.from);
                result.hop = Some(0);
                if !launch_buyers.contains(&direct.from) {
                    result.partial |= !row.parent_complete;
                    if !row.parent_complete {
                        result.root = None;
                        result.hop = None;
                        results.push(result);
                        continue;
                    }
                    if let Some(parent) = largest(&row.parent, direct.from, direct.wall_ms) {
                        if parent.from == row.wallet {
                            result.root = None;
                            result.hop = None;
                            result.partial = true;
                        } else if exclusions.contains(&parent.from) {
                            result.root = None;
                            result.hop = None;
                            result.excluded = true;
                        } else {
                            result.root = Some(parent.from);
                            result.hop = Some(1);
                        }
                    }
                }
            }
        }
        results.push(result);
    }
    let mut fanout = BTreeMap::<[u8; 32], usize>::new();
    for r in &results {
        if let Some(root) = r.root {
            *fanout.entry(root).or_default() += 1;
        }
    }
    for r in &mut results {
        if r.root.is_some_and(|root| fanout[&root] > 50) {
            r.root = None;
            r.hop = None;
            r.fanout_capped = true;
            r.excluded = true;
            r.partial = true;
        }
    }
    Some(results)
}
