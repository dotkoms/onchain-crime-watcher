//! Bounded observed launch membership and opening-bag continuity.
use super::Bag;
use crate::feed::{Side, Trade};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Buyer {
    pub wallet: [u8; 32],
    pub ordinal: u16,
    pub first_buy_mono_ms: u64,
    pub first_buy_wall_ms: u64,
    pub signature: String,
    pub bag: Bag,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WindowEvent {
    pub seq: u64,
    pub mono_ms: u64,
    pub wall_ms: u64,
    pub wallet: [u8; 32],
    pub signature: String,
    pub buy: bool,
    pub tokens: u64,
    pub lamports: u64,
    pub balance: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Launch {
    pub schema: u8,
    pub mint: [u8; 32],
    pub boot: String,
    pub t0_mono_ms: u64,
    pub t0_wall_ms: u64,
    pub creator: Option<[u8; 32]>,
    pub curve: Option<[u8; 32]>,
    pub buyers: BTreeMap<String, Buyer>,
    pub events: Vec<WindowEvent>,
    pub partial: bool,
    pub closed: bool,
    pub last_seq: u64,
    pub last_mono_ms: u64,
    pub updated_wall_ms: u64,
    pub buyer_overflow: u64,
    pub event_overflow: u64,
}
impl Launch {
    pub fn new(mint: [u8; 32], boot: &str, mono: u64, wall: u64) -> Option<Self> {
        if boot.is_empty() || boot.len() > 64 || !boot.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return None;
        }
        Some(Self {
            schema: 1,
            mint,
            boot: boot.into(),
            t0_mono_ms: mono,
            t0_wall_ms: wall,
            creator: None,
            curve: None,
            buyers: BTreeMap::new(),
            events: Vec::new(),
            partial: false,
            closed: false,
            last_seq: 0,
            last_mono_ms: mono,
            updated_wall_ms: wall,
            buyer_overflow: 0,
            event_overflow: 0,
        })
    }
    pub fn gap(&mut self) {
        self.partial = true;
        for buyer in self.buyers.values_mut() {
            buyer.bag.gap();
        }
    }
    pub fn tick(&mut self, mono: u64) {
        if mono < self.last_mono_ms {
            self.gap();
            return;
        }
        self.last_mono_ms = mono;
        if mono.saturating_sub(self.t0_mono_ms) >= 10_000 {
            self.closed = true;
        }
    }
    pub fn observe(&mut self, seq: u64, mono: u64, wall: u64, trade: &Trade, balance: Option<u64>) {
        if trade.mint != self.mint {
            return;
        }
        if seq <= self.last_seq || mono < self.last_mono_ms || wall < self.updated_wall_ms {
            self.gap();
            return;
        }
        self.last_seq = seq;
        self.last_mono_ms = mono;
        self.updated_wall_ms = wall;
        self.tick(mono);
        let key = bs58::encode(trade.trader).into_string();
        let mut opened = false;
        if !self.buyers.contains_key(&key) && !self.closed && trade.side == Side::Buy {
            if self.buyers.len() >= 256 {
                self.buyer_overflow = self.buyer_overflow.saturating_add(1);
                self.gap();
                return;
            }
            let Some(mut bag) = Bag::open(trade.token_amount, balance, trade.sol_lamports) else {
                self.gap();
                return;
            };
            if self.partial {
                bag.gap();
            }
            self.buyers.insert(
                key.clone(),
                Buyer {
                    wallet: trade.trader,
                    ordinal: self.buyers.len() as u16,
                    first_buy_mono_ms: mono,
                    first_buy_wall_ms: wall,
                    signature: bs58::encode(trade.signature).into_string(),
                    bag,
                },
            );
            opened = true;
        }
        if let Some(buyer) = self.buyers.get_mut(&key) {
            if !opened {
                match trade.side {
                    Side::Buy => buyer.bag.buy(trade.token_amount, balance),
                    Side::Sell => buyer
                        .bag
                        .sell(trade.token_amount, balance, trade.sol_lamports),
                }
            }
            if self.events.len() < 20_000 {
                self.events.push(WindowEvent {
                    seq,
                    mono_ms: mono,
                    wall_ms: wall,
                    wallet: trade.trader,
                    signature: bs58::encode(trade.signature).into_string(),
                    buy: trade.side == Side::Buy,
                    tokens: trade.token_amount,
                    lamports: trade.sol_lamports,
                    balance,
                });
            } else {
                self.event_overflow = self.event_overflow.saturating_add(1);
                self.gap();
            }
        }
    }
    pub fn validate(&self) -> bool {
        self.schema == 1
            && self.boot.len() <= 64
            && !self.boot.is_empty()
            && self.boot.bytes().all(|b| b.is_ascii_alphanumeric())
            && self.buyers.len() <= 256
            && self.events.len() <= 20_000
            && self.last_mono_ms >= self.t0_mono_ms
            && self.updated_wall_ms >= self.t0_wall_ms
            && self
                .buyers
                .values()
                .map(|b| b.ordinal)
                .collect::<std::collections::BTreeSet<_>>()
                .into_iter()
                .eq(0..self.buyers.len() as u16)
            && self.buyers.iter().all(|(key, b)| {
                crate::feed::key::<32>(key) == Ok(b.wallet)
                    && crate::feed::key::<64>(&b.signature).is_ok()
                    && b.bag.validate()
                    && b.first_buy_wall_ms >= self.t0_wall_ms
                    && b.first_buy_wall_ms <= self.updated_wall_ms
                    && b.first_buy_mono_ms >= self.t0_mono_ms
                    && b.first_buy_mono_ms - self.t0_mono_ms < 10_000
            })
            && self.events.windows(2).all(|w| {
                w[0].seq < w[1].seq && w[0].mono_ms <= w[1].mono_ms && w[0].wall_ms <= w[1].wall_ms
            })
            && self.events.iter().all(|e| {
                self.buyers
                    .contains_key(&bs58::encode(e.wallet).into_string())
                    && e.tokens > 0
                    && e.seq <= self.last_seq
                    && e.mono_ms <= self.last_mono_ms
                    && crate::feed::key::<64>(&e.signature).is_ok()
            })
    }
}
