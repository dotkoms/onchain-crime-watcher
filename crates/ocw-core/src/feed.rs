use serde_json::Value;
use std::collections::{HashMap, VecDeque};

pub fn key<const N: usize>(s: &str) -> Result<[u8; N], String> {
    if s.len() > 96 {
        return Err("invalid address/signature".into());
    }
    let mut out = [0; N];
    let size = bs58::decode(s)
        .onto(&mut out)
        .map_err(|_| "invalid address/signature")?;
    if size != N {
        return Err("invalid address/signature length".into());
    }
    Ok(out)
}

/// Parse provider decimal JSON without a float round trip. No fractional raw units are invented.
pub fn units(s: &str, scale: u32) -> Option<u64> {
    if s.is_empty() || s.len() > 80 || s.starts_with('-') || scale > 18 {
        return None;
    }
    let (base, exp) = match s.split_once(['e', 'E']) {
        Some((b, e)) => (b, e.parse::<i32>().ok()?),
        None => (s, 0),
    };
    let mut value = 0u128;
    let mut fractional = 0i32;
    let mut dot = false;
    let mut digits = 0;
    for c in base.bytes() {
        if c == b'.' && !dot {
            dot = true;
            continue;
        }
        if !c.is_ascii_digit() {
            return None;
        }
        value = value.checked_mul(10)?.checked_add((c - b'0') as u128)?;
        digits += 1;
        if dot {
            fractional += 1;
        }
    }
    if digits == 0 {
        return None;
    }
    let power = (scale as i32).checked_add(exp)?.checked_sub(fractional)?;
    if power.unsigned_abs() > 38 {
        return None;
    }
    let factor = 10u128.checked_pow(power.unsigned_abs())?;
    let raw = if power >= 0 {
        value.checked_mul(factor)?
    } else {
        if value % factor != 0 {
            return None;
        }
        value / factor
    };
    raw.try_into().ok()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Side {
    Buy,
    Sell,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SolSource {
    Payload,
    ReserveDelta,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trade {
    pub mint: [u8; 32],
    pub trader: [u8; 32],
    pub signature: [u8; 64],
    pub side: Side,
    pub token_amount: u64,
    pub sol_lamports: u64,
    pub sol_source: SolSource,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    Create([u8; 32]),
    Migration([u8; 32]),
    Trade(Trade),
    Duplicate,
    Unresolved,
    Other,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct Identity {
    signature: [u8; 64],
    mint: [u8; 32],
    trader: [u8; 32],
    side: Side,
    amount: u64,
}
pub struct Decoder {
    seen: HashMap<Identity, u64>,
    order: VecDeque<(Identity, u64)>,
    reserves: HashMap<[u8; 32], u64>,
    capacity: usize,
    window: u64,
}
impl Decoder {
    pub fn new(capacity: usize, window: u64) -> Self {
        Self {
            seen: HashMap::new(),
            order: VecDeque::new(),
            reserves: HashMap::new(),
            capacity: capacity.max(1),
            window,
        }
    }
    pub fn reset_session(&mut self) {
        self.reserves.clear();
    }
    pub fn dedup_len(&self) -> usize {
        self.seen.len()
    }
    pub fn decode(&mut self, bytes: &[u8], now: u64) -> Result<Event, String> {
        let v: Value = serde_json::from_slice(bytes).map_err(|_| "invalid JSON")?;
        let kind = v.get("txType").and_then(Value::as_str).unwrap_or("");
        if !matches!(kind, "buy" | "sell" | "create" | "migration") {
            return Ok(Event::Other);
        }
        let mint = field_key::<32>(&v, "mint")?;
        if kind == "migration" {
            self.reserves.remove(&mint);
            return Ok(Event::Migration(mint));
        }
        let reserve = number(&v, "vSolInBondingCurve", 9)?;
        if kind == "create" {
            self.set_reserve(mint, reserve);
            return Ok(Event::Create(mint));
        }
        let side = if kind == "buy" { Side::Buy } else { Side::Sell };
        let signature = field_key::<64>(&v, "signature")?;
        let trader = field_key::<32>(&v, "traderPublicKey")?;
        let token_amount = number(&v, "tokenAmount", 6)?.ok_or("missing token amount")?;
        if token_amount == 0 {
            return Err("zero token amount".into());
        }
        let identity = Identity {
            signature,
            mint,
            trader,
            side,
            amount: token_amount,
        };
        while self
            .order
            .front()
            .is_some_and(|(_, t)| now.saturating_sub(*t) > self.window)
        {
            if let Some((id, _)) = self.order.pop_front() {
                self.seen.remove(&id);
            }
        }
        if self.seen.contains_key(&identity) {
            return Ok(Event::Duplicate);
        }
        let payload = number(&v, "solAmount", 9)?;
        let previous = self.reserves.get(&mint).copied();
        let (sol, source) = match payload {
            Some(p) => (Some(p), SolSource::Payload),
            None => (
                previous
                    .zip(reserve)
                    .and_then(|(before, after)| match side {
                        Side::Buy => after.checked_sub(before),
                        Side::Sell => before.checked_sub(after),
                    }),
                SolSource::ReserveDelta,
            ),
        };
        self.set_reserve(mint, reserve);
        if self.seen.len() == self.capacity {
            if let Some((id, _)) = self.order.pop_front() {
                self.seen.remove(&id);
            }
        }
        self.seen.insert(identity.clone(), now);
        self.order.push_back((identity, now));
        let Some(sol_lamports) = sol.filter(|x| *x > 0) else {
            return Ok(Event::Unresolved);
        };
        Ok(Event::Trade(Trade {
            mint,
            trader,
            signature,
            side,
            token_amount,
            sol_lamports,
            sol_source: source,
        }))
    }
    fn set_reserve(&mut self, mint: [u8; 32], reserve: Option<u64>) {
        if !self.reserves.contains_key(&mint) && self.reserves.len() >= self.capacity {
            self.reserves.clear();
        }
        if let Some(r) = reserve {
            self.reserves.insert(mint, r);
        } else {
            self.reserves.remove(&mint);
        }
    }
}
fn field_key<const N: usize>(v: &Value, name: &str) -> Result<[u8; N], String> {
    key(v
        .get(name)
        .and_then(Value::as_str)
        .ok_or("missing address/signature")?)
}
fn number(v: &Value, name: &str, scale: u32) -> Result<Option<u64>, String> {
    match v.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => units(&n.to_string(), scale)
            .map(Some)
            .ok_or_else(|| "invalid amount".into()),
        _ => Err("invalid amount type".into()),
    }
}
