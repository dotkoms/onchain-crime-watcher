//! Decode native-SOL Pump TradeEvent facts from confirmed RPC transaction logs.
//! Unknown layouts, token quotes and truncated invocation stacks remain unresolved.
use crate::feed::{self, Side, SolSource, Trade};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde_json::Value;
pub const PUMP_PROGRAM: &str = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
pub const WRAPPED_SOL: &str = "So11111111111111111111111111111111111111112";
const TRADE: [u8; 8] = [189, 219, 127, 211, 78, 230, 97, 238];
#[derive(Clone, Debug)]
pub struct ChainTrade {
    pub trade: Trade,
    pub event_index: u32,
    pub slot: u64,
    pub block_time_ms: u64,
    pub transaction_fee: u64,
    pub transaction_fee_payer: [u8; 32],
}
#[derive(Default)]
pub struct Decoded {
    pub trades: Vec<ChainTrade>,
    pub unresolved: u32,
}
struct Cursor<'a>(&'a [u8]);
impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8], &'static str> {
        let out = self.0.get(..n).ok_or("short chain event")?;
        self.0 = &self.0[n..];
        Ok(out)
    }
    fn u64(&mut self) -> Result<u64, &'static str> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().map_err(|_| "short integer")?,
        ))
    }
    fn key(&mut self) -> Result<[u8; 32], &'static str> {
        self.take(32)?.try_into().map_err(|_| "short key")
    }
    fn boolean(&mut self) -> Result<bool, &'static str> {
        match self.take(1)?[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err("invalid boolean"),
        }
    }
    fn count(&mut self, cap: usize) -> Result<usize, &'static str> {
        let n = u32::from_le_bytes(self.take(4)?.try_into().map_err(|_| "short count")?) as usize;
        if n > cap {
            return Err("chain event bound exceeded");
        }
        Ok(n)
    }
}
fn event(raw: &[u8], signature: [u8; 64]) -> Result<Trade, &'static str> {
    let mut c = Cursor(raw);
    if c.take(8)? != TRADE {
        return Err("unsupported chain event");
    }
    let mint = c.key()?;
    let sol = c.u64()?;
    let token_amount = c.u64()?;
    let buy = c.boolean()?;
    let trader = c.key()?;
    c.take(8 + 4 * 8 + 32 + 8 + 8 + 32 + 8 + 8)?; // timestamp, reserves, protocol/creator fee fields
    c.boolean()?;
    c.take(4 * 8)?; // track-volume and accumulator fields
    let size = c.count(64)?;
    let ix = c.take(size)?;
    c.boolean()?;
    c.take(4 * 8)?;
    let shareholders = c.count(50)?;
    c.take(shareholders * 34)?;
    let quote = c.key()?;
    let quote_amount = c.u64()?;
    c.take(5 * 8)?;
    if !c.0.is_empty() || token_amount == 0 {
        return Err("unsupported chain event layout");
    }
    let wrapped = feed::key::<32>(WRAPPED_SOL).map_err(|_| "invalid built-in key")?;
    // Legacy instruction names retain native-SOL semantics. New quotes require explicit WSOL.
    let legacy = quote == [0; 32]
        && matches!(ix, b"buy" | b"sell" | b"buy_exact_sol_in")
        && quote_amount == 0;
    if quote != wrapped && !legacy {
        return Err("non-SOL quote unresolved");
    }
    if sol > 0 && quote_amount > 0 && sol != quote_amount {
        return Err("ambiguous native quote amount");
    }
    let sol_lamports = sol.max(quote_amount);
    if sol_lamports == 0 {
        return Err("zero native amount");
    }
    Ok(Trade {
        mint,
        trader,
        signature,
        side: if buy { Side::Buy } else { Side::Sell },
        token_amount,
        sol_lamports,
        sol_source: SolSource::Instruction,
    })
}
/// Accept the `result` object of getTransaction (encoding=json/jsonParsed). The caller must
/// verify the JSON-RPC response ID; this function also binds the transaction to its signature.
pub fn decode(result: &Value, signature: &str) -> Result<Decoded, &'static str> {
    let signature_bytes =
        feed::key::<64>(signature).map_err(|_| "invalid transaction signature")?;
    if result["transaction"]["signatures"][0].as_str() != Some(signature)
        || result["meta"].is_null()
        || result["meta"].get("err") != Some(&Value::Null)
    {
        return Err("transaction missing, mismatched or failed");
    }
    let slot = result["slot"].as_u64().ok_or("missing chain slot")?;
    let block_time_ms = result["blockTime"]
        .as_u64()
        .and_then(|n| n.checked_mul(1000))
        .ok_or("missing chain time")?;
    let fee = result["meta"]["fee"]
        .as_u64()
        .ok_or("missing transaction fee")?;
    let payer = &result["transaction"]["message"]["accountKeys"][0];
    let payer = feed::key::<32>(
        payer
            .as_str()
            .or_else(|| payer["pubkey"].as_str())
            .ok_or("missing fee payer")?,
    )
    .map_err(|_| "invalid fee payer")?;
    let logs = result["meta"]["logMessages"]
        .as_array()
        .filter(|v| !v.is_empty() && v.len() <= 4096)
        .ok_or("missing or oversized logs")?;
    let mut stack: Vec<&str> = Vec::new();
    let mut output = Decoded::default();
    let mut index = 0u32;
    let mut bytes_seen = 0;
    for value in logs {
        let line = value.as_str().ok_or("invalid chain log")?;
        bytes_seen += line.len();
        if bytes_seen > 1024 * 1024 || line.contains("Log truncated") {
            return Err("truncated or oversized chain logs");
        }
        if let Some(rest) = line.strip_prefix("Program ") {
            if let Some((program, depth)) = rest.split_once(" invoke [") {
                let depth = depth
                    .strip_suffix(']')
                    .and_then(|n| n.parse::<usize>().ok())
                    .ok_or("invalid invocation depth")?;
                feed::key::<32>(program).map_err(|_| "invalid invoking program")?;
                if depth != stack.len() + 1 || depth > 16 {
                    return Err("incomplete invocation stack");
                }
                stack.push(program);
                continue;
            }
            if let Some(program) = rest.strip_suffix(" success") {
                if stack.pop() != Some(program) {
                    return Err("incomplete invocation stack");
                }
                continue;
            }
            if rest.contains(" failed:") {
                return Err("failed program execution");
            }
        }
        if let Some(encoded) = line.strip_prefix("Program data: ") {
            index = index.checked_add(1).ok_or("event index overflow")?;
            if stack.last() != Some(&PUMP_PROGRAM) {
                continue;
            }
            if encoded.len() > 12_000 {
                return Err("oversized chain event");
            }
            let data = STANDARD
                .decode(encoded)
                .map_err(|_| "invalid chain event encoding")?;
            match event(&data, signature_bytes) {
                Ok(trade) => output.trades.push(ChainTrade {
                    trade,
                    event_index: index,
                    slot,
                    block_time_ms,
                    transaction_fee: fee,
                    transaction_fee_payer: payer,
                }),
                Err(_) => output.unresolved += 1,
            }
        }
    }
    if !stack.is_empty() {
        return Err("incomplete invocation stack");
    }
    Ok(output)
}
#[cfg(test)]
mod tests {
    use super::*;
    fn raw(quote: [u8; 32]) -> Vec<u8> {
        let mut b = TRADE.to_vec();
        b.extend([1; 32]);
        b.extend(123u64.to_le_bytes());
        b.extend(456u64.to_le_bytes());
        b.push(1);
        b.extend([2; 32]);
        b.extend(vec![0; 8 + 4 * 8 + 32 + 8 + 8 + 32 + 8 + 8]);
        b.push(0);
        b.extend([0; 32]);
        b.extend(3u32.to_le_bytes());
        b.extend(b"buy");
        b.push(0);
        b.extend([0; 32]);
        b.extend(0u32.to_le_bytes());
        b.extend(quote);
        b.extend(123u64.to_le_bytes());
        b.extend([0; 40]);
        b
    }
    fn tx(logs: Vec<String>) -> (Value, String) {
        let sig = bs58::encode([3; 64]).into_string();
        (
            serde_json::json!({"slot":1,"blockTime":10,"transaction":{"signatures":[sig],"message":{"accountKeys":[bs58::encode([2;32]).into_string()]}},"meta":{"err":null,"fee":5000,"logMessages":logs}}),
            sig,
        )
    }
    #[test]
    fn native_event_amounts_and_identical_fills_preserve_instruction_identity() {
        let raw = raw(feed::key::<32>(WRAPPED_SOL).unwrap());
        let data = format!("Program data: {}", STANDARD.encode(raw));
        let (value, sig) = tx(vec![
            format!("Program {PUMP_PROGRAM} invoke [1]"),
            data.clone(),
            data,
            format!("Program {PUMP_PROGRAM} success"),
        ]);
        let decoded = decode(&value, &sig).unwrap();
        assert_eq!(decoded.trades.len(), 2);
        assert_eq!(decoded.trades[0].trade.sol_lamports, 123);
        assert_eq!(decoded.trades[1].event_index, 2);
        assert_eq!(decoded.trades[0].trade.token_amount, 456);
    }
    #[test]
    fn other_quotes_spoofed_emitters_and_truncated_logs_never_become_sol_volume() {
        let data = format!("Program data: {}", STANDARD.encode(raw([4; 32])));
        let (value, sig) = tx(vec![
            format!("Program {PUMP_PROGRAM} invoke [1]"),
            data.clone(),
            format!("Program {PUMP_PROGRAM} success"),
        ]);
        let decoded = decode(&value, &sig).unwrap();
        assert!(decoded.trades.is_empty());
        assert_eq!(decoded.unresolved, 1);
        let (value, sig) = tx(vec![
            "Program 11111111111111111111111111111111 invoke [1]".into(),
            data,
            "Program 11111111111111111111111111111111 success".into(),
        ]);
        assert!(decode(&value, &sig).unwrap().trades.is_empty());
        let (value, sig) = tx(vec![
            format!("Program {PUMP_PROGRAM} invoke [1]"),
            "Log truncated".into(),
        ]);
        assert!(decode(&value, &sig).is_err());
    }
    #[test]
    fn null_failed_or_signature_mismatched_rpc_results_are_unresolved() {
        let (mut value, sig) = tx(vec![]);
        value["meta"]["err"] = serde_json::json!({"error":1});
        assert!(decode(&value, &sig).is_err());
        assert!(decode(&Value::Null, &sig).is_err());
        let (value, _) = tx(vec![]);
        assert!(decode(&value, &bs58::encode([9; 64]).into_string()).is_err());
    }
}
