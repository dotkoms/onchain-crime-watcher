//! Bounded read-only Solana JSON-RPC. Every request reserves a durable call before HTTP.
use crate::{
    collector::wall_ms,
    discord, feed,
    spend::Ledger,
    storage::{ProcessLock, atomic_write},
};
use serde_json::{Value, json};
use std::{
    collections::VecDeque,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::Semaphore;
use url::Url;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    Budget,
    RateLimited,
    Unavailable,
    Invalid,
    Storage,
}
#[derive(Clone, Copy)]
pub enum Method {
    Signatures,
    Transaction,
    Block,
    MultipleAccounts,
    TokenAccounts,
    AccountInfo,
}
impl Method {
    fn name(self) -> &'static str {
        match self {
            Self::Signatures => "getSignaturesForAddress",
            Self::Transaction => "getTransaction",
            Self::Block => "getBlock",
            Self::MultipleAccounts => "getMultipleAccounts",
            Self::TokenAccounts => "getTokenAccountsByOwner",
            Self::AccountInfo => "getAccountInfo",
        }
    }
}
struct Budget {
    minute: VecDeque<Instant>,
    paused: Option<Instant>,
}
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RateState {
    schema: u8,
    times: VecDeque<u64>,
    paused_until: u64,
}
struct Rate {
    path: std::path::PathBuf,
    state: RateState,
    limit: usize,
}
impl Rate {
    fn open(path: std::path::PathBuf, limit: usize) -> Result<Self, Failure> {
        let state = if path.exists() {
            let meta = std::fs::symlink_metadata(&path).map_err(|_| Failure::Storage)?;
            if !meta.is_file() || meta.len() > 64 * 1024 {
                return Err(Failure::Storage);
            }
            serde_json::from_slice::<RateState>(
                &std::fs::read(&path).map_err(|_| Failure::Storage)?,
            )
            .map_err(|_| Failure::Storage)?
        } else {
            RateState {
                schema: 1,
                times: VecDeque::new(),
                paused_until: 0,
            }
        };
        if state.schema != 1
            || state.times.len() > 600
            || state
                .times
                .iter()
                .zip(state.times.iter().skip(1))
                .any(|(a, b)| a > b)
        {
            return Err(Failure::Storage);
        }
        Ok(Self { path, state, limit })
    }
    fn save(&self) -> Result<(), Failure> {
        atomic_write(
            &self.path,
            &serde_json::to_vec(&self.state).map_err(|_| Failure::Storage)?,
        )
        .map_err(|_| Failure::Storage)
    }
    fn reserve(&mut self, now: u64) -> Result<(), Failure> {
        if now < self.state.paused_until {
            return Err(Failure::RateLimited);
        }
        if self.state.times.back().is_some_and(|last| now < *last) {
            return Err(Failure::Budget);
        }
        while self
            .state
            .times
            .front()
            .is_some_and(|at| now - *at >= 60_000)
        {
            self.state.times.pop_front();
        }
        if self.state.times.len() >= self.limit {
            return Err(Failure::Budget);
        }
        self.state.times.push_back(now);
        self.save()
    }
    fn pause(&mut self, now: u64, seconds: u64) -> Result<(), Failure> {
        self.state.paused_until = self
            .state
            .paused_until
            .max(now.saturating_add(seconds.saturating_mul(1000)));
        self.save()
    }
}
#[derive(Clone)]
pub struct Client {
    client: reqwest::Client,
    url: Url,
    ledger: Arc<Mutex<Ledger>>,
    rate: Arc<Mutex<Rate>>,
    call_lock: Arc<ProcessLock>,
    budget: Arc<Mutex<Budget>>,
    permits: Arc<Semaphore>,
    guard: Arc<ProcessLock>,
    counter: Arc<AtomicU64>,
    per_minute: usize,
}
impl Client {
    pub async fn open(
        raw: &str,
        data: &Path,
        guard: Arc<ProcessLock>,
        daily: u64,
        per_minute: usize,
    ) -> Result<Self, Failure> {
        let url = Url::parse(raw).map_err(|_| Failure::Invalid)?;
        if raw.len() > 2048
            || url.scheme() != "https"
            || url.host_str().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || !(1..=50_000).contains(&daily)
            || !(1..=600).contains(&per_minute)
        {
            return Err(Failure::Invalid);
        }
        let root = data.join("rpc");
        let call_lock = Arc::new(ProcessLock::acquire(&root).map_err(|_| Failure::Storage)?);
        let path = root.join("spend.json");
        let rate_path = root.join("rate.json");
        let opening_lock = call_lock.clone();
        let ownership = guard.clone();
        let ledger = tokio::task::spawn_blocking(move || {
            let _ownership = ownership;
            let _call_lock = opening_lock;
            let day = wall_ms() / 86_400_000;
            if !path.exists() {
                Ledger::initialize(&path, day).map_err(|_| Failure::Storage)?;
            }
            Ledger::open(&path, day, daily, 0, 1).map_err(|_| Failure::Storage)
        })
        .await
        .map_err(|_| Failure::Storage)??;
        Ok(Self {
            client: discord::client(Duration::from_secs(2)).map_err(|_| Failure::Unavailable)?,
            url,
            ledger: Arc::new(Mutex::new(ledger)),
            rate: Arc::new(Mutex::new(Rate::open(rate_path, per_minute)?)),
            call_lock,
            budget: Arc::new(Mutex::new(Budget {
                minute: VecDeque::new(),
                paused: None,
            })),
            permits: Arc::new(Semaphore::new(8)),
            guard,
            counter: Arc::new(AtomicU64::new(1)),
            per_minute,
        })
    }
    pub async fn call(&self, method: Method, params: Value) -> Result<Value, Failure> {
        if serde_json::to_vec(&params)
            .map_err(|_| Failure::Invalid)?
            .len()
            > 32 * 1024
        {
            return Err(Failure::Invalid);
        }
        let _permit = tokio::time::timeout(Duration::from_secs(2), self.permits.acquire())
            .await
            .map_err(|_| Failure::Budget)?
            .map_err(|_| Failure::Unavailable)?;
        let now = Instant::now();
        {
            let mut budget = self.budget.lock().map_err(|_| Failure::Storage)?;
            if budget.paused.is_some_and(|until| now < until) {
                return Err(Failure::RateLimited);
            }
            while budget
                .minute
                .front()
                .is_some_and(|at| now.duration_since(*at) >= Duration::from_secs(60))
            {
                budget.minute.pop_front();
            }
            if budget.minute.len() >= self.per_minute {
                return Err(Failure::Budget);
            }
            budget.minute.push_back(now);
        }
        let ledger = self.ledger.clone();
        let rate = self.rate.clone();
        let call_lock = self.call_lock.clone();
        let guard = self.guard.clone();
        let granted = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let _call_lock = call_lock;
            rate.lock()
                .map_err(|_| Failure::Storage)?
                .reserve(wall_ms())?;
            ledger
                .lock()
                .map_err(|_| Failure::Storage)?
                .reserve(wall_ms() / 86_400_000)
                .map_err(|_| Failure::Storage)
        })
        .await
        .map_err(|_| Failure::Storage)??;
        if granted == 0 {
            return Err(Failure::Budget);
        }
        let id = self.counter.fetch_add(1, Ordering::Relaxed);
        let mut response = self
            .client
            .post(self.url.clone())
            .json(&json!({"jsonrpc":"2.0","id":id,"method":method.name(),"params":params}))
            .send()
            .await
            .map_err(|_| Failure::Unavailable)?;
        if response.status().as_u16() == 429 {
            let delay = response
                .headers()
                .get("retry-after")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .filter(|n| *n <= 86400)
                .unwrap_or(60)
                .max(1);
            self.budget.lock().map_err(|_| Failure::Storage)?.paused =
                Some(Instant::now() + Duration::from_secs(delay));
            let rate = self.rate.clone();
            let guard = self.guard.clone();
            let call_lock = self.call_lock.clone();
            tokio::task::spawn_blocking(move || {
                let _guard = guard;
                let _call_lock = call_lock;
                rate.lock()
                    .map_err(|_| Failure::Storage)?
                    .pause(wall_ms(), delay)
            })
            .await
            .map_err(|_| Failure::Storage)??;
            return Err(Failure::RateLimited);
        }
        if !response.status().is_success() {
            return Err(Failure::Unavailable);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| Failure::Unavailable)? {
            if bytes.len() + chunk.len() > 2 * 1024 * 1024 {
                return Err(Failure::Invalid);
            }
            bytes.extend(chunk);
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| Failure::Invalid)?;
        if value["jsonrpc"] != "2.0"
            || value["id"].as_u64() != Some(id)
            || value.get("error").is_some()
            || value.get("result").is_none()
        {
            return Err(Failure::Invalid);
        }
        Ok(value["result"].clone())
    }
    pub async fn signatures(
        &self,
        address: &str,
        before: Option<&str>,
        limit: usize,
    ) -> Result<Value, Failure> {
        feed::key::<32>(address).map_err(|_| Failure::Invalid)?;
        if !(1..=1000).contains(&limit) || before.is_some_and(|v| feed::key::<64>(v).is_err()) {
            return Err(Failure::Invalid);
        }
        let mut config = json!({"commitment":"confirmed","limit":limit});
        if let Some(before) = before {
            config["before"] = json!(before);
        }
        self.call(Method::Signatures, json!([address, config]))
            .await
    }
    pub async fn transaction(&self, signature: &str) -> Result<Value, Failure> {
        feed::key::<64>(signature).map_err(|_| Failure::Invalid)?;
        self.call(Method::Transaction,json!([signature,{"commitment":"confirmed","encoding":"jsonParsed","maxSupportedTransactionVersion":0}])).await
    }
}
#[cfg(test)]
mod tests;
