use super::{capture::Launch, store::Store};
use crate::{
    feed::Trade,
    health::{Health, inc, set_state},
    storage::ProcessLock,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{sync::mpsc, task::JoinHandle};
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub enabled: bool,
    pub funding: bool,
    pub max_bytes: u64,
    pub retention_days: u64,
    pub exclusions: Vec<String>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            enabled: false,
            funding: false,
            max_bytes: 512 * 1024 * 1024,
            retention_days: 45,
            exclusions: Vec::new(),
        }
    }
}
impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if !(64 * 1024 * 1024..=2 * 1024 * 1024 * 1024).contains(&self.max_bytes)
            || !(1..=90).contains(&self.retention_days)
            || self.exclusions.len() > 200
            || self
                .exclusions
                .iter()
                .any(|v| crate::feed::key::<32>(v).is_err())
            || self.funding && !self.enabled
        {
            return Err("invalid bundle configuration".into());
        }
        Ok(())
    }
}
pub(crate) enum Input {
    Create(Box<Launch>),
    Trade {
        seq: u64,
        mono: u64,
        wall: u64,
        trade: Trade,
        balance: Option<u64>,
    },
    Clock(u64),
    Gap(Option<[u8; 32]>),
    Forget([u8; 32]),
}
#[derive(Clone)]
pub(crate) struct Handle {
    tx: mpsc::Sender<Input>,
    active: Arc<AtomicBool>,
    lost: Arc<AtomicBool>,
    health: Arc<Health>,
}
impl Handle {
    pub fn submit(&self, input: Input) {
        if !self.active.load(Ordering::Acquire) || self.tx.try_send(input).is_err() {
            self.lost.store(true, Ordering::Release);
            self.active.store(false, Ordering::Release);
            inc(&self.health.bundle_dropped);
        }
    }
}
pub(crate) struct Runtime {
    pub handle: Handle,
    stop: Arc<AtomicBool>,
    task: Option<JoinHandle<Result<(), &'static str>>>,
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
    }
}
impl Runtime {
    pub async fn start(
        root: PathBuf,
        config: Config,
        guard: Arc<ProcessLock>,
        health: Arc<Health>,
    ) -> Result<Self, String> {
        let (tx, rx) = mpsc::channel(1024);
        let active = Arc::new(AtomicBool::new(true));
        let lost = Arc::new(AtomicBool::new(false));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = Handle {
            tx,
            active: active.clone(),
            lost: lost.clone(),
            health: health.clone(),
        };
        let (ready, wait) = tokio::sync::oneshot::channel();
        let shutdown = stop.clone();
        let task = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let mut store = match Store::open(root, config.max_bytes * 3 / 4, config.retention_days)
            {
                Ok(s) => s,
                Err(_) => {
                    let _ = ready.send(false);
                    return Err("bundle storage unavailable");
                }
            };
            let _ = ready.send(true);
            set_state(&health.bundle_state, "recording");
            let result = worker(&mut store, rx, shutdown, lost, health.clone());
            active.store(false, Ordering::Release);
            if result.is_err() {
                let _ = store.loss();
                set_state(&health.bundle_state, "error");
            } else {
                set_state(&health.bundle_state, "sealed");
            }
            result
        });
        let runtime = Self {
            handle,
            stop,
            task: Some(task),
        };
        if !matches!(
            tokio::time::timeout(Duration::from_secs(10), wait).await,
            Ok(Ok(true))
        ) {
            return Err("bundle storage startup failed".into());
        }
        Ok(runtime)
    }
    pub async fn completed(&mut self) {
        if let Some(task) = &mut self.task {
            let _ = task.await;
            self.task = None;
        } else {
            std::future::pending::<()>().await;
        }
    }
    pub async fn shutdown(mut self) -> Result<(), &'static str> {
        self.handle.active.store(false, Ordering::Release);
        self.stop.store(true, Ordering::Release);
        let task = self.task.as_mut().ok_or("bundle worker stopped")?;
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .map_err(|_| "bundle shutdown timed out")?
            .map_err(|_| "bundle worker failed")?
    }
}
fn worker(
    store: &mut Store,
    mut rx: mpsc::Receiver<Input>,
    stop: Arc<AtomicBool>,
    lost: Arc<AtomicBool>,
    health: Arc<Health>,
) -> Result<(), &'static str> {
    let mut rows = BTreeMap::<[u8; 32], Launch>::new();
    let mut dirty = BTreeSet::new();
    let mut last = Instant::now();
    let mut count = 0;
    loop {
        health.pulse(&health.bundle_tick_ms);
        if lost.load(Ordering::Acquire) {
            store.loss().map_err(|_| "bundle loss persistence failed")?;
            for row in rows.values_mut() {
                row.gap();
                dirty.insert(row.mint);
            }
            flush(store, &rows, &mut dirty)?;
            return Err("bundle queue overflow");
        }
        let next = rx.try_recv();
        match next {
            Ok(input) => {
                count += 1;
                match input {
                    Input::Create(row) => {
                        if !rows.contains_key(&row.mint) && !store.contains(&row.mint) {
                            if rows.len() >= 20 {
                                return Err("bundle active capacity reached");
                            }
                            dirty.insert(row.mint);
                            rows.insert(row.mint, *row);
                        }
                    }
                    Input::Trade {
                        seq,
                        mono,
                        wall,
                        trade,
                        balance,
                    } => {
                        if let Some(row) = rows.get_mut(&trade.mint) {
                            row.observe(seq, mono, wall, &trade, balance);
                            dirty.insert(row.mint);
                        }
                    }
                    Input::Clock(now) => {
                        for row in rows.values_mut() {
                            let was = row.closed;
                            row.tick(now);
                            if row.closed != was {
                                dirty.insert(row.mint);
                            }
                        }
                    }
                    Input::Gap(mint) => {
                        for row in rows
                            .values_mut()
                            .filter(|row| mint.is_none_or(|mint| mint == row.mint))
                        {
                            row.gap();
                            dirty.insert(row.mint);
                        }
                    }
                    Input::Forget(mint) => {
                        if let Some(mut row) = rows.remove(&mint) {
                            row.gap();
                            store
                                .write_protected(&row, &rows.keys().copied().collect())
                                .map_err(|_| "bundle snapshot failed")?;
                            dirty.remove(&mint);
                        }
                    }
                }
            }
            Err(mpsc::error::TryRecvError::Empty) => {
                if stop.load(Ordering::Acquire) {
                    for row in rows.values_mut() {
                        row.gap();
                        dirty.insert(row.mint);
                    }
                    flush(store, &rows, &mut dirty)?;
                    return Ok(());
                }
                std::thread::sleep(Duration::from_millis(5));
            }
            Err(mpsc::error::TryRecvError::Disconnected) => {
                for row in rows.values_mut() {
                    row.gap();
                    dirty.insert(row.mint);
                }
                flush(store, &rows, &mut dirty)?;
                return Ok(());
            }
        }
        crate::health::set(&health.bundle_pruned, store.pruned);
        if count >= 64 || last.elapsed() >= Duration::from_millis(250) {
            flush(store, &rows, &mut dirty)?;
            count = 0;
            last = Instant::now();
        }
    }
}
fn flush(
    store: &mut Store,
    rows: &BTreeMap<[u8; 32], Launch>,
    dirty: &mut BTreeSet<[u8; 32]>,
) -> Result<(), &'static str> {
    for key in dirty.iter() {
        store
            .write_protected(&rows[key], &rows.keys().copied().collect())
            .map_err(|_| "bundle snapshot failed")?;
    }
    dirty.clear();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn overflow_latches_capture_loss_and_disables_submissions() {
        let (tx, _rx) = mpsc::channel(1);
        let health = Arc::new(Health::default());
        let handle = Handle {
            tx,
            active: Arc::new(AtomicBool::new(true)),
            lost: Arc::new(AtomicBool::new(false)),
            health: health.clone(),
        };
        handle.submit(Input::Clock(1));
        handle.submit(Input::Clock(2));
        assert!(!handle.active.load(Ordering::Acquire));
        assert!(handle.lost.load(Ordering::Acquire));
        assert_eq!(crate::health::value(&health.bundle_dropped), 1);
        handle.submit(Input::Clock(3));
        assert_eq!(crate::health::value(&health.bundle_dropped), 2);
    }
}
