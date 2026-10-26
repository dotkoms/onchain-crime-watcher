use super::Journal;
use crate::{
    collector::wall_ms,
    health::{Health, inc, set, set_state},
    storage::ProcessLock,
};
use serde_json::{Value, json};
use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinHandle,
};
struct Item {
    wall: u64,
    kind: String,
    data: Value,
}
#[derive(Clone)]
pub(crate) struct Handle {
    tx: mpsc::Sender<Item>,
    active: Arc<AtomicBool>,
    lost: Arc<AtomicU64>,
    health: Arc<Health>,
}
impl Handle {
    pub fn record(&self, kind: &str, data: Value) {
        if !self.active.load(Ordering::Acquire) {
            inc(&self.health.history_dropped);
            return;
        }
        let now = wall_ms();
        if self
            .tx
            .try_send(Item {
                wall: now,
                kind: kind.into(),
                data,
            })
            .is_err()
        {
            self.lost
                .compare_exchange(0, now, Ordering::AcqRel, Ordering::Relaxed)
                .ok();
            self.active.store(false, Ordering::Release);
            inc(&self.health.history_dropped);
            set_state(&self.health.history_state, "overflow");
        }
    }
}
pub(crate) struct Runtime {
    pub handle: Handle,
    stop: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}
impl Runtime {
    pub async fn start(
        root: PathBuf,
        boot: String,
        cap: u64,
        health: Arc<Health>,
        ownership: Arc<ProcessLock>,
    ) -> Result<Self, String> {
        let (tx, mut rx) = mpsc::channel::<Item>(1024);
        let active = Arc::new(AtomicBool::new(true));
        let lost = Arc::new(AtomicU64::new(0));
        let handle = Handle {
            tx,
            active: active.clone(),
            lost: lost.clone(),
            health: health.clone(),
        };
        let (stop, stopped) = watch::channel(false);
        let (ready, started) = tokio::sync::oneshot::channel();
        let task = tokio::task::spawn_blocking(move || {
            let _ownership = ownership;
            let mut journal = match Journal::open(&root, cap, &boot, wall_ms()) {
                Ok(journal) => journal,
                Err(_) => {
                    let _ = ready.send(false);
                    active.store(false, Ordering::Release);
                    set_state(&health.history_state, "error");
                    return;
                }
            };
            set_state(&health.history_state, "recording");
            health.pulse(&health.history_tick_ms);
            if ready.send(true).is_err() {
                return;
            }
            let mut last = Instant::now();
            let result = (|| -> std::io::Result<()> {
                loop {
                    match rx.try_recv() {
                        Ok(item) => {
                            let seq = match journal.append(item.wall, &item.kind, item.data) {
                                Ok(seq) => seq,
                                Err(error) => {
                                    inc(&health.history_dropped);
                                    return Err(error);
                                }
                            };
                            set(&health.history_written, seq);
                        }
                        Err(mpsc::error::TryRecvError::Empty) => {
                            let missing = lost.load(Ordering::Acquire);
                            if missing > 0 {
                                journal.append(wall_ms(),"coverage_gap",json!({"from_wall_ms":missing,"until":"unknown","reason":"queue_loss","complete":false}))?;
                                journal.checkpoint(
                                    json!({"coverage":"partial","reason":"queue_loss"}),
                                )?;
                                return Err(std::io::Error::other("history queue loss"));
                            }
                            if *stopped.borrow() {
                                break;
                            }
                            std::thread::park_timeout(Duration::from_millis(10));
                        }
                        Err(mpsc::error::TryRecvError::Disconnected) => break,
                    }
                    if last.elapsed() >= Duration::from_secs(1) {
                        journal.checkpoint(json!({"coverage":"observed_subset_unverified"}))?;
                        set(&health.history_durable, journal.sequence());
                        health.pulse(&health.history_tick_ms);
                        last = Instant::now();
                    }
                }
                journal.append(
                    wall_ms(),
                    "shutdown",
                    json!({"covered_until_wall_ms":wall_ms(),"chain_completeness":"unverified"}),
                )?;
                journal
                    .checkpoint(json!({"closed":true,"coverage":"observed_subset_unverified"}))?;
                set(&health.history_durable, journal.sequence());
                health.pulse(&health.history_tick_ms);
                Ok(())
            })();
            active.store(false, Ordering::Release);
            if result.is_err() {
                while rx.try_recv().is_ok() {
                    inc(&health.history_dropped);
                }
                let _ = journal.checkpoint(json!({"coverage":"partial","reason":"writer_failure"}));
            }
            set_state(
                &health.history_state,
                if result.is_ok() { "sealed" } else { "error" },
            );
        });
        let runtime = Self {
            handle,
            stop,
            task: Some(task),
        };
        match tokio::time::timeout(Duration::from_secs(10), started).await {
            Ok(Ok(true)) => Ok(runtime),
            _ => Err("history journal unavailable; refusing incomplete dependent state".into()),
        }
    }
    pub async fn completed(&mut self) {
        let Some(task) = &mut self.task else {
            return std::future::pending().await;
        };
        if task.await.is_err() {
            set_state(&self.handle.health.history_state, "error");
        }
        self.task = None;
    }
    pub async fn shutdown(mut self) -> Result<(), &'static str> {
        self.handle.active.store(false, Ordering::Release);
        let _ = self.stop.send(true);
        let Some(task) = &mut self.task else {
            return Err("history worker stopped unexpectedly");
        };
        match tokio::time::timeout(Duration::from_secs(5), task).await {
            Ok(Ok(())) if self.handle.health.snapshot()["history"]["state"] == "sealed" => Ok(()),
            _ => Err("history flush incomplete"),
        }
    }
}
impl Drop for Runtime {
    fn drop(&mut self) {
        self.handle.active.store(false, Ordering::Release);
        let _ = self.stop.send(true);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn full_history_queue_fails_the_family_instead_of_silently_dropping_a_delta() {
        let health = Arc::new(Health::default());
        let (tx, _rx) = mpsc::channel(1);
        let handle = Handle {
            tx,
            active: Arc::new(AtomicBool::new(true)),
            lost: Arc::new(AtomicU64::new(0)),
            health: health.clone(),
        };
        handle.record("observation", json!({"type":"trade"}));
        handle.record("observation", json!({"type":"trade"}));
        handle.record("observation", json!({"type":"trade"}));
        assert!(!handle.active.load(Ordering::Acquire));
        assert!(handle.lost.load(Ordering::Acquire) > 0);
        assert_eq!(health.snapshot()["history"]["dropped"], 2);
        assert_eq!(health.snapshot()["history"]["state"], "overflow");
    }
    #[tokio::test]
    async fn history_worker_seals_real_queue_and_releases_directory_ownership() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let guard = Arc::new(ProcessLock::acquire(&data).unwrap());
        let health = Arc::new(Health::default());
        let runtime = Runtime::start(
            data.join("history"),
            "boot1".into(),
            16 * 1024 * 1024,
            health.clone(),
            guard.clone(),
        )
        .await
        .unwrap();
        drop(guard);
        runtime.handle.record(
            "observation",
            json!({"type":"create","mint":"11111111111111111111111111111111"}),
        );
        runtime.shutdown().await.unwrap();
        assert!(ProcessLock::acquire(&data).is_ok());
        let mut entries = Vec::new();
        super::super::read(
            &data.join("history/observations.ocwh"),
            16 * 1024 * 1024,
            |entry| {
                entries.push(entry);
                Ok(())
            },
        )
        .unwrap();
        assert_eq!(
            entries
                .iter()
                .filter(|entry| entry.kind == "observation")
                .count(),
            1
        );
        assert_eq!(entries.last().unwrap().kind, "shutdown");
        assert_eq!(health.snapshot()["history"]["state"], "sealed");
    }
    #[tokio::test]
    async fn dropping_history_runtime_requests_stop_without_releasing_an_active_writer_early() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        let guard = Arc::new(ProcessLock::acquire(&data).unwrap());
        let health = Arc::new(Health::default());
        let runtime = Runtime::start(
            data.join("history"),
            "boot1".into(),
            16 * 1024 * 1024,
            health.clone(),
            guard.clone(),
        )
        .await
        .unwrap();
        drop(guard);
        drop(runtime);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if ProcessLock::acquire(&data).is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(health.snapshot()["history"]["state"], "sealed");
    }
}
