use super::{
    http::{Http, Outcome},
    policy::Queue,
    store::Journal,
};
use crate::{
    collector::wall_ms,
    discord::Pause,
    health::{Health, inc, set_state, value},
};
use std::{
    sync::{Arc, Mutex, atomic::Ordering},
    time::{Duration, Instant},
};
use tokio::sync::{Notify, watch};

struct Cleanup {
    queue: Arc<Mutex<Queue>>,
    health: Arc<Health>,
}
impl Drop for Cleanup {
    fn drop(&mut self) {
        if let Ok(mut queue) = self.queue.lock() {
            self.health
                .delivery_dropped
                .fetch_add(queue.clear() as u64, Ordering::Relaxed);
        }
    }
}
pub(super) async fn run(
    http: Http,
    mut journal: Journal,
    queue: Arc<Mutex<Queue>>,
    notify: Arc<Notify>,
    mut stop: watch::Receiver<bool>,
    health: Arc<Health>,
    global: Arc<Pause>,
) {
    let _cleanup = Cleanup {
        queue: queue.clone(),
        health: health.clone(),
    };
    let mut paused = Instant::now();
    loop {
        if *stop.borrow() {
            break;
        }
        let now = Instant::now();
        let generation = value(&health.generation);
        let rate_paused = now < paused || !global.remaining().is_zero();
        set_state(
            &health.delivery_state,
            if rate_paused {
                "rate_limited"
            } else if !health.feed_connected() {
                "waiting_for_feed"
            } else {
                "ready"
            },
        );
        let next = {
            let Ok(mut q) = queue.lock() else {
                set_state(&health.delivery_state, "queue_error");
                return;
            };
            health
                .delivery_expired
                .fetch_add(q.prune(now, generation) as u64, Ordering::Relaxed);
            if now >= paused && global.remaining().is_zero() && health.feed_connected() {
                q.take(now, generation)
            } else {
                None
            }
        };
        let Some(mut candidate) = next else {
            tokio::select! {
                _=stop.changed()=>{},_ = notify.notified()=>{},
                _=tokio::time::sleep(Duration::from_millis(100))=>{},
            }
            continue;
        };
        if !candidate.reserved {
            if journal.contains(&candidate.id) {
                inc(&health.delivery_duplicates);
                continue;
            }
            match journal.reserve(&candidate.id, candidate.tier, wall_ms()) {
                Ok(true) => candidate.reserved = true,
                Ok(false) => {
                    inc(&health.delivery_budget);
                    continue;
                }
                Err(_) => {
                    set_state(&health.delivery_state, "journal_error");
                    return;
                }
            }
        }
        let now = Instant::now();
        if now >= candidate.expires
            || candidate.generation != value(&health.generation)
            || !health.feed_connected()
        {
            inc(&health.delivery_expired);
            if journal.finish(&candidate.id, None, false).is_err() {
                set_state(&health.delivery_state, "journal_error");
                return;
            }
            continue;
        }
        let Some(payload) = candidate.payload(now) else {
            inc(&health.delivery_rejected);
            if journal.finish(&candidate.id, None, false).is_err() {
                set_state(&health.delivery_state, "journal_error");
                return;
            }
            continue;
        };
        inc(&health.delivery_attempted);
        candidate.attempts += 1;
        let report = tokio::select! {biased;
            _=stop.changed()=>{
                inc(&health.delivery_unknown);
                let result=journal.finish(&candidate.id,None,true);
                set_state(&health.delivery_state,if result.is_err(){"journal_error"}else{"stopped"});
                return;
            },
            report=http.post(&payload,candidate.expires)=>report,
        };
        paused = Instant::now()
            .checked_add(report.pause)
            .unwrap_or(candidate.expires);
        let (message, unknown) = match report.outcome {
            Outcome::Sent(id) => {
                inc(&health.delivery_sent);
                (Some(id), false)
            }
            Outcome::Unknown => {
                inc(&health.delivery_unknown);
                (None, true)
            }
            Outcome::Rejected => {
                inc(&health.delivery_rejected);
                (None, false)
            }
            Outcome::WrongTarget => {
                inc(&health.delivery_unknown);
                let result = journal.finish(&candidate.id, None, true);
                set_state(
                    &health.delivery_state,
                    if result.is_err() {
                        "journal_error"
                    } else {
                        "target_changed"
                    },
                );
                return;
            }
            Outcome::Broken => {
                inc(&health.delivery_rejected);
                let result = journal.finish(&candidate.id, None, false);
                set_state(
                    &health.delivery_state,
                    if result.is_err() {
                        "journal_error"
                    } else {
                        "blocked"
                    },
                );
                return;
            }
            Outcome::RateLimited => {
                inc(&health.delivery_limited);
                if candidate.attempts < 3
                    && paused < candidate.expires
                    && candidate.generation == value(&health.generation)
                {
                    candidate.ready = candidate.ready.max(paused);
                    let Ok(mut q) = queue.lock() else {
                        set_state(&health.delivery_state, "queue_error");
                        return;
                    };
                    let replaced = q.replaced;
                    if q.insert(candidate) {
                        health
                            .delivery_dropped
                            .fetch_add(q.replaced - replaced, Ordering::Relaxed);
                        continue;
                    }
                    // A bounded queue may prefer a newer fire while a request is in flight.
                    // The durable reservation remains consumed and cannot be posted again.
                    inc(&health.delivery_dropped);
                    continue;
                }
                inc(&health.delivery_expired);
                (None, false)
            }
        };
        if journal
            .finish(&candidate.id, message.as_deref(), unknown)
            .is_err()
        {
            set_state(&health.delivery_state, "journal_error");
            return;
        }
    }
    set_state(&health.delivery_state, "stopped");
}
