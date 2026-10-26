use crate::{
    collector::{Pipeline, Secrets, wall_ms},
    config::Config,
    health::{inc, set, set_state, value},
    recovery::{CreateWatchdog, Heartbeat, RetryPolicy},
    spend::Ledger,
};
use futures_util::{SinkExt, StreamExt};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{Instant, timeout};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{Message, protocol::WebSocketConfig},
};
use url::Url;

#[derive(Clone, Copy)]
enum Fault {
    Connect,
    ConnectTimeout,
    Write,
    Closed,
    Transport,
    Protocol,
    Idle,
    Pong,
    Authentication,
    RateLimited,
}
impl Fault {
    fn label(self) -> &'static str {
        match self {
            Self::Connect => "connect",
            Self::ConnectTimeout => "connect_timeout",
            Self::Write => "write",
            Self::Closed => "closed",
            Self::Transport => "transport",
            Self::Protocol => "protocol",
            Self::Idle => "idle_timeout",
            Self::Pong => "pong_timeout",
            Self::Authentication => "authentication",
            Self::RateLimited => "provider_rate_limit",
        }
    }
    fn terminal(self) -> bool {
        matches!(self, Self::Authentication | Self::RateLimited)
    }
}
enum End {
    Budget,
    Window,
    Failure(Fault),
}
struct Exit {
    end: End,
    healthy_ms: u64,
}
impl Exit {
    fn failed(fault: Fault) -> Self {
        Self {
            end: End::Failure(fault),
            healthy_ms: 0,
        }
    }
}
struct Budget {
    ledger: Arc<Mutex<Ledger>>,
    credits: u64,
    credit_day: u64,
}
fn advance_day(credits: &mut u64, credit_day: &mut u64, today: u64) {
    if today > *credit_day {
        *credits = 0;
        *credit_day = today;
    }
}
impl Budget {
    async fn refill(&mut self, pipeline: &mut Pipeline) -> Result<bool, String> {
        let day = wall_ms() / 86_400_000;
        advance_day(&mut self.credits, &mut self.credit_day, day);
        if self.credits > 0 {
            return Ok(true);
        }
        let ledger = self.ledger.clone();
        let ownership = pipeline.ownership.clone();
        let (grant, reserved) = tokio::task::spawn_blocking(move || {
            let _ownership = ownership;
            let mut ledger = ledger.lock().map_err(|_| "spend ledger lock failed")?;
            let grant = ledger
                .reserve(day)
                .map_err(|_| "spend reservation failed")?;
            Ok::<_, String>((grant, ledger.reserved()))
        })
        .await
        .map_err(|_| "spend worker failed")??;
        self.credits = grant;
        set(&pipeline.health.budget_reserved, reserved);
        pipeline.control(
            "spend_reservation",
            serde_json::json!({"grant":grant,"reserved":reserved}),
        );
        Ok(grant > 0)
    }
}

pub(crate) async fn live(
    config: &Config,
    pipeline: &mut Pipeline,
    secrets: &Secrets,
    endpoint: Url,
    paid_requested: bool,
    retry: &mut RetryPolicy,
) -> Result<(), String> {
    let mut budget = if paid_requested {
        match Ledger::open(
            &config.data_dir.join("feed-spend.json"),
            wall_ms() / 86_400_000,
            config.gate.daily_messages,
            config.gate.stop_margin_messages,
            config.gate.reservation_messages,
        ) {
            Ok(ledger) => Some(Budget {
                ledger: Arc::new(Mutex::new(ledger)),
                credits: 0,
                credit_day: wall_ms() / 86_400_000,
            }),
            Err(_) => {
                set_state(&pipeline.health.paid_state, "ledger_unavailable");
                None
            }
        }
    } else {
        None
    };
    if let Some(b) = &mut budget {
        match b.refill(pipeline).await {
            Ok(true) => {}
            Ok(false) => {
                set_state(&pipeline.health.paid_state, "exhausted");
                budget = None;
            }
            Err(_) => {
                set_state(&pipeline.health.paid_state, "ledger_unavailable");
                budget = None;
            }
        }
    }
    let mut paid = budget.is_some();
    loop {
        pipeline.decoder.reset_session();
        pipeline.reset_volume();
        pipeline.generation += 1;
        set(&pipeline.health.generation, pipeline.generation);
        let exit = one(
            config,
            pipeline,
            secrets,
            &endpoint,
            if paid { budget.as_mut() } else { None },
        )
        .await;
        match exit.end {
            End::Failure(fault) => {
                set_state(&pipeline.health.feed_state, "reconnecting");
                set_state(&pipeline.health.transport_state, "failed");
                set_state(&pipeline.health.last_failure, fault.label());
                if matches!(fault, Fault::Pong | Fault::Idle | Fault::ConnectTimeout) {
                    inc(&pipeline.health.transport_timeouts);
                }
                pipeline.decoder.reset_session();
                pipeline.reset_volume();
                pipeline.control(
                    "transport_gap",
                    serde_json::json!({"class":fault.label(),"healed":false}),
                );
                if paid {
                    paid = false;
                    budget = None;
                    set_state(&pipeline.health.paid_state, "interrupted");
                    pipeline.control(
                        "paid_stream_stopped",
                        serde_json::json!({"reason":"interrupted","replacement":"free-only"}),
                    );
                }
                let delay = if fault.terminal() {
                    None
                } else {
                    retry.next(exit.healthy_ms)
                };
                let Some(delay) = delay else {
                    let reason = if fault.terminal() {
                        fault.label()
                    } else if config.recovery.reconnect_attempts == 0 {
                        "disabled"
                    } else {
                        inc(&pipeline.health.recovery_exhausted);
                        "budget_exhausted"
                    };
                    let event = serde_json::json!({"event":"recovery_stopped","reason":reason,"class":fault.label()});
                    pipeline.control("recovery_stopped", event.clone());
                    println!("{event}");
                    set_state(&pipeline.health.feed_state, "failed");
                    return Err("feed connection ended (endpoint and credentials redacted)".into());
                };
                set(&pipeline.health.backoff_ms, delay);
                set_state(&pipeline.health.transport_state, "backoff");
                pipeline.control(
                    "reconnect_scheduled",
                    serde_json::json!({"delay_ms":delay,"subscription":"free-only"}),
                );
                tokio::time::sleep(Duration::from_millis(delay)).await;
                set(&pipeline.health.backoff_ms, 0);
                inc(&pipeline.health.reconnects);
                continue;
            }
            End::Budget => {
                inc(&pipeline.health.budget_stops);
                set_state(&pipeline.health.paid_state, "exhausted");
            }
            End::Window => set_state(&pipeline.health.paid_state, "window_ended"),
        }
        if !paid {
            return Ok(());
        }
        paid = false;
        budget = None;
        pipeline.reset_volume();
        pipeline.control(
            "paid_stream_stopped",
            serde_json::json!({"replacement":"free-only"}),
        );
        // `one` dropped its socket before the next connection can be attempted.
    }
}

async fn one(
    config: &Config,
    pipeline: &mut Pipeline,
    secrets: &Secrets,
    endpoint: &Url,
    mut budget: Option<&mut Budget>,
) -> Exit {
    let mut url = endpoint.clone();
    if let Some(key) = &secrets.api_key {
        url.query_pairs_mut().append_pair("api-key", key);
    }
    let io_timeout = Duration::from_millis(config.feed.connect_timeout_ms);
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(config.feed.max_frame_bytes))
        .max_frame_size(Some(config.feed.max_frame_bytes));
    set_state(&pipeline.health.feed_state, "connecting");
    set_state(&pipeline.health.transport_state, "connecting");
    set_state(&pipeline.health.application_state, "awaiting_create");
    set(&pipeline.health.create_received_ms, 0);
    set(&pipeline.health.create_ewma_ms, 0);
    set(&pipeline.health.create_threshold_ms, 30_000);
    pipeline.control("session_connecting", serde_json::json!({}));
    let mut socket = match timeout(
        io_timeout,
        connect_async_with_config(url.as_str(), Some(ws_config), true),
    )
    .await
    {
        Ok(Ok((socket, _))) => socket,
        Ok(Err(tokio_tungstenite::tungstenite::Error::Http(response))) => {
            return Exit::failed(match response.status().as_u16() {
                401 | 403 => Fault::Authentication,
                429 => Fault::RateLimited,
                _ => Fault::Connect,
            });
        }
        Ok(Err(_)) => return Exit::failed(Fault::Connect),
        Err(_) => return Exit::failed(Fault::ConnectTimeout),
    };
    for method in ["subscribeNewToken", "subscribeMigration"] {
        let command = serde_json::json!({"method":method});
        if !matches!(
            timeout(
                io_timeout,
                socket.send(Message::Text(command.to_string().into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            return Exit::failed(Fault::Write);
        }
        pipeline.control("subscription", command);
    }
    if budget.is_some() {
        let Some(mint) = &config.feed.mint else {
            return Exit::failed(Fault::Protocol);
        };
        let command = serde_json::json!({"method":"subscribeTokenTrade","keys":[mint]});
        if !matches!(
            timeout(
                io_timeout,
                socket.send(Message::Text(command.to_string().into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            return Exit::failed(Fault::Write);
        }
        pipeline.control("subscription", command);
        set_state(&pipeline.health.paid_state, "observing_one_mint");
    }
    set(&pipeline.health.backoff_ms, 0);
    set_state(&pipeline.health.feed_state, "connected");
    set_state(&pipeline.health.transport_state, "connected");
    pipeline.control("session_open", serde_json::json!({"paid":budget.is_some()}));
    let opened = Instant::now();
    let expires = opened + Duration::from_secs(config.feed.paid_window_s);
    let mut last_receive = opened;
    let mut healthy_since = opened;
    let mut heartbeat = Heartbeat::default();
    let mut app = CreateWatchdog::default();
    let mut ping = tokio::time::interval_at(
        opened + Duration::from_millis(config.recovery.ping_interval_ms),
        Duration::from_millis(config.recovery.ping_interval_ms),
    );
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let end = loop {
        if let Some(b) = &mut budget
            && !matches!(b.refill(pipeline).await, Ok(true))
        {
            break End::Budget;
        }
        let pong_deadline = heartbeat
            .deadline()
            .map(|ms| opened + Duration::from_millis(ms));
        tokio::select! {biased;
            _=tokio::time::sleep_until(pong_deadline.unwrap_or(opened)),if pong_deadline.is_some()=>break End::Failure(Fault::Pong),
            _=tokio::time::sleep_until(last_receive+Duration::from_millis(config.recovery.transport_idle_ms))=>break End::Failure(Fault::Idle),
            _=tokio::time::sleep_until(expires),if budget.is_some()=>break End::Window,
            _=tick.tick()=>{
                if observe_application(&mut app,pipeline,elapsed_ms(opened)) {healthy_since=Instant::now();}
                pipeline.tick_volume();
            },
            _=ping.tick()=>{
                if heartbeat.deadline().is_none() {
                    let payload=heartbeat.sent(elapsed_ms(opened),config.recovery.pong_timeout_ms);
                    let until=opened+Duration::from_millis(heartbeat.deadline().expect("just created ping deadline"));
                    if !matches!(tokio::time::timeout_at(until.min(Instant::now()+io_timeout),socket.send(Message::Ping(payload.to_vec().into()))).await,Ok(Ok(()))) {
                        break End::Failure(Fault::Write);
                    }
                }
            },
            frame=socket.next()=>{
                if frame.as_ref().is_some_and(Result::is_ok) {
                    last_receive=Instant::now();set(&pipeline.health.transport_received_ms,wall_ms());
                }
                match frame {
                    Some(Ok(message @ (Message::Text(_)|Message::Binary(_))))=>{
                        let bytes=message.into_data();
                        if budget.is_some() && billable(&bytes) {
                            inc(&pipeline.health.billable_received);
                            if let Some(b)=&mut budget {b.credits=b.credits.saturating_sub(1);}
                        }
                        let creates=value(&pipeline.health.creates);
                        pipeline.ingest(bytes);
                        if value(&pipeline.health.creates)>creates {
                            let first=!app.seen();
                            let recovered=app.observe(elapsed_ms(opened));
                            if recovered {inc(&pipeline.health.app_recoveries);}
                            if first || recovered {healthy_since=Instant::now();}
                            set(&pipeline.health.create_received_ms,wall_ms());
                            observe_application(&mut app,pipeline,elapsed_ms(opened));
                        }
                    },
                    Some(Ok(Message::Ping(_)))=>{
                        let until=heartbeat.deadline().map(|ms|opened+Duration::from_millis(ms)).unwrap_or(Instant::now()+io_timeout);
                        if !matches!(tokio::time::timeout_at(until.min(Instant::now()+io_timeout),socket.flush()).await,Ok(Ok(()))) {break End::Failure(Fault::Write);}
                    },
                    Some(Ok(Message::Pong(bytes)))=>{
                        if heartbeat.pong(&bytes,elapsed_ms(opened)) {
                            inc(&pipeline.health.matching_pongs);set(&pipeline.health.last_pong_ms,wall_ms());
                        }else{inc(&pipeline.health.wrong_pongs);}
                    },
                    Some(Ok(Message::Close(_)))|None=>break End::Failure(Fault::Closed),
                    Some(Ok(_))=>break End::Failure(Fault::Protocol),
                    Some(Err(_))=>break End::Failure(Fault::Transport),
                }
            },
        }
    };
    let healthy_ms = if app.seen() && !app.check(elapsed_ms(opened)) {
        elapsed_ms(healthy_since)
    } else {
        0
    };
    set_state(&pipeline.health.feed_state, "closing");
    let _ = timeout(Duration::from_secs(1), socket.close(None)).await;
    pipeline.control("session_close",serde_json::json!({"reason":match end {End::Budget=>"budget",End::Window=>"window",End::Failure(fault)=>fault.label()}}));
    Exit { end, healthy_ms }
}
fn elapsed_ms(opened: Instant) -> u64 {
    opened.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}
fn observe_application(app: &mut CreateWatchdog, pipeline: &mut Pipeline, now: u64) -> bool {
    let suspected = app.check(now);
    let was = pipeline
        .health
        .application_state
        .lock()
        .is_ok_and(|state| *state == "suspected_unprobed");
    if suspected && !was {
        inc(&pipeline.health.app_suspicions);
        pipeline.control(
            "application_liveness",
            serde_json::json!({"state":"suspected_unprobed","action":"suppress_only"}),
        );
    }
    set_state(
        &pipeline.health.application_state,
        if suspected {
            "suspected_unprobed"
        } else if app.seen() {
            "observed"
        } else {
            "awaiting_create"
        },
    );
    set_state(
        &pipeline.health.feed_state,
        if suspected { "suspected" } else { "connected" },
    );
    set(&pipeline.health.create_ewma_ms, app.ewma());
    set(&pipeline.health.create_threshold_ms, app.threshold());
    suspected
}

fn billable(bytes: &[u8]) -> bool {
    let value: Result<serde_json::Value, _> = serde_json::from_slice(bytes);
    !matches!(
        value
            .ok()
            .and_then(|v| v["txType"].as_str().map(str::to_owned))
            .as_deref(),
        Some("create" | "migration")
    )
}

#[cfg(test)]
mod tests;
