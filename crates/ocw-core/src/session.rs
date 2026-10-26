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
    Application,
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
            Self::Application => "verified_missing_trades",
        }
    }
    fn terminal(self) -> bool {
        matches!(self, Self::Authentication | Self::RateLimited)
    }
}
enum End {
    Budget,
    Window,
    AdmissionCap,
    Failure(Fault),
}
struct Exit {
    end: End,
    healthy_ms: u64,
    cursors: Vec<crate::probe::Sample>,
}
impl Exit {
    fn failed(fault: Fault) -> Self {
        Self {
            end: End::Failure(fault),
            healthy_ms: 0,
            cursors: Vec::new(),
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
    let mut healing: Option<crate::supervision::AbortOnDrop<Vec<crate::heal::Report>>> = None;
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
            &mut healing,
        )
        .await;
        match exit.end {
            End::Failure(fault) => {
                if config.probe.heal_enabled {
                    pipeline.control("coverage_gap",serde_json::json!({"generation":pipeline.generation,"state":"partial","reason":"socket_gap","creates":"unhealed","live_baseline":"cold"}));
                }
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
                    if config.probe.heal_enabled {
                        set_state(&pipeline.health.heal_state, "unhealed_recovery_stopped");
                        pipeline.control(
                            "heal_unavailable",
                            serde_json::json!({"reason":"recovery_stopped","state":"partial"}),
                        );
                    }
                    set_state(&pipeline.health.feed_state, "failed");
                    return Err("feed connection ended (endpoint and credentials redacted)".into());
                };
                if config.probe.heal_enabled {
                    if healing.is_none()
                        && let Some(client) = &pipeline.rpc
                    {
                        let client = client.clone();
                        healing = Some(crate::supervision::AbortOnDrop(tokio::spawn(
                            crate::heal::run(client, exit.cursors, pipeline.generation),
                        )));
                        set_state(&pipeline.health.heal_state, "scanning");
                    } else {
                        pipeline.control(
                            "heal_unavailable",
                            serde_json::json!({"reason":"previous_gap_active","state":"partial"}),
                        );
                    }
                }
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
            End::AdmissionCap => set_state(&pipeline.health.paid_state, "admission_cap"),
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
    healing: &mut Option<crate::supervision::AbortOnDrop<Vec<crate::heal::Report>>>,
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
    let mut planner = if budget.is_some() && config.feed.auto_watch {
        match crate::watch::Planner::new(config.watch.clone()) {
            Ok(planner) => Some(planner),
            Err(_) => return Exit::failed(Fault::Protocol),
        }
    } else {
        None
    };
    if planner.is_some() {
        set_state(&pipeline.health.paid_state, "observing_auto");
    }
    if budget.is_some() && planner.is_none() {
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
    let mut book = crate::probe::Book::default();
    let mut probe_job: Option<crate::supervision::AbortOnDrop<Vec<crate::probe::ResultRow>>> = None;
    let mut next_probe = 0;
    let mut quiet_until = 0;
    set_state(
        &pipeline.health.probe_state,
        if pipeline.rpc.is_some() {
            "waiting"
        } else {
            "disabled"
        },
    );
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
            _=tokio::time::sleep_until(expires),if budget.is_some() && planner.is_none()=>break End::Window,
            result=async {healing.as_mut().expect("guarded heal task").await},if healing.is_some()=>{
                *healing=None;
                match result {
                    Ok(reports)=>record_heals(pipeline,reports),
                    Err(_)=>{
                        set_state(&pipeline.health.heal_state,"failed");
                        pipeline.control("heal_unavailable",serde_json::json!({"reason":"worker_failed","state":"partial"}));
                    }
                }
            },
            result=async {probe_job.as_mut().expect("guarded probe job").await},if probe_job.is_some()=>{
                probe_job = None;
                let outcome = result.ok().map(|rows|crate::probe::classify(&book,&rows)).unwrap_or("unavailable");
                pipeline.control("chain_probe",serde_json::json!({"outcome":outcome,"scope":"three_watched_curves","global_coverage_verified":false}));
                set_state(&pipeline.health.probe_state,outcome);
                if app.check(elapsed_ms(opened)) {
                    if outcome == "missing_trades" {break End::Failure(Fault::Application);}
                    if outcome == "sample_quiet" {quiet_until=pipeline.now_ms().saturating_add(config.probe.interval_s*1000);}
                }
            },
            _=tick.tick()=>{
                if pipeline.now_ms() < quiet_until && app.check(elapsed_ms(opened)) {
                    set_state(&pipeline.health.application_state,"quiet_confirmed");
                    set_state(&pipeline.health.feed_state,"connected");
                } else if observe_application(&mut app,pipeline,elapsed_ms(opened)) {healthy_since=Instant::now();}
                pipeline.health.pulse(&pipeline.health.pipeline_tick_ms);
                if let Some(planner) = &mut planner {
                    let now = pipeline.now_ms();
                    let actions = planner.reconcile(now);
                    if !apply_actions(actions, pipeline, &mut socket, now, io_timeout).await {
                        break End::Failure(Fault::Write);
                    }
                    if planner.exhausted { break End::AdmissionCap; }
                }
                let now = pipeline.now_ms();
                if let Some(planner) = &planner {book.retain(|key|planner.contains(key));}
                if app.check(elapsed_ms(opened)) {
                    if now < quiet_until {
                        set_state(&pipeline.health.application_state,"quiet_confirmed");
                        set_state(&pipeline.health.feed_state,"connected");
                    }
                    if now >= next_probe && probe_job.is_none() && let Some(rpc) = &pipeline.rpc {
                        next_probe = now.saturating_add(config.probe.interval_s*1000);
                        let samples = book.samples(now);
                        if samples.len()==3 {
                            let rpc = rpc.clone();
                            probe_job=Some(crate::supervision::AbortOnDrop(tokio::spawn(async move {crate::probe::run(rpc,samples).await})));
                            set_state(&pipeline.health.probe_state,"running");
                        } else {set_state(&pipeline.health.probe_state,"insufficient_watched_cursors");}
                    }
                }
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
                        if let Some(planner) = &mut planner {
                            if let Some(stamp) = pipeline.receive(&bytes) {
                                let parsed: Option<serde_json::Value> = serde_json::from_slice(&bytes).ok();
                                let kind = parsed.as_ref().and_then(|v|v["txType"].as_str());
                                let mint = parsed.as_ref().and_then(|v|v["mint"].as_str())
                                    .and_then(|s|crate::feed::key::<32>(s).ok());
                                let mut actions = planner.reconcile(stamp.now_ms);
                                if kind == Some("create") && let Some(mint) = mint { actions.extend(planner.create(mint,stamp.now_ms)); }
                                // Membership is decided before decoding; stale frames cannot mutate
                                // reserve caches. Raw frames were already captured above.
                                let active = mint.is_some_and(|key|planner.contains(&key));
                                prepare_actions(&actions,pipeline,stamp.now_ms);
                                if matches!(kind,Some("buy"|"sell")) && !active {
                                    pipeline.discard_received(stamp,"inactive_subscription");
                                } else {
                                    pipeline.ingest_received(bytes.clone(),stamp,active);
                                    if let Some(trade) = &pipeline.last_trade {
                                        planner.trade(&trade.mint,trade.sol_lamports,stamp.now_ms);
                                    }
                                }
                                if !send_actions(actions,pipeline,&mut socket,io_timeout).await {
                                    break End::Failure(Fault::Write);
                                }
                                if planner.exhausted { break End::AdmissionCap; }
                            }
                        } else { pipeline.ingest(bytes.clone()); }
                        if let Some(planner) = &planner {book.retain(|key|planner.contains(key));}
                        if value(&pipeline.health.creates)>creates {
                            let raw: Option<serde_json::Value> = serde_json::from_slice(&bytes).ok();
                            if let Some(raw) = raw {
                                let mint = raw["mint"].as_str().and_then(|s|crate::feed::key::<32>(s).ok());
                                let curve = raw["bondingCurveKey"].as_str().and_then(|s|crate::feed::key::<32>(s).ok());
                                if let (Some(mint),Some(curve)) = (mint,curve) {
                                    let watched = planner.as_ref().map(|p|p.contains(&mint)).unwrap_or_else(||budget.is_some() && config.feed.mint.as_deref().is_some_and(|s|crate::feed::key::<32>(s)==Ok(mint)));
                                    if watched {book.create(mint,curve,pipeline.now_ms());}
                                }
                            }
                            let first=!app.seen();
                            let recovered=app.observe(elapsed_ms(opened));
                            if recovered {inc(&pipeline.health.app_recoveries);}
                            if first || recovered {healthy_since=Instant::now();}
                            set(&pipeline.health.create_received_ms,wall_ms());
                            observe_application(&mut app,pipeline,elapsed_ms(opened));
                        }
                        if let Ok(raw) = serde_json::from_slice::<serde_json::Value>(&bytes)
                            && raw["txType"] == "migration"
                            && let Some(mint) = raw["mint"].as_str().and_then(|s|crate::feed::key::<32>(s).ok()) {book.migrate(&mint);}
                        if let Some(trade) = &pipeline.last_trade {book.trade(trade,pipeline.now_ms());}
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
    pipeline.control("session_close",serde_json::json!({"reason":match end {End::Budget=>"budget",End::Window=>"window",End::AdmissionCap=>"admission_cap",End::Failure(fault)=>fault.label()}}));
    Exit {
        end,
        healthy_ms,
        cursors: book.cursors(),
    }
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

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;
fn prepare_actions(actions: &[crate::watch::Action], pipeline: &mut Pipeline, now: u64) {
    for action in actions {
        match *action {
            crate::watch::Action::Subscribe {
                mint,
                created_ms,
                partial,
            } => pipeline.attach_mint(mint, created_ms, now, partial),
            crate::watch::Action::Unsubscribe(mint) => pipeline.forget_mint(&mint),
        }
    }
}
async fn apply_actions(
    actions: Vec<crate::watch::Action>,
    pipeline: &mut Pipeline,
    socket: &mut Socket,
    now: u64,
    deadline: Duration,
) -> bool {
    prepare_actions(&actions, pipeline, now);
    send_actions(actions, pipeline, socket, deadline).await
}
async fn send_actions(
    actions: Vec<crate::watch::Action>,
    pipeline: &mut Pipeline,
    socket: &mut Socket,
    deadline: Duration,
) -> bool {
    for action in actions {
        let (method, mint) = match action {
            crate::watch::Action::Subscribe {
                mint,
                created_ms,
                partial,
            } => {
                pipeline.control("watch_admitted",serde_json::json!({"mint":bs58::encode(mint).into_string(),"observed_create_ms":created_ms,"partial":partial}));
                ("subscribeTokenTrade", mint)
            }
            crate::watch::Action::Unsubscribe(mint) => ("unsubscribeTokenTrade", mint),
        };
        let command =
            serde_json::json!({"method":method,"keys":[bs58::encode(mint).into_string()]});
        if !matches!(
            timeout(
                deadline,
                socket.send(Message::Text(command.to_string().into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            return false;
        }
        pipeline.control("subscription", command);
    }
    true
}

fn record_heals(pipeline: &mut Pipeline, reports: Vec<crate::heal::Report>) {
    let partial = reports.is_empty()
        || reports
            .iter()
            .any(|r| r.stop != crate::heal::Stop::Complete);
    for report in reports {
        let mint = bs58::encode(report.sample.mint).into_string();
        let anchor = bs58::encode(report.sample.signature).into_string();
        for chunk in report.trades.chunks(16) {
            let events: Vec<_> = chunk.iter().map(|recovered| {
                let event=&recovered.event;
                let trade=&event.trade;
                serde_json::json!({
                    "signature":bs58::encode(trade.signature).into_string(),"event_index":event.event_index,
                    "slot":event.slot,"transaction_index":recovered.transaction_index,"block_time_ms":event.block_time_ms,
                    "trader":bs58::encode(trade.trader).into_string(),"side":if trade.side==crate::feed::Side::Buy {"buy"}else{"sell"},
                    "tokens_raw":trade.token_amount.to_string(),"sol_lamports":trade.sol_lamports.to_string(),
                    "transaction_fee_lamports":event.transaction_fee.to_string(),"fee_payer":bs58::encode(event.transaction_fee_payer).into_string()
                })
            }).collect();
            pipeline.control("healed_chain_batch",serde_json::json!({"source":"independent_rpc","source_generation":report.generation,"mint":mint,"anchor":anchor,"healed":true,"live_overlap":"unresolved","applied_to_detectors":false,"events":events}));
        }
        pipeline.control("curve_heal_result",serde_json::json!({"mint":mint,"anchor":anchor,"source_generation":report.generation,"from_observed_mono_ms":report.sample.observed_ms,"outcome":report.stop.label(),"calls":report.calls,"signatures":report.signatures,"chain_events":report.trades.len(),"global_coverage":"partial","creates":"unhealed","applied_to_detectors":false}));
    }
    set_state(
        &pipeline.health.heal_state,
        if partial {
            "partial"
        } else {
            "curve_scans_complete_global_partial"
        },
    );
}
