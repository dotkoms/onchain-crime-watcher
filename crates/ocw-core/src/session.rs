use crate::{
    collector::{Pipeline, Secrets, wall_ms},
    config::Config,
    health::{inc, set, set_state},
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

enum End {
    Budget,
    Window,
    Failure,
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
        let (grant, reserved) = tokio::task::spawn_blocking(move || {
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
        let end = one(
            config,
            pipeline,
            secrets,
            &endpoint,
            if paid { budget.as_mut() } else { None },
        )
        .await;
        match end {
            End::Budget => {
                inc(&pipeline.health.budget_stops);
                set_state(&pipeline.health.paid_state, "exhausted");
            }
            End::Window => set_state(&pipeline.health.paid_state, "window_ended"),
            End::Failure => {
                set_state(&pipeline.health.feed_state, "failed");
                return Err("feed connection ended (endpoint and credentials redacted)".into());
            }
        }
        if !paid {
            return Ok(());
        }
        paid = false;
        pipeline.reset_volume();
        pipeline.control(
            "paid_stream_stopped",
            serde_json::json!({"replacement":"free-only"}),
        );
        // `one` has returned and dropped its socket before this next connection is attempted.
    }
}

async fn one(
    config: &Config,
    pipeline: &mut Pipeline,
    secrets: &Secrets,
    endpoint: &Url,
    mut budget: Option<&mut Budget>,
) -> End {
    let mut url = endpoint.clone();
    if let Some(key) = &secrets.api_key {
        url.query_pairs_mut().append_pair("api-key", key);
    }
    let timeout_ms = Duration::from_millis(config.feed.connect_timeout_ms);
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(config.feed.max_frame_bytes))
        .max_frame_size(Some(config.feed.max_frame_bytes));
    set_state(&pipeline.health.feed_state, "connecting");
    pipeline.control("session_connecting", serde_json::json!({}));
    let Ok(Ok((mut socket, _))) = timeout(
        timeout_ms,
        connect_async_with_config(url.as_str(), Some(ws_config), true),
    )
    .await
    else {
        return End::Failure;
    };
    for method in ["subscribeNewToken", "subscribeMigration"] {
        let command = serde_json::json!({"method":method});
        if !matches!(
            timeout(
                timeout_ms,
                socket.send(Message::Text(command.to_string().into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            return End::Failure;
        }
        pipeline.control("subscription", command);
    }
    if budget.is_some() {
        let Some(mint) = &config.feed.mint else {
            return End::Failure;
        };
        let command = serde_json::json!({"method":"subscribeTokenTrade","keys":[mint]});
        if !matches!(
            timeout(
                timeout_ms,
                socket.send(Message::Text(command.to_string().into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            return End::Failure;
        }
        pipeline.control("subscription", command);
        set_state(&pipeline.health.paid_state, "observing_one_mint");
    }
    set_state(&pipeline.health.feed_state, "connected");
    pipeline.control("session_open", serde_json::json!({"paid":budget.is_some()}));
    let expires = Instant::now() + Duration::from_secs(config.feed.paid_window_s);
    let mut last_receive = Instant::now();
    let mut ping = tokio::time::interval_at(
        Instant::now() + Duration::from_secs(20),
        Duration::from_secs(20),
    );
    ping.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut volume_tick = tokio::time::interval(Duration::from_secs(1));
    volume_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let end = loop {
        if let Some(b) = &mut budget
            && !matches!(b.refill(pipeline).await, Ok(true))
        {
            break End::Budget;
        }
        tokio::select! {
            _=volume_tick.tick()=>pipeline.tick_volume(),
            frame=socket.next()=>{
                last_receive=Instant::now();
                match frame {
                    Some(Ok(message @ (Message::Text(_)|Message::Binary(_))))=>{
                        let bytes=message.into_data();
                        if budget.is_some() && billable(&bytes) {inc(&pipeline.health.billable_received);if let Some(b)=&mut budget {b.credits=b.credits.saturating_sub(1);}}
                        pipeline.ingest(bytes);
                    },
                    Some(Ok(Message::Ping(_)))=>{if socket.flush().await.is_err() {break End::Failure;}},
                    Some(Ok(Message::Pong(_)))=>{},
                    Some(Ok(_))|Some(Err(_))|None=>break End::Failure,
                }
            },
            _=tokio::time::sleep_until(expires),if budget.is_some()=>break End::Window,
            _=ping.tick()=>{if !matches!(timeout(timeout_ms,socket.send(Message::Ping(Default::default()))).await,Ok(Ok(()))) {break End::Failure;}},
            _=tokio::time::sleep_until(last_receive+Duration::from_secs(60))=>break End::Failure,
        }
    };
    let _ = timeout(Duration::from_secs(1), socket.close(None)).await;
    pipeline.control("session_close",serde_json::json!({"reason":match end {End::Budget=>"budget",End::Window=>"window",End::Failure=>"transport"}}));
    end
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
mod tests {
    #[test]
    fn credits_from_yesterday_cannot_cross_into_a_new_daily_allowance() {
        let mut credits = 3;
        let mut day = 10;
        super::advance_day(&mut credits, &mut day, 11);
        assert_eq!(credits, 0);
        assert_eq!(day, 11);
        credits = 2;
        super::advance_day(&mut credits, &mut day, 9);
        assert_eq!(credits, 2);
        assert_eq!(day, 11);
    }
}
