use ocw_core::{
    collector::{self, Options},
    config::Config,
    spend::Ledger,
    storage::ProcessLock,
};
use serde_json::json;
use std::{io::Read, path::PathBuf, time::Duration};

#[derive(Default)]
struct Cli {
    config: Option<PathBuf>,
    fixture: Option<PathBuf>,
    replay: Option<PathBuf>,
    analysis_replay: Option<PathBuf>,
    delivery_replay: Option<PathBuf>,
    linked_replay: Option<(PathBuf, PathBuf)>,
    notification_replay: Option<PathBuf>,
    inspect: Option<PathBuf>,
    live: bool,
    status: bool,
    json: bool,
    preview_volume: bool,
    staging_delivery: bool,
    paid: bool,
    dry: bool,
    check: bool,
    preflight: bool,
    init: bool,
    seconds: Option<u64>,
}
fn parse() -> Result<Option<Cli>, String> {
    let mut cli = Cli::default();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--help" | "-h" => {
                println!(
                    "ocw [--status]                               Waiting-for-data status (default)\nocw --replay-capture <segment.ocwcap> [--json] Recorded real frames and volume previews\nocw --replay-analysis <segment.ocwcap> [--json] Exact recorded analysis steps (new captures)\nocw --replay-delivery <observations.ocwh> [--json] Recorded staging policy and outcomes\nocw --replay-linked <capture.ocwcap> <observations.ocwh> [--json] Paired analysis/delivery verification\nocw --replay-notifications <observations.ocwh> [--json] Recorded CTO/bundle/status/outage receipts\nocw --config <file> --fixture <raw-JSONL>     Recorded frame inspection\nocw --live --config <file> [--run-for-secs N] [--enable-paid] [--preview-volume] [--enable-staging-delivery]\nocw --config <file> --dry | --check-config | --preflight | --init-spend-ledger\nocw --inspect-capture <segment.ocwcap>\nNetworking requires --live. No sample data or generated signals are supplied.\nPaid mode requires feed.mint or feed.auto_watch, OCW_PUMPPORTAL_KEY and an explicitly initialized ledger.\nStaging delivery requires its explicit flag, OCW_DISCORD_STAGING_URL and delivery.staging_channel_id. Production delivery remains disabled."
                );
                return Ok(None);
            }
            "--config" => cli.config = Some(args.next().ok_or("--config requires a path")?.into()),
            "--fixture" => {
                cli.fixture = Some(args.next().ok_or("--fixture requires a path")?.into())
            }
            "--replay-notifications" => {
                cli.notification_replay = Some(
                    args.next()
                        .ok_or("--replay-notifications requires a path")?
                        .into(),
                );
            }
            "--replay-linked" => {
                let capture = args
                    .next()
                    .ok_or("--replay-linked requires capture and history paths")?;
                let history = args
                    .next()
                    .ok_or("--replay-linked requires capture and history paths")?;
                cli.linked_replay = Some((capture.into(), history.into()));
            }
            "--replay-delivery" => {
                cli.delivery_replay = Some(
                    args.next()
                        .ok_or("--replay-delivery requires a path")?
                        .into(),
                );
            }
            "--replay-analysis" => {
                cli.analysis_replay = Some(
                    args.next()
                        .ok_or("--replay-analysis requires a path")?
                        .into(),
                )
            }
            "--replay-capture" => {
                cli.replay = Some(
                    args.next()
                        .ok_or("--replay-capture requires a path")?
                        .into(),
                )
            }
            "--live" => cli.live = true,
            "--status" => cli.status = true,
            "--preview-volume" => cli.preview_volume = true,
            "--enable-staging-delivery" => cli.staging_delivery = true,
            "--json" => cli.json = true,
            "--inspect-capture" => {
                cli.inspect = Some(
                    args.next()
                        .ok_or("--inspect-capture requires a path")?
                        .into(),
                )
            }
            "--run-for-secs" => {
                let n = args
                    .next()
                    .ok_or("--run-for-secs requires a number")?
                    .parse::<u64>()
                    .map_err(|_| "invalid duration")?;
                if !(1..=86_400).contains(&n) {
                    return Err("duration must be 1..86400 seconds".into());
                }
                cli.seconds = Some(n);
            }
            "--enable-paid" => cli.paid = true,
            "--dry" => cli.dry = true,
            "--check-config" => cli.check = true,
            "--preflight" => cli.preflight = true,
            "--init-spend-ledger" => cli.init = true,
            _ => return Err("unknown argument; use --help (argument values redacted)".into()),
        }
    }
    let modes = [
        cli.dry,
        cli.check,
        cli.preflight,
        cli.init,
        cli.inspect.is_some(),
        cli.live,
        cli.status,
        cli.fixture.is_some(),
        cli.replay.is_some(),
        cli.analysis_replay.is_some(),
        cli.delivery_replay.is_some(),
        cli.linked_replay.is_some(),
        cli.notification_replay.is_some(),
    ]
    .into_iter()
    .filter(|b| *b)
    .count();
    if modes > 1 {
        return Err("choose one collector/inspection mode".into());
    }
    if cli.paid && !cli.live && !cli.dry && !cli.preflight {
        return Err(
            "--enable-paid requires explicit --live (or --dry for a subscription preview)".into(),
        );
    }
    if cli.seconds.is_some() && !cli.live {
        return Err(
            "--run-for-secs requires --live; recorded captures use their recorded receive times"
                .into(),
        );
    }
    if cli.paid && (cli.check || cli.init || cli.inspect.is_some()) {
        return Err("--enable-paid is only valid with --live or --dry".into());
    }
    if modes == 0 {
        cli.status = true;
    }
    if cli.json
        && cli.replay.is_none()
        && cli.analysis_replay.is_none()
        && cli.delivery_replay.is_none()
        && cli.linked_replay.is_none()
        && cli.notification_replay.is_none()
    {
        return Err("--json is only valid for capture replay".into());
    }
    if cli.staging_delivery && !cli.live && !cli.preflight {
        return Err("--enable-staging-delivery requires explicit --live".into());
    }
    if cli.preview_volume && !cli.live {
        return Err(
            "--preview-volume requires --live; capture replay evaluates volume automatically"
                .into(),
        );
    }
    Ok(Some(cli))
}
fn read_config(path: PathBuf) -> Result<Config, String> {
    let f = std::fs::File::open(path)
        .map_err(|_| "cannot open configuration; use --config config/example.toml")?;
    let mut text = String::new();
    f.take(65_537)
        .read_to_string(&mut text)
        .map_err(|_| "cannot read configuration")?;
    if text.len() > 65_536 {
        return Err("configuration too large".into());
    }
    Config::parse(&text)
}
async fn execute(cli: Cli) -> Result<(), String> {
    if let Some(path) = cli.notification_replay {
        if cli.config.is_some() {
            return Err("notification replay uses recorded evidence; omit --config".into());
        }
        let result = ocw_core::notifications::run(&path)?;
        println!("{result}");
        return Ok(());
    }

    if let Some((capture, history)) = cli.linked_replay {
        if cli.config.is_some() {
            return Err("linked replay uses recorded configuration; omit --config".into());
        }
        let result = ocw_core::replay::linked::run(&capture, &history)?;
        println!("{result}");
        return Ok(());
    }

    if let Some(path) = cli.delivery_replay {
        if cli.config.is_some() {
            return Err("delivery replay uses recorded configuration; omit --config".into());
        }
        let result = ocw_core::delivery::replay::run(&path)?;
        println!("{result}");
        return Ok(());
    }

    if cli.analysis_replay.is_some() && cli.config.is_some() {
        return Err("analysis replay uses its recorded configuration; omit --config".into());
    }
    if cli.status || cli.replay.is_some() || cli.analysis_replay.is_some() {
        let analysis = cli.analysis_replay.is_some();
        let config = match cli.config {
            Some(path) => read_config(path)?,
            None => Config::default(),
        };
        let Some(path) = cli.analysis_replay.or(cli.replay) else {
            println!(
                "{}",
                json!({"state":"waiting_for_data","provider_connected":false,
                "trades":0,"last_trade_ms":null,"alerts_sent":0,"signals_enabled":false,"staging_delivery_enabled":false,
                "network":"disabled; requires explicit --live","data":"none; no sample data"})
            );
            return Ok(());
        };
        use std::io::Write;
        let mut out = std::io::BufWriter::new(std::io::stdout().lock());
        let mut emit = |decision: &ocw_core::volume::Decision, now| {
            let preview = decision.card(now);
            if cli.json {
                writeln!(
                    out,
                    "{}",
                    json!({"event":"volume_decision","source":"recorded_capture",
                    "decision":decision,"preview":preview,"staging_payload":ocw_core::delivery::staging_payload(decision,now)})
                )
                .map_err(|_| "cannot write preview")?;
            } else if let Some(card) = preview {
                writeln!(
                    out,
                    "Recorded capture (historical, not live):\n{}",
                    card["content"].as_str().unwrap_or_default()
                )
                .map_err(|_| "cannot write preview")?;
            }
            Ok(())
        };
        let result = if analysis {
            ocw_core::replay::analysis::run(&path, &mut emit)
        } else {
            ocw_core::replay::run(&path, &config.feed, &mut emit)
        }?;
        writeln!(out, "{result}").map_err(|_| "cannot write summary")?;
        out.flush().map_err(|_| "cannot flush preview")?;
        return Ok(());
    }
    if let Some(path) = cli.inspect {
        let mut frames = 0u64;
        let mut records = 0u64;
        let (_, torn) = ocw_core::capture::scan(&path, |r| {
            records += 1;
            if r.kind == "frame" {
                frames += 1;
            }
            Ok(())
        })
        .map_err(|_| "capture invalid, corrupt or unreadable")?;
        println!(
            "{}",
            json!({"records":records,"frames":frames,"torn_tail":torn,"replay_mode":"decoder-only"})
        );
        return Ok(());
    }
    let config = read_config(cli.config.unwrap_or_else(|| "config/example.toml".into()))?;
    if cli.preflight {
        let result = ocw_core::setup::check(&config, cli.paid, cli.staging_delivery, |key| {
            std::env::var(key).ok()
        });
        println!("{result}");
        return if result["configuration_ready"] == true {
            Ok(())
        } else {
            Err("offline preflight found missing or invalid configuration; values redacted".into())
        };
    }
    if cli.check {
        println!("{}", json!({"configuration":"valid","paid_default":false}));
        return Ok(());
    }
    if cli.dry {
        let mut subscriptions = vec![
            json!({"method":"subscribeNewToken"}),
            json!({"method":"subscribeMigration"}),
        ];
        if cli.paid && !config.feed.auto_watch {
            let mint = config
                .feed
                .mint
                .as_ref()
                .ok_or("--enable-paid requires feed.mint")?;
            subscriptions.push(json!({"method":"subscribeTokenTrade","keys":[mint]}));
        }
        println!(
            "{}",
            json!({"mode":"dry","subscriptions":subscriptions,"signals_enabled":false,"paid_requested":cli.paid,"automatic_watch":cli.paid && config.feed.auto_watch,"max_subscriptions":if config.feed.auto_watch {config.watch.max_subs} else {1}})
        );
        return Ok(());
    }
    if cli.init {
        let _lock = ProcessLock::acquire(&config.data_dir)
            .map_err(|_| "cannot acquire private data directory/collector lock")?;
        Ledger::initialize(
            &config.data_dir.join("feed-spend.json"),
            collector::wall_ms() / 86_400_000,
        )
        .map_err(|_| "cannot initialize ledger; an existing ledger is never overwritten")?;
        println!(
            "{}",
            json!({"event":"spend_ledger_initialized","reserved_messages":0})
        );
        return Ok(());
    }
    let options = Options {
        paid: cli.paid,
        preview_volume: cli.preview_volume || cli.staging_delivery,
        staging_delivery: cli.staging_delivery,
        fixture: cli.fixture,
        run_for: cli.seconds.map(Duration::from_secs),
    };
    #[cfg(unix)]
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|_| "cannot install termination handler")?;
    let result = collector::run(config, options, async {
        #[cfg(unix)]
        tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=terminate.recv()=>{} }
        #[cfg(not(unix))]
        let _ = tokio::signal::ctrl_c().await;
    })
    .await?;
    println!("{result}");
    Ok(())
}
pub async fn main() {
    let result = match parse() {
        Ok(Some(cli)) => execute(cli).await,
        Ok(None) => Ok(()),
        Err(e) => Err(e),
    };
    if let Err(error) = result {
        eprintln!("ocw: {error}");
        std::process::exit(2);
    }
}
