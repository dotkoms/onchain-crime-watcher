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
    inspect: Option<PathBuf>,
    paid: bool,
    dry: bool,
    check: bool,
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
                    "ocw --config <file> [--run-for-secs N] [--enable-paid]\nocw --config <file> --fixture <JSONL>\nocw --config <file> --dry | --check-config | --init-spend-ledger\nocw --inspect-capture <segment.ocwcap>\nPaid mode requires one feed.mint, OCW_PUMPPORTAL_KEY and an explicitly initialized ledger.\nM1a has no signal alerts. Health binds only to localhost."
                );
                return Ok(None);
            }
            "--config" => cli.config = Some(args.next().ok_or("--config requires a path")?.into()),
            "--fixture" => {
                cli.fixture = Some(args.next().ok_or("--fixture requires a path")?.into())
            }
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
            "--init-spend-ledger" => cli.init = true,
            _ => return Err("unknown argument; use --help (argument values redacted)".into()),
        }
    }
    let modes = [cli.dry, cli.check, cli.init, cli.inspect.is_some()]
        .into_iter()
        .filter(|b| *b)
        .count();
    if modes > 1 || (modes > 0 && cli.fixture.is_some()) {
        return Err("choose one collector/inspection mode".into());
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
    if cli.check {
        println!("{}", json!({"configuration":"valid","paid_default":false}));
        return Ok(());
    }
    if cli.dry {
        let mut subscriptions = vec![
            json!({"method":"subscribeNewToken"}),
            json!({"method":"subscribeMigration"}),
        ];
        if cli.paid {
            let mint = config
                .feed
                .mint
                .as_ref()
                .ok_or("--enable-paid requires feed.mint")?;
            subscriptions.push(json!({"method":"subscribeTokenTrade","keys":[mint]}));
        }
        println!(
            "{}",
            json!({"mode":"dry","subscriptions":subscriptions,"signals_enabled":false,"paid_requested":cli.paid})
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
        fixture: cli.fixture,
        run_for: cli.seconds.map(Duration::from_secs),
    };
    let result = collector::run(config, options, async {
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
