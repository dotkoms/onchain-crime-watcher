fn main() {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("--check-config") {
        println!("Usage: ocw --check-config <config.toml>");
        return;
    }
    let result = args
        .next()
        .ok_or("config path required".to_string())
        .and_then(|path| {
            std::fs::read_to_string(path).map_err(|_| "cannot read configuration".to_string())
        })
        .and_then(|text| ocw_core::config::Config::parse(&text));
    match result {
        Ok(config) => println!(
            "{}",
            serde_json::to_string(&config).expect("config is serializable")
        ),
        Err(error) => {
            eprintln!("ocw: {error}");
            std::process::exit(2);
        }
    }
}
