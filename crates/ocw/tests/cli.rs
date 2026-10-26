use std::{fs, process::Command};

fn config(dir: &std::path::Path) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    fs::write(
        &path,
        format!(
            "data_dir = {:?}\nhealth_bind = '127.0.0.1:0'\n[capture]\nmin_free_bytes = 0\n",
            dir.join("data").to_str().unwrap()
        ),
    )
    .unwrap();
    path
}

#[test]
fn dry_mode_never_creates_data_or_connects() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let result = Command::new(env!("CARGO_BIN_EXE_ocw"))
        .args(["--config", config.to_str().unwrap(), "--dry"])
        .output()
        .unwrap();
    assert!(result.status.success());
    assert!(!dir.path().join("data").exists());
    let output: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
    assert_eq!(output["mode"], "dry");
    assert_eq!(output["subscriptions"].as_array().unwrap().len(), 2);
}

#[test]
fn fixture_runs_through_capture_decoder_and_health_without_live_credentials() {
    let dir = tempfile::tempdir().unwrap();
    let config = config(dir.path());
    let fixture = dir.path().join("fixture.jsonl");
    let mint = "1".repeat(32);
    let signature = "1".repeat(64);
    let create = serde_json::json!({"txType":"create","mint":mint,"vSolInBondingCurve":30});
    let trade = serde_json::json!({"txType":"buy","mint":mint,"traderPublicKey":mint,"signature":signature,"tokenAmount":100,"solAmount":1});
    let raw = format!("{create}\n{trade}\n{trade}\n{{\"txType\":\"buy\",\"mint\":\"invalid\"}}\n");
    fs::write(&fixture, &raw).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_ocw"))
        .args([
            "--config",
            config.to_str().unwrap(),
            "--fixture",
            fixture.to_str().unwrap(),
        ])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    let result: serde_json::Value = serde_json::from_str(text.lines().last().unwrap()).unwrap();
    assert_eq!(result["feed"]["frames"], 4);
    assert_eq!(result["feed"]["creates"], 1);
    assert_eq!(result["feed"]["trades"], 1);
    assert_eq!(result["feed"]["duplicates"], 1);
    assert_eq!(result["feed"]["decode_errors"], 1);
    assert_eq!(result["capture"]["state"], "sealed");
    assert_eq!(result["paid"]["billable_received"], 0);
    let capture = dir.path().join("data/capture");
    let mut frames = Vec::new();
    for day in fs::read_dir(capture).unwrap() {
        for file in fs::read_dir(day.unwrap().path()).unwrap() {
            let path = file.unwrap().path();
            if path.extension().is_some_and(|x| x == "ocwcap") {
                ocw_core::capture::scan(&path, |record| {
                    if record.kind == "frame" {
                        frames.extend_from_slice(&record.payload);
                    }
                    Ok(())
                })
                .unwrap();
            }
        }
    }
    assert_eq!(frames, raw.as_bytes());
}

#[test]
fn invalid_config_never_echoes_values_and_initialization_never_overwrites() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.toml");
    fs::write(&path, "api_key = 'private-value-do-not-log'").unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_ocw"))
        .args(["--config", path.to_str().unwrap(), "--check-config"])
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(!String::from_utf8_lossy(&out.stderr).contains("private-value"));
    let path = config(dir.path());
    for expected in [true, false] {
        let out = Command::new(env!("CARGO_BIN_EXE_ocw"))
            .args(["--config", path.to_str().unwrap(), "--init-spend-ledger"])
            .output()
            .unwrap();
        assert_eq!(out.status.success(), expected);
    }
}

#[test]
fn default_start_waits_for_real_data_without_credentials_network_or_disk_writes() {
    let dir = tempfile::tempdir().unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_ocw"))
        .current_dir(dir.path())
        .env("OCW_PUMPPORTAL_KEY", "invalid-on-purpose")
        .env("OCW_DISCORD_STATUS_URL", "invalid-on-purpose")
        .env("OCW_DISCORD_STAGING_URL", "invalid-on-purpose")
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let state: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(state["state"], "waiting_for_data");
    assert_eq!(state["provider_connected"], false);
    assert_eq!(state["trades"], 0);
    assert_eq!(state["alerts_sent"], 0);
    assert!(state["last_trade_ms"].is_null());
    assert!(!String::from_utf8_lossy(&out.stdout).contains("invalid-on-purpose"));
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[test]
fn live_flags_require_explicit_live_and_reject_mixed_modes_before_side_effects() {
    for args in [
        vec!["--enable-paid"],
        vec!["--run-for-secs", "1"],
        vec!["--live", "--status"],
        vec!["--status", "--enable-paid"],
        vec!["--live", "--fixture", "missing.jsonl"],
        vec!["--replay-capture", "missing.ocwcap", "--dry"],
        vec!["--preview-volume"],
        vec!["--demo"],
        vec!["--enable-staging-delivery"],
        vec!["--dry", "--enable-staging-delivery"],
        vec![
            "--replay-capture",
            "missing.ocwcap",
            "--enable-staging-delivery",
        ],
    ] {
        let dir = tempfile::tempdir().unwrap();
        let out = Command::new(env!("CARGO_BIN_EXE_ocw"))
            .current_dir(dir.path())
            .args(args)
            .output()
            .unwrap();
        assert!(!out.status.success());
        assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

#[test]
fn executable_replays_observed_frames_without_credentials_or_invented_signals() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("real.ocwcap");
    let mut segment = ocw_core::capture::Segment::create(&path).unwrap();
    for (i, line) in include_str!("../../../tests/fixtures/pumpportal-create-observed.jsonl")
        .lines()
        .enumerate()
    {
        segment
            .append(&ocw_core::capture::Record::frame(
                i as u64,
                "observed",
                1,
                i as u64 * 1_000_000_000,
                0,
                line.as_bytes().to_vec().into(),
            ))
            .unwrap();
    }
    segment.sync().unwrap();
    drop(segment);
    let before = fs::read(&path).unwrap();
    let out = Command::new(env!("CARGO_BIN_EXE_ocw"))
        .current_dir(dir.path())
        .env("OCW_PUMPPORTAL_KEY", "invalid-on-purpose")
        .env("OCW_DISCORD_STATUS_URL", "invalid-on-purpose")
        .env("OCW_DISCORD_STAGING_URL", "invalid-on-purpose")
        .args(["--replay-capture", path.to_str().unwrap(), "--json"])
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(summary["frames"], 4);
    assert_eq!(summary["creates"], 2);
    assert_eq!(summary["trades"], 0);
    assert_eq!(summary["volume"]["fire"], 0);
    assert_eq!(summary["alerts_sent"], 0);
    assert_eq!(fs::read(&path).unwrap(), before);
    assert_eq!(fs::read_dir(dir.path()).unwrap().count(), 1);
}
