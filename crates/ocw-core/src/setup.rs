//! Offline input checks. Never creates storage, reserves spend, or contacts a service.
use crate::{config::Config, discord, spend::Ledger};
use serde_json::{Value, json};
use std::os::unix::fs::PermissionsExt;
pub fn check(
    config: &Config,
    paid: bool,
    staging: bool,
    env: impl Fn(&str) -> Option<String>,
) -> Value {
    let mut checks = Vec::new();
    let mut add = |component: &str, ready: bool, status: &str| {
        checks.push(json!({"component":component,"ready":ready,"status":status}))
    };
    add(
        "configuration",
        config.validate().is_ok(),
        if config.validate().is_ok() {
            "valid"
        } else {
            "invalid"
        },
    );
    let data = std::fs::symlink_metadata(&config.data_dir);
    let private = data.as_ref().is_ok_and(|m| {
        m.is_dir() && !m.file_type().is_symlink() && m.permissions().mode() & 0o077 == 0
    });
    let absent = data
        .as_ref()
        .is_err_and(|e| e.kind() == std::io::ErrorKind::NotFound);
    add(
        "data_directory",
        private || absent,
        if private {
            "private"
        } else if absent {
            "will_be_created_on_explicit_start"
        } else {
            "must_be_a_private_real_directory"
        },
    );
    let restore = crate::storage::restore_review_required(&config.data_dir);
    add(
        "restore_review",
        !restore,
        if restore {
            "reconciliation_required"
        } else {
            "clear"
        },
    );
    let api = env("OCW_PUMPPORTAL_KEY").filter(|v| !v.is_empty());
    let api_ok = api.as_ref().is_some_and(|v| (32..=512).contains(&v.len()));
    add(
        "pumpportal_credential",
        if paid {
            api_ok
        } else {
            api.is_none() || api_ok
        },
        if api_ok {
            "present_format_only"
        } else if api.is_some() {
            "invalid_length"
        } else if paid {
            "required"
        } else {
            "not_required_for_free_stream"
        },
    );
    if paid {
        add(
            "watch_policy",
            config.feed.mint.is_some() || config.feed.auto_watch,
            "mint_or_automatic_watch_required",
        );
        let ledger = Ledger::open(
            &config.data_dir.join("feed-spend.json"),
            crate::collector::wall_ms() / 86_400_000,
            config.gate.daily_messages,
            config.gate.stop_margin_messages,
            config.gate.reservation_messages,
        );
        add(
            "feed_spend_ledger",
            ledger.is_ok(),
            if ledger.is_ok() {
                "readable_integrity_checked"
            } else {
                "initialize_or_repair_before_paid_start"
            },
        );
    }
    let status = env("OCW_DISCORD_STATUS_URL").filter(|v| !v.is_empty());
    let status_url = status.as_ref().and_then(|v| discord::webhook_url(v).ok());
    let status_required = config.monitor.enabled && config.monitor.discord_status;
    add(
        "discord_status",
        if status_required {
            status_url.is_some()
        } else {
            status.is_none() || status_url.is_some()
        },
        if status_required {
            "webhook_and_configured_channel_required_format_only"
        } else {
            "optional_outage_webhook_format_only"
        },
    );
    if staging {
        let staging_url = env("OCW_DISCORD_STAGING_URL")
            .filter(|v| !v.is_empty())
            .and_then(|v| discord::webhook_url(&v).ok());
        add(
            "discord_staging",
            staging_url.is_some() && config.delivery.staging_channel_id.is_some(),
            "webhook_and_configured_channel_required_format_only",
        );
        add(
            "discord_channel_separation",
            staging_url.is_none() || staging_url != status_url,
            "distinct_status_and_staging_webhooks_required",
        );
    }
    if config.cto.staging_delivery {
        let target = env("OCW_DISCORD_CTO_URL").and_then(|raw| discord::webhook_url(&raw).ok());
        let staging_target =
            env("OCW_DISCORD_STAGING_URL").and_then(|raw| discord::webhook_url(&raw).ok());
        add(
            "discord_cto",
            staging && target.is_some() && target != status_url && target != staging_target,
            "explicit_staging_flag_and_distinct_cto_webhook_required_format_only",
        );
    }
    if config.probe.enabled {
        let rpc = env("SOLANA_RPC_URL").filter(|v| !v.is_empty());
        add(
            "independent_rpc",
            rpc.as_deref()
                .is_some_and(|v| crate::rpc::endpoint(v).is_ok()),
            "https_endpoint_required_format_only",
        );
        add(
            "probe_sampling_policy",
            paid && config.feed.auto_watch && config.watch.max_subs >= 3,
            "paid_auto_watch_required_for_three_mint_samples",
        );
    }
    if config.monitor.deadman {
        add(
            "external_deadman",
            env("OCW_DEADMAN_URL")
                .as_deref()
                .is_some_and(|v| crate::monitor::deadman_url(v).is_ok()),
            "healthchecks_ping_required_format_only_notifications_not_verified",
        );
    }
    let ready = checks.iter().all(|row| row["ready"] == true);
    json!({"mode":"offline_preflight","configuration_ready":ready,"network":"disabled","production_ready":false,"external_acceptance":"not_checked","checks":checks,"pending_product_features":["full_decision_backtesting","bundle_tracking","wallet_rankings_and_alerts","secondary_provider_failover"]})
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn missing_inputs_never_write_or_expose_values_and_free_preflight_can_pass() {
        let dir = tempfile::tempdir().unwrap();
        let config = Config {
            data_dir: dir.path().join("data"),
            ..Config::default()
        };
        let report = check(&config, false, false, |_| None);
        assert_eq!(report["configuration_ready"], true);
        assert!(!config.data_dir.exists());
        let report = check(&config, true, true, |_| {
            Some("private-invalid-credential".into())
        });
        assert_eq!(report["configuration_ready"], false);
        assert!(!report.to_string().contains("private-invalid-credential"));
        assert!(!config.data_dir.exists());
    }
}
