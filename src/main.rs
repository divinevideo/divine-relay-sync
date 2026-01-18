// ABOUTME: CLI entry point for relay-sync tool
// ABOUTME: Handles argument parsing, signal handling, and orchestrates sync

use anyhow::Result;
use clap::Parser;
use relay_sync::cli::Cli;
use relay_sync::config::Config;
use relay_sync::state::StateManager;
use relay_sync::sync::{SyncEngine, SyncOptions, DEFAULT_EXCLUDE_KINDS, DEFAULT_EXCLUDE_TAGS};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// Global counter for expired events
static EXPIRED_EVENTS: AtomicU64 = AtomicU64::new(0);
static OVERSIZED_EVENTS: AtomicU64 = AtomicU64::new(0);

/// Custom layer that counts specific warnings
struct WarningCounter;

impl<S> tracing_subscriber::Layer<S> for WarningCounter
where
    S: tracing::Subscriber,
{
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        // Check if this is a warning we want to count
        let mut visitor = MessageVisitor::default();
        event.record(&mut visitor);

        if let Some(msg) = visitor.message {
            if msg.contains("event expired") {
                EXPIRED_EVENTS.fetch_add(1, Ordering::Relaxed);
            } else if msg.contains("too large") {
                OVERSIZED_EVENTS.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[derive(Default)]
struct MessageVisitor {
    message: Option<String>,
}

impl tracing::field::Visit for MessageVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = Some(format!("{:?}", value));
        }
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "message" {
            self.message = Some(value.to_string());
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup logging - count nostr-sdk warnings but don't display them
    let counter_filter = EnvFilter::new("warn"); // Counter sees all warnings
    let display_filter = if cli.verbose {
        EnvFilter::new("debug")
    } else if cli.quiet {
        EnvFilter::new("error")
    } else {
        EnvFilter::new("info,nostr_sdk::relay::inner=error")
    };

    tracing_subscriber::registry()
        .with(WarningCounter.with_filter(counter_filter))
        .with(tracing_subscriber::fmt::layer().with_target(false).with_filter(display_filter))
        .init();

    // Load config if specified
    let config = if let Some(config_path) = &cli.config {
        Some(Config::from_file(config_path)?)
    } else {
        None
    };

    // Determine source, dest, and filter options
    let (source_url, dest_url, kinds, authors, cfg_include_notes, cfg_include_deletions, cfg_exclude_tags, cfg_tags) =
        if let Some(ref config) = config {
            let sync_config = if let Some(name) = &cli.name {
                config.find_sync(name).ok_or_else(|| {
                    anyhow::anyhow!("sync config '{}' not found", name)
                })?
            } else if config.sync.len() == 1 {
                &config.sync[0]
            } else {
                return Err(anyhow::anyhow!(
                    "multiple sync configs found, use --name to specify one"
                ));
            };

            (
                relay_sync::cli::normalize_relay_url(&sync_config.source),
                relay_sync::cli::normalize_relay_url(&sync_config.dest),
                sync_config.kinds.clone().unwrap_or_default(),
                sync_config.authors.clone().unwrap_or_default(),
                sync_config.include_notes,
                sync_config.include_deletions,
                sync_config.exclude_tags.clone(),
                sync_config.tags.clone(),
            )
        } else {
            let source = cli.source_url().ok_or_else(|| {
                anyhow::anyhow!("source relay URL required")
            })?;
            let dest = cli.dest_url().ok_or_else(|| {
                anyhow::anyhow!("destination relay URL required")
            })?;

            (source, dest, cli.kinds.clone(), cli.authors.clone(), false, false, vec![], vec![])
        };

    // Get nsec from CLI, config, or env
    let nsec = cli.nsec.clone().or_else(|| {
        config.as_ref().and_then(|c| c.nsec())
    });

    // Setup state manager
    let state_dir = PathBuf::from(".relay-sync-state");
    let state_manager = Arc::new(StateManager::new(&state_dir)?);

    // Acquire lock
    let _lock = state_manager
        .acquire_lock(&source_url, &dest_url, &kinds, &authors)?
        .ok_or_else(|| anyhow::anyhow!("another sync is already running"))?;

    // Setup shutdown handling
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutdown signal received, draining...");
        shutdown_clone.cancel();
    });

    // Parse date ranges
    let since = if let Some(ref s) = cli.since {
        Some(relay_sync::cli::parse_date(s).map_err(|e| anyhow::anyhow!(e))?)
    } else {
        None
    };

    let until = if let Some(ref s) = cli.until {
        Some(relay_sync::cli::parse_date(s).map_err(|e| anyhow::anyhow!(e))?)
    } else {
        None
    };

    // Build exclude kinds list (default excludes kind 1 and 5)
    // CLI flags or config can re-include them
    let mut exclude_kinds: Vec<u16> = DEFAULT_EXCLUDE_KINDS.to_vec();
    if cli.include_notes || cfg_include_notes {
        exclude_kinds.retain(|&k| k != 1);
    }
    if cli.include_deletions || cfg_include_deletions {
        exclude_kinds.retain(|&k| k != 5);
    }

    // Build exclude tags list (default excludes pink.momostr)
    let mut exclude_tags: Vec<(String, String)> = DEFAULT_EXCLUDE_TAGS
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    // Add config exclude tags
    for tag_str in &cfg_exclude_tags {
        if let Some(parsed) = relay_sync::cli::parse_tag_filter(tag_str) {
            exclude_tags.push(parsed);
        }
    }
    // Add CLI exclude tags
    exclude_tags.extend(cli.parsed_exclude_tags());

    // Get include tags from config and CLI
    let mut include_tags: Vec<(String, String)> = vec![];
    for tag_str in &cfg_tags {
        if let Some(parsed) = relay_sync::cli::parse_tag_filter(tag_str) {
            include_tags.push(parsed);
        }
    }
    include_tags.extend(cli.parsed_tags());

    // Create sync options
    let options = SyncOptions {
        source_url: source_url.clone(),
        dest_url: dest_url.clone(),
        kinds,
        authors,
        since,
        until,
        fresh: cli.fresh,
        dry_run: cli.dry_run,
        nsec,
        exclude_kinds,
        exclude_tags,
        include_tags,
    };

    // Run sync
    tracing::info!("Starting sync: {} -> {}", options.source_url, options.dest_url);

    let engine = SyncEngine::new(options, state_manager, shutdown);
    let result = engine.run().await?;

    // Get warning counts
    let expired = EXPIRED_EVENTS.load(Ordering::Relaxed);
    let oversized = OVERSIZED_EVENTS.load(Ordering::Relaxed);

    // Report results
    if cli.json {
        println!("{}", serde_json::to_string_pretty(&serde_json::json!({
            "events_synced": result.events_synced,
            "events_skipped": result.events_skipped,
            "events_failed": result.events_failed,
            "events_filtered": result.events_filtered,
            "events_expired": expired,
            "events_oversized": oversized,
            "mode": format!("{:?}", result.mode),
        }))?);
    } else {
        tracing::info!(
            "Sync complete: {} synced, {} skipped, {} failed, {} filtered, {} expired, {} oversized (mode: {:?})",
            result.events_synced,
            result.events_skipped,
            result.events_failed,
            result.events_filtered,
            expired,
            oversized,
            result.mode
        );
    }

    // Exit code based on failures
    if result.events_failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}
