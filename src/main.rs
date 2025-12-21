// ABOUTME: CLI entry point for relay-sync tool
// ABOUTME: Handles argument parsing, signal handling, and orchestrates sync

use anyhow::Result;
use clap::Parser;
use relay_sync::cli::Cli;
use relay_sync::config::Config;
use relay_sync::state::StateManager;
use relay_sync::sync::{SyncEngine, SyncOptions};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup logging
    let filter = if cli.verbose {
        EnvFilter::new("debug")
    } else if cli.quiet {
        EnvFilter::new("error")
    } else {
        EnvFilter::new("info")
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    // Load config if specified
    let config = if let Some(config_path) = &cli.config {
        Some(Config::from_file(config_path)?)
    } else {
        None
    };

    // Determine source and dest
    let (source_url, dest_url, kinds, authors) = if let Some(ref config) = config {
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
        )
    } else {
        let source = cli.source_url().ok_or_else(|| {
            anyhow::anyhow!("source relay URL required")
        })?;
        let dest = cli.dest_url().ok_or_else(|| {
            anyhow::anyhow!("destination relay URL required")
        })?;

        (source, dest, cli.kinds.clone(), cli.authors.clone())
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

    // Create sync options
    let options = SyncOptions {
        source_url: source_url.clone(),
        dest_url: dest_url.clone(),
        kinds,
        authors,
        since: None, // TODO: parse from CLI
        until: None, // TODO: parse from CLI
        fresh: cli.fresh,
        dry_run: cli.dry_run,
        nsec,
    };

    // Run sync
    tracing::info!("Starting sync: {} -> {}", options.source_url, options.dest_url);

    let engine = SyncEngine::new(options, state_manager, shutdown);
    let result = engine.run().await?;

    // Report results
    if cli.json {
        println!("{}", serde_json::to_string_pretty(&serde_json::json!({
            "events_synced": result.events_synced,
            "events_skipped": result.events_skipped,
            "events_failed": result.events_failed,
            "mode": format!("{:?}", result.mode),
        }))?);
    } else {
        tracing::info!(
            "Sync complete: {} synced, {} skipped, {} failed (mode: {:?})",
            result.events_synced,
            result.events_skipped,
            result.events_failed,
            result.mode
        );
    }

    // Exit code based on failures
    if result.events_failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}
