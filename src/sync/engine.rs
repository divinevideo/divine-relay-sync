// ABOUTME: Main sync engine coordinating event flow
// ABOUTME: Manages fetcher and publisher tasks with checkpointing

use crate::error::Result;
use crate::relay::connection::RelayConnection;
use crate::state::{StateManager, SyncState};
use crate::sync::fetcher::fetch_events;
use crate::sync::publisher::publish_event;
use crate::sync::RateLimiter;
use nostr_sdk::prelude::*;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Configuration options for sync operation
#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub source_url: String,
    pub dest_url: String,
    pub kinds: Vec<u16>,
    pub authors: Vec<String>,
    pub since: Option<i64>,
    pub until: Option<i64>,
    pub fresh: bool,
    pub dry_run: bool,
    pub nsec: Option<String>,
}

/// Result of sync operation
#[derive(Debug, Clone)]
pub struct SyncResult {
    pub events_synced: u64,
    pub events_skipped: u64,
    pub events_failed: u64,
    pub mode: SyncMode,
}

/// Sync mode used
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Timestamp,
    Negentropy,
}

/// Main sync engine
pub struct SyncEngine {
    options: SyncOptions,
    state_manager: Arc<StateManager>,
    shutdown: CancellationToken,
}

impl SyncEngine {
    /// Create a new sync engine
    pub fn new(
        options: SyncOptions,
        state_manager: Arc<StateManager>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            options,
            state_manager,
            shutdown,
        }
    }

    /// Run the sync operation
    pub async fn run(&self) -> Result<SyncResult> {
        info!("Starting sync from {} to {}", self.options.source_url, self.options.dest_url);

        // Connect to source and destination relays
        let source = RelayConnection::connect(&self.options.source_url).await?;
        let dest = RelayConnection::connect(&self.options.dest_url).await?;

        info!("Source relay: {} (negentropy: {})", source.url, source.info.supports_negentropy);
        info!("Dest relay: {} (negentropy: {})", dest.url, dest.info.supports_negentropy);

        // Determine sync mode
        let mode = if source.info.supports_negentropy && dest.info.supports_negentropy {
            info!("Using negentropy mode (not yet implemented, falling back to timestamp)");
            SyncMode::Timestamp
        } else {
            info!("Using timestamp mode");
            SyncMode::Timestamp
        };

        // Load or create state
        let mut state = if self.options.fresh {
            info!("Fresh sync requested, starting from scratch");
            SyncState::new(&self.options.source_url, &self.options.dest_url)
        } else {
            match self.state_manager.load(
                &self.options.source_url,
                &self.options.dest_url,
                &self.options.kinds,
                &self.options.authors,
            )? {
                Some(s) => {
                    info!("Resuming from previous state (synced: {}, cursor: {:?})",
                        s.events_synced, s.cursor_created_at);
                    s
                }
                None => {
                    info!("No previous state found, starting fresh");
                    SyncState::new(&self.options.source_url, &self.options.dest_url)
                }
            }
        };

        // Create event pipeline channel
        const CHANNEL_SIZE: usize = 1000;
        let (tx, mut rx) = mpsc::channel::<Event>(CHANNEL_SIZE);

        // Build filter
        let filter = self.build_filter(&state);
        debug!("Filter: {:?}", filter);

        // Spawn fetcher task
        let fetcher_source = source.client().clone();
        let fetcher_shutdown = self.shutdown.clone();
        let fetcher_state_cursor = state.cursor_created_at;

        let fetcher_handle = tokio::spawn(async move {
            fetch_events(
                fetcher_source,
                filter,
                fetcher_state_cursor,
                tx,
                fetcher_shutdown,
            )
            .await
        });

        // Track results
        let mut events_synced = 0u64;
        let mut events_skipped = 0u64;
        let mut events_failed = 0u64;
        let mut checkpoint_counter = 0u64;
        const CHECKPOINT_INTERVAL: u64 = 100;

        // Create rate limiter for adaptive rate limiting
        let rate_limiter = RateLimiter::default();

        // Process events from channel
        while let Some(event) = rx.recv().await {
            if self.shutdown.is_cancelled() {
                info!("Shutdown requested, stopping sync");
                break;
            }

            // Publish event (or skip in dry-run mode)
            if self.options.dry_run {
                debug!("DRY RUN: Would publish event {}", event.id);
                events_synced += 1;
            } else {
                match publish_event(dest.client(), &event, Some(&rate_limiter)).await {
                    Ok(true) => {
                        debug!("Published event {}", event.id);
                        events_synced += 1;
                    }
                    Ok(false) => {
                        debug!("Event {} already exists (duplicate)", event.id);
                        events_skipped += 1;
                    }
                    Err(e) => {
                        warn!("Failed to publish event {}: {}", event.id, e);
                        events_failed += 1;

                        // Log failure
                        let _ = self.state_manager.log_failure(
                            &self.options.source_url,
                            &self.options.dest_url,
                            &self.options.kinds,
                            &self.options.authors,
                            &event.id.to_hex(),
                            &e.message,
                        );
                    }
                }
            }

            // Update cursor
            state.update_cursor(event.created_at.as_u64() as i64, event.id.to_hex());
            state.increment_events(1);

            // Checkpoint periodically
            checkpoint_counter += 1;
            if checkpoint_counter >= CHECKPOINT_INTERVAL {
                debug!("Checkpointing state (synced: {}, cursor: {})",
                    state.events_synced, event.created_at.as_u64());
                self.state_manager.save(&state)?;
                checkpoint_counter = 0;
            }
        }

        // Wait for fetcher to complete
        if let Err(e) = fetcher_handle.await {
            warn!("Fetcher task failed: {}", e);
        }

        // Final state save
        info!("Saving final state");
        self.state_manager.save(&state)?;

        // Disconnect from relays
        source.disconnect().await;
        dest.disconnect().await;

        info!(
            "Sync complete: synced={}, skipped={}, failed={}",
            events_synced, events_skipped, events_failed
        );

        Ok(SyncResult {
            events_synced,
            events_skipped,
            events_failed,
            mode,
        })
    }

    /// Build filter from options and state
    fn build_filter(&self, state: &SyncState) -> Filter {
        let mut filter = Filter::new();

        // Add kinds if specified
        if !self.options.kinds.is_empty() {
            let kinds: Vec<Kind> = self.options.kinds
                .iter()
                .map(|&k| Kind::from(k))
                .collect();
            filter = filter.kinds(kinds);
        }

        // Add authors if specified
        if !self.options.authors.is_empty() {
            let authors: std::result::Result<Vec<PublicKey>, _> = self.options.authors
                .iter()
                .map(|a| PublicKey::from_hex(a))
                .collect();

            if let Ok(authors) = authors {
                filter = filter.authors(authors);
            }
        }

        // Set time range
        let since = if let Some(cursor) = state.cursor_created_at {
            // Resume from cursor (cursor is i64, need to convert to u64)
            Timestamp::from((cursor + 1).max(0) as u64)
        } else if let Some(since) = self.options.since {
            // Use explicit since
            Timestamp::from(since.max(0) as u64)
        } else {
            // Default to epoch
            Timestamp::from(0u64)
        };

        filter = filter.since(since);

        // Set until if specified
        if let Some(until) = self.options.until {
            filter = filter.until(Timestamp::from(until.max(0) as u64));
        }

        filter
    }
}
