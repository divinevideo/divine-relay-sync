// ABOUTME: Main sync engine coordinating event flow
// ABOUTME: Manages fetcher and publisher tasks with checkpointing

use crate::error::Result;
use crate::relay::connection::RelayConnection;
use crate::state::{StateManager, SyncState};
use crate::sync::fetcher::fetch_events;
use nostr_sdk::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Maximum concurrent publish operations
const MAX_CONCURRENT_PUBLISHES: usize = 200;

/// Default kinds to exclude (notes and deletions)
pub const DEFAULT_EXCLUDE_KINDS: &[u16] = &[1, 5];

/// Default tags to exclude
pub const DEFAULT_EXCLUDE_TAGS: &[(&str, &str)] = &[("L", "pink.momostr")];

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
    /// Kinds to exclude from sync
    pub exclude_kinds: Vec<u16>,
    /// Tags to exclude (tag_name, tag_value)
    pub exclude_tags: Vec<(String, String)>,
    /// Tags to require (tag_name, tag_value)
    pub include_tags: Vec<(String, String)>,
}

/// Result of sync operation
#[derive(Debug, Clone)]
pub struct SyncResult {
    pub events_synced: u64,
    pub events_skipped: u64,
    pub events_failed: u64,
    pub events_filtered: u64,
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

    /// Check if an event should be filtered out (excluded from sync)
    fn should_filter_event(event: &Event, options: &SyncOptions) -> bool {
        // Check excluded kinds
        let kind_num = event.kind.as_u16();
        if options.exclude_kinds.contains(&kind_num) {
            debug!("Filtering event {} - excluded kind {}", event.id, kind_num);
            return true;
        }

        // Check excluded tags
        for (tag_name, tag_value) in &options.exclude_tags {
            for tag in event.tags.iter() {
                let tag_vec: Vec<&str> = tag.as_slice().iter().map(|s| s.as_str()).collect();
                if tag_vec.len() >= 2 && tag_vec[0] == tag_name && tag_vec[1] == tag_value {
                    debug!(
                        "Filtering event {} - excluded tag [{}, {}]",
                        event.id, tag_name, tag_value
                    );
                    return true;
                }
            }
        }

        // Check required tags (if any specified, event must have at least one)
        if !options.include_tags.is_empty() {
            let has_required_tag = options.include_tags.iter().any(|(tag_name, tag_value)| {
                event.tags.iter().any(|tag| {
                    let tag_vec: Vec<&str> = tag.as_slice().iter().map(|s| s.as_str()).collect();
                    tag_vec.len() >= 2 && tag_vec[0] == tag_name && tag_vec[1] == tag_value
                })
            });

            if !has_required_tag {
                debug!("Filtering event {} - missing required tag", event.id);
                return true;
            }
        }

        false
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
            info!("Both relays support negentropy, using NIP-77 sync");
            SyncMode::Negentropy
        } else {
            info!("Using timestamp-based sync (negentropy not supported by both relays)");
            SyncMode::Timestamp
        };

        // Load or create state
        let state = if self.options.fresh {
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

        // Spawn fetcher/reconciler task based on mode
        let fetcher_handle = match mode {
            SyncMode::Negentropy => {
                let source_client = source.client().clone();
                let source_url = self.options.source_url.clone();
                let filter_clone = filter.clone();
                let tx_clone = tx;
                let shutdown_clone = self.shutdown.clone();

                tokio::spawn(async move {
                    let result = super::reconciler::reconcile_with_client(
                        &source_client,
                        &source_url,
                        filter_clone,
                        tx_clone,
                        shutdown_clone,
                    ).await;

                    match result {
                        Ok(count) => {
                            info!("Negentropy found {} events", count);
                            Ok(())
                        }
                        Err(e) => {
                            warn!("Negentropy error: {}", e);
                            Err(Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                        }
                    }
                })
            }
            SyncMode::Timestamp => {
                let fetcher_source = source.client().clone();
                let fetcher_shutdown = self.shutdown.clone();
                let fetcher_state_cursor = state.cursor_created_at;

                tokio::spawn(async move {
                    fetch_events(
                        fetcher_source,
                        filter,
                        fetcher_state_cursor,
                        tx,
                        fetcher_shutdown,
                    )
                    .await
                })
            }
        };

        // Track results with atomics for concurrent access
        let events_synced = Arc::new(AtomicU64::new(0));
        let events_skipped = Arc::new(AtomicU64::new(0));
        let events_failed = Arc::new(AtomicU64::new(0));
        let events_filtered = Arc::new(AtomicU64::new(0));

        // Semaphore to limit concurrent publishes
        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_PUBLISHES));

        // Clone for the publisher task
        let dest_client = dest.client().clone();
        let synced = events_synced.clone();
        let skipped = events_skipped.clone();
        let failed = events_failed.clone();
        let filtered = events_filtered.clone();
        let shutdown = self.shutdown.clone();
        let dry_run = self.options.dry_run;
        let filter_options = self.options.clone();

        // Spawn publisher workers that pull from channel
        let publisher_handle = tokio::spawn(async move {
            let mut handles = Vec::new();

            while let Some(event) = rx.recv().await {
                if shutdown.is_cancelled() {
                    break;
                }

                // Apply exclusion filters
                if Self::should_filter_event(&event, &filter_options) {
                    filtered.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // Dry run mode - just count
                if dry_run {
                    debug!("DRY RUN: Would publish event {}", event.id);
                    synced.fetch_add(1, Ordering::Relaxed);
                    continue;
                }

                // Acquire semaphore permit
                let permit = semaphore.clone().acquire_owned().await.unwrap();
                let client = dest_client.clone();
                let synced_clone = synced.clone();
                let skipped_clone = skipped.clone();
                let failed_clone = failed.clone();

                // Spawn task for this event (fire and forget style)
                let handle = tokio::spawn(async move {
                    let _permit = permit; // Hold permit until done

                    match client.send_event(event.clone()).await {
                        Ok(output) => {
                            if output.success.is_empty() {
                                // Check for duplicate
                                if let Some((_, reason)) = output.failed.iter().next() {
                                    let msg = reason.as_ref().map(|s| s.as_str()).unwrap_or("");
                                    if msg.contains("duplicate") || msg.contains("already have") {
                                        skipped_clone.fetch_add(1, Ordering::Relaxed);
                                        return;
                                    }
                                }
                                failed_clone.fetch_add(1, Ordering::Relaxed);
                            } else {
                                synced_clone.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        Err(_) => {
                            failed_clone.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                });
                handles.push(handle);

                // Log progress periodically (every 500 events spawned)
                if handles.len() % 500 == 0 {
                    let total = synced.load(Ordering::Relaxed)
                        + skipped.load(Ordering::Relaxed)
                        + failed.load(Ordering::Relaxed);
                    info!(
                        "Publishing: {} sent, {} completed ({} synced, {} skipped, {} failed)",
                        handles.len(),
                        total,
                        synced.load(Ordering::Relaxed),
                        skipped.load(Ordering::Relaxed),
                        failed.load(Ordering::Relaxed)
                    );
                }
            }

            // Wait for all remaining tasks
            for handle in handles {
                let _ = handle.await;
            }
        });

        // Wait for fetcher to complete
        if let Err(e) = fetcher_handle.await {
            warn!("Fetcher task failed: {}", e);
        }

        // Wait for publisher to complete
        if let Err(e) = publisher_handle.await {
            warn!("Publisher task failed: {}", e);
        }

        let final_synced = events_synced.load(Ordering::Relaxed);
        let final_skipped = events_skipped.load(Ordering::Relaxed);
        let final_failed = events_failed.load(Ordering::Relaxed);
        let final_filtered = events_filtered.load(Ordering::Relaxed);

        // Final state save (skip in dry run mode)
        if !self.options.dry_run {
            info!("Saving final state");
            self.state_manager.save(&state)?;
        } else {
            info!("Dry run complete, state not saved");
        }

        // Disconnect from relays
        source.disconnect().await;
        dest.disconnect().await;

        info!(
            "Sync complete: synced={}, skipped={}, failed={}, filtered={}",
            final_synced, final_skipped, final_failed, final_filtered
        );

        Ok(SyncResult {
            events_synced: final_synced,
            events_skipped: final_skipped,
            events_failed: final_failed,
            events_filtered: final_filtered,
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
