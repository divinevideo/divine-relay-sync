// ABOUTME: Timestamp-based event fetcher
// ABOUTME: Paginates backwards through events (newest to oldest)

use nostr_sdk::prelude::*;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Fetch events from source relay using timestamp pagination
/// Paginates backwards in time (relays return newest events first)
pub async fn fetch_events(
    client: Client,
    filter: Filter,
    cursor: Option<i64>,
    sender: mpsc::Sender<Event>,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting event fetcher");

    // Start with the provided filter, apply cursor if resuming
    let mut current_filter = if let Some(cursor_ts) = cursor {
        // Resume from cursor - fetch events BEFORE the cursor timestamp
        info!("Resuming from cursor timestamp {}", cursor_ts);
        filter.clone().until(Timestamp::from(cursor_ts as u64))
    } else {
        filter.clone()
    };

    let mut total_fetched = 0u64;
    let mut batch_count = 0u64;

    loop {
        if shutdown.is_cancelled() {
            info!("Fetcher received shutdown signal");
            break;
        }

        // Fetch batch
        batch_count += 1;
        debug!("Fetching batch {} (filter: {:?})", batch_count, current_filter);

        let events = match client
            .fetch_events(vec![current_filter.clone()], None)
            .await
        {
            Ok(events) => events,
            Err(e) => {
                warn!("Error fetching events: {}", e);
                break;
            }
        };

        let batch_size = events.len();
        debug!("Fetched {} events in batch {}", batch_size, batch_count);

        if batch_size == 0 {
            info!("No more events to fetch");
            break;
        }

        // Sort events by created_at (oldest first for consistent publishing)
        let mut sorted_events: Vec<_> = events.into_iter().collect();
        sorted_events.sort_by_key(|e| e.created_at);

        // Track oldest timestamp for pagination (first after sorting = oldest)
        let oldest_timestamp = sorted_events.first().map(|e| e.created_at.as_u64());

        // Send events to channel
        for event in sorted_events {
            // Check shutdown between each event
            if shutdown.is_cancelled() {
                info!("Fetcher received shutdown signal during batch");
                return Ok(());
            }
            if sender.send(event).await.is_err() {
                warn!("Failed to send event to channel, receiver dropped");
                return Ok(());
            }
            total_fetched += 1;
        }

        // Log progress
        info!("Progress: {} events fetched ({} batches)", total_fetched, batch_count);

        // Update filter to fetch older events (before the oldest in this batch)
        if let Some(ts) = oldest_timestamp {
            if ts == 0 {
                // Can't go earlier than epoch
                info!("Reached earliest possible events (timestamp 0)");
                break;
            }
            debug!("Moving cursor to before timestamp {}", ts);
            current_filter = filter.clone().until(Timestamp::from(ts - 1));
        } else {
            break;
        }
    }

    info!("Fetcher complete: {} events across {} batches", total_fetched, batch_count);
    Ok(())
}
