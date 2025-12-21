// ABOUTME: Timestamp-based event fetcher
// ABOUTME: Paginates through events in chronological order

use nostr_sdk::prelude::*;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

const BATCH_SIZE: usize = 500;

/// Fetch events from source relay using timestamp pagination
pub async fn fetch_events(
    client: Client,
    filter: Filter,
    _cursor: Option<i64>,
    sender: mpsc::Sender<Event>,
    shutdown: CancellationToken,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    info!("Starting event fetcher (batch_size: {})", BATCH_SIZE);

    let mut current_filter = filter.clone();
    let mut total_fetched = 0u64;
    let mut batch_count = 0u64;

    loop {
        if shutdown.is_cancelled() {
            info!("Fetcher received shutdown signal");
            break;
        }

        // Fetch batch
        debug!("Fetching batch {} (filter: {:?})", batch_count + 1, current_filter);

        let events = match client
            .fetch_events(vec![current_filter.clone()], None)
            .await
        {
            Ok(events) => events,
            Err(e) => {
                warn!("Error fetching events: {}", e);
                // Continue on error instead of failing completely
                break;
            }
        };

        let batch_size = events.len();
        debug!("Fetched {} events in batch {}", batch_size, batch_count + 1);

        if batch_size == 0 {
            info!("No more events to fetch");
            break;
        }

        // Sort events by created_at (oldest first)
        let mut sorted_events: Vec<_> = events.into_iter().collect();
        sorted_events.sort_by_key(|e| e.created_at);

        // Send events to channel
        let mut last_timestamp = None;
        for event in sorted_events {
            last_timestamp = Some(event.created_at.as_u64());

            if sender.send(event).await.is_err() {
                warn!("Failed to send event to channel, receiver dropped");
                return Ok(());
            }

            total_fetched += 1;
        }

        batch_count += 1;

        // Update filter for next batch
        if let Some(ts) = last_timestamp {
            debug!("Updating cursor to timestamp {}", ts);
            current_filter = current_filter.since(Timestamp::from(ts + 1));
        }

        // If we got fewer than BATCH_SIZE events, we're likely done
        if batch_size < BATCH_SIZE {
            info!("Received partial batch, likely at end of available events");
            break;
        }
    }

    info!("Fetcher complete: {} events across {} batches", total_fetched, batch_count);
    Ok(())
}
