// ABOUTME: NIP-77 negentropy reconciliation using nostr-sdk built-in support
// ABOUTME: Efficiently syncs events by exchanging fingerprints instead of full events

use crate::error::{Error, ErrorKind, Result};
use nostr_sdk::prelude::*;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

/// Reconcile using Client directly
/// Returns number of events found that need syncing
pub async fn reconcile_with_client(
    client: &Client,
    source_url: &str,
    filter: Filter,
    tx: mpsc::Sender<Event>,
    shutdown: CancellationToken,
) -> Result<u64> {
    info!("Starting negentropy reconciliation with {}", source_url);

    let opts = SyncOptions::default()
        .direction(SyncDirection::Down);

    let source_relay = RelayUrl::parse(source_url).map_err(|e| {
        Error::with_source(ErrorKind::ConfigError, "invalid source URL", e)
    })?;

    // Perform reconciliation
    let reconciliation = client
        .sync_with([source_relay.clone()], filter, &opts)
        .await
        .map_err(|e| {
            Error::with_source(ErrorKind::NegentropyError, "negentropy sync failed", e)
        })?;

    let mut events_found = 0u64;

    // Check for successful reconciliation
    if reconciliation.success.contains(&source_relay) {
        info!("Negentropy with {}: {} events received", source_relay, reconciliation.val.received.len());

        // Get database reference
        let db = client.database();

        // Fetch each received event from the database
        for event_id in &reconciliation.val.received {
            if shutdown.is_cancelled() {
                info!("Shutdown during negentropy reconciliation");
                break;
            }

            // Query event from database
            match db.event_by_id(event_id).await {
                Ok(Some(event)) => {
                    if tx.send(event).await.is_err() {
                        warn!("Publisher channel closed during reconciliation");
                        break;
                    }
                    events_found += 1;
                }
                Ok(None) => {
                    warn!("Event {} was received but not found in database", event_id);
                }
                Err(e) => {
                    warn!("Failed to query event {} from database: {}", event_id, e);
                }
            }
        }
    } else {
        warn!("Negentropy with {} was not successful", source_relay);
    }

    // Log failures
    for (url, err) in reconciliation.failed.iter() {
        warn!("Negentropy failed with {}: {:?}", url, err);
    }

    drop(tx); // Signal completion
    info!("Negentropy reconciliation complete: {} events to sync", events_found);
    Ok(events_found)
}

/// Check if negentropy can be used (both relays support NIP-77)
pub fn can_use_negentropy(source_supports: bool, dest_supports: bool) -> bool {
    source_supports && dest_supports
}
