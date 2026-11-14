// ABOUTME: Publishes to the single destination without losing relay rejection reasons.
// ABOUTME: Duplicate acknowledgements are safe replay; other failures prevent checkpoints.

use crate::error::{Error, ErrorKind, Result};
use crate::sync::RateLimiter;
use nostr_sdk::prelude::*;
use std::time::Duration;

/// Returns true for accepted events and false for explicit duplicates.
pub async fn publish_event(
    relay: &Relay,
    event: &Event,
    limiter: Option<&RateLimiter>,
) -> Result<bool> {
    if let Some(limiter) = limiter {
        limiter.wait().await;
    }
    for attempt in 0..=3 {
        let error = match relay.send_event(event.clone()).await {
            Ok(_) => {
                if let Some(limiter) = limiter {
                    limiter.record_success();
                }
                return Ok(true);
            }
            Err(nostr_sdk::pool::relay::Error::EventNotPublished(reason)) => {
                let kind = ErrorKind::from_relay_message(&reason);
                if kind == ErrorKind::Duplicate {
                    return Ok(false);
                }
                if kind == ErrorKind::RateLimited {
                    if let Some(limiter) = limiter {
                        limiter.record_rate_limited();
                    }
                }
                Error::new(kind, reason)
            }
            Err(error) => {
                Error::with_source(ErrorKind::NetworkError, "destination publish failed", error)
            }
        };
        if !error.is_retryable() || attempt == 3 {
            return Err(error);
        }
        // A rejected publish is retried only after background authentication
        // had a round trip to finish; other errors keep the short ladder.
        let backoff_ms = if error.kind == ErrorKind::AuthRequired {
            1_000
        } else {
            100 * (1 << attempt)
        };
        tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
    }
    unreachable!("last attempt returns its error")
}
