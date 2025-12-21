// ABOUTME: Event publisher with retry logic
// ABOUTME: Publishes events to destination relay with exponential backoff

use crate::error::{Error, ErrorKind, Result};
use crate::sync::RateLimiter;
use nostr_sdk::prelude::*;
use std::time::Duration;
use tracing::{debug, warn};

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 100;

/// Publish an event to the destination relay
/// Returns Ok(true) if published, Ok(false) if duplicate, Err on failure
pub async fn publish_event(client: &Client, event: &Event, rate_limiter: Option<&RateLimiter>) -> Result<bool> {
    // Wait for rate limiter permission if provided
    if let Some(limiter) = rate_limiter {
        limiter.wait().await;
    }

    let mut retries = 0;
    let mut backoff_ms = INITIAL_BACKOFF_MS;

    loop {
        match client.send_event(event.clone()).await {
            Ok(send_output) => {
                // Check if event was accepted by checking the output
                if send_output.success.is_empty() {
                    // No relay accepted - check if we have failures
                    if !send_output.failed.is_empty() {
                        // Extract error from first failure
                        if let Some((_, reason)) = send_output.failed.iter().next() {
                            let error_msg = reason.as_ref().map(|s| s.as_str()).unwrap_or("unknown error");
                            let kind = ErrorKind::from_relay_message(error_msg);

                            // Handle duplicate specially
                            if kind == ErrorKind::Duplicate {
                                debug!("Event {} is duplicate", event.id);
                                return Ok(false);
                            }

                            // Handle rate limiting feedback
                            if kind == ErrorKind::RateLimited {
                                if let Some(limiter) = rate_limiter {
                                    limiter.record_rate_limited();
                                }
                            }

                            // Check if we should retry
                            if kind.is_retryable() && retries < MAX_RETRIES {
                                retries += 1;
                                warn!(
                                    "Retryable error publishing event {} (attempt {}/{}): {}",
                                    event.id, retries, MAX_RETRIES, error_msg
                                );

                                // Exponential backoff
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                backoff_ms *= 2;
                                continue;
                            }

                            // Non-retryable or max retries exceeded
                            return Err(Error::new(kind, error_msg.to_string()));
                        }
                    }

                    // No success and no clear error - treat as unknown error
                    return Err(Error::new(
                        ErrorKind::Unknown,
                        "event not accepted by any relay",
                    ));
                }

                // Event was accepted
                debug!("Event {} published successfully", event.id);
                if let Some(limiter) = rate_limiter {
                    limiter.record_success();
                }
                return Ok(true);
            }
            Err(e) => {
                // SDK error
                let error_msg = e.to_string();
                let kind = ErrorKind::NetworkError;

                if kind.is_retryable() && retries < MAX_RETRIES {
                    retries += 1;
                    warn!(
                        "Network error publishing event {} (attempt {}/{}): {}",
                        event.id, retries, MAX_RETRIES, error_msg
                    );

                    // Exponential backoff
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms *= 2;
                    continue;
                }

                return Err(Error::with_source(kind, error_msg, e));
            }
        }
    }
}
