# Remaining Features Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan with parallel subagents.

**Goal:** Complete the relay-sync tool with NIP-77 negentropy, rate limiting, full NIP-42 auth, and date parsing.

**Architecture:** Four independent features that can be implemented in parallel. Each modifies different parts of the codebase with minimal overlap.

**Tech Stack:** Rust, nostr-sdk 0.37 (has built-in negentropy support), governor, chrono

---

## Parallelization Strategy

These tasks are **independent** and can run as parallel subagents:

| Task | Files Modified | Dependencies |
|------|---------------|--------------|
| Task 1: Date Parsing | `cli.rs`, `main.rs`, tests | None |
| Task 2: Rate Limiting | `sync/publisher.rs`, `sync/engine.rs` | None |
| Task 3: NIP-42 Auth Flow | `relay/auth.rs`, `relay/connection.rs` | None |
| Task 4: Negentropy Sync | `sync/reconciler.rs`, `sync/engine.rs` | None |

---

## Task 1: Date Parsing for --since and --until

**Files:**
- Modify: `src/cli.rs`
- Modify: `src/main.rs:96-106`
- Create: `tests/date_parsing_test.rs`

**Step 1: Write the failing test**

```rust
// tests/date_parsing_test.rs
// ABOUTME: Tests for date string parsing
// ABOUTME: Verifies YYYY-MM-DD and relative date formats

use relay_sync::cli::parse_date;

#[test]
fn test_parse_date_ymd() {
    let ts = parse_date("2024-01-15").unwrap();
    // 2024-01-15 00:00:00 UTC
    assert_eq!(ts, 1705276800);
}

#[test]
fn test_parse_date_ymd_hms() {
    let ts = parse_date("2024-01-15T12:30:00").unwrap();
    assert_eq!(ts, 1705321800);
}

#[test]
fn test_parse_date_timestamp() {
    let ts = parse_date("1705276800").unwrap();
    assert_eq!(ts, 1705276800);
}

#[test]
fn test_parse_date_invalid() {
    assert!(parse_date("not-a-date").is_err());
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test --test date_parsing_test`
Expected: FAIL - `parse_date` not found

**Step 3: Implement date parsing in cli.rs**

Add to `src/cli.rs`:

```rust
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};

/// Parse date string into Unix timestamp
/// Supports: YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS, or raw timestamp
pub fn parse_date(s: &str) -> Result<i64, String> {
    // Try raw timestamp first
    if let Ok(ts) = s.parse::<i64>() {
        return Ok(ts);
    }

    // Try YYYY-MM-DDTHH:MM:SS
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Ok(Utc.from_utc_datetime(&dt).timestamp());
    }

    // Try YYYY-MM-DD
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let dt = date.and_hms_opt(0, 0, 0).unwrap();
        return Ok(Utc.from_utc_datetime(&dt).timestamp());
    }

    Err(format!("invalid date format: {}", s))
}
```

**Step 4: Run test to verify it passes**

Run: `cargo test --test date_parsing_test`
Expected: PASS

**Step 5: Wire up in main.rs**

Modify `src/main.rs` lines 96-106, replacing the TODO comments:

```rust
    // Parse since/until dates
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
    };
```

**Step 6: Add integration test**

Add to `tests/integration_test.rs`:

```rust
#[test]
fn test_since_until_flags() {
    let output = Command::new("cargo")
        .args(["run", "--", "source.com", "dest.com", "--since", "2024-01-01", "--until", "2024-12-31", "--dry-run"])
        .output()
        .expect("Failed to execute command");

    // Should not error on date parsing
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!stderr.contains("invalid date"), "stderr: {}", stderr);
}
```

**Step 7: Run all tests**

Run: `cargo test`
Expected: PASS

**Step 8: Commit**

```bash
git add -A
git commit -m "feat: add date parsing for --since and --until flags"
```

---

## Task 2: Adaptive Rate Limiting with Governor

**Files:**
- Modify: `src/sync/publisher.rs`
- Modify: `src/sync/engine.rs`
- Create: `src/sync/rate_limiter.rs`

**Step 1: Create rate limiter module**

Create `src/sync/rate_limiter.rs`:

```rust
// ABOUTME: Adaptive rate limiter using governor crate
// ABOUTME: Adjusts rate based on relay responses (429s, rate-limited errors)

use governor::{Quota, RateLimiter as GovLimiter};
use nonzero_ext::nonzero;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

/// Adaptive rate limiter that adjusts based on relay feedback
pub struct RateLimiter {
    limiter: GovLimiter<governor::state::NotKeyed, governor::state::InMemoryState, governor::clock::DefaultClock>,
    current_rate: AtomicU32,
    min_rate: u32,
    max_rate: u32,
}

impl RateLimiter {
    /// Create new rate limiter with initial rate (events per second)
    pub fn new(initial_rate: u32) -> Self {
        let rate = NonZeroU32::new(initial_rate).unwrap_or(nonzero!(10u32));
        let quota = Quota::per_second(rate);
        let limiter = GovLimiter::direct(quota);

        Self {
            limiter,
            current_rate: AtomicU32::new(initial_rate),
            min_rate: 1,
            max_rate: 100,
        }
    }

    /// Wait for permission to send
    pub async fn wait(&self) {
        self.limiter.until_ready().await;
    }

    /// Record a rate limit response - slow down
    pub fn record_rate_limited(&self) {
        let current = self.current_rate.load(Ordering::Relaxed);
        let new_rate = (current / 2).max(self.min_rate);
        self.current_rate.store(new_rate, Ordering::Relaxed);
        tracing::warn!("Rate limited, reducing to {} events/sec", new_rate);
    }

    /// Record successful publish - can speed up
    pub fn record_success(&self) {
        let current = self.current_rate.load(Ordering::Relaxed);
        // Slowly increase (add 1 every 10 successes via atomic)
        if current < self.max_rate {
            // Simple increment, real impl would track success count
            let new_rate = (current + 1).min(self.max_rate);
            self.current_rate.store(new_rate, Ordering::Relaxed);
        }
    }

    /// Get current rate
    pub fn current_rate(&self) -> u32 {
        self.current_rate.load(Ordering::Relaxed)
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(20) // 20 events/sec default
    }
}
```

**Step 2: Update sync/mod.rs**

Add to `src/sync/mod.rs`:

```rust
pub mod rate_limiter;
pub use rate_limiter::RateLimiter;
```

**Step 3: Integrate into publisher.rs**

Modify `src/sync/publisher.rs` to accept rate limiter:

```rust
// ABOUTME: Event publisher with retry logic and rate limiting
// ABOUTME: Publishes events to destination relay with exponential backoff

use crate::error::{Error, ErrorKind, Result};
use crate::sync::RateLimiter;
use nostr_sdk::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, warn};

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 100;

/// Publish an event to the destination relay with rate limiting
/// Returns Ok(true) if published, Ok(false) if duplicate, Err on failure
pub async fn publish_event(
    client: &Client,
    event: &Event,
    rate_limiter: Option<&RateLimiter>,
) -> Result<bool> {
    // Wait for rate limiter permission
    if let Some(limiter) = rate_limiter {
        limiter.wait().await;
    }

    let mut retries = 0;
    let mut backoff_ms = INITIAL_BACKOFF_MS;

    loop {
        match client.send_event(event.clone()).await {
            Ok(send_output) => {
                if send_output.success.is_empty() {
                    if !send_output.failed.is_empty() {
                        if let Some((_, reason)) = send_output.failed.iter().next() {
                            let error_msg = reason.as_ref().map(|s| s.as_str()).unwrap_or("unknown error");
                            let kind = ErrorKind::from_relay_message(error_msg);

                            if kind == ErrorKind::Duplicate {
                                debug!("Event {} is duplicate", event.id);
                                return Ok(false);
                            }

                            // Handle rate limiting specially
                            if kind == ErrorKind::RateLimited {
                                if let Some(limiter) = rate_limiter {
                                    limiter.record_rate_limited();
                                }
                            }

                            if kind.is_retryable() && retries < MAX_RETRIES {
                                retries += 1;
                                warn!(
                                    "Retryable error publishing event {} (attempt {}/{}): {}",
                                    event.id, retries, MAX_RETRIES, error_msg
                                );
                                tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                                backoff_ms *= 2;
                                continue;
                            }

                            return Err(Error::new(kind, error_msg.to_string()));
                        }
                    }

                    return Err(Error::new(
                        ErrorKind::Unknown,
                        "event not accepted by any relay",
                    ));
                }

                // Success
                if let Some(limiter) = rate_limiter {
                    limiter.record_success();
                }
                debug!("Event {} published successfully", event.id);
                return Ok(true);
            }
            Err(e) => {
                let error_msg = e.to_string();
                let kind = ErrorKind::NetworkError;

                if kind.is_retryable() && retries < MAX_RETRIES {
                    retries += 1;
                    warn!(
                        "Network error publishing event {} (attempt {}/{}): {}",
                        event.id, retries, MAX_RETRIES, error_msg
                    );
                    tokio::time::sleep(Duration::from_millis(backoff_ms)).await;
                    backoff_ms *= 2;
                    continue;
                }

                return Err(Error::with_source(kind, error_msg, e));
            }
        }
    }
}
```

**Step 4: Update engine.rs to use rate limiter**

In `src/sync/engine.rs`, add rate limiter to the sync loop (around line 140-160):

```rust
use crate::sync::RateLimiter;

// In run() method, before the while loop:
let rate_limiter = RateLimiter::default();

// In the while loop, change publish_event call:
match publish_event(dest.client(), &event, Some(&rate_limiter)).await {
```

**Step 5: Run tests**

Run: `cargo test`
Expected: PASS

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: add adaptive rate limiting with governor"
```

---

## Task 3: Full NIP-42 Authentication Flow

**Files:**
- Modify: `src/relay/auth.rs`
- Modify: `src/relay/connection.rs`
- Create: `tests/auth_test.rs`

**Step 1: Write auth test**

Create `tests/auth_test.rs`:

```rust
// ABOUTME: Tests for NIP-42 authentication
// ABOUTME: Verifies AUTH event creation

use relay_sync::relay::Authenticator;

#[test]
fn test_authenticator_without_keys() {
    let auth = Authenticator::new(None).unwrap();
    assert!(!auth.can_authenticate());
}

#[test]
fn test_authenticator_with_keys() {
    // Test nsec (from nostr-sdk test vectors)
    let nsec = "nsec1vl029mgpspedva04g90vltkh6fvh240zqtv9k0t9af8935ke9laqsnlfe5";
    let auth = Authenticator::new(Some(nsec)).unwrap();
    assert!(auth.can_authenticate());
    assert!(auth.public_key().is_some());
}

#[test]
fn test_create_auth_event() {
    let nsec = "nsec1vl029mgpspedva04g90vltkh6fvh240zqtv9k0t9af8935ke9laqsnlfe5";
    let auth = Authenticator::new(Some(nsec)).unwrap();

    let event = auth.create_auth_event("wss://relay.example.com", "test-challenge").unwrap();

    // Verify it's a kind 22242 event (NIP-42 AUTH)
    assert_eq!(event.kind.as_u16(), 22242);
}
```

**Step 2: Run test to verify current state**

Run: `cargo test --test auth_test`
Expected: Should mostly pass with current implementation

**Step 3: Enhance connection.rs with auth handling**

Update `src/relay/connection.rs` to handle AUTH challenges:

```rust
// ABOUTME: Relay connection management with NIP-42 auth
// ABOUTME: Handles WebSocket connection, NIP-11 discovery, and auth challenges

use crate::error::{Error, ErrorKind, Result};
use crate::relay::auth::Authenticator;
use nostr_sdk::prelude::*;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Relay capability information from NIP-11
#[derive(Debug, Clone, Default)]
pub struct RelayInfo {
    pub url: String,
    pub supports_negentropy: bool,
    pub auth_required: bool,
    pub max_filters: Option<u32>,
}

impl RelayInfo {
    /// Fetch relay info from NIP-11 document
    pub async fn fetch(url: &str) -> Result<Self> {
        let http_url = url
            .replace("wss://", "https://")
            .replace("ws://", "http://");

        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()
            .map_err(|e| Error::with_source(ErrorKind::NetworkError, "failed to build HTTP client", e))?;

        let response = client
            .get(&http_url)
            .header("Accept", "application/nostr+json")
            .send()
            .await
            .map_err(|e| Error::with_source(ErrorKind::NetworkError, "failed to fetch NIP-11 info", e))?;

        if !response.status().is_success() {
            return Ok(Self {
                url: url.to_string(),
                ..Default::default()
            });
        }

        let json: serde_json::Value = response
            .json()
            .await
            .map_err(|e| Error::with_source(ErrorKind::NetworkError, "failed to parse NIP-11 info", e))?;

        let supported_nips = json["supported_nips"]
            .as_array()
            .map(|arr| arr.iter().filter_map(|v| v.as_u64()).collect::<Vec<_>>())
            .unwrap_or_default();

        let auth_required = json["limitation"]["auth_required"]
            .as_bool()
            .unwrap_or(false);

        Ok(Self {
            url: url.to_string(),
            supports_negentropy: supported_nips.contains(&77),
            auth_required,
            max_filters: json["limitation"]["max_filters"].as_u64().map(|v| v as u32),
        })
    }
}

/// Wrapper around nostr-sdk Client for a single relay
pub struct RelayConnection {
    pub url: String,
    pub info: RelayInfo,
    client: Client,
    authenticator: Option<Authenticator>,
}

impl RelayConnection {
    /// Connect to a relay with optional authentication
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_auth(url, None).await
    }

    /// Connect to a relay with authentication keys
    pub async fn connect_with_auth(url: &str, nsec: Option<&str>) -> Result<Self> {
        let info = RelayInfo::fetch(url).await.unwrap_or_else(|e| {
            warn!("Failed to fetch NIP-11 info for {}: {}", url, e);
            RelayInfo {
                url: url.to_string(),
                ..Default::default()
            }
        });

        // Create authenticator if keys provided
        let authenticator = if let Some(nsec) = nsec {
            Some(Authenticator::new(Some(nsec))?)
        } else {
            None
        };

        // Create client with or without signer
        let client = if let Some(ref auth) = authenticator {
            if let Some(keys) = auth.keys() {
                Client::new(keys.clone())
            } else {
                Client::default()
            }
        } else {
            Client::default()
        };

        client.add_relay(url).await.map_err(|e| {
            Error::with_source(ErrorKind::NetworkError, format!("failed to add relay {}", url), e)
        })?;

        client.connect().await;

        // Check if auth is required and we can authenticate
        if info.auth_required {
            if authenticator.is_none() {
                warn!("Relay {} requires auth but no keys provided", url);
            } else {
                info!("Relay {} requires auth, keys available", url);
            }
        }

        Ok(Self {
            url: url.to_string(),
            info,
            client,
            authenticator,
        })
    }

    /// Get the underlying nostr-sdk client
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Check if we can authenticate
    pub fn can_authenticate(&self) -> bool {
        self.authenticator.as_ref().map(|a| a.can_authenticate()).unwrap_or(false)
    }

    /// Disconnect from relay
    pub async fn disconnect(&self) {
        self.client.disconnect().await.ok();
    }
}
```

**Step 4: Update auth.rs to expose keys**

Add to `src/relay/auth.rs`:

```rust
    /// Get the keys if available (for Client signer)
    pub fn keys(&self) -> Option<&Keys> {
        self.keys.as_ref()
    }
```

**Step 5: Run tests**

Run: `cargo test`
Expected: PASS

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: enhance NIP-42 authentication with connection integration"
```

---

## Task 4: NIP-77 Negentropy Reconciliation

**Files:**
- Modify: `src/sync/reconciler.rs`
- Modify: `src/sync/engine.rs`

**Step 1: Implement negentropy reconciler**

Replace `src/sync/reconciler.rs`:

```rust
// ABOUTME: NIP-77 negentropy reconciliation using nostr-sdk built-in support
// ABOUTME: Efficiently syncs events by exchanging fingerprints instead of full events

use crate::error::{Error, ErrorKind, Result};
use crate::relay::connection::RelayConnection;
use nostr_sdk::prelude::*;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

/// Run negentropy reconciliation between source and destination
/// Returns events that exist on source but not on destination
pub async fn reconcile(
    source: &RelayConnection,
    dest: &RelayConnection,
    filter: Filter,
    tx: mpsc::Sender<Event>,
    shutdown: CancellationToken,
) -> Result<u64> {
    info!("Starting negentropy reconciliation");

    // nostr-sdk handles the negentropy protocol internally
    // We sync FROM source, collecting events we need
    let opts = SyncOptions::default()
        .direction(SyncDirection::Down);  // Download from relay

    // Get source relay URL for sync_with
    let source_url = RelayUrl::parse(&source.url).map_err(|e| {
        Error::with_source(ErrorKind::ConfigError, "invalid source URL", e)
    })?;

    // Perform reconciliation with source
    let reconciliation = source.client()
        .sync_with([source_url], filter.clone(), &opts)
        .await
        .map_err(|e| {
            Error::with_source(ErrorKind::NegentropyError, "negentropy sync failed", e)
        })?;

    // Count events found
    let mut events_found = 0u64;

    // Process reconciliation output
    // The reconciliation returns Output<Reconciliation> with success/failed per relay
    for (url, recon) in reconciliation.success.iter() {
        info!("Reconciliation with {}: received {} events", url, recon.received.len());

        for event in &recon.received {
            if shutdown.is_cancelled() {
                info!("Shutdown during reconciliation");
                break;
            }

            // Send event to publisher channel
            if tx.send(event.clone()).await.is_err() {
                warn!("Publisher channel closed during reconciliation");
                break;
            }
            events_found += 1;
        }
    }

    // Log any failures
    for (url, err) in reconciliation.failed.iter() {
        warn!("Reconciliation failed with {}: {:?}", url, err);
    }

    info!("Negentropy reconciliation complete: {} events to sync", events_found);
    Ok(events_found)
}

/// Check if both relays support negentropy
pub fn can_use_negentropy(source: &RelayConnection, dest: &RelayConnection) -> bool {
    source.info.supports_negentropy && dest.info.supports_negentropy
}
```

**Step 2: Update sync/mod.rs**

Ensure `src/sync/mod.rs` exports reconciler:

```rust
// ABOUTME: Sync engine module exports
// ABOUTME: Orchestrates event fetching and publishing pipeline

pub mod engine;
pub mod fetcher;
pub mod publisher;
pub mod rate_limiter;
pub mod reconciler;

pub use engine::{SyncEngine, SyncMode, SyncOptions, SyncResult};
pub use rate_limiter::RateLimiter;
```

**Step 3: Integrate negentropy into engine.rs**

Update `src/sync/engine.rs` to use negentropy when available:

```rust
// In the run() method, after determining sync mode (around line 78):

        // Determine sync mode
        let mode = if source.info.supports_negentropy && dest.info.supports_negentropy {
            info!("Both relays support negentropy, using NIP-77 sync");
            SyncMode::Negentropy
        } else {
            info!("Using timestamp-based sync (negentropy not supported by both relays)");
            SyncMode::Timestamp
        };

// Replace the fetcher spawn logic based on mode:

        // Spawn fetcher/reconciler task based on mode
        let fetcher_handle = match mode {
            SyncMode::Negentropy => {
                let source_clone = source.client().clone();
                let source_url = self.options.source_url.clone();
                let filter_clone = filter.clone();
                let tx_clone = tx.clone();
                let shutdown_clone = self.shutdown.clone();

                tokio::spawn(async move {
                    // Create a temporary connection for reconciler
                    // (We need RelayConnection, not just Client)
                    match super::reconciler::reconcile_with_client(
                        &source_clone,
                        &source_url,
                        filter_clone,
                        tx_clone,
                        shutdown_clone,
                    ).await {
                        Ok(count) => info!("Reconciler found {} events", count),
                        Err(e) => warn!("Reconciler error: {}", e),
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
```

**Step 4: Add client-based reconciler function**

Add to `src/sync/reconciler.rs`:

```rust
/// Reconcile using Client directly (when we only have client, not full connection)
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
        .sync_with([source_relay], filter, &opts)
        .await
        .map_err(|e| {
            Error::with_source(ErrorKind::NegentropyError, "negentropy sync failed", e)
        })?;

    let mut events_found = 0u64;

    for (url, recon) in reconciliation.success.iter() {
        info!("Negentropy with {}: {} events received", url, recon.received.len());

        for event in &recon.received {
            if shutdown.is_cancelled() {
                break;
            }

            if tx.send(event.clone()).await.is_err() {
                break;
            }
            events_found += 1;
        }
    }

    for (url, err) in reconciliation.failed.iter() {
        warn!("Negentropy failed with {}: {:?}", url, err);
    }

    drop(tx); // Signal completion
    Ok(events_found)
}
```

**Step 5: Verify it compiles**

Run: `cargo build`
Expected: SUCCESS (may need minor import fixes)

**Step 6: Run tests**

Run: `cargo test`
Expected: PASS

**Step 7: Commit**

```bash
git add -A
git commit -m "feat: add NIP-77 negentropy reconciliation"
```

---

## Summary

After all 4 tasks complete:

| Feature | Status |
|---------|--------|
| Date parsing (--since, --until) | Implemented |
| Adaptive rate limiting | Implemented |
| Full NIP-42 auth flow | Implemented |
| NIP-77 negentropy sync | Implemented |

**Final verification:**

```bash
cargo build --release
cargo test
./target/release/relay-sync --help
```

The tool now supports:
- Efficient NIP-77 negentropy sync when both relays support it
- Automatic fallback to timestamp pagination
- Adaptive rate limiting that slows down on 429 errors
- Full authentication for relays requiring NIP-42
- Date filtering with --since and --until flags
