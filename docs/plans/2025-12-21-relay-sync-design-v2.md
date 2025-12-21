# Divine Relay Sync - Design Document v2

## Overview

A Rust CLI tool to sync Nostr events from one relay to another, with smart pagination (negentropy when supported, timestamp-based fallback) and robust error handling.

**Target relays:**
- relay.divine.video
- relay3.openvine.co
- shugur.poc.dvines.org
- relay.poc.dvines.org

## Architecture (Revised)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           relay-sync CLI                                     │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                  │
│   │   Source    │     │  Reconciler │     │    Dest     │                  │
│   │  Connector  │◄───►│             │◄───►│  Connector  │                  │
│   └─────────────┘     └──────┬──────┘     └─────────────┘                  │
│         │                    │                   │                          │
│         ▼                    ▼                   ▼                          │
│   ┌─────────────┐     ┌─────────────┐     ┌─────────────┐                  │
│   │  NIP-11     │     │  Negentropy │     │  Publisher  │                  │
│   │  Discovery  │     │   Engine    │     │  (Adaptive) │                  │
│   └─────────────┘     └─────────────┘     └─────────────┘                  │
│         │                    │                   │                          │
│         │           ┌───────┴───────┐           │                          │
│         │           ▼               ▼           │                          │
│         │    ┌───────────┐   ┌───────────┐     │                          │
│         │    │ Negentropy│   │ Timestamp │     │                          │
│         │    │   Mode    │   │   Mode    │     │                          │
│         │    └───────────┘   └───────────┘     │                          │
│         │           │               │           │                          │
│         │           └───────┬───────┘           │                          │
│         │                   ▼                   │                          │
│         │           ┌─────────────┐             │                          │
│         │           │   Bounded   │             │                          │
│         └──────────►│   Channel   │◄────────────┘                          │
│                     │  (Events)   │                                         │
│                     └──────┬──────┘                                         │
│                            │                                                 │
│                     ┌──────┴──────┐                                         │
│                     │    State    │                                         │
│                     │   Manager   │                                         │
│                     └─────────────┘                                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Key crates:**
- `nostr-sdk` (0.35+) - Nostr protocol, NIP-77 negentropy support built-in
- `tokio` - Async runtime with channels
- `tokio-util` - CancellationToken for graceful shutdown
- `clap` - CLI parsing
- `indicatif` - Progress bars (with tracing integration)
- `governor` - Adaptive rate limiting
- `anyhow` / `thiserror` - Error handling

## Negentropy Architecture (Fixed)

### The Correct Flow

NIP-77 negentropy is **set reconciliation** - it efficiently finds the diff between two sets without transferring all IDs.

**For relay-to-relay sync:**

```
1. DISCOVER CAPABILITIES
   ├── Query source relay NIP-11 info document
   ├── Query dest relay NIP-11 info document
   └── Check supported_nips for NIP-77 (negentropy)

2. RECONCILIATION (if both support negentropy)
   ├── Open NEG-OPEN to SOURCE with filter
   │   → Build local negentropy model from source's events
   ├── Open NEG-OPEN to DEST with same filter
   │   → Build local negentropy model from dest's events
   ├── Run local negentropy reconciliation
   │   → Computes: source_set MINUS dest_set = missing_ids
   └── Result: List of event IDs missing from dest

3. RECONCILIATION (if only source supports negentropy)
   ├── Open NEG-OPEN to SOURCE with filter
   │   → Get all event IDs from source via negentropy
   ├── Query DEST with standard REQ (batched by ID)
   │   → Check which events dest already has
   └── Result: List of event IDs to sync

4. FALLBACK (timestamp pagination)
   ├── Use since/until + LIMIT filters on SOURCE
   ├── Paginate by created_at cursor
   └── Publish all to dest (let dest dedupe)
```

### NIP-77 Message Protocol

```
Client → Relay: ["NEG-OPEN", <subscription_id>, <filter>, <hex_initial_msg>]
Relay → Client: ["NEG-MSG", <subscription_id>, <hex_response_msg>]
Client → Relay: ["NEG-MSG", <subscription_id>, <hex_next_msg>]
... (iterate until reconciliation complete)
Client → Relay: ["NEG-CLOSE", <subscription_id>]

Error: ["NEG-ERR", <subscription_id>, <reason>]
  - "CLOSED": subscription inactive for too long
  - "BLOCKED": filter too broad or records too old
```

### Capability Detection

Query relay info document (NIP-11):
```bash
curl -H "Accept: application/nostr+json" https://relay.divine.video
```

Response includes:
```json
{
  "supported_nips": [1, 11, 42, 77],
  "limitation": {
    "auth_required": false,
    "max_negentropy_items": 100000
  }
}
```

## Async Pipeline Architecture

### Channel-Based Pipeline with Backpressure

```rust
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

// Bounded channel provides natural backpressure
// When publisher is slow, fetcher blocks on send()
let (event_tx, event_rx) = mpsc::channel::<Event>(1000);

// Shutdown coordination
let shutdown = CancellationToken::new();
```

### Pipeline Flow

```
┌─────────────┐      ┌─────────────────┐      ┌─────────────┐
│   Fetcher   │─────►│ Bounded Channel │─────►│  Publisher  │
│   Task      │      │   (1000 cap)    │      │    Task     │
└─────────────┘      └─────────────────┘      └─────────────┘
      │                      │                       │
      │                      │                       │
      ▼                      ▼                       ▼
 Backpressure:         Automatic              Rate Limited:
 send().await          flow control           adaptive backoff
 blocks when full                             on errors
```

### Graceful Shutdown Pattern

```rust
use tokio::signal;
use tokio_util::sync::CancellationToken;

async fn run_sync(shutdown: CancellationToken) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<Event>(1000);

    // Fetcher task
    let fetch_shutdown = shutdown.clone();
    let fetcher = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = fetch_shutdown.cancelled() => {
                    log::info!("Fetcher stopping, closing channel");
                    drop(tx); // Signal end of events
                    break;
                }
                result = fetch_next_batch() => {
                    for event in result? {
                        tx.send(event).await?;
                    }
                }
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    // Publisher task - drains channel on shutdown
    let publisher = tokio::spawn(async move {
        while let Some(event) = rx.recv().await {
            publish_with_retry(event).await?;
            checkpoint_state()?; // Checkpoint after each successful publish
        }
        log::info!("Channel drained, publisher done");
        Ok::<_, anyhow::Error>(())
    });

    // Wait for SIGINT
    signal::ctrl_c().await?;
    log::info!("Shutdown signal received");
    shutdown.cancel();

    // Wait for tasks with timeout
    tokio::time::timeout(
        Duration::from_secs(30),
        async { tokio::try_join!(fetcher, publisher) }
    ).await??;

    Ok(())
}
```

## NIP-42 Authentication Support

### When AUTH is Required

Relays may require authentication for:
- Reading certain event kinds
- Publishing events
- High-volume queries

Detection:
1. NIP-11 `limitation.auth_required` field
2. `auth-required:` prefix in CLOSED/OK error messages

### AUTH Flow

```
1. Relay → Client: ["AUTH", "<challenge-string>"]

2. Client creates Kind 22242 event:
   {
     "kind": 22242,
     "content": "",
     "tags": [
       ["relay", "wss://relay.divine.video"],
       ["challenge", "<challenge-string>"]
     ],
     "created_at": <now>,
     ... signed with user's private key
   }

3. Client → Relay: ["AUTH", <signed-event-json>]

4. Relay → Client: ["OK", "<event-id>", true, ""]
```

### CLI Key Configuration

```bash
# Via environment variable
export RELAY_SYNC_NSEC="nsec1..."
relay-sync source dest

# Via flag
relay-sync source dest --nsec "nsec1..."

# Via config file
relay-sync --config relays.toml
```

```toml
# relays.toml
[auth]
nsec = "nsec1..."  # Or use env var reference: "${RELAY_SYNC_NSEC}"

[[sync]]
name = "divine-to-openvine"
source = "relay.divine.video"
dest = "relay3.openvine.co"
```

## State Management (Fixed)

### Key Insight: Negentropy is Inherently Resumable

When using negentropy:
- Re-running reconciliation shows only new events
- Events we already synced are now on dest
- The diff only contains what's still missing

**State tracking is primarily for timestamp fallback mode.**

### State Per Sync Configuration

State key = hash of: `sorted(source, dest, kinds, authors)`

```
.relay-sync-state/
├── a3f8c2e1.json           # State file
├── a3f8c2e1.failures.log   # Failed event IDs (append-only)
└── a3f8c2e1.lock           # Lock file for concurrent run prevention
```

### State File Format (Versioned)

```json
{
  "version": 1,
  "source": "relay.divine.video",
  "dest": "relay3.openvine.co",
  "filters": {
    "kinds": [34235],
    "authors": null
  },
  "mode": "negentropy",
  "negentropy": {
    "last_reconciled_at": 1703185200,
    "events_synced": 12453
  },
  "timestamp_fallback": {
    "cursor_created_at": 1703185100,
    "cursor_event_id": "abc123..."
  },
  "failure_count": 23
}
```

### Failures File Format

One entry per line (append-only, parseable):
```
1703185200:abc123def456...:rate_limited
1703185201:789xyz000111...:rejected:duplicate
```

Format: `<timestamp>:<event_id>:<reason>:<detail>`

### Lock File

Simple PID-based lock:
```json
{
  "pid": 12345,
  "started_at": 1703185200,
  "hostname": "macbook.local"
}
```

On startup:
1. Check if lock exists
2. If exists, check if PID is still running
3. If stale, remove and acquire
4. If active, exit with error

## Failure Classification

| Error Type | Action | Retry? |
|------------|--------|--------|
| `duplicate` | Skip silently | No |
| `rate_limited` | Backoff, retry | Yes (with delay) |
| `blocked` | Log, skip | No |
| `invalid` | Log, skip | No |
| `auth_required` | Try AUTH if key available | Once |
| `network_error` | Retry with backoff | Yes (3 attempts) |
| `relay_closed` | Reconnect | Yes |

### Detecting Error Types

Nostr OK message format: `["OK", "<event-id>", false, "reason: message"]`

Parse reason prefix:
- `duplicate:` → already exists
- `blocked:` → policy rejection
- `invalid:` → signature/schema error
- `rate-limited:` → slow down
- `auth-required:` → need authentication
- `restricted:` → authenticated but not authorized
- `error:` → generic server error

## Batch Processing

### Event ID Batching for REQ

Most relays limit `ids` filter to ~1000 entries:

```rust
const BATCH_SIZE: usize = 500; // Conservative

async fn fetch_events_by_ids(
    relay: &Relay,
    ids: Vec<EventId>,
) -> Result<Vec<Event>> {
    let mut all_events = Vec::new();

    for chunk in ids.chunks(BATCH_SIZE) {
        let filter = Filter::new().ids(chunk.to_vec());
        let events = relay.fetch_events(filter).await?;
        all_events.extend(events);
    }

    Ok(all_events)
}
```

### Timestamp Pagination

Use `LIMIT` + cursor-based pagination (more reliable than time windows):

```rust
async fn paginate_by_timestamp(
    relay: &Relay,
    filter: Filter,
    cursor: Option<(Timestamp, EventId)>,
) -> Result<(Vec<Event>, Option<(Timestamp, EventId)>)> {
    let mut filter = filter.limit(500);

    if let Some((ts, id)) = cursor {
        filter = filter.until(ts);
        // Handle same-timestamp events by also checking ID
    }

    let events = relay.fetch_events(filter).await?;

    let next_cursor = events.last().map(|e| (e.created_at, e.id));

    Ok((events, next_cursor))
}
```

## Shutdown State Consistency

### The Problem

On SIGINT, events may be in three states:
1. **Fetched and published** - Safe, state reflects these
2. **In channel** - Fetched but not yet published
3. **In-flight** - Sent to dest, awaiting OK response

### The Solution: Conservative State Tracking

**Principle:** State only advances after confirmed publish.

```rust
// Publisher marks events as "in progress" before sending
// State only updates after OK response

async fn publish_with_state(event: Event, state: &State) -> Result<()> {
    // Send to relay
    let result = relay.publish(&event).await;

    match result {
        Ok(_) => {
            // SUCCESS: Update state checkpoint
            state.mark_synced(event.id, event.created_at)?;
        }
        Err(e) if e.is_duplicate() => {
            // Already exists: Still update state (it's synced)
            state.mark_synced(event.id, event.created_at)?;
        }
        Err(e) => {
            // Failed: Log to failures, don't update checkpoint
            state.log_failure(event.id, &e)?;
        }
    }

    Ok(())
}
```

**On shutdown:**
1. Fetcher stops immediately (channel closes)
2. Publisher drains channel with 30s timeout
3. Events that complete publish → state updated
4. Events still in channel when timeout expires → will be re-fetched next run
5. In-flight events at timeout → will be deduped by dest relay

**Result:** State never advances past what's confirmed. Re-running may re-sync some events, but dest dedupes them.

### Same-Timestamp Pagination

Events can have identical `created_at`. To avoid skipping or duplicating:

```rust
struct Cursor {
    created_at: Timestamp,
    last_event_id: EventId,
}

async fn paginate(cursor: Option<Cursor>) -> Result<(Vec<Event>, Option<Cursor>)> {
    let mut filter = Filter::new().limit(500);

    if let Some(c) = cursor {
        // Events at or before this timestamp
        filter = filter.until(c.created_at);
    }

    let mut events = relay.fetch_events(filter).await?;

    if let Some(c) = cursor {
        // Remove events we've already seen (same timestamp, ID <= cursor)
        events.retain(|e| {
            if e.created_at == c.created_at {
                e.id > c.last_event_id  // Lexicographic ID comparison
            } else {
                true
            }
        });
    }

    let next_cursor = events.last().map(|e| Cursor {
        created_at: e.created_at,
        last_event_id: e.id,
    });

    Ok((events, next_cursor))
}
```

### Negentropy Mid-Stream Failure

If relay drops during NEG-MSG exchange:

```rust
async fn reconcile_with_retry(source: &Relay, dest: &Relay) -> Result<Vec<EventId>> {
    let mut attempts = 0;
    loop {
        match try_reconcile(source, dest).await {
            Ok(ids) => return Ok(ids),
            Err(e) if e.is_connection_error() && attempts < 3 => {
                attempts += 1;
                log::warn!("Negentropy interrupted, retrying ({}/3)", attempts);
                tokio::time::sleep(Duration::from_secs(1 << attempts)).await;
                // Reconnect happens automatically in nostr-sdk
            }
            Err(e) => return Err(e),
        }
    }
}
```

If retries exhausted: fall back to timestamp pagination mode.

## Rate Limiting (Adaptive)

Using `governor` crate with adaptive backoff:

```rust
use governor::{Quota, RateLimiter, state::NotKeyed, clock::DefaultClock};
use std::sync::{Arc, atomic::{AtomicU32, Ordering}};

// Wrapped in Arc for sharing across tasks
struct AdaptivePublisher {
    relay: Relay,
    limiter: Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>,
    current_rate: AtomicU32,
    min_rate: u32,
    max_rate: u32,
}

impl AdaptivePublisher {
    async fn publish(&self, event: Event) -> Result<()> {
        self.limiter.until_ready().await;

        match self.relay.publish(event).await {
            Ok(_) => {
                // Success: gradually increase rate
                self.increase_rate();
            }
            Err(e) if e.is_rate_limited() => {
                // Rate limited: back off exponentially
                self.decrease_rate();
                tokio::time::sleep(self.backoff_duration()).await;
                // Retry will happen on next iteration
            }
            Err(e) => return Err(e.into()),
        }

        Ok(())
    }

    fn increase_rate(&self) {
        let current = self.current_rate.load(Ordering::Relaxed);
        let new_rate = (current + current / 10).min(self.max_rate); // +10%
        self.current_rate.store(new_rate, Ordering::Relaxed);
        self.update_limiter(new_rate);
    }

    fn decrease_rate(&self) {
        let current = self.current_rate.load(Ordering::Relaxed);
        let new_rate = (current / 2).max(self.min_rate); // Halve on error
        self.current_rate.store(new_rate, Ordering::Relaxed);
        self.update_limiter(new_rate);
    }
}
```

## CLI Interface

### Basic Usage

```bash
# Sync all events (wss:// prefix optional)
relay-sync relay.divine.video relay3.openvine.co

# With explicit wss://
relay-sync wss://shugur.poc.dvines.org wss://relay.poc.dvines.org
```

### Filtering Options

```bash
relay-sync source dest --kind 34235        # Video events only
relay-sync source dest --kind 1 --kind 7   # Notes and reactions (OR)
relay-sync source dest --author npub1...   # Specific pubkey
relay-sync source dest --since 2024-01-01  # From date
relay-sync source dest --until 2024-12-01  # To date
```

**Filter semantics:** Multiple `--kind` = OR, multiple `--author` = OR, kind + author = AND (matches Nostr filter behavior).

### Control Flags

```bash
relay-sync source dest --fresh             # Ignore state, start over
relay-sync source dest --retry-failures    # Retry only failed events
relay-sync source dest --dry-run           # Show what would sync (no publish)
relay-sync source dest --quiet             # Minimal output
relay-sync source dest --json              # Machine-readable output
relay-sync source dest --nsec "nsec1..."   # Auth key for private relays
```

### Dry Run Behavior

`--dry-run`:
- Connects to relays (tests connectivity)
- Runs negentropy/pagination (counts events)
- Does NOT publish events
- Does NOT update state
- Output: "Would sync N events from source to dest"

### Config File

```bash
relay-sync --config relays.toml
relay-sync --config relays.toml --name divine-to-openvine
relay-sync --config relays.toml --all  # Run all sync configs
```

**relays.toml:**

```toml
[auth]
nsec = "${RELAY_SYNC_NSEC}"  # Environment variable

[[sync]]
name = "divine-to-openvine"
source = "relay.divine.video"
dest = "relay3.openvine.co"

[[sync]]
name = "videos-only"
source = "shugur.poc.dvines.org"
dest = "relay.poc.dvines.org"
kinds = [34235]
```

**Precedence:** CLI flags override config file values.

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Source relay unreachable | Retry 3x with backoff, exit code 2 |
| Dest relay unreachable | Same - retry, then fail |
| Source drops mid-sync | Reconnect, resume from channel |
| Dest drops mid-sync | Reconnect, retry in-flight event |
| Event rejected (duplicate) | Skip silently, continue |
| Event rejected (blocked) | Log to failures, continue |
| Rate limited | Adaptive backoff, retry |
| AUTH required | Try auth if key available, else skip |
| Negentropy too large | Fall back to timestamp pagination |
| SIGINT | Cancel fetcher, drain publisher (30s timeout) |
| Invalid event from source | Log warning, skip |

### Exit Codes

- `0` - Success (all events synced)
- `1` - Partial success (some failures, logged)
- `2` - Connection failed
- `3` - Config/argument error
- `4` - Auth required but no key provided
- `130` - Interrupted (SIGINT)

## Project Structure

```
divine-relay-sync/
├── Cargo.toml
├── src/
│   ├── main.rs              # CLI entry, signal handling
│   ├── lib.rs               # Public API
│   ├── cli.rs               # Clap definitions
│   ├── config.rs            # TOML config + env vars
│   ├── relay/
│   │   ├── mod.rs
│   │   ├── connection.rs    # WebSocket, auto wss://, NIP-11
│   │   ├── auth.rs          # NIP-42 authentication
│   │   └── negentropy.rs    # NIP-77 reconciliation
│   ├── sync/
│   │   ├── mod.rs
│   │   ├── engine.rs        # Pipeline orchestration
│   │   ├── reconciler.rs    # Negentropy vs timestamp logic
│   │   ├── fetcher.rs       # Source relay fetching
│   │   └── publisher.rs     # Dest relay publishing + rate limit
│   ├── state/
│   │   ├── mod.rs
│   │   ├── manager.rs       # State file + locking
│   │   └── failures.rs      # Failures log handling
│   ├── error.rs             # Error types with thiserror
│   └── output/
│       ├── mod.rs
│       ├── progress.rs      # Progress bar + logs
│       └── json.rs          # --json output
└── tests/
    ├── integration/
    │   ├── negentropy_test.rs
    │   ├── timestamp_test.rs
    │   └── auth_test.rs
    └── fixtures/
        └── test_events.json
```

## Dependencies

```toml
[dependencies]
# Nostr
nostr-sdk = "0.35"

# Async runtime
tokio = { version = "1", features = ["full", "signal"] }
tokio-util = { version = "0.7", features = ["sync"] }

# CLI
clap = { version = "4", features = ["derive", "env"] }

# Config
toml = "0.8"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Output
indicatif = "0.17"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
tracing-indicatif = "0.3"  # Progress bar integration

# Rate limiting
governor = "0.6"

# Error handling
anyhow = "1"
thiserror = "2"

# Utilities
chrono = { version = "0.4", features = ["serde"] }
sha2 = "0.10"  # For state file hashing
```

## Design Decisions

1. **Negentropy-first architecture** - Use set reconciliation when both relays support NIP-77. Fall back to timestamp pagination only when necessary.

2. **Inherent resumability** - Negentropy mode doesn't need progress tracking; re-running computes fresh diff. State is primarily for timestamp fallback.

3. **Pipeline with backpressure** - Bounded mpsc channel between fetcher and publisher. Automatic flow control when publisher is slower.

4. **Checkpoint after publish** - State updates after successful publish acknowledgment, not after fetch. Prevents duplicate sync on crash.

5. **Adaptive rate limiting** - Starts at max speed, backs off on errors, recovers gradually. No manual tuning needed.

6. **Failure classification** - Different error types get different handling. Duplicates are silent, rate limits retry, blocks are logged.

7. **Graceful shutdown** - SIGINT cancels fetcher, drains remaining events from channel with timeout.

8. **State file versioning** - Version field enables future migrations without breaking existing state.

9. **Lock files for concurrency** - Prevents multiple instances from corrupting same state file.

10. **CLI precedence over config** - Explicit flags override config file for flexibility.

## Testing Strategy

### Unit Tests
- Filter parsing and serialization
- State file read/write/versioning
- Failure classification logic
- URL normalization (add wss://)

### Integration Tests (against local relay)
- Negentropy reconciliation flow
- Timestamp pagination
- AUTH challenge/response
- Rate limiting behavior
- Graceful shutdown + drain

### Test Relay Setup
```bash
# Use strfry for testing (supports negentropy)
docker run -p 7777:7777 hoytech/strfry
```

## Future Enhancements

- **Bidirectional sync** - Full merge between relays (requires conflict handling)
- **Multi-relay fan-out** - Sync from one source to multiple destinations
- **Continuous mode** - `--follow` flag to keep syncing new events
- **Prometheus metrics** - Events/sec, failures, latency
- **WebSocket reconnection** - Automatic reconnect with backoff
