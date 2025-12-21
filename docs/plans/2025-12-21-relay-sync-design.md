# Divine Relay Sync - Design Document

## Overview

A Rust CLI tool to sync Nostr events from one relay to another, with smart pagination (negentropy when available, timestamp-based fallback) and robust error handling.

**Target relays:**
- relay.divine.video
- relay3.openvine.co
- shugur.poc.dvines.org
- relay.poc.dvines.org

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                      relay-sync CLI                         │
├─────────────────────────────────────────────────────────────┤
│  Source Relay          Sync Engine           Dest Relay     │
│  ┌───────────┐        ┌───────────┐        ┌───────────┐   │
│  │ Connector │───────▶│ Paginator │───────▶│ Publisher │   │
│  │           │        │           │        │           │   │
│  │ • ws/wss  │        │ • Negen-  │        │ • Adaptive│   │
│  │ • auto-   │        │   tropy   │        │   rate    │   │
│  │   prefix  │        │ • Time-   │        │ • Retry   │   │
│  └───────────┘        │   stamp   │        │ • Backoff │   │
│                       └───────────┘        └───────────┘   │
│                            │                               │
│                       ┌────┴────┐                          │
│                       │  State  │                          │
│                       │  File   │                          │
│                       └─────────┘                          │
└─────────────────────────────────────────────────────────────┘
```

**Key crates:**
- `nostr-sdk` - Nostr protocol, event handling, relay connections
- `negentropy` - Efficient set reconciliation (NIP-77)
- `clap` - CLI parsing
- `tokio` - Async runtime
- `indicatif` - Progress bars

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
relay-sync source dest --kind 34235        # video events only
relay-sync source dest --author npub1...   # specific pubkey
relay-sync source dest --since 2024-01-01  # from date
relay-sync source dest --until 2024-12-01  # to date
```

### Control Flags

```bash
relay-sync source dest --fresh             # ignore state, start over
relay-sync source dest --retry-failures    # retry only failed events
relay-sync source dest --dry-run           # show what would sync
relay-sync source dest --quiet             # minimal output
relay-sync source dest --json              # machine-readable output
```

### Config File (Optional)

```bash
relay-sync --config relays.toml
relay-sync --config relays.toml --name divine-to-openvine
```

**relays.toml format:**

```toml
[[sync]]
name = "divine-to-openvine"
source = "relay.divine.video"
dest = "relay3.openvine.co"

[[sync]]
name = "shugur-to-poc"
source = "shugur.poc.dvines.org"
dest = "relay.poc.dvines.org"
kinds = [34235]  # only video events
```

## Sync Algorithm

### 1. CONNECT
- Parse relay URLs (add wss:// if missing)
- Connect to source relay
- Connect to dest relay
- Load state file if exists (unless --fresh)

### 2. NEGOTIATE
- Send NIP-77 negentropy handshake to both relays
- If source supports negentropy:
  - Use set reconciliation (efficient diff)
  - Get exact list of missing event IDs
- Else:
  - Fall back to timestamp pagination
  - Use since/until windows (e.g., 1 hour chunks)
  - Work backwards from now to last synced timestamp

### 3. FETCH
- Request events from source (by ID if negentropy, by time window if timestamp)
- Filter locally if --kind/--author specified
- Stream events to publisher

### 4. PUBLISH
- Send events to dest relay
- Adaptive rate limiting:
  - Start at max speed
  - On OK responses: maintain or increase
  - On rate-limit/error: exponential backoff
- Retry failed events (3 attempts, backoff)
- Log failures to separate file

### 5. CHECKPOINT
- Update state file periodically (every N events)
- On completion: write final state
- On interrupt (SIGINT): save current progress

## State Management

State is keyed by unique sync configuration (source + dest + filters).

### Directory Structure

```
.relay-sync-state/
├── a3f8c2e1.json           # state for one config
├── a3f8c2e1.failures       # failed event IDs
├── b7d4a9f0.json           # state for another config
└── b7d4a9f0.failures
```

### State File Format

```json
{
  "source": "relay.divine.video",
  "dest": "relay3.openvine.co",
  "filters": {
    "kinds": [34235],
    "authors": null,
    "since": null,
    "until": null
  },
  "last_synced_at": 1703185200,
  "last_event_created_at": 1703185100,
  "events_synced": 12453,
  "failure_count": 23
}
```

### Failures File Format

One event ID per line (append-only):

```
abc123def456...
789xyz000111...
```

## Output Format

### Progress with Logs (Default)

```
Connecting to relay.divine.video... OK
Connecting to relay3.openvine.co... OK
Checking negentropy support... source: yes, dest: no
Using state: .relay-sync-state/a3f8c2e1.json (last run: 2 hours ago)
Syncing events...
[████████░░░░░░░░] 2,341 / ~5,000 events | 47/sec | eta 56s
  → kind:34235 video event e3a8f2...
  → kind:1 note 8b2c91...
✓ Synced 5,012 events (23 already existed, 1 failed)
```

## Error Handling

| Scenario | Behavior |
|----------|----------|
| Source relay unreachable | Retry 3x with backoff, then exit with error |
| Dest relay unreachable | Same - retry, then fail |
| Source drops mid-sync | Reconnect, resume from last checkpoint |
| Dest drops mid-sync | Reconnect, continue from current event |
| Event rejected by dest | Log to failures file, continue |
| Rate limited by dest | Back off exponentially (100ms → 200ms → 400ms...) |
| Invalid event from source | Skip, warn in logs |
| Ctrl+C interrupt | Save state immediately, exit cleanly |
| Disk full (can't write state) | Warn loudly, continue sync, exit 1 |
| Negentropy handshake fails | Fall back to timestamp pagination silently |

### Exit Codes

- `0` - Success (all events synced or already existed)
- `1` - Partial success (some failures, logged to file)
- `2` - Connection failed (couldn't reach relay)
- `3` - Config error (bad args, missing relay)

## Project Structure

```
divine-relay-sync/
├── Cargo.toml
├── src/
│   ├── main.rs              # CLI entry, arg parsing
│   ├── lib.rs               # Public API (for library use)
│   ├── cli.rs               # Clap definitions
│   ├── config.rs            # TOML config parsing
│   ├── relay/
│   │   ├── mod.rs
│   │   ├── connection.rs    # WebSocket handling, auto wss://
│   │   └── negentropy.rs    # NIP-77 detection & reconciliation
│   ├── sync/
│   │   ├── mod.rs
│   │   ├── engine.rs        # Main sync orchestration
│   │   ├── paginator.rs     # Negentropy vs timestamp logic
│   │   └── publisher.rs     # Adaptive rate, retry, backoff
│   ├── state/
│   │   ├── mod.rs
│   │   ├── manager.rs       # State file CRUD
│   │   └── failures.rs      # Failures file handling
│   └── output/
│       ├── mod.rs
│       ├── progress.rs      # Progress bar + logs
│       └── json.rs          # --json output format
└── tests/
    └── integration/         # Against local test relays
```

## Dependencies

```toml
[dependencies]
nostr-sdk = "0.35"
negentropy = "0.3"
tokio = { version = "1", features = ["full"] }
clap = { version = "4", features = ["derive"] }
indicatif = "0.17"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"
tracing = "0.1"
tracing-subscriber = "0.3"
```

## Design Decisions

1. **One-way sync only** - Bidirectional adds complexity (conflict handling, loop prevention). Can add later if needed.

2. **Adaptive rate limiting** - Starts fast, backs off automatically. No manual tuning required.

3. **State per configuration** - Different filter combinations get separate state files. Changing filters doesn't corrupt existing sync progress.

4. **Failures in separate file** - Keeps state file small, allows thousands of failures without performance impact.

5. **Negentropy first, timestamp fallback** - Best efficiency when supported, graceful degradation when not.
