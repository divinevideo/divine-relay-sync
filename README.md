# relay-sync

Sync Nostr events between relays using [NIP-77 negentropy](https://github.com/nostr-protocol/nips/blob/master/77.md) for efficient reconciliation.

## Installation

```bash
cargo install --path .
```

Or build from source:

```bash
cargo build --release
```

## Usage

```bash
# Basic sync (wss:// prefix optional)
relay-sync source.relay.com dest.relay.com

# Filter by event kinds
relay-sync source.relay.com dest.relay.com -k 1 -k 30023

# Filter by authors
relay-sync source.relay.com dest.relay.com -a <pubkey>

# Date range
relay-sync source.relay.com dest.relay.com --since 2024-01-01 --until 2024-12-31

# Dry run (show what would sync)
relay-sync source.relay.com dest.relay.com --dry-run

# With authentication (for write-protected relays)
RELAY_SYNC_NSEC=nsec1... relay-sync source.relay.com dest.relay.com
```

## Options

| Flag | Description |
|------|-------------|
| `-k, --kinds <KIND>` | Filter by event kind (repeatable) |
| `-a, --authors <PUBKEY>` | Filter by author pubkey (repeatable) |
| `--since <DATE>` | Sync events after date (YYYY-MM-DD) |
| `--until <DATE>` | Sync events before date (YYYY-MM-DD) |
| `--fresh` | Ignore saved state, start fresh |
| `--dry-run` | Show what would sync without publishing |
| `-q, --quiet` | Minimal output |
| `-v, --verbose` | Debug logging |
| `--json` | Output results as JSON |
| `-c, --config <FILE>` | Use config file |
| `--name <NAME>` | Select sync config by name |

## Config File

For recurring syncs, use a TOML config file:

```toml
[auth]
nsec = "${RELAY_SYNC_NSEC}"  # reads from environment variable

[[sync]]
name = "my-sync"
source = "source.relay.com"
dest = "dest.relay.com"
kinds = [1, 30023]
authors = ["pubkey1", "pubkey2"]
```

Then run:

```bash
relay-sync -c config.toml --name my-sync
```

## How It Works

1. Connects to both relays
2. Uses NIP-77 negentropy to efficiently find missing events
3. Fetches missing events from source
4. Publishes to destination with rate limiting
5. Saves progress for resumable syncs

State is saved in `.relay-sync-state/` for resuming interrupted syncs.

## License

MIT
