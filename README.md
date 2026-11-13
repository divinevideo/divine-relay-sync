# Divine Relay Sync

A command-line tool for copying Nostr events from one relay to another. It uses [NIP-77 negentropy](https://github.com/nostr-protocol/nips/blob/master/77.md) to reconcile the source event set, then fetches and republishes events to the destination. Divine runs its own relay infrastructure, and `relay-sync` is the utility that keeps events consistent across those relays, seeds a new relay from an existing one, or mirrors specific content between relays.

## Features

- **Source-set reconciliation** via NIP-77 negentropy when both relays support it, with explicit duplicate handling at the destination.
- **Strict timestamp fallback** when either relay lacks NIP-77 support. Exact NIP-45 counts are required to verify each fetched window; unsupported, approximate or missing counts fail the run.
- **Relay capability discovery** through NIP-11, used to detect negentropy support and authentication requirements.
- **NIP-42 authentication** for reading from or writing to protected relays, with the signing key supplied via environment variable.
- **Connection fallback** that tries `ws://` when `wss://` fails (and vice versa).
- **Flexible filtering** by event kind, author, date range, and tags. By default, kind 1 (notes), kind 5 (deletions), and `L:pink.momostr` tagged events are excluded to focus on long-form content and metadata.
- **Successful-run checkpoints** with on-disk state. Interrupted or partial runs replay their uncommitted interval rather than skipping uncertain events.
- **Bounded concurrent publishing** with retries for retryable destination errors.
- **Config file support** for defining and re-running named syncs.

## Architecture

`relay-sync` connects to a source relay and a destination relay, then runs a fetch-and-publish pipeline between them.

**Capability discovery.** On connect, the tool fetches each relay's NIP-11 document to learn whether it supports NIP-77 negentropy (NIP 77 in `supported_nips`) and whether it requires authentication.

**Sync mode selection.** If both relays advertise NIP-77 support, the tool uses negentropy reconciliation. It exchanges compact fingerprints with the source to identify event IDs absent from this run's local receive database, then streams the received events into the publish pipeline. The destination set is not preloaded; explicit duplicate acknowledgements handle events already present there. If either relay lacks NIP-77 support, it uses exact NIP-45 counts before and after each timestamp-window read. Capped windows split into disjoint time ranges. If a single second still cannot be fully retrieved, or the source cannot provide an exact count, the run fails without advancing its checkpoint. Counts describe the selected source relay, not global Nostr coverage. Matching counts do not provide an atomic snapshot of a changing relay.

**Pipeline.** Fetched events flow through an in-memory channel to a pool of publisher workers that send events to the destination concurrently (at most 200 active publisher tasks), applying kind and tag exclusion filters along the way. Duplicates reported by the destination are counted as skipped rather than failures.

**State and resumption.** State is atomically replaced under `.relay-sync-state/` only after the entire requested interval has been fetched and all selected events have been acknowledged by the destination (including explicit duplicates). The key includes source, destination, kinds, authors, time bounds, tag/exclusion policy and authentication public key. Credentials are not stored in the selection key. Failures, cancellation and dry runs leave the prior checkpoint unchanged; already accepted events may be replayed on retry. A process lock prevents concurrent runs with the same selection, and only a demonstrably dead local process has its stale lock removed.

Each successful run records its fixed upper time bound and acknowledged-event progress. The next run re-reads that boundary second inclusively, then scans newer timestamps. `--fresh` ignores that checkpoint and scans the whole configured range. Legacy checkpoints use a different namespace and are deliberately not reused, resulting in one safe full rescan.

Timestamp checkpoints cannot discover a newly arriving, backdated event older than their lower bound. For recurring contact-list reconciliation, refresh discovered authors over the full configured range with `--fresh`; a forward timestamp scan alone is not a freshness guarantee. Required `--tag p:<target>` filtering is suitable for discovering candidates, but it discards a newer contact list that removes that target. Refresh authors without that required tag to propagate unfollows.

```bash
# Refresh previously discovered authors, including newer unfollows
relay-sync source.relay.com dest.relay.com -k 3 -a <hex-pubkey> --fresh
```

The CLI exits nonzero for incomplete source reads, unsupported/approximate counts, timeouts, cancellation or rejected destination publishes. No success checkpoint is written in those cases. NIP-77 failures and missing received events also fail rather than silently reporting a successful transfer. The SDK retains at most 100,000 events in memory for NIP-77; a run exceeding that retention may fail closed and must be narrowed with kinds, authors or time bounds. Timestamp windows retain at most 500 events each and split when a source serves fewer than its exact count.

Within Divine's relay infrastructure, this makes `relay-sync` the building block for keeping relays in sync, backfilling a new relay, and mirroring targeted subsets of content between relays.

## Getting started

Build from source with Cargo:

```bash
cargo build --release
```

Or install the binary onto your `PATH`:

```bash
cargo install --path .
```

Run it directly during development:

```bash
cargo run -- source.relay.com dest.relay.com
```

### Usage

```bash
# Basic sync (wss:// prefix optional)
relay-sync source.relay.com dest.relay.com

# Filter by event kinds
relay-sync source.relay.com dest.relay.com -k 1 -k 30023

# Filter by authors (hex pubkeys)
relay-sync source.relay.com dest.relay.com -a <pubkey>

# Date range
relay-sync source.relay.com dest.relay.com --since 2024-01-01 --until 2024-12-31

# Dry run (count what would sync without publishing)
relay-sync source.relay.com dest.relay.com --dry-run

# With authentication (for protected relays)
RELAY_SYNC_NSEC=nsec1... relay-sync source.relay.com dest.relay.com
```

Relay arguments accept a bare host, a `wss://` URL, or a `ws://` URL. Dates accept `YYYY-MM-DD`, `YYYY-MM-DDTHH:MM:SS`, or a raw Unix timestamp. The process exits nonzero if source coverage is incomplete or any selected event fails to publish.

## Configuration

### CLI flags

| Flag | Description |
|------|-------------|
| `-k, --kinds <KIND>` | Filter by event kind (repeatable) |
| `-a, --authors <PUBKEY>` | Filter by author pubkey, hex (repeatable) |
| `--since <DATE>` | Sync events created after this date |
| `--until <DATE>` | Sync events created before this date |
| `--include-notes` | Include kind 1 events (excluded by default) |
| `--include-deletions` | Include kind 5 events (excluded by default) |
| `--exclude-tag <TAG:VALUE>` | Exclude events with a tag (e.g. `L:pink.momostr`) |
| `--tag <TAG:VALUE>` | Require events to have a tag (e.g. `t:nostr`) |
| `--fresh` | Ignore saved state and start from scratch |
| `--dry-run` | Count what would sync without publishing |
| `-q, --quiet` | Minimal output |
| `-v, --verbose` | Debug logging |
| `--json` | Emit results as JSON |
| `-c, --config <FILE>` | Load a TOML config file |
| `--name <NAME>` | Select a named sync from the config file |

By default, kind 1 (notes), kind 5 (deletions), and events tagged `L:pink.momostr` are excluded. Use `--include-notes` and `--include-deletions` to re-include the first two.

### Authentication

The signing key for NIP-42 authentication is read from the `RELAY_SYNC_NSEC` environment variable (preferred) or the `--nsec` flag. Prefer the environment variable so the key does not appear in shell history or process listings.

### Config file

For recurring or multi-relay syncs, define them in a TOML file:

```toml
[auth]
nsec = "${RELAY_SYNC_NSEC}"  # ${VAR} is resolved from the environment

[[sync]]
name = "my-sync"
source = "source.relay.com"
dest = "dest.relay.com"
kinds = [1, 30023]
authors = ["pubkey1", "pubkey2"]
include_notes = true         # include kind 1 (default: false)
include_deletions = false    # include kind 5 (default: false)
exclude_tags = ["L:pink.momostr"]
tags = ["t:nostr"]           # require events to have this tag
```

Then run a named sync:

```bash
relay-sync -c config.toml --name my-sync
```

A config with a single `[[sync]]` block runs without `--name`; if it defines more than one, `--name` selects which to run.

## Deployment

`relay-sync` is a standalone binary with no server component. For a one-off migration or backfill, run it directly. For ongoing consistency between relays, schedule it (for example with cron or a systemd timer) using a config file; successful-run checkpoints under `.relay-sync-state/` let each scheduled run replay the last boundary second and fetch newer events, while the selection lock prevents overlapping runs. Interrupted intervals are retried from the last successful checkpoint. Source quotas, scheduling, durable author inventory and a production reconciliation service are outside this CLI. Provide `RELAY_SYNC_NSEC` in the scheduled environment when the destination relay requires authentication.

## Development

```bash
cargo check   # fast compile-only pass
cargo test    # run the test suite
cargo build --release
```

See `AGENTS.md` for repository conventions and contribution guardrails.

## License

MIT

---

Part of [Divine](https://divine.video) — your playground for human creativity · [Brand guidelines](https://github.com/divinevideo/brand-guidelines)
