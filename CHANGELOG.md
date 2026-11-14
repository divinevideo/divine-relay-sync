# Changelog

## [Unreleased]

### Added
- **Event filtering**: Default exclusion of kind 1 (notes) and kind 5 (deletions) to focus on long-form content and metadata
  - `--include-notes` flag to re-include kind 1 events
  - `--include-deletions` flag to re-include kind 5 events
  - `--exclude-tag TAG:VALUE` to exclude events with specific tags
  - `--tag TAG:VALUE` to require events have specific tags
  - Config file support for all filtering options
- **Connection fallback**: Automatically tries ws:// if wss:// fails (and vice versa)
- **Stale lock detection**: Removes a lock whose local owner process is dead; a lock from another host or with an unreadable owner is kept until it is removed by hand, and run age alone never releases one
- **Warning counters**: Tracks expired and oversized events separately without log spam
- Default exclusion of `L:pink.momostr` tagged events (momostr reactions)

### Changed
- Improved graceful shutdown handling in the event fetcher
- Better connection status verification before proceeding with sync

### Fixed
- Refuse incomplete timestamp scans: verify exact NIP-45 counts, split capped time windows, and fail instead of skipping unseen events in a saturated second.
- Save atomic checkpoints under the full selection policy only after successful fetching and publishing; replay failed intervals and the last completed second safely.
- Preserve explicit duplicate acknowledgements and propagate source, NIP-77, cancellation and destination errors without advancing progress.
- Retain received events for NIP-77 transfers, and keep active process locks even during long-running syncs.
- Checkpoint the run start time when `--until` lies in the future, so the next run still scans events created after this one.
- Skip NIP-40-expired source events in timestamp mode instead of republishing them.
- Connection failures now properly detected instead of silently proceeding
