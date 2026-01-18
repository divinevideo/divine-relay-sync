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
- **Stale lock detection**: Automatically removes locks from dead processes or locks older than 1 hour
- **Warning counters**: Tracks expired and oversized events separately without log spam
- Default exclusion of `L:pink.momostr` tagged events (momostr reactions)

### Changed
- Improved graceful shutdown handling in the event fetcher
- Better connection status verification before proceeding with sync

### Fixed
- Connection failures now properly detected instead of silently proceeding
