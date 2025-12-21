// ABOUTME: CLI argument definitions using clap
// ABOUTME: Supports positional args, filters, and control flags

use clap::Parser;
use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};

#[derive(Parser, Debug)]
#[command(name = "relay-sync")]
#[command(about = "Sync Nostr events between relays")]
#[command(version)]
pub struct Cli {
    /// Source relay URL (wss:// prefix optional)
    pub source: Option<String>,

    /// Destination relay URL (wss:// prefix optional)
    pub dest: Option<String>,

    /// Filter by event kind (can specify multiple)
    #[arg(short, long, value_name = "KIND")]
    pub kinds: Vec<u16>,

    /// Filter by author pubkey (can specify multiple)
    #[arg(short, long, value_name = "PUBKEY")]
    pub authors: Vec<String>,

    /// Sync events created after this date (YYYY-MM-DD)
    #[arg(long, value_name = "DATE")]
    pub since: Option<String>,

    /// Sync events created before this date (YYYY-MM-DD)
    #[arg(long, value_name = "DATE")]
    pub until: Option<String>,

    /// Ignore saved state, start fresh
    #[arg(long)]
    pub fresh: bool,

    /// Retry only previously failed events
    #[arg(long)]
    pub retry_failures: bool,

    /// Show what would sync without publishing
    #[arg(long)]
    pub dry_run: bool,

    /// Minimal output
    #[arg(short, long)]
    pub quiet: bool,

    /// Output as JSON
    #[arg(long)]
    pub json: bool,

    /// Verbose debug logging
    #[arg(short, long)]
    pub verbose: bool,

    /// Private key for authentication (prefer env var RELAY_SYNC_NSEC)
    #[arg(long, env = "RELAY_SYNC_NSEC", hide_env_values = true)]
    pub nsec: Option<String>,

    /// Path to config file
    #[arg(short, long, value_name = "FILE")]
    pub config: Option<String>,

    /// Sync config name (when using --config)
    #[arg(long, value_name = "NAME")]
    pub name: Option<String>,

    /// Run all sync configs (when using --config)
    #[arg(long)]
    pub all: bool,
}

/// Normalize relay URL by adding wss:// if missing
pub fn normalize_relay_url(url: &str) -> String {
    if url.starts_with("wss://") || url.starts_with("ws://") {
        url.to_string()
    } else {
        format!("wss://{}", url)
    }
}

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

impl Cli {
    /// Get normalized source URL
    pub fn source_url(&self) -> Option<String> {
        self.source.as_ref().map(|s| normalize_relay_url(s))
    }

    /// Get normalized destination URL
    pub fn dest_url(&self) -> Option<String> {
        self.dest.as_ref().map(|s| normalize_relay_url(s))
    }
}
