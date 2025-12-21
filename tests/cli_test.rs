// ABOUTME: Tests for CLI argument parsing
// ABOUTME: Verifies all flags and argument combinations

use clap::Parser;
use relay_sync::cli::{Cli, normalize_relay_url};

#[test]
fn test_basic_sync_args() {
    let cli = Cli::parse_from(["relay-sync", "relay.source.com", "relay.dest.com"]);
    assert_eq!(cli.source, Some("relay.source.com".to_string()));
    assert_eq!(cli.dest, Some("relay.dest.com".to_string()));
}

#[test]
fn test_kind_filter() {
    let cli = Cli::parse_from([
        "relay-sync",
        "source",
        "dest",
        "--kinds", "1",
        "--kinds", "7",
    ]);
    assert_eq!(cli.kinds, vec![1, 7]);
}

#[test]
fn test_control_flags() {
    let cli = Cli::parse_from([
        "relay-sync",
        "source",
        "dest",
        "--fresh",
        "--dry-run",
        "--verbose",
    ]);
    assert!(cli.fresh);
    assert!(cli.dry_run);
    assert!(cli.verbose);
}

#[test]
fn test_url_normalization() {
    assert_eq!(
        normalize_relay_url("relay.example.com"),
        "wss://relay.example.com"
    );
    assert_eq!(
        normalize_relay_url("wss://relay.example.com"),
        "wss://relay.example.com"
    );
    assert_eq!(
        normalize_relay_url("ws://relay.example.com"),
        "ws://relay.example.com"
    );
}
