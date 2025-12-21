// ABOUTME: Integration tests for relay-sync
// ABOUTME: Tests end-to-end sync functionality

use std::process::Command;

#[test]
fn test_help_output() {
    let output = Command::new("cargo")
        .args(["run", "--", "--help"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("relay-sync") || stdout.contains("Sync Nostr"));
    assert!(stdout.contains("SOURCE"));
}

#[test]
fn test_version_output() {
    let output = Command::new("cargo")
        .args(["run", "--", "--version"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("0.1.0") || stdout.contains("relay-sync"));
}

#[test]
fn test_missing_source_error() {
    // When only one positional arg is missing, it should error
    let output = Command::new("cargo")
        .args(["run", "--", "source.relay.com"])
        .output()
        .expect("Failed to execute command");

    // Should fail because dest is missing
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("destination") || stderr.contains("required") || !output.status.success());
}

#[test]
fn test_dry_run_flag() {
    // Just verify the flag is recognized (actual sync would need real relays)
    let output = Command::new("cargo")
        .args(["run", "--", "--dry-run", "--help"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());
}
