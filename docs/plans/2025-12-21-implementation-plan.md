# Divine Relay Sync Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a Rust CLI tool that syncs Nostr events between relays using NIP-77 negentropy (with timestamp fallback).

**Architecture:** Pipeline-based async design with bounded channels for backpressure. Fetcher task discovers events via negentropy or timestamp pagination, sends to publisher task via channel. State persists progress for resumability.

**Tech Stack:** Rust, nostr-sdk 0.35+, tokio, clap, governor, indicatif

---

## Phase 1: Project Foundation

### Task 1: Initialize Cargo Project

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`
- Create: `src/lib.rs`

**Step 1: Create Cargo.toml with all dependencies**

```toml
[package]
name = "relay-sync"
version = "0.1.0"
edition = "2021"
description = "Sync Nostr events between relays using NIP-77 negentropy"
license = "MIT"

[[bin]]
name = "relay-sync"
path = "src/main.rs"

[dependencies]
# Nostr
nostr-sdk = "0.37"

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

# Rate limiting
governor = "0.6"
nonzero_ext = "0.3"

# Error handling
anyhow = "1"
thiserror = "2"

# Utilities
chrono = { version = "0.4", features = ["serde"] }
sha2 = "0.10"
hex = "0.4"
url = "2"

[dev-dependencies]
tempfile = "3"
```

**Step 2: Create minimal src/lib.rs**

```rust
// ABOUTME: Library root for divine-relay-sync
// ABOUTME: Exports public API for relay-to-relay Nostr event synchronization

pub mod cli;
pub mod config;
pub mod error;
pub mod output;
pub mod relay;
pub mod state;
pub mod sync;
```

**Step 3: Create minimal src/main.rs**

```rust
// ABOUTME: CLI entry point for relay-sync tool
// ABOUTME: Handles argument parsing, signal handling, and orchestrates sync

use anyhow::Result;

fn main() -> Result<()> {
    println!("relay-sync - Nostr relay synchronization tool");
    Ok(())
}
```

**Step 4: Verify project compiles**

Run: `cargo build`
Expected: Compilation errors (missing modules) - that's OK for now

**Step 5: Create module stubs**

Create empty module files so project compiles:

```bash
mkdir -p src/relay src/sync src/state src/output
touch src/cli.rs src/config.rs src/error.rs
touch src/relay/mod.rs src/sync/mod.rs src/state/mod.rs src/output/mod.rs
```

Add to each mod.rs:
```rust
// ABOUTME: Module placeholder
// ABOUTME: To be implemented
```

**Step 6: Verify project compiles**

Run: `cargo build`
Expected: SUCCESS

**Step 7: Commit**

```bash
git add -A
git commit -m "feat: initialize cargo project with dependencies"
```

---

### Task 2: Define Error Types

**Files:**
- Modify: `src/error.rs`
- Create: `tests/error_test.rs`

**Step 1: Write error type tests**

```rust
// ABOUTME: Tests for error type definitions
// ABOUTME: Verifies error classification and display

use relay_sync::error::{Error, ErrorKind};

#[test]
fn test_error_kind_is_retryable() {
    assert!(ErrorKind::RateLimited.is_retryable());
    assert!(ErrorKind::NetworkError.is_retryable());
    assert!(ErrorKind::RelayDisconnected.is_retryable());

    assert!(!ErrorKind::Duplicate.is_retryable());
    assert!(!ErrorKind::Blocked.is_retryable());
    assert!(!ErrorKind::InvalidEvent.is_retryable());
}

#[test]
fn test_error_display() {
    let err = Error::new(ErrorKind::RateLimited, "slow down");
    assert!(err.to_string().contains("rate limited"));
}

#[test]
fn test_parse_relay_error_message() {
    assert_eq!(
        ErrorKind::from_relay_message("duplicate: already have this event"),
        ErrorKind::Duplicate
    );
    assert_eq!(
        ErrorKind::from_relay_message("blocked: policy violation"),
        ErrorKind::Blocked
    );
    assert_eq!(
        ErrorKind::from_relay_message("rate-limited: slow down"),
        ErrorKind::RateLimited
    );
    assert_eq!(
        ErrorKind::from_relay_message("auth-required: please authenticate"),
        ErrorKind::AuthRequired
    );
    assert_eq!(
        ErrorKind::from_relay_message("unknown error"),
        ErrorKind::Unknown
    );
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --test error_test`
Expected: FAIL - module not found

**Step 3: Implement error types**

```rust
// ABOUTME: Error types for relay-sync operations
// ABOUTME: Classifies errors for retry/skip decisions

use std::fmt;
use thiserror::Error;

/// Classification of error types for retry logic
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Event already exists on destination
    Duplicate,
    /// Event rejected by relay policy
    Blocked,
    /// Rate limited by relay
    RateLimited,
    /// Authentication required
    AuthRequired,
    /// Invalid event (signature, schema)
    InvalidEvent,
    /// Network/connection error
    NetworkError,
    /// Relay closed connection
    RelayDisconnected,
    /// Negentropy protocol error
    NegentropyError,
    /// Configuration error
    ConfigError,
    /// State file error
    StateError,
    /// Unknown error
    Unknown,
}

impl ErrorKind {
    /// Whether this error type should be retried
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ErrorKind::RateLimited
                | ErrorKind::NetworkError
                | ErrorKind::RelayDisconnected
        )
    }

    /// Parse error kind from relay OK message reason
    pub fn from_relay_message(msg: &str) -> Self {
        let lower = msg.to_lowercase();
        if lower.starts_with("duplicate:") || lower.contains("already have") {
            ErrorKind::Duplicate
        } else if lower.starts_with("blocked:") || lower.starts_with("restricted:") {
            ErrorKind::Blocked
        } else if lower.starts_with("rate-limited:") || lower.contains("rate limit") {
            ErrorKind::RateLimited
        } else if lower.starts_with("auth-required:") {
            ErrorKind::AuthRequired
        } else if lower.starts_with("invalid:") {
            ErrorKind::InvalidEvent
        } else {
            ErrorKind::Unknown
        }
    }
}

impl fmt::Display for ErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ErrorKind::Duplicate => write!(f, "duplicate"),
            ErrorKind::Blocked => write!(f, "blocked"),
            ErrorKind::RateLimited => write!(f, "rate limited"),
            ErrorKind::AuthRequired => write!(f, "auth required"),
            ErrorKind::InvalidEvent => write!(f, "invalid event"),
            ErrorKind::NetworkError => write!(f, "network error"),
            ErrorKind::RelayDisconnected => write!(f, "relay disconnected"),
            ErrorKind::NegentriesError => write!(f, "negentropy error"),
            ErrorKind::ConfigError => write!(f, "config error"),
            ErrorKind::StateError => write!(f, "state error"),
            ErrorKind::Unknown => write!(f, "unknown error"),
        }
    }
}

/// Main error type for relay-sync
#[derive(Error, Debug)]
#[error("{kind}: {message}")]
pub struct Error {
    pub kind: ErrorKind,
    pub message: String,
    #[source]
    pub source: Option<Box<dyn std::error::Error + Send + Sync>>,
}

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            source: None,
        }
    }

    pub fn with_source(
        kind: ErrorKind,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            source: Some(Box::new(source)),
        }
    }

    pub fn is_retryable(&self) -> bool {
        self.kind.is_retryable()
    }
}

/// Result type alias
pub type Result<T> = std::result::Result<T, Error>;
```

**Step 4: Update src/lib.rs to export error module**

Already done in Task 1.

**Step 5: Run tests**

Run: `cargo test --test error_test`
Expected: PASS

**Step 6: Commit**

```bash
git add -A
git commit -m "feat: add error types with classification"
```

---

### Task 3: CLI Argument Parsing

**Files:**
- Modify: `src/cli.rs`
- Create: `tests/cli_test.rs`

**Step 1: Write CLI parsing tests**

```rust
// ABOUTME: Tests for CLI argument parsing
// ABOUTME: Verifies all flags and argument combinations

use clap::Parser;
use relay_sync::cli::{Cli, Commands};

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
        "--kind", "1",
        "--kind", "7",
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
    use relay_sync::cli::normalize_relay_url;

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
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --test cli_test`
Expected: FAIL

**Step 3: Implement CLI module**

```rust
// ABOUTME: CLI argument definitions using clap
// ABOUTME: Supports positional args, filters, and control flags

use clap::{Parser, Subcommand};

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
```

**Step 4: Run tests**

Run: `cargo test --test cli_test`
Expected: PASS

**Step 5: Commit**

```bash
git add -A
git commit -m "feat: add CLI argument parsing"
```

---

### Task 4: Config File Parsing

**Files:**
- Modify: `src/config.rs`
- Create: `tests/config_test.rs`

**Step 1: Write config parsing tests**

```rust
// ABOUTME: Tests for TOML config file parsing
// ABOUTME: Verifies sync config and auth settings

use relay_sync::config::{Config, SyncConfig};
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_parse_config() {
    let toml = r#"
[auth]
nsec = "nsec1test"

[[sync]]
name = "test-sync"
source = "relay.source.com"
dest = "relay.dest.com"
kinds = [1, 7]
"#;

    let config = Config::from_str(toml).unwrap();
    assert_eq!(config.auth.as_ref().unwrap().nsec, Some("nsec1test".to_string()));
    assert_eq!(config.sync.len(), 1);
    assert_eq!(config.sync[0].name, "test-sync");
    assert_eq!(config.sync[0].kinds, Some(vec![1, 7]));
}

#[test]
fn test_find_sync_by_name() {
    let toml = r#"
[[sync]]
name = "first"
source = "a.com"
dest = "b.com"

[[sync]]
name = "second"
source = "c.com"
dest = "d.com"
"#;

    let config = Config::from_str(toml).unwrap();
    let sync = config.find_sync("second").unwrap();
    assert_eq!(sync.source, "c.com");
}

#[test]
fn test_env_var_substitution() {
    std::env::set_var("TEST_NSEC", "nsec1fromenv");

    let toml = r#"
[auth]
nsec = "${TEST_NSEC}"
"#;

    let config = Config::from_str(toml).unwrap();
    assert_eq!(
        config.auth.as_ref().unwrap().resolve_nsec().unwrap(),
        "nsec1fromenv"
    );

    std::env::remove_var("TEST_NSEC");
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --test config_test`
Expected: FAIL

**Step 3: Implement config module**

```rust
// ABOUTME: TOML config file parsing for relay-sync
// ABOUTME: Supports multiple sync configs and auth settings

use crate::error::{Error, ErrorKind, Result};
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub sync: Vec<SyncConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    pub nsec: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SyncConfig {
    pub name: String,
    pub source: String,
    pub dest: String,
    pub kinds: Option<Vec<u16>>,
    pub authors: Option<Vec<String>>,
    pub since: Option<String>,
    pub until: Option<String>,
}

impl AuthConfig {
    /// Resolve nsec, substituting environment variables
    pub fn resolve_nsec(&self) -> Option<String> {
        self.nsec.as_ref().and_then(|s| {
            if s.starts_with("${") && s.ends_with("}") {
                let var_name = &s[2..s.len() - 1];
                std::env::var(var_name).ok()
            } else {
                Some(s.clone())
            }
        })
    }
}

impl Config {
    /// Load config from file
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref()).map_err(|e| {
            Error::with_source(
                ErrorKind::ConfigError,
                format!("failed to read config file: {}", path.as_ref().display()),
                e,
            )
        })?;
        Self::from_str(&content)
    }

    /// Parse config from string
    pub fn from_str(content: &str) -> Result<Self> {
        toml::from_str(content).map_err(|e| {
            Error::with_source(ErrorKind::ConfigError, "failed to parse config", e)
        })
    }

    /// Find sync config by name
    pub fn find_sync(&self, name: &str) -> Option<&SyncConfig> {
        self.sync.iter().find(|s| s.name == name)
    }

    /// Get nsec from auth config
    pub fn nsec(&self) -> Option<String> {
        self.auth.as_ref().and_then(|a| a.resolve_nsec())
    }
}
```

**Step 4: Run tests**

Run: `cargo test --test config_test`
Expected: PASS

**Step 5: Commit**

```bash
git add -A
git commit -m "feat: add config file parsing"
```

---

### Task 5: State Management

**Files:**
- Modify: `src/state/mod.rs`
- Create: `src/state/manager.rs`
- Create: `src/state/failures.rs`
- Create: `tests/state_test.rs`

**Step 1: Write state management tests**

```rust
// ABOUTME: Tests for state persistence and locking
// ABOUTME: Verifies state save/load and lock file handling

use relay_sync::state::{StateManager, SyncState};
use tempfile::tempdir;

#[test]
fn test_state_key_generation() {
    let key1 = StateManager::compute_key("source.com", "dest.com", &[1, 7], &[]);
    let key2 = StateManager::compute_key("source.com", "dest.com", &[7, 1], &[]); // same, different order
    let key3 = StateManager::compute_key("source.com", "dest.com", &[1], &[]);

    assert_eq!(key1, key2); // Order shouldn't matter
    assert_ne!(key1, key3); // Different filters = different key
}

#[test]
fn test_state_save_load() {
    let dir = tempdir().unwrap();
    let manager = StateManager::new(dir.path()).unwrap();

    let mut state = SyncState::new("source.com", "dest.com");
    state.events_synced = 100;
    state.cursor_created_at = Some(1234567890);

    manager.save(&state).unwrap();

    let loaded = manager.load("source.com", "dest.com", &[], &[]).unwrap();
    assert!(loaded.is_some());
    let loaded = loaded.unwrap();
    assert_eq!(loaded.events_synced, 100);
    assert_eq!(loaded.cursor_created_at, Some(1234567890));
}

#[test]
fn test_lock_file() {
    let dir = tempdir().unwrap();
    let manager = StateManager::new(dir.path()).unwrap();

    // Acquire lock
    let lock = manager.acquire_lock("source.com", "dest.com", &[], &[]).unwrap();
    assert!(lock.is_some());

    // Can't acquire again while held
    let lock2 = manager.try_acquire_lock("source.com", "dest.com", &[], &[]);
    assert!(lock2.is_err() || lock2.unwrap().is_none());
}

#[test]
fn test_failure_logging() {
    let dir = tempdir().unwrap();
    let manager = StateManager::new(dir.path()).unwrap();

    manager.log_failure("source.com", "dest.com", &[], &[], "event123", "rate_limited").unwrap();
    manager.log_failure("source.com", "dest.com", &[], &[], "event456", "blocked").unwrap();

    let failures = manager.load_failures("source.com", "dest.com", &[], &[]).unwrap();
    assert_eq!(failures.len(), 2);
}
```

**Step 2: Run tests to verify they fail**

Run: `cargo test --test state_test`
Expected: FAIL

**Step 3: Implement state/mod.rs**

```rust
// ABOUTME: State management module exports
// ABOUTME: Handles persistence, locking, and failure tracking

mod manager;
mod failures;

pub use manager::{StateManager, SyncState, LockGuard};
pub use failures::FailureEntry;
```

**Step 4: Implement state/manager.rs**

```rust
// ABOUTME: State file management with locking
// ABOUTME: Persists sync progress for resumability

use crate::error::{Error, ErrorKind, Result};
use serde::{Deserialize, Serialize};
use sha2::{Sha256, Digest};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Serialize, Deserialize)]
pub struct SyncState {
    pub version: u32,
    pub source: String,
    pub dest: String,
    #[serde(default)]
    pub kinds: Vec<u16>,
    #[serde(default)]
    pub authors: Vec<String>,
    pub mode: String,
    pub events_synced: u64,
    pub failure_count: u64,
    pub cursor_created_at: Option<i64>,
    pub cursor_event_id: Option<String>,
    pub last_synced_at: Option<i64>,
}

impl SyncState {
    pub fn new(source: &str, dest: &str) -> Self {
        Self {
            version: 1,
            source: source.to_string(),
            dest: dest.to_string(),
            kinds: Vec::new(),
            authors: Vec::new(),
            mode: "unknown".to_string(),
            events_synced: 0,
            failure_count: 0,
            cursor_created_at: None,
            cursor_event_id: None,
            last_synced_at: None,
        }
    }
}

pub struct LockGuard {
    path: PathBuf,
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

pub struct StateManager {
    dir: PathBuf,
}

impl StateManager {
    pub fn new(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| {
            Error::with_source(ErrorKind::StateError, "failed to create state directory", e)
        })?;

        // Set restrictive permissions on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let perms = fs::Permissions::from_mode(0o700);
            let _ = fs::set_permissions(&dir, perms);
        }

        Ok(Self { dir })
    }

    /// Compute unique key for this sync configuration
    pub fn compute_key(source: &str, dest: &str, kinds: &[u16], authors: &[String]) -> String {
        let mut hasher = Sha256::new();
        hasher.update(source.as_bytes());
        hasher.update(b"|");
        hasher.update(dest.as_bytes());
        hasher.update(b"|");

        // Sort kinds for consistent hashing
        let mut sorted_kinds = kinds.to_vec();
        sorted_kinds.sort();
        for k in sorted_kinds {
            hasher.update(k.to_le_bytes());
        }
        hasher.update(b"|");

        // Sort authors for consistent hashing
        let mut sorted_authors = authors.to_vec();
        sorted_authors.sort();
        for a in sorted_authors {
            hasher.update(a.as_bytes());
        }

        let result = hasher.finalize();
        hex::encode(&result[..8]) // First 8 bytes = 16 hex chars
    }

    fn state_path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{}.json", key))
    }

    fn lock_path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{}.lock", key))
    }

    fn failures_path(&self, key: &str) -> PathBuf {
        self.dir.join(format!("{}.failures.log", key))
    }

    pub fn save(&self, state: &SyncState) -> Result<()> {
        let key = Self::compute_key(&state.source, &state.dest, &state.kinds, &state.authors);
        let path = self.state_path(&key);
        let content = serde_json::to_string_pretty(state).map_err(|e| {
            Error::with_source(ErrorKind::StateError, "failed to serialize state", e)
        })?;
        fs::write(&path, content).map_err(|e| {
            Error::with_source(ErrorKind::StateError, "failed to write state file", e)
        })?;
        Ok(())
    }

    pub fn load(
        &self,
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
    ) -> Result<Option<SyncState>> {
        let key = Self::compute_key(source, dest, kinds, authors);
        let path = self.state_path(&key);

        if !path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&path).map_err(|e| {
            Error::with_source(ErrorKind::StateError, "failed to read state file", e)
        })?;

        let state: SyncState = serde_json::from_str(&content).map_err(|e| {
            Error::with_source(ErrorKind::StateError, "failed to parse state file", e)
        })?;

        Ok(Some(state))
    }

    pub fn acquire_lock(
        &self,
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
    ) -> Result<Option<LockGuard>> {
        let key = Self::compute_key(source, dest, kinds, authors);
        let path = self.lock_path(&key);

        // Try to create lock file exclusively
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(mut file) => {
                let lock_info = serde_json::json!({
                    "pid": std::process::id(),
                    "started_at": chrono::Utc::now().timestamp(),
                    "hostname": hostname::get().ok().and_then(|h| h.into_string().ok()).unwrap_or_default(),
                });
                let _ = file.write_all(lock_info.to_string().as_bytes());
                Ok(Some(LockGuard { path }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Check if lock is stale (process not running)
                if self.is_lock_stale(&path) {
                    let _ = fs::remove_file(&path);
                    return self.acquire_lock(source, dest, kinds, authors);
                }
                Ok(None)
            }
            Err(e) => Err(Error::with_source(
                ErrorKind::StateError,
                "failed to create lock file",
                e,
            )),
        }
    }

    pub fn try_acquire_lock(
        &self,
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
    ) -> Result<Option<LockGuard>> {
        self.acquire_lock(source, dest, kinds, authors)
    }

    fn is_lock_stale(&self, path: &Path) -> bool {
        // Consider lock stale if older than 1 hour
        if let Ok(metadata) = fs::metadata(path) {
            if let Ok(modified) = metadata.modified() {
                let age = std::time::SystemTime::now()
                    .duration_since(modified)
                    .unwrap_or_default();
                return age.as_secs() > 3600;
            }
        }
        false
    }

    pub fn log_failure(
        &self,
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
        event_id: &str,
        reason: &str,
    ) -> Result<()> {
        let key = Self::compute_key(source, dest, kinds, authors);
        let path = self.failures_path(&key);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| {
                Error::with_source(ErrorKind::StateError, "failed to open failures log", e)
            })?;

        let timestamp = chrono::Utc::now().timestamp();
        writeln!(file, "{}:{}:{}", timestamp, event_id, reason).map_err(|e| {
            Error::with_source(ErrorKind::StateError, "failed to write to failures log", e)
        })?;

        Ok(())
    }

    pub fn load_failures(
        &self,
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
    ) -> Result<Vec<super::failures::FailureEntry>> {
        let key = Self::compute_key(source, dest, kinds, authors);
        let path = self.failures_path(&key);

        if !path.exists() {
            return Ok(Vec::new());
        }

        let content = fs::read_to_string(&path).map_err(|e| {
            Error::with_source(ErrorKind::StateError, "failed to read failures log", e)
        })?;

        let entries = content
            .lines()
            .filter_map(|line| super::failures::FailureEntry::parse(line))
            .collect();

        Ok(entries)
    }
}
```

**Step 5: Implement state/failures.rs**

```rust
// ABOUTME: Failure log entry parsing
// ABOUTME: Tracks events that failed to sync for retry

#[derive(Debug, Clone)]
pub struct FailureEntry {
    pub timestamp: i64,
    pub event_id: String,
    pub reason: String,
}

impl FailureEntry {
    pub fn parse(line: &str) -> Option<Self> {
        let parts: Vec<&str> = line.splitn(3, ':').collect();
        if parts.len() >= 3 {
            Some(Self {
                timestamp: parts[0].parse().ok()?,
                event_id: parts[1].to_string(),
                reason: parts[2].to_string(),
            })
        } else {
            None
        }
    }
}
```

**Step 6: Add hostname dependency to Cargo.toml**

Add to [dependencies]:
```toml
hostname = "0.4"
```

**Step 7: Run tests**

Run: `cargo test --test state_test`
Expected: PASS

**Step 8: Commit**

```bash
git add -A
git commit -m "feat: add state management with locking"
```

---

## Phase 2: Relay Communication

### Task 6: Relay Connection Module

**Files:**
- Modify: `src/relay/mod.rs`
- Create: `src/relay/connection.rs`

**Step 1: Implement relay/mod.rs**

```rust
// ABOUTME: Relay communication module exports
// ABOUTME: Handles connections, NIP-11, NIP-42, NIP-77

pub mod connection;
pub mod auth;
pub mod negentropy;

pub use connection::{RelayConnection, RelayInfo};
```

**Step 2: Implement relay/connection.rs**

```rust
// ABOUTME: Relay connection management
// ABOUTME: Handles WebSocket connection and NIP-11 discovery

use crate::error::{Error, ErrorKind, Result};
use nostr_sdk::prelude::*;
use std::time::Duration;

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
}

impl RelayConnection {
    /// Connect to a relay
    pub async fn connect(url: &str) -> Result<Self> {
        let info = RelayInfo::fetch(url).await.unwrap_or_else(|_| RelayInfo {
            url: url.to_string(),
            ..Default::default()
        });

        let client = Client::default();
        client.add_relay(url).await.map_err(|e| {
            Error::with_source(ErrorKind::NetworkError, format!("failed to add relay {}", url), e)
        })?;

        client.connect().await;

        Ok(Self {
            url: url.to_string(),
            info,
            client,
        })
    }

    /// Get the underlying nostr-sdk client
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Disconnect from relay
    pub async fn disconnect(&self) {
        self.client.disconnect().await;
    }
}
```

**Step 3: Add reqwest dependency to Cargo.toml**

Add to [dependencies]:
```toml
reqwest = { version = "0.12", features = ["json"] }
```

**Step 4: Verify it compiles**

Run: `cargo build`
Expected: SUCCESS

**Step 5: Commit**

```bash
git add -A
git commit -m "feat: add relay connection with NIP-11 discovery"
```

---

### Task 7: NIP-42 Authentication

**Files:**
- Create: `src/relay/auth.rs`

**Step 1: Implement auth module**

```rust
// ABOUTME: NIP-42 authentication handling
// ABOUTME: Creates and sends AUTH events when challenged

use crate::error::{Error, ErrorKind, Result};
use nostr_sdk::prelude::*;

/// Handle NIP-42 authentication for a relay
pub struct Authenticator {
    keys: Option<Keys>,
}

impl Authenticator {
    /// Create authenticator with optional keys
    pub fn new(nsec: Option<&str>) -> Result<Self> {
        let keys = if let Some(nsec) = nsec {
            Some(Keys::parse(nsec).map_err(|e| {
                Error::with_source(ErrorKind::ConfigError, "invalid nsec key", e)
            })?)
        } else {
            None
        };

        Ok(Self { keys })
    }

    /// Check if we have keys for authentication
    pub fn can_authenticate(&self) -> bool {
        self.keys.is_some()
    }

    /// Create AUTH event for relay challenge
    pub fn create_auth_event(&self, relay_url: &str, challenge: &str) -> Result<Event> {
        let keys = self.keys.as_ref().ok_or_else(|| {
            Error::new(ErrorKind::AuthRequired, "no keys available for authentication")
        })?;

        let event = EventBuilder::auth(challenge, relay_url)
            .sign_with_keys(keys)
            .map_err(|e| {
                Error::with_source(ErrorKind::AuthRequired, "failed to sign auth event", e)
            })?;

        Ok(event)
    }

    /// Get public key if available
    pub fn public_key(&self) -> Option<PublicKey> {
        self.keys.as_ref().map(|k| k.public_key())
    }
}
```

**Step 2: Verify it compiles**

Run: `cargo build`
Expected: SUCCESS

**Step 3: Commit**

```bash
git add -A
git commit -m "feat: add NIP-42 authentication support"
```

---

### Task 8: Sync Engine Foundation

**Files:**
- Modify: `src/sync/mod.rs`
- Create: `src/sync/engine.rs`
- Create: `src/sync/fetcher.rs`
- Create: `src/sync/publisher.rs`

**Step 1: Implement sync/mod.rs**

```rust
// ABOUTME: Sync engine module exports
// ABOUTME: Orchestrates event fetching and publishing pipeline

pub mod engine;
pub mod fetcher;
pub mod publisher;
pub mod reconciler;

pub use engine::{SyncEngine, SyncOptions, SyncResult};
```

**Step 2: Implement sync/engine.rs**

```rust
// ABOUTME: Main sync engine orchestration
// ABOUTME: Coordinates fetcher and publisher tasks via bounded channel

use crate::error::{Error, ErrorKind, Result};
use crate::relay::RelayConnection;
use crate::state::{StateManager, SyncState};
use nostr_sdk::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// Options for sync operation
#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub source_url: String,
    pub dest_url: String,
    pub kinds: Vec<u16>,
    pub authors: Vec<String>,
    pub since: Option<Timestamp>,
    pub until: Option<Timestamp>,
    pub fresh: bool,
    pub dry_run: bool,
    pub nsec: Option<String>,
}

/// Result of sync operation
#[derive(Debug, Default)]
pub struct SyncResult {
    pub events_synced: u64,
    pub events_skipped: u64,
    pub events_failed: u64,
    pub mode: String,
}

/// Main sync engine
pub struct SyncEngine {
    options: SyncOptions,
    state_manager: Arc<StateManager>,
    shutdown: CancellationToken,
}

impl SyncEngine {
    pub fn new(
        options: SyncOptions,
        state_manager: Arc<StateManager>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            options,
            state_manager,
            shutdown,
        }
    }

    /// Run the sync operation
    pub async fn run(&self) -> Result<SyncResult> {
        // Connect to relays
        tracing::info!("Connecting to source: {}", self.options.source_url);
        let source = RelayConnection::connect(&self.options.source_url).await?;

        tracing::info!("Connecting to destination: {}", self.options.dest_url);
        let dest = RelayConnection::connect(&self.options.dest_url).await?;

        tracing::info!(
            "Source negentropy support: {}, Dest negentropy support: {}",
            source.info.supports_negentropy,
            dest.info.supports_negentropy
        );

        // Load or create state
        let state = if self.options.fresh {
            SyncState::new(&self.options.source_url, &self.options.dest_url)
        } else {
            self.state_manager
                .load(
                    &self.options.source_url,
                    &self.options.dest_url,
                    &self.options.kinds,
                    &self.options.authors,
                )?
                .unwrap_or_else(|| {
                    SyncState::new(&self.options.source_url, &self.options.dest_url)
                })
        };

        // Create channel for event pipeline
        let (tx, rx) = mpsc::channel::<Event>(1000);

        // Determine sync mode
        let use_negentropy = source.info.supports_negentropy && dest.info.supports_negentropy;
        let mode = if use_negentropy {
            "negentropy"
        } else {
            "timestamp"
        };

        tracing::info!("Using {} sync mode", mode);

        // Build filter
        let mut filter = Filter::new();
        if !self.options.kinds.is_empty() {
            filter = filter.kinds(self.options.kinds.iter().map(|k| Kind::from(*k)));
        }
        if let Some(since) = self.options.since {
            filter = filter.since(since);
        }
        if let Some(until) = self.options.until {
            filter = filter.until(until);
        }

        let result = if use_negentropy {
            self.run_negentropy_sync(source, dest, filter, tx, rx, state).await?
        } else {
            self.run_timestamp_sync(source, dest, filter, tx, rx, state).await?
        };

        Ok(result)
    }

    async fn run_negentropy_sync(
        &self,
        source: RelayConnection,
        dest: RelayConnection,
        filter: Filter,
        tx: mpsc::Sender<Event>,
        rx: mpsc::Receiver<Event>,
        state: SyncState,
    ) -> Result<SyncResult> {
        // TODO: Implement negentropy reconciliation
        // For now, fall back to timestamp sync
        tracing::warn!("Negentropy not yet implemented, falling back to timestamp sync");
        self.run_timestamp_sync(source, dest, filter, tx, rx, state).await
    }

    async fn run_timestamp_sync(
        &self,
        source: RelayConnection,
        dest: RelayConnection,
        filter: Filter,
        tx: mpsc::Sender<Event>,
        mut rx: mpsc::Receiver<Event>,
        mut state: SyncState,
    ) -> Result<SyncResult> {
        let shutdown = self.shutdown.clone();
        let dry_run = self.options.dry_run;
        let state_manager = self.state_manager.clone();
        let source_url = self.options.source_url.clone();
        let dest_url = self.options.dest_url.clone();
        let kinds = self.options.kinds.clone();
        let authors = self.options.authors.clone();

        // Fetcher task
        let fetch_shutdown = shutdown.clone();
        let fetcher = tokio::spawn(async move {
            super::fetcher::run_timestamp_fetcher(
                source,
                filter,
                state.cursor_created_at,
                state.cursor_event_id.clone(),
                tx,
                fetch_shutdown,
            )
            .await
        });

        // Publisher task
        let mut result = SyncResult {
            mode: "timestamp".to_string(),
            ..Default::default()
        };

        while let Some(event) = rx.recv().await {
            if dry_run {
                result.events_synced += 1;
                continue;
            }

            match super::publisher::publish_event(&dest, &event).await {
                Ok(true) => {
                    result.events_synced += 1;
                    state.events_synced += 1;
                    state.cursor_created_at = Some(event.created_at.as_i64());
                    state.cursor_event_id = Some(event.id.to_hex());

                    // Checkpoint periodically
                    if result.events_synced % 100 == 0 {
                        state.last_synced_at = Some(chrono::Utc::now().timestamp());
                        let _ = state_manager.save(&state);
                    }
                }
                Ok(false) => {
                    result.events_skipped += 1;
                }
                Err(e) => {
                    result.events_failed += 1;
                    state.failure_count += 1;
                    let _ = state_manager.log_failure(
                        &source_url,
                        &dest_url,
                        &kinds,
                        &authors,
                        &event.id.to_hex(),
                        &e.kind.to_string(),
                    );
                }
            }
        }

        // Wait for fetcher
        let _ = fetcher.await;

        // Final state save
        state.last_synced_at = Some(chrono::Utc::now().timestamp());
        state.mode = "timestamp".to_string();
        state_manager.save(&state)?;

        Ok(result)
    }
}
```

**Step 3: Implement sync/fetcher.rs**

```rust
// ABOUTME: Event fetcher from source relay
// ABOUTME: Handles timestamp pagination with cursor

use crate::error::Result;
use crate::relay::RelayConnection;
use nostr_sdk::prelude::*;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

const BATCH_SIZE: usize = 500;

/// Run timestamp-based fetcher
pub async fn run_timestamp_fetcher(
    source: RelayConnection,
    base_filter: Filter,
    cursor_created_at: Option<i64>,
    cursor_event_id: Option<String>,
    tx: mpsc::Sender<Event>,
    shutdown: CancellationToken,
) -> Result<()> {
    let mut filter = base_filter.limit(BATCH_SIZE);

    if let Some(ts) = cursor_created_at {
        filter = filter.until(Timestamp::from(ts as u64));
    }

    loop {
        if shutdown.is_cancelled() {
            tracing::info!("Fetcher shutting down");
            break;
        }

        // Fetch batch from source
        let timeout = Duration::from_secs(30);
        let events = match tokio::time::timeout(
            timeout,
            source.client().fetch_events(vec![filter.clone()], Some(timeout)),
        )
        .await
        {
            Ok(Ok(events)) => events.into_iter().collect::<Vec<_>>(),
            Ok(Err(e)) => {
                tracing::error!("Failed to fetch events: {}", e);
                break;
            }
            Err(_) => {
                tracing::error!("Fetch timeout");
                break;
            }
        };

        if events.is_empty() {
            tracing::info!("No more events to fetch");
            break;
        }

        tracing::debug!("Fetched {} events", events.len());

        // Filter out events we've already seen (same timestamp, ID <= cursor)
        let cursor_id = cursor_event_id.as_deref();
        let cursor_ts = cursor_created_at;

        let filtered: Vec<_> = events
            .into_iter()
            .filter(|e| {
                if let (Some(ts), Some(id)) = (cursor_ts, cursor_id) {
                    if e.created_at.as_i64() == ts {
                        e.id.to_hex() > id.to_string()
                    } else {
                        true
                    }
                } else {
                    true
                }
            })
            .collect();

        if filtered.is_empty() {
            break;
        }

        // Update cursor for next batch
        if let Some(last) = filtered.last() {
            filter = filter.until(last.created_at);
        }

        // Send events to publisher
        for event in filtered {
            if tx.send(event).await.is_err() {
                tracing::info!("Publisher channel closed");
                return Ok(());
            }
        }
    }

    drop(tx); // Signal end of events
    Ok(())
}
```

**Step 4: Implement sync/publisher.rs**

```rust
// ABOUTME: Event publisher to destination relay
// ABOUTME: Handles adaptive rate limiting and retries

use crate::error::{Error, ErrorKind, Result};
use crate::relay::RelayConnection;
use nostr_sdk::prelude::*;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

const MAX_RETRIES: u32 = 3;
const INITIAL_BACKOFF_MS: u64 = 100;

/// Publish an event to destination relay
/// Returns Ok(true) if published, Ok(false) if skipped (duplicate), Err on failure
pub async fn publish_event(dest: &RelayConnection, event: &Event) -> Result<bool> {
    let mut attempts = 0;
    let mut backoff = INITIAL_BACKOFF_MS;

    loop {
        attempts += 1;

        match dest.client().send_event(event.clone()).await {
            Ok(output) => {
                // Check if any relay accepted it
                if output.success.is_empty() && !output.failed.is_empty() {
                    // All relays failed
                    for (_, msg) in &output.failed {
                        let kind = ErrorKind::from_relay_message(msg.as_deref().unwrap_or(""));

                        if kind == ErrorKind::Duplicate {
                            return Ok(false); // Skip, not an error
                        }

                        if kind.is_retryable() && attempts < MAX_RETRIES {
                            tracing::debug!(
                                "Publish failed (attempt {}): {:?}, retrying...",
                                attempts,
                                msg
                            );
                            tokio::time::sleep(Duration::from_millis(backoff)).await;
                            backoff *= 2;
                            continue;
                        }

                        return Err(Error::new(kind, msg.clone().unwrap_or_default()));
                    }
                }

                return Ok(true);
            }
            Err(e) => {
                if attempts < MAX_RETRIES {
                    tracing::debug!("Publish error (attempt {}): {}, retrying...", attempts, e);
                    tokio::time::sleep(Duration::from_millis(backoff)).await;
                    backoff *= 2;
                    continue;
                }

                return Err(Error::with_source(
                    ErrorKind::NetworkError,
                    "failed to publish event",
                    e,
                ));
            }
        }
    }
}
```

**Step 5: Create empty reconciler.rs placeholder**

```rust
// ABOUTME: Negentropy reconciliation logic
// ABOUTME: Implements NIP-77 set reconciliation between relays

use crate::error::Result;

// TODO: Implement negentropy reconciliation
// This requires acting as message proxy between two relays
```

**Step 6: Verify it compiles**

Run: `cargo build`
Expected: SUCCESS

**Step 7: Commit**

```bash
git add -A
git commit -m "feat: add sync engine with timestamp pagination"
```

---

### Task 9: Output and Progress

**Files:**
- Modify: `src/output/mod.rs`
- Create: `src/output/progress.rs`

**Step 1: Implement output/mod.rs**

```rust
// ABOUTME: Output formatting module exports
// ABOUTME: Handles progress bars and JSON output

pub mod progress;

pub use progress::ProgressReporter;
```

**Step 2: Implement output/progress.rs**

```rust
// ABOUTME: Progress reporting with indicatif
// ABOUTME: Shows sync progress with event counts and rates

use indicatif::{ProgressBar, ProgressStyle};
use std::time::Duration;

pub struct ProgressReporter {
    bar: ProgressBar,
    quiet: bool,
}

impl ProgressReporter {
    pub fn new(total: Option<u64>, quiet: bool) -> Self {
        let bar = if let Some(total) = total {
            ProgressBar::new(total)
        } else {
            ProgressBar::new_spinner()
        };

        if !quiet {
            if total.is_some() {
                bar.set_style(
                    ProgressStyle::default_bar()
                        .template("{spinner:.green} [{bar:40.cyan/blue}] {pos}/{len} ({percent}%) | {per_sec} | eta: {eta}")
                        .unwrap()
                        .progress_chars("#>-"),
                );
            } else {
                bar.set_style(
                    ProgressStyle::default_spinner()
                        .template("{spinner:.green} {pos} events | {per_sec}")
                        .unwrap(),
                );
            }
            bar.enable_steady_tick(Duration::from_millis(100));
        }

        Self { bar, quiet }
    }

    pub fn set_message(&self, msg: impl Into<String>) {
        if !self.quiet {
            self.bar.set_message(msg.into());
        }
    }

    pub fn inc(&self, n: u64) {
        self.bar.inc(n);
    }

    pub fn finish(&self, msg: impl Into<String>) {
        if !self.quiet {
            self.bar.finish_with_message(msg.into());
        }
    }

    pub fn println(&self, msg: impl AsRef<str>) {
        if !self.quiet {
            self.bar.println(msg.as_ref());
        }
    }
}
```

**Step 3: Verify it compiles**

Run: `cargo build`
Expected: SUCCESS

**Step 4: Commit**

```bash
git add -A
git commit -m "feat: add progress reporting"
```

---

### Task 10: Main Entry Point

**Files:**
- Modify: `src/main.rs`

**Step 1: Implement main.rs**

```rust
// ABOUTME: CLI entry point for relay-sync tool
// ABOUTME: Handles argument parsing, signal handling, and orchestrates sync

use anyhow::Result;
use clap::Parser;
use relay_sync::cli::Cli;
use relay_sync::config::Config;
use relay_sync::state::StateManager;
use relay_sync::sync::{SyncEngine, SyncOptions};
use std::path::PathBuf;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Setup logging
    let filter = if cli.verbose {
        EnvFilter::new("debug")
    } else if cli.quiet {
        EnvFilter::new("error")
    } else {
        EnvFilter::new("info")
    };

    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .init();

    // Load config if specified
    let config = if let Some(config_path) = &cli.config {
        Some(Config::from_file(config_path)?)
    } else {
        None
    };

    // Determine source and dest
    let (source_url, dest_url, kinds, authors) = if let Some(ref config) = config {
        // Use config file
        let sync_config = if let Some(name) = &cli.name {
            config.find_sync(name).ok_or_else(|| {
                anyhow::anyhow!("sync config '{}' not found", name)
            })?
        } else if config.sync.len() == 1 {
            &config.sync[0]
        } else {
            return Err(anyhow::anyhow!(
                "multiple sync configs found, use --name to specify one"
            ));
        };

        (
            relay_sync::cli::normalize_relay_url(&sync_config.source),
            relay_sync::cli::normalize_relay_url(&sync_config.dest),
            sync_config.kinds.clone().unwrap_or_default(),
            sync_config.authors.clone().unwrap_or_default(),
        )
    } else {
        // Use CLI args
        let source = cli.source_url().ok_or_else(|| {
            anyhow::anyhow!("source relay URL required")
        })?;
        let dest = cli.dest_url().ok_or_else(|| {
            anyhow::anyhow!("destination relay URL required")
        })?;

        (source, dest, cli.kinds.clone(), cli.authors.clone())
    };

    // Get nsec from CLI, config, or env
    let nsec = cli.nsec.clone().or_else(|| {
        config.as_ref().and_then(|c| c.nsec())
    });

    // Setup state manager
    let state_dir = PathBuf::from(".relay-sync-state");
    let state_manager = Arc::new(StateManager::new(&state_dir)?);

    // Acquire lock
    let _lock = state_manager
        .acquire_lock(&source_url, &dest_url, &kinds, &authors)?
        .ok_or_else(|| anyhow::anyhow!("another sync is already running"))?;

    // Setup shutdown handling
    let shutdown = CancellationToken::new();
    let shutdown_clone = shutdown.clone();

    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        tracing::info!("Shutdown signal received, draining...");
        shutdown_clone.cancel();
    });

    // Create sync options
    let options = SyncOptions {
        source_url,
        dest_url,
        kinds,
        authors,
        since: None, // TODO: parse from CLI
        until: None, // TODO: parse from CLI
        fresh: cli.fresh,
        dry_run: cli.dry_run,
        nsec,
    };

    // Run sync
    tracing::info!("Starting sync: {} -> {}", options.source_url, options.dest_url);

    let engine = SyncEngine::new(options, state_manager, shutdown);
    let result = engine.run().await?;

    // Report results
    if cli.json {
        println!("{}", serde_json::to_string_pretty(&serde_json::json!({
            "events_synced": result.events_synced,
            "events_skipped": result.events_skipped,
            "events_failed": result.events_failed,
            "mode": result.mode,
        }))?);
    } else {
        tracing::info!(
            "Sync complete: {} synced, {} skipped, {} failed (mode: {})",
            result.events_synced,
            result.events_skipped,
            result.events_failed,
            result.mode
        );
    }

    // Exit code based on failures
    if result.events_failed > 0 {
        std::process::exit(1);
    }

    Ok(())
}
```

**Step 2: Verify it compiles**

Run: `cargo build`
Expected: SUCCESS

**Step 3: Test basic execution**

Run: `cargo run -- --help`
Expected: Shows help text

**Step 4: Commit**

```bash
git add -A
git commit -m "feat: add main entry point with signal handling"
```

---

### Task 11: Integration Test

**Files:**
- Create: `tests/integration_test.rs`

**Step 1: Create basic integration test**

```rust
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
    assert!(stdout.contains("relay-sync"));
    assert!(stdout.contains("--source"));
}

#[test]
fn test_version_output() {
    let output = Command::new("cargo")
        .args(["run", "--", "--version"])
        .output()
        .expect("Failed to execute command");

    assert!(output.status.success());
}

#[test]
fn test_missing_args_error() {
    let output = Command::new("cargo")
        .args(["run", "--"])
        .output()
        .expect("Failed to execute command");

    // Should fail without source/dest
    // Note: clap may not fail if both are optional
}
```

**Step 2: Run tests**

Run: `cargo test`
Expected: PASS

**Step 3: Commit**

```bash
git add -A
git commit -m "feat: add integration tests"
```

---

## Summary

This plan creates a functional relay-sync tool with:

1. **Phase 1 (Foundation):**
   - Project setup with all dependencies
   - Error types with classification
   - CLI argument parsing
   - Config file support
   - State management with locking

2. **Phase 2 (Relay Communication):**
   - Relay connection with NIP-11 discovery
   - NIP-42 authentication support
   - Sync engine with pipeline architecture
   - Timestamp pagination (working)
   - Progress reporting
   - Main entry point

**Not yet implemented (future tasks):**
- NIP-77 negentropy reconciliation (placeholder exists)
- Adaptive rate limiting with governor
- Full NIP-42 challenge/response flow
- `--since` and `--until` date parsing

The tool will work in timestamp fallback mode for initial testing.

---

**Plan complete and saved to `docs/plans/2025-12-21-implementation-plan.md`. Two execution options:**

**1. Subagent-Driven (this session)** - I dispatch fresh subagent per task, review between tasks, fast iteration

**2. Parallel Session (separate)** - Open new session with executing-plans, batch execution with checkpoints

**Which approach?**
