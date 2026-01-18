// ABOUTME: State manager for persistence and locking
// ABOUTME: Manages sync state, lock files, and failure logs

#[cfg(unix)]
use libc;

use crate::error::{Error, ErrorKind};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use super::failures::FailureEntry;

/// Manages state persistence for relay synchronization
pub struct StateManager {
    state_dir: PathBuf,
}

/// Sync state persisted to disk
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncState {
    pub source: String,
    pub dest: String,
    pub events_synced: u64,
    pub cursor_created_at: Option<i64>,
    pub cursor_id: Option<String>,
    pub last_updated: i64,
}

/// Lock guard that releases lock on drop
pub struct LockGuard {
    path: PathBuf,
}

impl SyncState {
    pub fn new(source: &str, dest: &str) -> Self {
        Self {
            source: source.to_string(),
            dest: dest.to_string(),
            events_synced: 0,
            cursor_created_at: None,
            cursor_id: None,
            last_updated: now(),
        }
    }

    pub fn update_cursor(&mut self, created_at: i64, event_id: String) {
        self.cursor_created_at = Some(created_at);
        self.cursor_id = Some(event_id);
        self.last_updated = now();
    }

    pub fn increment_events(&mut self, count: u64) {
        self.events_synced += count;
        self.last_updated = now();
    }
}

impl StateManager {
    /// Create a new StateManager
    pub fn new(state_dir: &Path) -> Result<Self, Error> {
        fs::create_dir_all(state_dir).map_err(|e| {
            Error::new(ErrorKind::StateError, format!("Failed to create state directory: {}", e))
        })?;

        #[cfg(unix)]
        {
            // Set directory permissions to 700 (owner only)
            let perms = fs::Permissions::from_mode(0o700);
            fs::set_permissions(state_dir, perms).map_err(|e| {
                Error::new(ErrorKind::StateError, format!("Failed to set directory permissions: {}", e))
            })?;
        }

        Ok(Self {
            state_dir: state_dir.to_path_buf(),
        })
    }

    /// Compute unique state key from sync parameters
    pub fn compute_key(
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
    ) -> String {
        let mut hasher = Sha256::new();
        hasher.update(source.as_bytes());
        hasher.update(dest.as_bytes());

        // Sort kinds for consistency
        let mut sorted_kinds = kinds.to_vec();
        sorted_kinds.sort_unstable();
        for kind in sorted_kinds {
            hasher.update(kind.to_string().as_bytes());
        }

        // Sort authors for consistency
        let mut sorted_authors = authors.to_vec();
        sorted_authors.sort();
        for author in sorted_authors {
            hasher.update(author.as_bytes());
        }

        hex::encode(hasher.finalize())
    }

    fn state_path(&self, key: &str) -> PathBuf {
        self.state_dir.join(format!("{}.json", key))
    }

    fn lock_path(&self, key: &str) -> PathBuf {
        self.state_dir.join(format!("{}.lock", key))
    }

    fn failure_path(&self, key: &str) -> PathBuf {
        self.state_dir.join(format!("{}.failures.log", key))
    }

    /// Save state to disk
    pub fn save(&self, state: &SyncState) -> Result<(), Error> {
        let key = Self::compute_key(&state.source, &state.dest, &[], &[]);
        let path = self.state_path(&key);

        let json = serde_json::to_string_pretty(state)
            .map_err(|e| Error::new(ErrorKind::StateError, format!("Failed to serialize state: {}", e)))?;

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .map_err(|e| Error::new(ErrorKind::StateError, format!("Failed to open state file: {}", e)))?;

        #[cfg(unix)]
        {
            // Set file permissions to 600 (owner read/write only)
            let perms = fs::Permissions::from_mode(0o600);
            file.set_permissions(perms).map_err(|e| {
                Error::new(ErrorKind::StateError, format!("Failed to set file permissions: {}", e))
            })?;
        }

        file.write_all(json.as_bytes())
            .map_err(|e| Error::new(ErrorKind::StateError, format!("Failed to write state: {}", e)))?;

        Ok(())
    }

    /// Load state from disk
    pub fn load(
        &self,
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
    ) -> Result<Option<SyncState>, Error> {
        let key = Self::compute_key(source, dest, kinds, authors);
        let path = self.state_path(&key);

        if !path.exists() {
            return Ok(None);
        }

        let data = fs::read_to_string(&path)
            .map_err(|e| Error::new(ErrorKind::StateError, format!("Failed to read state file: {}", e)))?;

        let state: SyncState = serde_json::from_str(&data)
            .map_err(|e| Error::new(ErrorKind::StateError, format!("Failed to parse state: {}", e)))?;

        Ok(Some(state))
    }

    /// Check if a process is still running
    fn is_process_running(pid: u32) -> bool {
        #[cfg(unix)]
        {
            // On Unix, signal 0 checks if process exists without actually sending a signal
            unsafe { libc::kill(pid as i32, 0) == 0 }
        }
        #[cfg(not(unix))]
        {
            // On non-Unix, assume process is running (safer default)
            true
        }
    }

    /// Check if lock is stale (process dead or too old)
    fn is_lock_stale(lock_path: &Path) -> bool {
        if let Ok(content) = fs::read_to_string(lock_path) {
            // Format: hostname:pid:timestamp
            let parts: Vec<&str> = content.split(':').collect();
            if parts.len() >= 3 {
                // Check if PID is still running (only if same host)
                let lock_hostname = parts[0];
                let current_hostname = hostname::get()
                    .map(|h| h.to_string_lossy().to_string())
                    .unwrap_or_else(|_| "unknown".to_string());

                if lock_hostname == current_hostname {
                    if let Ok(pid) = parts[1].parse::<u32>() {
                        if !Self::is_process_running(pid) {
                            tracing::info!("Removing stale lock (PID {} no longer running)", pid);
                            return true;
                        }
                    }
                }

                // Also consider lock stale if older than 1 hour
                if let Ok(timestamp) = parts[2].parse::<i64>() {
                    let age = now() - timestamp;
                    if age > 3600 {
                        tracing::info!("Removing stale lock (older than 1 hour)");
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Acquire lock (blocking)
    pub fn acquire_lock(
        &self,
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
    ) -> Result<Option<LockGuard>, Error> {
        let key = Self::compute_key(source, dest, kinds, authors);
        let lock_path = self.lock_path(&key);

        // Check for and remove stale locks
        if lock_path.exists() && Self::is_lock_stale(&lock_path) {
            let _ = fs::remove_file(&lock_path);
        }

        // Try to create lock file exclusively
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
        {
            Ok(mut file) => {
                let hostname = hostname::get()
                    .map(|h| h.to_string_lossy().to_string())
                    .unwrap_or_else(|_| "unknown".to_string());
                let pid = std::process::id();
                let timestamp = now();

                let lock_info = format!("{}:{}:{}", hostname, pid, timestamp);
                file.write_all(lock_info.as_bytes()).map_err(|e| {
                    Error::new(ErrorKind::StateError, format!("Failed to write lock file: {}", e))
                })?;

                #[cfg(unix)]
                {
                    // Set lock file permissions to 600
                    let perms = fs::Permissions::from_mode(0o600);
                    file.set_permissions(perms).map_err(|e| {
                        Error::new(ErrorKind::StateError, format!("Failed to set lock permissions: {}", e))
                    })?;
                }

                Ok(Some(LockGuard { path: lock_path }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                // Lock already held
                Ok(None)
            }
            Err(e) => Err(Error::new(ErrorKind::StateError, format!(
                "Failed to create lock file: {}",
                e
            ))),
        }
    }

    /// Try to acquire lock (non-blocking)
    pub fn try_acquire_lock(
        &self,
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
    ) -> Result<Option<LockGuard>, Error> {
        self.acquire_lock(source, dest, kinds, authors)
    }

    /// Log a failure to sync an event
    pub fn log_failure(
        &self,
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
        event_id: &str,
        reason: &str,
    ) -> Result<(), Error> {
        let key = Self::compute_key(source, dest, kinds, authors);
        let path = self.failure_path(&key);

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| Error::new(ErrorKind::StateError, format!("Failed to open failure log: {}", e)))?;

        #[cfg(unix)]
        {
            // Set file permissions to 600
            let perms = fs::Permissions::from_mode(0o600);
            file.set_permissions(perms).map_err(|e| {
                Error::new(ErrorKind::StateError, format!("Failed to set failure log permissions: {}", e))
            })?;
        }

        let timestamp = now();
        let line = format!("{}:{}:{}\n", timestamp, event_id, reason);
        file.write_all(line.as_bytes())
            .map_err(|e| Error::new(ErrorKind::StateError, format!("Failed to write failure log: {}", e)))?;

        Ok(())
    }

    /// Load failure log
    pub fn load_failures(
        &self,
        source: &str,
        dest: &str,
        kinds: &[u16],
        authors: &[String],
    ) -> Result<Vec<FailureEntry>, Error> {
        let key = Self::compute_key(source, dest, kinds, authors);
        let path = self.failure_path(&key);

        if !path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&path)
            .map_err(|e| Error::new(ErrorKind::StateError, format!("Failed to open failure log: {}", e)))?;

        let reader = BufReader::new(file);
        let mut failures = Vec::new();

        for line in reader.lines() {
            let line = line.map_err(|e| {
                Error::new(ErrorKind::StateError, format!("Failed to read failure log: {}", e))
            })?;
            if let Some(entry) = FailureEntry::parse(&line) {
                failures.push(entry);
            }
        }

        Ok(failures)
    }
}

impl Drop for LockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}
