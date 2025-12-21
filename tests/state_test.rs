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
