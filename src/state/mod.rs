// ABOUTME: State management module exports
// ABOUTME: Handles persistence, locking, and failure tracking

mod manager;
mod failures;

pub use manager::{StateManager, SyncState, LockGuard};
pub use failures::FailureEntry;