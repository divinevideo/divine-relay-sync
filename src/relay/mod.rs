// ABOUTME: Relay communication module exports
// ABOUTME: Handles connections, NIP-11, NIP-42, NIP-77

pub mod connection;
pub mod auth;

pub use connection::{RelayConnection, RelayInfo};