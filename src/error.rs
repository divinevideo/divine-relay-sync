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
            ErrorKind::NegentropyError => write!(f, "negentropy error"),
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
