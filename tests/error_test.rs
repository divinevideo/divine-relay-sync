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
