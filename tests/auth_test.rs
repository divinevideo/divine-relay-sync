// ABOUTME: Tests for NIP-42 authentication
// ABOUTME: Verifies AUTH event creation

use relay_sync::relay::Authenticator;

#[test]
fn test_authenticator_without_keys() {
    let auth = Authenticator::new(None).unwrap();
    assert!(!auth.can_authenticate());
}

#[test]
fn test_authenticator_with_keys() {
    // Test nsec (from nostr-sdk test vectors)
    let nsec = "nsec1vl029mgpspedva04g90vltkh6fvh240zqtv9k0t9af8935ke9laqsnlfe5";
    let auth = Authenticator::new(Some(nsec)).unwrap();
    assert!(auth.can_authenticate());
    assert!(auth.public_key().is_some());
}

#[test]
fn test_create_auth_event() {
    let nsec = "nsec1vl029mgpspedva04g90vltkh6fvh240zqtv9k0t9af8935ke9laqsnlfe5";
    let auth = Authenticator::new(Some(nsec)).unwrap();

    let event = auth.create_auth_event("wss://relay.example.com", "test-challenge").unwrap();

    // Verify it's a kind 22242 event (NIP-42 AUTH)
    assert_eq!(event.kind.as_u16(), 22242);
}

#[test]
fn test_authenticator_keys_getter() {
    let nsec = "nsec1vl029mgpspedva04g90vltkh6fvh240zqtv9k0t9af8935ke9laqsnlfe5";
    let auth = Authenticator::new(Some(nsec)).unwrap();
    assert!(auth.keys().is_some());
}
