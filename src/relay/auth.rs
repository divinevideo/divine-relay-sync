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

        let relay = RelayUrl::parse(relay_url).map_err(|e| {
            Error::with_source(ErrorKind::ConfigError, "invalid relay URL", e)
        })?;

        let event = EventBuilder::auth(challenge, relay)
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

    /// Get the keys if available (for Client signer)
    pub fn keys(&self) -> Option<&Keys> {
        self.keys.as_ref()
    }
}
