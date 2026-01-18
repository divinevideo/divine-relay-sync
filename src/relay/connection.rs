// ABOUTME: Relay connection management
// ABOUTME: Handles WebSocket connection and NIP-11 discovery

use crate::error::{Error, ErrorKind, Result};
use crate::relay::auth::Authenticator;
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
    authenticator: Option<Authenticator>,
}

impl RelayConnection {
    /// Connect to a relay
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_auth(url, None).await
    }

    /// Connect to a relay with authentication keys
    /// Tries wss:// first, falls back to ws:// if that fails
    pub async fn connect_with_auth(url: &str, nsec: Option<&str>) -> Result<Self> {
        // Build list of URLs to try (wss first, then ws fallback)
        let urls_to_try = Self::get_fallback_urls(url);

        let mut last_error = None;

        for try_url in &urls_to_try {
            tracing::debug!("Attempting connection to {}", try_url);

            match Self::try_connect(try_url, nsec).await {
                Ok(conn) => {
                    if try_url != url {
                        tracing::info!("Connected using fallback URL: {}", try_url);
                    }
                    return Ok(conn);
                }
                Err(e) => {
                    tracing::warn!("Failed to connect to {}: {}", try_url, e);
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            Error::new(ErrorKind::NetworkError, format!("failed to connect to {}", url))
        }))
    }

    /// Get list of URLs to try with fallback (wss first, then ws)
    fn get_fallback_urls(url: &str) -> Vec<String> {
        let mut urls = Vec::new();

        if url.starts_with("wss://") {
            // Try wss first, then ws fallback
            urls.push(url.to_string());
            urls.push(url.replace("wss://", "ws://"));
        } else if url.starts_with("ws://") {
            // Try ws first, then wss fallback
            urls.push(url.to_string());
            urls.push(url.replace("ws://", "wss://"));
        } else {
            // No protocol - try wss first, then ws
            urls.push(format!("wss://{}", url));
            urls.push(format!("ws://{}", url));
        }

        urls
    }

    /// Attempt a single connection (no fallback)
    async fn try_connect(url: &str, nsec: Option<&str>) -> Result<Self> {
        let info = RelayInfo::fetch(url).await.unwrap_or_else(|e| {
            tracing::warn!("Failed to fetch NIP-11 info for {}: {}", url, e);
            RelayInfo {
                url: url.to_string(),
                ..Default::default()
            }
        });

        // Create authenticator if keys provided
        let authenticator = if let Some(nsec) = nsec {
            Some(Authenticator::new(Some(nsec))?)
        } else {
            None
        };

        // Create client with or without signer
        let client = if let Some(ref auth) = authenticator {
            if let Some(keys) = auth.keys() {
                Client::new(keys.clone())
            } else {
                Client::default()
            }
        } else {
            Client::default()
        };

        // Configure relay options for persistent connections
        let relay_opts = RelayOptions::new()
            .ping(false)  // Disable ping - some relays don't respond
            .reconnect(true);  // Auto-reconnect on disconnect

        // Use pool directly to set custom options
        let relay_url = RelayUrl::parse(url).map_err(|e| {
            Error::with_source(ErrorKind::ConfigError, format!("invalid relay URL: {}", url), e)
        })?;

        client.pool().add_relay(relay_url.clone(), relay_opts).await.map_err(|e| {
            Error::with_source(ErrorKind::NetworkError, format!("failed to add relay {}", url), e)
        })?;

        client.connect().await;

        // Poll for connection status with timeout
        let relay = client.relay(relay_url).await.map_err(|e| {
            Error::with_source(ErrorKind::NetworkError, format!("failed to get relay {}", url), e)
        })?;

        // Wait up to 5 seconds for connection, checking every 100ms
        let mut connected = false;
        for _ in 0..50 {
            if relay.is_connected() {
                connected = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        if !connected {
            // Disconnect and clean up before returning error
            let _ = client.disconnect().await;
            return Err(Error::new(
                ErrorKind::NetworkError,
                format!("failed to establish connection to {}", url),
            ));
        }

        // Check if auth is required and we can authenticate
        if info.auth_required {
            if authenticator.is_none() {
                tracing::warn!("Relay {} requires auth but no keys provided", url);
            } else {
                tracing::info!("Relay {} requires auth, keys available", url);
            }
        }

        Ok(Self {
            url: url.to_string(),
            info,
            client,
            authenticator,
        })
    }

    /// Check if we can authenticate
    pub fn can_authenticate(&self) -> bool {
        self.authenticator.as_ref().map(|a| a.can_authenticate()).unwrap_or(false)
    }

    /// Get the underlying nostr-sdk client
    pub fn client(&self) -> &Client {
        &self.client
    }

    /// Disconnect from relay
    pub async fn disconnect(&self) {
        let _ = self.client.disconnect().await;
    }
}
