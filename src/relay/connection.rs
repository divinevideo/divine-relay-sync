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
        let _ = self.client.disconnect().await;
    }
}
