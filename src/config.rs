// ABOUTME: TOML config file parsing for relay-sync
// ABOUTME: Supports multiple sync configs and auth settings

use crate::error::{Error, ErrorKind, Result};
use serde::Deserialize;
use std::fs;
use std::path::Path;

#[derive(Debug, Deserialize, Clone)]
pub struct Config {
    pub auth: Option<AuthConfig>,
    #[serde(default)]
    pub sync: Vec<SyncConfig>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct AuthConfig {
    pub nsec: Option<String>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct SyncConfig {
    pub name: String,
    pub source: String,
    pub dest: String,
    pub kinds: Option<Vec<u16>>,
    pub authors: Option<Vec<String>>,
    pub since: Option<String>,
    pub until: Option<String>,
}

impl AuthConfig {
    /// Resolve nsec, substituting environment variables
    pub fn resolve_nsec(&self) -> Option<String> {
        self.nsec.as_ref().and_then(|s| {
            if s.starts_with("${") && s.ends_with("}") {
                let var_name = &s[2..s.len() - 1];
                std::env::var(var_name).ok()
            } else {
                Some(s.clone())
            }
        })
    }
}

impl Config {
    /// Load config from file
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self> {
        let content = fs::read_to_string(path.as_ref()).map_err(|e| {
            Error::with_source(
                ErrorKind::ConfigError,
                format!("failed to read config file: {}", path.as_ref().display()),
                e,
            )
        })?;
        Self::from_str(&content)
    }

    /// Parse config from string
    pub fn from_str(content: &str) -> Result<Self> {
        toml::from_str(content).map_err(|e| {
            Error::with_source(ErrorKind::ConfigError, "failed to parse config", e)
        })
    }

    /// Find sync config by name
    pub fn find_sync(&self, name: &str) -> Option<&SyncConfig> {
        self.sync.iter().find(|s| s.name == name)
    }

    /// Get nsec from auth config
    pub fn nsec(&self) -> Option<String> {
        self.auth.as_ref().and_then(|a| a.resolve_nsec())
    }
}