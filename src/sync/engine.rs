// ABOUTME: Coordinates bounded event transfers and successful-run checkpoints.
// ABOUTME: Failed or cancelled runs never advance the completed time watermark.

use crate::error::{Error, ErrorKind, Result};
use crate::relay::{auth::Authenticator, connection::RelayConnection};
use crate::state::{StateManager, SyncState};
use nostr_sdk::prelude::*;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

const MAX_CONCURRENT_PUBLISHES: usize = 200;
pub const DEFAULT_EXCLUDE_KINDS: &[u16] = &[1, 5];
pub const DEFAULT_EXCLUDE_TAGS: &[(&str, &str)] = &[("L", "pink.momostr")];

/// Selection and execution policy for a transfer.
#[derive(Debug, Clone)]
pub struct SyncOptions {
    pub source_url: String,
    pub dest_url: String,
    pub kinds: Vec<u16>,
    pub authors: Vec<String>,
    pub since: Option<i64>,
    pub until: Option<i64>,
    pub fresh: bool,
    pub dry_run: bool,
    pub nsec: Option<String>,
    pub exclude_kinds: Vec<u16>,
    pub exclude_tags: Vec<(String, String)>,
    pub include_tags: Vec<(String, String)>,
}

impl SyncOptions {
    /// Full checkpoint identity. Credentials are never serialized: only the
    /// signing public key affects the authenticated selection's namespace.
    pub fn state_key(&self) -> Result<String> {
        fn canonical<T: Ord + Clone>(values: &[T]) -> Vec<T> {
            let mut values = values.to_vec();
            values.sort();
            values.dedup();
            values
        }
        let identity = Authenticator::new(self.nsec.as_deref())?.public_key();
        let encoded = serde_json::to_vec(&(
            "successful-window-v2",
            &self.source_url,
            &self.dest_url,
            canonical(&self.kinds),
            canonical(&self.authors),
            self.since,
            self.until,
            canonical(&self.exclude_kinds),
            canonical(&self.exclude_tags),
            canonical(&self.include_tags),
            identity,
        ))
        .map_err(|e| Error::with_source(ErrorKind::StateError, "serialize selection policy", e))?;
        Ok(hex::encode(Sha256::digest(encoded)))
    }
}

#[derive(Debug, Clone)]
pub struct SyncResult {
    pub events_synced: u64,
    pub events_skipped: u64,
    pub events_failed: u64,
    pub events_filtered: u64,
    pub mode: SyncMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Timestamp,
    Negentropy,
}

pub struct SyncEngine {
    options: SyncOptions,
    state_manager: Arc<StateManager>,
    shutdown: CancellationToken,
}

impl SyncEngine {
    pub fn new(
        options: SyncOptions,
        state_manager: Arc<StateManager>,
        shutdown: CancellationToken,
    ) -> Self {
        Self {
            options,
            state_manager,
            shutdown,
        }
    }

    fn should_filter_event(event: &Event, options: &SyncOptions) -> bool {
        let has_tag = |(name, value): &(String, String)| {
            event.tags.iter().any(|tag| {
                let tag = tag.as_slice();
                tag.len() >= 2 && &tag[0] == name && &tag[1] == value
            })
        };
        options.exclude_kinds.contains(&event.kind.as_u16())
            || options.exclude_tags.iter().any(has_tag)
            || (!options.include_tags.is_empty() && !options.include_tags.iter().any(has_tag))
    }

    fn build_filter(&self, state: &SyncState, end: u64) -> Result<Filter> {
        let lower = self.options.since.unwrap_or(0);
        if lower < 0 || self.options.until.is_some_and(|until| until < lower) {
            return Err(Error::new(ErrorKind::ConfigError, "invalid time range"));
        }
        // Replay the boundary second. An event arriving later at the same
        // timestamp must not be skipped by a cursor + 1 lower bound.
        let since = lower.max(state.cursor_created_at.unwrap_or(0)) as u64;
        if since > end {
            return Err(Error::new(
                ErrorKind::ConfigError,
                "checkpoint lies after requested upper bound",
            ));
        }
        let mut filter = Filter::new()
            .since(Timestamp::from(since))
            .until(Timestamp::from(end));
        if !self.options.kinds.is_empty() {
            filter = filter.kinds(self.options.kinds.iter().copied().map(Kind::from));
        }
        if !self.options.authors.is_empty() {
            let authors = self
                .options
                .authors
                .iter()
                .map(PublicKey::from_hex)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|_| Error::new(ErrorKind::ConfigError, "invalid author public key"))?;
            filter = filter.authors(authors);
        }
        Ok(filter)
    }

    pub async fn run(&self) -> Result<SyncResult> {
        let key = self.options.state_key()?;
        let _lock = self
            .state_manager
            .acquire_scope_lock(&key)?
            .ok_or_else(|| Error::new(ErrorKind::StateError, "another sync is already running"))?;
        let mut state = if self.options.fresh {
            None
        } else {
            self.state_manager.load_for_scope(&key)?
        }
        .unwrap_or_else(|| SyncState::new(&self.options.source_url, &self.options.dest_url));
        let started_at = Timestamp::now().as_u64() as i64;
        let end = self.options.until.unwrap_or(started_at);
        if end < 0 {
            return Err(Error::new(ErrorKind::ConfigError, "negative upper bound"));
        }
        let filter = self.build_filter(&state, end as u64)?;
        let shutdown = self.shutdown.child_token();
        if shutdown.is_cancelled() {
            return Err(Error::new(ErrorKind::NetworkError, "sync cancelled"));
        }
        let source = RelayConnection::connect_with_auth(
            &self.options.source_url,
            self.options.nsec.as_deref(),
        )
        .await?;
        let dest = match RelayConnection::connect_with_auth(
            &self.options.dest_url,
            self.options.nsec.as_deref(),
        )
        .await
        {
            Ok(dest) => dest,
            Err(error) => {
                source.disconnect().await;
                return Err(error);
            }
        };
        let mode = if source.info.supports_negentropy && dest.info.supports_negentropy {
            SyncMode::Negentropy
        } else {
            SyncMode::Timestamp
        };
        let mut result = SyncResult {
            events_synced: 0,
            events_skipped: 0,
            events_failed: 0,
            events_filtered: 0,
            mode,
        };
        let (sender, mut receiver) = mpsc::channel(1000);
        let source_client = source.client().clone();
        let source_url = source.url.clone();
        let nsec = self.options.nsec.clone();
        let fetch_shutdown = shutdown.clone();
        let fetcher = tokio::spawn(async move {
            match mode {
                SyncMode::Timestamp => {
                    super::fetcher::fetch_events(
                        &source_url,
                        filter,
                        nsec.as_deref(),
                        sender,
                        fetch_shutdown,
                    )
                    .await
                }
                SyncMode::Negentropy => super::reconciler::reconcile_with_client(
                    &source_client,
                    &source_url,
                    filter,
                    sender,
                    fetch_shutdown,
                )
                .await
                .map(|_| ()),
            }
        });
        let published = match dest.client().relay(dest.url.as_str()).await {
            Ok(relay) => {
                self.publish_stream(&mut receiver, &relay, &shutdown, &mut result)
                    .await
            }
            Err(error) => Err(Error::with_source(
                ErrorKind::NetworkError,
                "destination relay unavailable",
                error,
            )),
        };
        if published.is_err() {
            shutdown.cancel();
        }
        drop(receiver);
        let fetched = fetcher
            .await
            .map_err(|e| Error::with_source(ErrorKind::NetworkError, "fetcher task failed", e));
        source.disconnect().await;
        dest.disconnect().await;
        published?;
        fetched??;
        if shutdown.is_cancelled() {
            return Err(Error::new(ErrorKind::NetworkError, "sync cancelled"));
        }
        if !self.options.dry_run {
            state.increment_events(result.events_synced);
            // A future upper bound has not elapsed yet: events created between
            // now and that bound must still be scanned by the next run.
            state.cursor_created_at = Some(end.min(started_at));
            state.cursor_id = None;
            self.state_manager.save_for_scope(&state, &key)?;
        }
        Ok(result)
    }

    async fn publish_stream(
        &self,
        receiver: &mut mpsc::Receiver<Event>,
        client: &Relay,
        shutdown: &CancellationToken,
        stats: &mut SyncResult,
    ) -> Result<()> {
        let mut workers = JoinSet::new();
        loop {
            tokio::select! {
                _ = shutdown.cancelled() => return Err(Error::new(ErrorKind::NetworkError, "sync cancelled")),
                completed = workers.join_next(), if !workers.is_empty() => {
                    Self::record_publish(completed, stats)?;
                }
                event = receiver.recv(), if workers.len() < MAX_CONCURRENT_PUBLISHES => {
                    let Some(event) = event else { break; };
                    if Self::should_filter_event(&event, &self.options) { stats.events_filtered += 1; continue; }
                    if self.options.dry_run { stats.events_synced += 1; continue; }
                    let client = client.clone();
                    workers.spawn(async move { super::publisher::publish_event(&client, &event, None).await });
                }
            }
        }
        while !workers.is_empty() {
            tokio::select! {
                _ = shutdown.cancelled() => return Err(Error::new(ErrorKind::NetworkError, "sync cancelled")),
                completed = workers.join_next() => Self::record_publish(completed, stats)?,
            }
        }
        Ok(())
    }

    fn record_publish(
        completed: Option<std::result::Result<Result<bool>, tokio::task::JoinError>>,
        stats: &mut SyncResult,
    ) -> Result<()> {
        match completed.expect("nonempty worker set").map_err(|e| {
            Error::with_source(ErrorKind::NetworkError, "publisher task failed", e)
        })?? {
            true => stats.events_synced += 1,
            false => stats.events_skipped += 1,
        }
        Ok(())
    }
}
