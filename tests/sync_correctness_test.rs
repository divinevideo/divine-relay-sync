use futures::{SinkExt, StreamExt};
use nostr_sdk::prelude::*;
use relay_sync::state::StateManager;
use relay_sync::sync::{SyncEngine, SyncOptions};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_tungstenite::{
    tungstenite::{protocol::Role, Message},
    WebSocketStream,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Default)]
struct Behavior {
    cap: Option<usize>,
    approximate: bool,
    reject_count: bool,
    reject_write: bool,
    drop_eose: bool,
    hang_count: bool,
    require_auth: bool,
    offer_auth: bool,
    negentropy: bool,
    reject_negentropy: bool,
    drift_after_page: bool,
    auth_reply_noise: bool,
}

struct Relay {
    url: String,
    events: Arc<Mutex<HashMap<EventId, Event>>>,
    behavior: Arc<Mutex<Behavior>>,
    task: JoinHandle<()>,
    count_seen: Arc<tokio::sync::Notify>,
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Relay {
    async fn start(behavior: Behavior) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let events = Arc::new(Mutex::new(HashMap::<EventId, Event>::new()));
        let config = Arc::new(Mutex::new(behavior));
        let count_seen = Arc::new(tokio::sync::Notify::new());
        let seen = count_seen.clone();
        let stored = events.clone();
        let options = config.clone();
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let seen = seen.clone();
                let stored = stored.clone();
                let options = options.clone();
                tokio::spawn(async move {
                    let mut socket = socket;
                    let mut header = Vec::new();
                    while !header.ends_with(b"\r\n\r\n") {
                        let Ok(byte) = socket.read_u8().await else {
                            return;
                        };
                        header.push(byte);
                        if header.len() > 8192 {
                            return;
                        }
                    }
                    let header = String::from_utf8(header).unwrap();
                    if !header.to_ascii_lowercase().contains("upgrade: websocket") {
                        let advertise_negentropy = options.lock().unwrap().negentropy;
                        let body = json!({"supported_nips": if advertise_negentropy { vec![1,45,77] } else { vec![1,45] }}).to_string();
                        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/nostr+json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body);
                        socket.write_all(response.as_bytes()).await.unwrap();
                        return;
                    }
                    let key = header
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("sec-websocket-key")
                                .then_some(value.trim())
                        })
                        .unwrap();
                    let accept = tokio_tungstenite::tungstenite::handshake::derive_accept_key(
                        key.as_bytes(),
                    );
                    socket.write_all(format!("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n").as_bytes()).await.unwrap();
                    let mut ws = WebSocketStream::from_raw_socket(socket, Role::Server, None).await;
                    let require_auth = options.lock().unwrap().require_auth;
                    let mut authenticated = !require_auth;
                    if require_auth || options.lock().unwrap().offer_auth {
                        ws.send(Message::Text(
                            json!(["AUTH", "generated-test-challenge"]).to_string(),
                        ))
                        .await
                        .unwrap();
                    }
                    let mut neg_sessions = HashMap::<
                        String,
                        negentropy::Negentropy<negentropy::NegentropyStorageVector>,
                    >::new();
                    let mut page_seen = false;
                    while let Some(Ok(message)) = ws.next().await {
                        if !message.is_text() {
                            continue;
                        }
                        let Ok(v) = serde_json::from_str::<Value>(message.to_text().unwrap())
                        else {
                            continue;
                        };
                        let behavior = options.lock().unwrap().clone();
                        let mut replies = Vec::new();
                        match v[0].as_str() {
                            Some("NEG-OPEN") => {
                                if behavior.reject_negentropy {
                                    replies.push(json!([
                                        "NEG-ERR",
                                        v[1],
                                        "blocked: test rejection"
                                    ]));
                                } else {
                                    let filter: Filter =
                                        serde_json::from_value(v[2].clone()).unwrap();
                                    let mut storage = negentropy::NegentropyStorageVector::new();
                                    for event in stored
                                        .lock()
                                        .unwrap()
                                        .values()
                                        .filter(|e| filter.match_event(e))
                                    {
                                        storage
                                            .insert(
                                                event.created_at.as_u64(),
                                                negentropy::Id::new(event.id.to_bytes()),
                                            )
                                            .unwrap();
                                    }
                                    storage.seal().unwrap();
                                    let mut neg =
                                        negentropy::Negentropy::new(storage, 60_000).unwrap();
                                    let bytes = negentropy::Bytes::from(
                                        hex::decode(v[3].as_str().unwrap()).unwrap(),
                                    );
                                    let response = neg.reconcile(&bytes).unwrap();
                                    replies.push(json!([
                                        "NEG-MSG",
                                        v[1],
                                        hex::encode(response.as_ref())
                                    ]));
                                    neg_sessions.insert(v[1].as_str().unwrap().into(), neg);
                                }
                            }
                            Some("NEG-MSG") => {
                                let neg = neg_sessions.get_mut(v[1].as_str().unwrap()).unwrap();
                                let bytes = negentropy::Bytes::from(
                                    hex::decode(v[2].as_str().unwrap()).unwrap(),
                                );
                                let response = neg.reconcile(&bytes).unwrap();
                                replies.push(json!([
                                    "NEG-MSG",
                                    v[1],
                                    hex::encode(response.as_ref())
                                ]));
                            }
                            Some("AUTH") => {
                                let event: Event = serde_json::from_value(v[1].clone()).unwrap();
                                event.verify().unwrap();
                                assert_eq!(event.kind, Kind::Authentication);
                                authenticated = true;
                                if behavior.auth_reply_noise {
                                    replies.push(json!(["AUTH", "followup-test-challenge"]));
                                    replies.push(json!([
                                        "OK",
                                        "0000000000000000000000000000000000000000000000000000000000000000",
                                        true,
                                        ""
                                    ]));
                                }
                                replies.push(json!(["OK", event.id, true, ""]));
                            }
                            Some("EVENT") => {
                                let event: Event = serde_json::from_value(v[1].clone()).unwrap();
                                event.verify().unwrap();
                                let mut db = stored.lock().unwrap();
                                if behavior.reject_write {
                                    replies.push(json!([
                                        "OK",
                                        event.id,
                                        false,
                                        "blocked: test rejection"
                                    ]));
                                } else if let std::collections::hash_map::Entry::Vacant(entry) =
                                    db.entry(event.id)
                                {
                                    entry.insert(event.clone());
                                    replies.push(json!(["OK", event.id, true, ""]));
                                } else {
                                    replies.push(json!([
                                        "OK",
                                        event.id,
                                        false,
                                        "duplicate: already have"
                                    ]));
                                }
                            }

                            Some("REQ") | Some("COUNT") => {
                                if v[0] == "COUNT" {
                                    seen.notify_one();
                                }
                                if v[0] == "COUNT" && behavior.hang_count {
                                    continue;
                                }
                                if !authenticated {
                                    ws.send(Message::Text(
                                        json!(["CLOSED", v[1], "auth-required: test source"])
                                            .to_string(),
                                    ))
                                    .await
                                    .unwrap();
                                    continue;
                                }
                                let filter: Filter = serde_json::from_value(v[2].clone()).unwrap();
                                let mut found: Vec<_> = stored
                                    .lock()
                                    .unwrap()
                                    .values()
                                    .filter(|event| filter.match_event(event))
                                    .cloned()
                                    .collect();
                                found.sort_by(|a, b| {
                                    b.created_at
                                        .cmp(&a.created_at)
                                        .then_with(|| a.id.cmp(&b.id))
                                });
                                if v[0] == "COUNT" {
                                    if behavior.reject_count {
                                        replies.push(json!(["CLOSED", v[1], "unsupported: COUNT"]));
                                    } else {
                                        replies.push(json!(["COUNT", v[1], {"count":found.len() + usize::from(page_seen && behavior.drift_after_page),"approximate":behavior.approximate}]));
                                    }
                                } else {
                                    page_seen = true;
                                    let limit = behavior
                                        .cap
                                        .unwrap_or(usize::MAX)
                                        .min(filter.limit.unwrap_or(usize::MAX));
                                    replies.extend(
                                        found
                                            .into_iter()
                                            .take(limit)
                                            .map(|event| json!(["EVENT", v[1], event])),
                                    );
                                    if behavior.drop_eose {
                                        return;
                                    }
                                    replies.push(json!(["EOSE", v[1]]));
                                }
                            }
                            _ => {}
                        }
                        for reply in replies {
                            if ws.send(Message::Text(reply.to_string())).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        Self {
            url,
            events,
            behavior: config,
            task,
            count_seen,
        }
    }

    fn seed(&self, timestamp: u64) {
        let event = EventBuilder::new(Kind::ContactList, "")
            .custom_created_at(Timestamp::from(timestamp))
            .sign_with_keys(&Keys::generate())
            .unwrap();
        self.events.lock().unwrap().insert(event.id, event);
    }
}

fn options(source: &Relay, dest: &Relay) -> SyncOptions {
    SyncOptions {
        source_url: source.url.clone(),
        dest_url: dest.url.clone(),
        kinds: vec![3],
        authors: vec![],
        since: Some(0),
        until: Some(100),
        fresh: false,
        dry_run: false,
        nsec: None,
        exclude_kinds: vec![],
        exclude_tags: vec![],
        include_tags: vec![],
    }
}

async fn run(
    options: SyncOptions,
    dir: &TempDir,
) -> relay_sync::error::Result<relay_sync::sync::SyncResult> {
    SyncEngine::new(
        options,
        Arc::new(StateManager::new(dir.path()).unwrap()),
        CancellationToken::new(),
    )
    .run()
    .await
}

#[tokio::test]
async fn capped_equal_timestamp_cannot_report_success_or_save_progress() {
    let source = Relay::start(Behavior {
        cap: Some(1),
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(50);
    source.seed(50);
    let dir = TempDir::new().unwrap();
    assert!(run(options(&source, &dest), &dir).await.is_err());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn filtered_run_saves_progress_and_repeated_run_keeps_same_second_events() {
    let source = Relay::start(Behavior::default()).await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(100);
    let dir = TempDir::new().unwrap();
    run(options(&source, &dest), &dir).await.unwrap();
    let files: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|f| f.unwrap().path())
        .collect();
    assert_eq!(files.len(), 1);
    let state: Value = serde_json::from_slice(&std::fs::read(&files[0]).unwrap()).unwrap();
    assert_eq!(state["events_synced"], 1);
    assert_eq!(state["cursor_created_at"], 100);
    source.seed(100);
    run(options(&source, &dest), &dir).await.unwrap();
    assert_eq!(dest.events.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn capped_distinct_timestamps_are_split_and_all_copied() {
    let source = Relay::start(Behavior {
        cap: Some(1),
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(40);
    source.seed(50);
    source.seed(60);
    let dir = TempDir::new().unwrap();
    run(options(&source, &dest), &dir).await.unwrap();
    assert_eq!(dest.events.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn approximate_or_unsupported_counts_do_not_advance_state() {
    for behavior in [
        Behavior {
            approximate: true,
            ..Default::default()
        },
        Behavior {
            reject_count: true,
            ..Default::default()
        },
    ] {
        let source = Relay::start(behavior).await;
        let dest = Relay::start(Behavior::default()).await;
        source.seed(50);
        let dir = TempDir::new().unwrap();
        assert!(run(options(&source, &dest), &dir).await.is_err());
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    }
}

#[tokio::test]
async fn failed_publish_preserves_checkpoint_and_retry_delivers_missing_event() {
    let source = Relay::start(Behavior::default()).await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(100);
    let dir = TempDir::new().unwrap();
    run(options(&source, &dest), &dir).await.unwrap();
    let checkpoint = std::fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let before = std::fs::read(&checkpoint).unwrap();
    source.seed(100);
    dest.behavior.lock().unwrap().reject_write = true;
    assert!(run(options(&source, &dest), &dir).await.is_err());
    assert_eq!(std::fs::read(&checkpoint).unwrap(), before);
    dest.behavior.lock().unwrap().reject_write = false;
    run(options(&source, &dest), &dir).await.unwrap();
    assert_eq!(dest.events.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn disconnect_before_eose_is_not_a_complete_scan() {
    let source = Relay::start(Behavior {
        drop_eose: true,
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(50);
    let dir = TempDir::new().unwrap();
    assert!(run(options(&source, &dest), &dir).await.is_err());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn cancellation_during_count_stops_without_checkpointing() {
    let source = Relay::start(Behavior {
        hang_count: true,
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior::default()).await;
    let dir = TempDir::new().unwrap();
    let shutdown = CancellationToken::new();
    let engine = SyncEngine::new(
        options(&source, &dest),
        Arc::new(StateManager::new(dir.path()).unwrap()),
        shutdown.clone(),
    );
    let operation = tokio::spawn(async move { engine.run().await });
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        source.count_seen.notified(),
    )
    .await
    .unwrap();
    shutdown.cancel();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(1), operation)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn dry_run_does_not_checkpoint_and_epoch_events_are_not_skipped() {
    let source = Relay::start(Behavior::default()).await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(0);
    source.seed(1);
    let dir = TempDir::new().unwrap();
    let mut opts = options(&source, &dest);
    opts.dry_run = true;
    assert_eq!(run(opts, &dir).await.unwrap().events_synced, 2);
    assert!(dest.events.lock().unwrap().is_empty());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
    run(options(&source, &dest), &dir).await.unwrap();
    assert_eq!(dest.events.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn authenticated_timestamp_source_uses_signed_challenge() {
    let source = Relay::start(Behavior {
        require_auth: true,
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(50);
    let dir = TempDir::new().unwrap();
    let mut opts = options(&source, &dest);
    opts.nsec = Some(Keys::generate().secret_key().to_secret_hex());
    run(opts, &dir).await.unwrap();
    assert_eq!(dest.events.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn auth_reply_is_matched_after_unrelated_messages() {
    let source = Relay::start(Behavior {
        require_auth: true,
        auth_reply_noise: true,
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(50);
    let dir = TempDir::new().unwrap();
    let mut opts = options(&source, &dest);
    opts.nsec = Some(Keys::generate().secret_key().to_secret_hex());
    run(opts, &dir).await.unwrap();
    assert_eq!(dest.events.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn cli_returns_nonzero_for_incomplete_timestamp_scan() {
    let source = Relay::start(Behavior {
        cap: Some(1),
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(50);
    source.seed(50);
    let dir = TempDir::new().unwrap();
    let output = tokio::process::Command::new(env!("CARGO_BIN_EXE_relay-sync"))
        .args([
            &source.url,
            &dest.url,
            "-k",
            "3",
            "--until",
            "100",
            "--json",
        ])
        .current_dir(dir.path())
        .output()
        .await
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("incomplete timestamp sync"));
    assert_eq!(
        std::fs::read_dir(dir.path().join(".relay-sync-state"))
            .unwrap()
            .count(),
        0
    );
}

#[tokio::test]
async fn selection_changes_do_not_reuse_a_completed_checkpoint() {
    let source = Relay::start(Behavior::default()).await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(50);
    let dir = TempDir::new().unwrap();
    let mut opts = options(&source, &dest);
    opts.include_tags = vec![("t".into(), "test".into())];
    assert_eq!(run(opts, &dir).await.unwrap().events_filtered, 1);
    run(options(&source, &dest), &dir).await.unwrap();
    assert_eq!(dest.events.lock().unwrap().len(), 1);
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 2);
}

#[tokio::test]
async fn incomplete_refresh_preserves_existing_checkpoint() {
    // The source changes its count to an approximate response between runs;
    // a previous successful checkpoint must survive the failed refresh.
    let source = Relay::start(Behavior::default()).await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(100);
    let dir = TempDir::new().unwrap();
    run(options(&source, &dest), &dir).await.unwrap();
    let checkpoint = std::fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let before = std::fs::read(&checkpoint).unwrap();
    source.behavior.lock().unwrap().approximate = true;
    assert!(run(options(&source, &dest), &dir).await.is_err());
    assert_eq!(std::fs::read(checkpoint).unwrap(), before);
}

#[test]
fn full_selection_key_is_order_independent_and_separates_policy_changes() {
    let mut opts = SyncOptions {
        source_url: "ws://source.test".into(),
        dest_url: "ws://dest.test".into(),
        kinds: vec![3, 0],
        authors: vec![],
        since: Some(0),
        until: None,
        fresh: false,
        dry_run: false,
        nsec: None,
        exclude_kinds: vec![],
        exclude_tags: vec![],
        include_tags: vec![],
    };
    let original = opts.state_key().unwrap();
    opts.kinds = vec![0, 3, 3];
    assert_eq!(opts.state_key().unwrap(), original);
    opts.include_tags.push(("p".into(), "test".into()));
    assert_ne!(opts.state_key().unwrap(), original);
    opts.include_tags.clear();
    opts.until = Some(100);
    assert_ne!(opts.state_key().unwrap(), original);
    opts.until = None;
    opts.exclude_kinds.push(5);
    assert_ne!(opts.state_key().unwrap(), original);
    assert_ne!(
        StateManager::compute_key("source", "dest", &[1, 234], &[]),
        StateManager::compute_key("source", "dest", &[12, 34], &[])
    );
}

#[tokio::test]
async fn count_drift_between_req_and_eose_verification_fails_closed() {
    let source = Relay::start(Behavior {
        drift_after_page: true,
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(50);
    let dir = TempDir::new().unwrap();
    assert!(run(options(&source, &dest), &dir).await.is_err());
    assert!(dest.events.lock().unwrap().is_empty());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn negentropy_transfers_received_events_and_propagates_failure() {
    let source = Relay::start(Behavior {
        negentropy: true,
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior {
        negentropy: true,
        ..Default::default()
    })
    .await;
    source.seed(50);
    source.seed(50);
    let dir = TempDir::new().unwrap();
    let result = run(options(&source, &dest), &dir).await.unwrap();
    assert_eq!(result.mode, relay_sync::sync::engine::SyncMode::Negentropy);
    assert_eq!(dest.events.lock().unwrap().len(), 2);
    let checkpoint = std::fs::read_dir(dir.path())
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let before = std::fs::read(&checkpoint).unwrap();
    source.behavior.lock().unwrap().reject_negentropy = true;
    assert!(run(options(&source, &dest), &dir).await.is_err());
    assert_eq!(std::fs::read(checkpoint).unwrap(), before);
}

#[tokio::test]
async fn future_upper_bound_checkpoints_only_elapsed_time() {
    let source = Relay::start(Behavior::default()).await;
    let dest = Relay::start(Behavior::default()).await;
    let now = Timestamp::now().as_u64();
    source.seed(now - 10);
    let dir = TempDir::new().unwrap();
    let mut opts = options(&source, &dest);
    opts.until = Some((now + 86_400) as i64);
    run(opts.clone(), &dir).await.unwrap();
    // Created after the first run, but still inside the configured bound.
    source.seed(now + 60);
    run(opts, &dir).await.unwrap();
    assert_eq!(dest.events.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn expired_source_events_count_toward_coverage_but_are_not_republished() {
    let source = Relay::start(Behavior::default()).await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(40);
    let expired = EventBuilder::new(Kind::ContactList, "")
        .tag(Tag::expiration(Timestamp::from(60)))
        .custom_created_at(Timestamp::from(50))
        .sign_with_keys(&Keys::generate())
        .unwrap();
    source
        .events
        .lock()
        .unwrap()
        .insert(expired.id, expired.clone());
    let dir = TempDir::new().unwrap();
    run(options(&source, &dest), &dir).await.unwrap();
    let delivered = dest.events.lock().unwrap();
    assert_eq!(delivered.len(), 1);
    assert!(!delivered.contains_key(&expired.id));
}

#[test]
fn active_process_lock_does_not_expire_during_a_long_sync() {
    let dir = TempDir::new().unwrap();
    let manager = StateManager::new(dir.path()).unwrap();
    let key = "generated-selection";
    let _lock = manager.acquire_scope_lock(key).unwrap().unwrap();
    let contents = format!(
        "{}:{}:0",
        hostname::get().unwrap().to_string_lossy(),
        std::process::id()
    );
    std::fs::write(dir.path().join(format!("{key}.lock")), contents).unwrap();
    assert!(manager.acquire_scope_lock(key).unwrap().is_none());
}

#[tokio::test]
async fn fresh_rescans_backdated_arrivals_outside_the_forward_watermark() {
    let source = Relay::start(Behavior::default()).await;
    let dest = Relay::start(Behavior::default()).await;
    source.seed(50);
    let dir = TempDir::new().unwrap();
    run(options(&source, &dest), &dir).await.unwrap();
    source.seed(40);
    run(options(&source, &dest), &dir).await.unwrap();
    assert_eq!(dest.events.lock().unwrap().len(), 1);
    let mut opts = options(&source, &dest);
    opts.fresh = true;
    run(opts, &dir).await.unwrap();
    assert_eq!(dest.events.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn negentropy_missing_downloads_cannot_checkpoint_success() {
    let source = Relay::start(Behavior {
        negentropy: true,
        cap: Some(1),
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior {
        negentropy: true,
        ..Default::default()
    })
    .await;
    source.seed(50);
    source.seed(50);
    let dir = TempDir::new().unwrap();
    assert!(run(options(&source, &dest), &dir).await.is_err());
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn missing_count_response_times_out_without_checkpoint() {
    let source = Relay::start(Behavior {
        hang_count: true,
        ..Default::default()
    })
    .await;
    let dest = Relay::start(Behavior::default()).await;
    let dir = TempDir::new().unwrap();
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        run(options(&source, &dest), &dir),
    )
    .await
    .unwrap();
    assert!(result.unwrap_err().message.contains("COUNT timed out"));
    assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 0);
}

#[tokio::test]
async fn optional_auth_challenges_do_not_block_public_reads_with_or_without_keys() {
    for use_keys in [false, true] {
        let source = Relay::start(Behavior {
            offer_auth: true,
            ..Default::default()
        })
        .await;
        let dest = Relay::start(Behavior::default()).await;
        source.seed(50);
        let dir = TempDir::new().unwrap();
        let mut opts = options(&source, &dest);
        if use_keys {
            opts.nsec = Some(Keys::generate().secret_key().to_secret_hex());
        }
        run(opts, &dir).await.unwrap();
        assert_eq!(dest.events.lock().unwrap().len(), 1);
    }
}
