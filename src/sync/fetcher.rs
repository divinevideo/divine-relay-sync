// ABOUTME: Strict timestamp-window fetching with exact NIP-45 coverage checks.
// ABOUTME: Splits capped windows and refuses to checkpoint ambiguous results.

use crate::error::{Error, ErrorKind, Result};
use crate::relay::auth::Authenticator;
use futures::{SinkExt, StreamExt};
use nostr_sdk::prelude::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use tokio_util::sync::CancellationToken;

const PAGE_SIZE: usize = 500;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

fn incomplete(message: impl Into<String>) -> Error {
    Error::new(
        ErrorKind::NetworkError,
        format!("incomplete timestamp sync: {}", message.into()),
    )
}

struct Source {
    socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    auth: Authenticator,
    url: String,
    challenge: Option<String>,
}

impl Source {
    async fn send(&mut self, value: &Value) -> Result<()> {
        tokio::time::timeout(
            REQUEST_TIMEOUT,
            self.socket.send(Message::Text(value.to_string())),
        )
        .await
        .map_err(|_| incomplete("source send timed out"))?
        .map_err(|e| incomplete(format!("source send failed: {e}")))
    }

    async fn receive(&mut self) -> Result<Value> {
        loop {
            match self.socket.next().await {
                Some(Ok(Message::Text(text))) => {
                    return serde_json::from_str(&text)
                        .map_err(|_| incomplete("source returned malformed JSON"))
                }
                Some(Ok(Message::Close(_))) | None => {
                    return Err(incomplete("source disconnected before completing request"))
                }
                Some(Err(e)) => return Err(incomplete(format!("source read failed: {e}"))),
                _ => {}
            }
        }
    }

    async fn response(&mut self, request: &Value) -> Result<Value> {
        let mut retried_auth = false;
        loop {
            let message = self.receive().await?;
            if message[0] == "AUTH" {
                // A challenge is an offer, not evidence that a public read
                // requires authentication. Keep it for an explicit rejection.
                self.challenge = Some(
                    message[1]
                        .as_str()
                        .ok_or_else(|| incomplete("invalid auth challenge"))?
                        .to_owned(),
                );
                continue;
            }
            if message[1] == request[1] {
                if message[0] == "CLOSED" {
                    if !retried_auth
                        && message[2]
                            .as_str()
                            .is_some_and(|reason| reason.starts_with("auth-required:"))
                    {
                        let challenge = self.challenge.as_deref().ok_or_else(|| {
                            incomplete("source requires authentication without a challenge")
                        })?;
                        let event = self.auth.create_auth_event(&self.url, challenge)?;
                        self.send(&json!(["AUTH", event])).await?;
                        let reply = self.receive().await?;
                        if reply[0] != "OK" || reply[1] != event.id.to_hex() || reply[2] != true {
                            return Err(incomplete("source rejected authentication"));
                        }
                        retried_auth = true;
                        self.send(request).await?;
                        continue;
                    }
                    return Err(incomplete("source closed request"));
                }
                return Ok(message);
            }
            if message[0] == "NOTICE" {
                return Err(incomplete("source returned a NOTICE"));
            }
        }
    }

    async fn count(&mut self, filter: &Filter) -> Result<u64> {
        let request = json!(["COUNT", SubscriptionId::generate(), filter]);
        let result = tokio::time::timeout(REQUEST_TIMEOUT, async {
            self.send(&request).await?;
            let response = self.response(&request).await?;
            if response[0] != "COUNT" {
                return Err(incomplete("expected exact COUNT response"));
            }
            let body = &response[2];
            if body.get("approximate").is_some_and(|v| v != false) {
                return Err(incomplete("approximate COUNT cannot establish coverage"));
            }
            body["count"]
                .as_u64()
                .ok_or_else(|| incomplete("invalid COUNT response"))
        })
        .await
        .map_err(|_| incomplete("COUNT timed out"))?;
        // Subscription ids are distinct, so late COUNT replies cannot satisfy
        // the next request. CLOSE also releases relay-side resources.
        self.send(&json!(["CLOSE", request[1]])).await?;
        result
    }

    async fn page(&mut self, filter: &Filter) -> Result<Vec<Event>> {
        let request = json!([
            "REQ",
            SubscriptionId::generate(),
            filter.clone().limit(PAGE_SIZE)
        ]);
        let result = tokio::time::timeout(REQUEST_TIMEOUT, async {
            self.send(&request).await?;
            let mut events = BTreeMap::new();
            loop {
                let response = self.response(&request).await?;
                match response[0].as_str() {
                    Some("EOSE") => return Ok(events.into_values().collect()),
                    Some("EVENT") => {
                        let event: Event = serde_json::from_value(response[2].clone())
                            .map_err(|_| incomplete("invalid source event"))?;
                        event
                            .verify()
                            .map_err(|_| incomplete("invalid source signature"))?;
                        if !filter.match_event(&event) {
                            return Err(incomplete("source event does not match request"));
                        }
                        events.insert(event.id, event);
                        if events.len() > PAGE_SIZE {
                            return Err(incomplete("source exceeded requested page bound"));
                        }
                    }
                    _ => return Err(incomplete("unexpected request response")),
                }
            }
        })
        .await
        .map_err(|_| incomplete("REQ timed out before EOSE"))?;
        self.send(&json!(["CLOSE", request[1]])).await?;
        result
    }
}

/// Transfer only windows whose unique events agree with exact source counts
/// before and after REQ. Counts describe that relay, not global Nostr coverage.
pub async fn fetch_events(
    url: &str,
    filter: Filter,
    nsec: Option<&str>,
    sender: mpsc::Sender<Event>,
    shutdown: CancellationToken,
) -> Result<()> {
    tokio::select! {
        _ = shutdown.cancelled() => Err(incomplete("cancelled")),
        result = async {
            let (socket, _) = tokio::time::timeout(REQUEST_TIMEOUT, connect_async(url)).await
                .map_err(|_| incomplete("connection timed out"))?
                .map_err(|e| incomplete(format!("connection failed: {e}")))?;
            let mut source = Source { socket, auth: Authenticator::new(nsec)?, url: url.to_owned(), challenge: None };
            let since = filter.since.unwrap_or(Timestamp::from(0)).as_u64();
            let until = filter.until.unwrap_or_else(Timestamp::now).as_u64();
            let mut windows = vec![(since, until)];
            while let Some((start, end)) = windows.pop() {
                let window = filter.clone().since(Timestamp::from(start)).until(Timestamp::from(end));
                let before = source.count(&window).await?;
                let events = if before <= PAGE_SIZE as u64 { source.page(&window).await? } else { Vec::new() };
                if before > PAGE_SIZE as u64 || events.len() as u64 != before {
                    if start == end { return Err(incomplete(format!("capped or changing timestamp bucket at {start}; cannot skip its unseen events"))); }
                    let middle = start + (end - start) / 2;
                    // Newest window first, with disjoint inclusive boundaries.
                    windows.push((start, middle));
                    windows.push((middle + 1, end));
                    continue;
                }
                if source.count(&window).await? != before {
                    return Err(incomplete("source count changed during request; retry without advancing progress"));
                }
                for event in events {
                    sender.send(event).await.map_err(|_| incomplete("publisher stopped"))?;
                }
            }
            Ok(())
        } => result,
    }
}
