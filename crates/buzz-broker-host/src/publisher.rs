use std::time::Duration;

use base64::Engine as _;
use buzz_sdk::broker::{
    ActionOutcome, BrokerMessage, ChannelReadArgs, MessagePage, MessageReplyArgs,
};
use buzz_sdk::ThreadRef;
use nostr::Event;
use serde::Deserialize;
use sha2::{Digest as _, Sha256};

use crate::signing::HostSigner;

pub(crate) struct RelayPublisher {
    client: reqwest::Client,
}

const MAX_READ_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_READ_PAGE_EVENTS: u32 = 16;
const MAX_EXACT_EVENT_BYTES: usize = 1024 * 1024;

#[derive(Deserialize)]
struct RelayAck {
    accepted: bool,
    event_id: String,
}

impl RelayPublisher {
    pub(crate) fn new() -> Result<Self, PublishError> {
        let client = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(20))
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| PublishError::Unavailable)?;
        Ok(Self { client })
    }

    pub(crate) async fn resolve_reply_target(
        &self,
        signer: &HostSigner,
        community_relay_url: &str,
        args: &MessageReplyArgs,
    ) -> Result<ThreadRef, PublishError> {
        let parent = self
            .query_exact_channel_event(
                signer,
                community_relay_url,
                &args.channel_id,
                &args.reply_to_event_id,
            )
            .await?;
        let root_event_id = reply_root(&parent)?;
        if root_event_id != parent.id {
            self.query_exact_channel_event(
                signer,
                community_relay_url,
                &args.channel_id,
                &root_event_id.to_hex(),
            )
            .await?;
        }
        Ok(ThreadRef {
            root_event_id,
            parent_event_id: parent.id,
        })
    }

    async fn query_exact_channel_event(
        &self,
        signer: &HostSigner,
        community_relay_url: &str,
        channel_id: &str,
        event_id: &str,
    ) -> Result<Event, PublishError> {
        let url = relay_endpoint(community_relay_url, "/query")?;
        let body = serde_json::to_vec(&serde_json::json!([{
            "ids": [event_id],
            "kinds": [9],
            "#h": [channel_id],
            "limit": 1
        }]))
        .map_err(|_| PublishError::InvalidEvent)?;
        let authorization = signer
            .nip98_authorization(url.as_str(), &body)
            .map_err(|_| PublishError::Unavailable)?;
        let mut response = self
            .client
            .post(url)
            .header("authorization", authorization)
            .header("x-auth-tag", signer.auth_tag_json())
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| PublishError::Unavailable)?;
        if !response.status().is_success() {
            return Err(PublishError::Unavailable);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| PublishError::Unavailable)?
        {
            if chunk.len() > MAX_EXACT_EVENT_BYTES - bytes.len() {
                return Err(PublishError::ResponseTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        let mut events: Vec<Event> =
            serde_json::from_slice(&bytes).map_err(|_| PublishError::InvalidEvent)?;
        if events.len() != 1 {
            return Err(PublishError::InvalidEvent);
        }
        let event = events.pop().ok_or(PublishError::InvalidEvent)?;
        let channels: Vec<_> = event
            .tags
            .iter()
            .filter(|tag| tag.kind().to_string() == "h")
            .collect();
        if event.id.to_hex() != event_id
            || event.kind.as_u16() != 9
            || event.verify().is_err()
            || channels.len() != 1
            || channels[0].content() != Some(channel_id)
        {
            return Err(PublishError::InvalidEvent);
        }
        Ok(event)
    }

    pub(crate) async fn query_channel(
        &self,
        signer: &HostSigner,
        community_relay_url: &str,
        args: &ChannelReadArgs,
    ) -> Result<MessagePage, PublishError> {
        let url = relay_endpoint(community_relay_url, "/query")?;
        let scope = cursor_scope(args, community_relay_url, signer)?;
        let relay_limit = args.effective_limit().min(MAX_READ_PAGE_EVENTS);
        let mut filter = serde_json::json!({
            "kinds": [9, 40002, 40008, 45001, 45003],
            "#h": [args.channel_id],
            "limit": relay_limit,
        });
        if args.mentions_only {
            filter["#p"] = serde_json::json!([signer.pubkey_hex()]);
        }
        if let Some(root) = &args.root_event_id {
            filter["#e"] = serde_json::json!([root]);
        }
        if let Some(cursor) = &args.cursor {
            let (until, before_id) = decode_cursor(cursor, &scope, signer)?;
            filter["until"] = serde_json::json!(until);
            filter["before_id"] = serde_json::json!(before_id);
        }
        let body = serde_json::to_vec(&serde_json::json!([filter]))
            .map_err(|_| PublishError::InvalidEvent)?;
        let authorization = signer
            .nip98_authorization(url.as_str(), &body)
            .map_err(|_| PublishError::Unavailable)?;
        let mut response = self
            .client
            .post(url)
            .header("authorization", authorization)
            .header("x-auth-tag", signer.auth_tag_json())
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| PublishError::Unavailable)?;
        if !response.status().is_success() {
            return Err(PublishError::Unavailable);
        }
        let mut response_body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| PublishError::Unavailable)?
        {
            if chunk.len() > MAX_READ_RESPONSE_BYTES - response_body.len() {
                return Err(PublishError::ResponseTooLarge);
            }
            response_body.extend_from_slice(&chunk);
        }
        let messages: Vec<BrokerMessage> =
            serde_json::from_slice(&response_body).map_err(|_| PublishError::InvalidEvent)?;
        let signer_pubkey = signer.pubkey_hex();
        if messages.len() > relay_limit as usize
            || messages.iter().any(|message| {
                let channel_tags: Vec<_> = message
                    .0
                    .tags
                    .iter()
                    .filter(|tag| tag.kind().to_string() == "h")
                    .collect();
                let kind = message.0.kind.as_u16();
                message.verify().is_err()
                    || ![9, 40002, 40008, 45001, 45003].contains(&kind)
                    || channel_tags.len() != 1
                    || channel_tags[0].content() != Some(args.channel_id.as_str())
                    || (args.mentions_only
                        && !message.0.tags.iter().any(|tag| {
                            tag.kind().to_string() == "p"
                                && tag.content() == Some(signer_pubkey.as_str())
                        }))
                    || args.root_event_id.as_ref().is_some_and(|root| {
                        !message.0.tags.iter().any(|tag| {
                            tag.kind().to_string() == "e" && tag.content() == Some(root.as_str())
                        })
                    })
            })
        {
            return Err(PublishError::InvalidEvent);
        }
        ActionOutcome::ChannelRead(MessagePage {
            messages: messages.clone(),
            next_cursor: None,
        })
        .validate()
        .map_err(|_| PublishError::InvalidEvent)?;
        let next_cursor = if messages.len() == relay_limit as usize {
            messages
                .last()
                .map(|message| encode_cursor(message, &scope, signer))
        } else {
            None
        };
        Ok(MessagePage {
            messages,
            next_cursor,
        })
    }

    pub(crate) async fn publish(
        &self,
        signer: &HostSigner,
        community_relay_url: &str,
        event: &Event,
    ) -> Result<(), PublishError> {
        let url = relay_endpoint(community_relay_url, "/events")?;
        let body = serde_json::to_vec(event).map_err(|_| PublishError::InvalidEvent)?;
        let authorization = signer
            .nip98_authorization(url.as_str(), &body)
            .map_err(|_| PublishError::Unavailable)?;
        let response = self
            .client
            .post(url)
            .header("authorization", authorization)
            .header("x-auth-tag", signer.auth_tag_json())
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await;
        if let Ok(response) = response {
            if response.status().is_success() {
                if let Ok(ack) = response.json::<RelayAck>().await {
                    if ack.accepted && ack.event_id == event.id.to_hex() {
                        return Ok(());
                    }
                }
            }
        }
        if self.find(signer, community_relay_url, event).await? {
            Ok(())
        } else {
            Err(PublishError::Indeterminate)
        }
    }

    pub(crate) async fn publish_pending(
        &self,
        signer: &HostSigner,
        community_relay_url: &str,
        event: &Event,
    ) -> Result<(), PublishError> {
        if self.find(signer, community_relay_url, event).await? {
            Ok(())
        } else {
            self.publish(signer, community_relay_url, event).await
        }
    }

    async fn find(
        &self,
        signer: &HostSigner,
        community_relay_url: &str,
        event: &Event,
    ) -> Result<bool, PublishError> {
        let url = relay_endpoint(community_relay_url, "/query")?;
        let body = serde_json::to_vec(&serde_json::json!([{
            "ids": [event.id.to_hex()],
            "limit": 1
        }]))
        .map_err(|_| PublishError::Indeterminate)?;
        let authorization = signer
            .nip98_authorization(url.as_str(), &body)
            .map_err(|_| PublishError::Indeterminate)?;
        let response = self
            .client
            .post(url)
            .header("authorization", authorization)
            .header("x-auth-tag", signer.auth_tag_json())
            .header("content-type", "application/json")
            .body(body)
            .send()
            .await
            .map_err(|_| PublishError::Indeterminate)?;
        if !response.status().is_success() {
            return Err(PublishError::Indeterminate);
        }
        let found: Vec<Event> = response
            .json()
            .await
            .map_err(|_| PublishError::Indeterminate)?;
        Ok(found.iter().any(|candidate| {
            candidate.id == event.id
                && candidate.pubkey == event.pubkey
                && candidate.verify().is_ok()
        }))
    }
}

fn cursor_scope(
    args: &ChannelReadArgs,
    community_relay_url: &str,
    signer: &HostSigner,
) -> Result<[u8; 16], PublishError> {
    let scope = serde_json::to_vec(&(
        &args.channel_id,
        &args.root_event_id,
        args.mentions_only,
        args.effective_limit(),
        community_relay_url,
        signer.pubkey_hex(),
        signer.owner_hex(),
    ))
    .map_err(|_| PublishError::InvalidCursor)?;
    let digest = Sha256::digest(scope);
    let mut truncated = [0_u8; 16];
    truncated.copy_from_slice(&digest[..16]);
    Ok(truncated)
}

fn encode_cursor(message: &BrokerMessage, scope: &[u8; 16], signer: &HostSigner) -> String {
    let mut payload = Vec::with_capacity(73);
    payload.push(1);
    payload.extend_from_slice(&message.0.created_at.as_secs().to_be_bytes());
    payload.extend_from_slice(&message.0.id.to_bytes());
    payload.extend_from_slice(scope);
    payload.extend_from_slice(&signer.cursor_mac(&payload));
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload)
}

fn decode_cursor(
    cursor: &str,
    expected_scope: &[u8; 16],
    signer: &HostSigner,
) -> Result<(u64, String), PublishError> {
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| PublishError::InvalidCursor)?;
    if payload.len() != 73 || payload[0] != 1 || payload[41..57] != expected_scope[..] {
        return Err(PublishError::InvalidCursor);
    }
    let expected_mac = signer.cursor_mac(&payload[..57]);
    let mismatch = payload[57..]
        .iter()
        .zip(expected_mac)
        .fold(0_u8, |acc, (left, right)| acc | (left ^ right));
    if mismatch != 0 {
        return Err(PublishError::InvalidCursor);
    }
    let until = u64::from_be_bytes(
        payload[1..9]
            .try_into()
            .map_err(|_| PublishError::InvalidCursor)?,
    );
    Ok((until, hex::encode(&payload[9..41])))
}

fn reply_root(parent: &Event) -> Result<nostr::EventId, PublishError> {
    let markers = buzz_core::nip10::parse_thread_markers(&parent.tags);
    let Some((root, _)) = markers.resolve() else {
        return Ok(parent.id);
    };
    nostr::EventId::from_hex(&root).map_err(|_| PublishError::InvalidEvent)
}

fn relay_endpoint(community_relay_url: &str, path: &str) -> Result<reqwest::Url, PublishError> {
    let http_url = if let Some(rest) = community_relay_url.strip_prefix("wss://") {
        format!("https://{rest}")
    } else if let Some(rest) = community_relay_url.strip_prefix("ws://") {
        format!("http://{rest}")
    } else {
        return Err(PublishError::InvalidRelay);
    };
    let mut url = reqwest::Url::parse(&http_url).map_err(|_| PublishError::InvalidRelay)?;
    if url.username() != "" || url.password().is_some() || url.fragment().is_some() {
        return Err(PublishError::InvalidRelay);
    }
    url.set_path(path);
    url.set_query(None);
    Ok(url)
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum PublishError {
    #[error("relay URL is invalid")]
    InvalidRelay,
    #[error("signed event is invalid")]
    InvalidEvent,
    #[error("channel cursor is invalid for this scope")]
    InvalidCursor,
    #[error("relay channel response exceeds the host page cap")]
    ResponseTooLarge,
    #[error("relay publisher is unavailable")]
    Unavailable,
    #[error("relay publication outcome is indeterminate")]
    Indeterminate,
}
