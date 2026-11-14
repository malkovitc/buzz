use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message as ClientMessage, WebSocket};
use futures_util::{SinkExt as _, StreamExt as _};
use tokio_tungstenite::tungstenite::Message as RelayMessage;

use super::grant::GrantTarget;
use super::relay::AudioRelay;
use crate::credential::CredentialDigest;
use crate::signing::HostSigner;
use crate::store::AuthorityStore;

const MAX_FRAME_BYTES: usize = 4096;

pub(crate) async fn run(
    mut client: WebSocket,
    store: AuthorityStore,
    signer: Arc<HostSigner>,
    credential: CredentialDigest,
    target: GrantTarget,
) {
    let relay = AudioRelay::connect(
        &signer,
        &target.authority.community_relay_url,
        &target.channel_id,
        &target.parent_channel_id,
    )
    .await;
    let Ok(mut relay) = relay else {
        let _ = client
            .send(ClientMessage::Text(
                serde_json::json!({"type":"error","code":"relay_unavailable"})
                    .to_string()
                    .into(),
            ))
            .await;
        let _ = client.close().await;
        return;
    };
    let session = store
        .open_audio_session(&credential, &target.authority, &signer)
        .await;
    let Ok(session) = session else {
        let _ = relay.sink.send(RelayMessage::Close(None)).await;
        let _ = client
            .send(ClientMessage::Text(
                serde_json::json!({"type":"error","code":"authority_closed"})
                    .to_string()
                    .into(),
            ))
            .await;
        let _ = client.close().await;
        return;
    };
    let joined = serde_json::json!({
        "type": "joined",
        "channelId": target.channel_id,
        "huddleGeneration": target.huddle_generation,
    });
    let joined_sent = client
        .send(ClientMessage::Text(joined.to_string().into()))
        .await
        .is_ok();
    let mut relay_closed = false;
    let mut poll = tokio::time::interval(Duration::from_millis(25));
    while joined_sent && !relay_closed {
        tokio::select! {
            biased;
            _ = poll.tick() => {
                if !store.audio_session_can_send(&credential, &session).await.unwrap_or(false) {
                    break;
                }
            }
            message = relay.stream.next() => {
                match message {
                    Some(Ok(RelayMessage::Ping(payload))) => {
                        if relay.sink.send(RelayMessage::Pong(payload)).await.is_err() { break; }
                    }
                    Some(Ok(RelayMessage::Close(_))) => { relay_closed = true; break; }
                    Some(Ok(_)) => {},
                    Some(Err(_)) | None => break,
                }
            }
            message = client.recv() => {
                match message {
                    Some(Ok(ClientMessage::Binary(frame))) if valid_frame(&frame) => {
                        if !store.audio_session_can_send(&credential, &session).await.unwrap_or(false)
                            || relay.sink.send(RelayMessage::Binary(frame)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(ClientMessage::Ping(payload))) => {
                        if client.send(ClientMessage::Pong(payload)).await.is_err() { break; }
                    }
                    Some(Ok(ClientMessage::Close(_))) | None => break,
                    Some(Ok(_)) | Some(Err(_)) => break,
                }
            }
        }
    }
    if relay_closed || relay_close_ack(&mut relay).await {
        let _ = store.close_audio_session(&credential, &session).await;
    }
    let _ = client.close().await;
}

async fn relay_close_ack(relay: &mut AudioRelay) -> bool {
    if relay.sink.send(RelayMessage::Close(None)).await.is_err() {
        return false;
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            match relay.stream.next().await {
                Some(Ok(RelayMessage::Close(_))) => return true,
                Some(Ok(RelayMessage::Ping(payload))) => {
                    if relay.sink.send(RelayMessage::Pong(payload)).await.is_err() {
                        return false;
                    }
                }
                Some(Ok(_)) => {}
                Some(Err(_)) | None => return false,
            }
        }
    })
    .await
    .unwrap_or(false)
}

fn valid_frame(frame: &[u8]) -> bool {
    (9..=MAX_FRAME_BYTES).contains(&frame.len())
}
