use std::time::Duration;

use futures_util::{stream::SplitSink, stream::SplitStream, SinkExt as _, StreamExt as _};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};

use crate::signing::HostSigner;

type RelaySocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

pub(crate) struct AudioRelay {
    pub(crate) sink: SplitSink<RelaySocket, Message>,
    pub(crate) stream: SplitStream<RelaySocket>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum AudioRelayError {
    #[error("Huddle relay URL is invalid")]
    InvalidRelay,
    #[error("Huddle relay is unavailable")]
    Unavailable,
    #[error("Huddle relay handshake timed out")]
    Timeout,
    #[error("Huddle relay rejected audio join")]
    Rejected,
    #[error("Huddle relay handshake is invalid")]
    Protocol,
}

impl AudioRelay {
    pub(crate) async fn connect(
        signer: &HostSigner,
        community_relay_url: &str,
        channel_id: &str,
        parent_channel_id: &str,
    ) -> Result<Self, AudioRelayError> {
        tokio::time::timeout(
            Duration::from_secs(5),
            Self::connect_inner(signer, community_relay_url, channel_id, parent_channel_id),
        )
        .await
        .map_err(|_| AudioRelayError::Timeout)?
    }

    async fn connect_inner(
        signer: &HostSigner,
        community_relay_url: &str,
        channel_id: &str,
        parent_channel_id: &str,
    ) -> Result<Self, AudioRelayError> {
        let origin = relay_origin(community_relay_url)?;
        let url = format!("{origin}/huddle/{channel_id}/audio");
        let config = WebSocketConfig::default()
            .read_buffer_size(8192)
            .write_buffer_size(8192)
            .max_write_buffer_size(16384)
            .max_message_size(Some(8192))
            .max_frame_size(Some(8192));
        let (socket, _) = connect_async_with_config(url.as_str(), Some(config), true)
            .await
            .map_err(|_| AudioRelayError::Unavailable)?;
        let (mut sink, mut stream) = socket.split();
        let challenge = next_control(&mut stream).await?;
        if challenge["type"] != "challenge" {
            return Err(AudioRelayError::Protocol);
        }
        let challenge = challenge["challenge"]
            .as_str()
            .filter(|value| !value.is_empty() && value.len() <= 128)
            .ok_or(AudioRelayError::Protocol)?;
        let event = signer
            .sign_huddle_auth(&origin, challenge)
            .map_err(|_| AudioRelayError::Protocol)?;
        let auth = serde_json::json!({
            "type": "auth",
            "event": event,
            "parent_channel_id": parent_channel_id,
            "protocol_version": 2,
        });
        sink.send(Message::Text(auth.to_string().into()))
            .await
            .map_err(|_| AudioRelayError::Unavailable)?;
        let joined = next_control(&mut stream).await?;
        if joined["type"] == "error" || joined["type"] == "upgrade_required" {
            return Err(AudioRelayError::Rejected);
        }
        if joined["type"] != "joined"
            || joined["broker_audio_local_only"] != true
            || joined["peer_index"]
                .as_u64()
                .and_then(|value| u8::try_from(value).ok())
                .is_none()
        {
            return Err(AudioRelayError::Protocol);
        }
        Ok(Self { sink, stream })
    }
}

async fn next_control(
    stream: &mut SplitStream<RelaySocket>,
) -> Result<serde_json::Value, AudioRelayError> {
    match stream.next().await {
        Some(Ok(Message::Text(text))) => {
            serde_json::from_str(&text).map_err(|_| AudioRelayError::Protocol)
        }
        Some(Ok(Message::Close(_))) => Err(AudioRelayError::Rejected),
        Some(Err(_)) | None => Err(AudioRelayError::Unavailable),
        Some(Ok(_)) => Err(AudioRelayError::Protocol),
    }
}

fn relay_origin(community_relay_url: &str) -> Result<String, AudioRelayError> {
    let mut url =
        reqwest::Url::parse(community_relay_url).map_err(|_| AudioRelayError::InvalidRelay)?;
    if !matches!(url.scheme(), "ws" | "wss")
        || url.username() != ""
        || url.password().is_some()
        || url.fragment().is_some()
    {
        return Err(AudioRelayError::InvalidRelay);
    }
    url.set_path("");
    url.set_query(None);
    Ok(url.to_string().trim_end_matches('/').to_owned())
}
