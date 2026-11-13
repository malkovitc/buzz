use std::time::Duration;

use nostr::Event;
use serde::Deserialize;

use crate::signing::HostSigner;

pub(crate) struct RelayPublisher {
    client: reqwest::Client,
}

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
    #[error("relay publisher is unavailable")]
    Unavailable,
    #[error("relay publication outcome is indeterminate")]
    Indeterminate,
}
