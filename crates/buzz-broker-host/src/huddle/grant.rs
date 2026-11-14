use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use buzz_sdk::broker::AuthorityIdentity;
use nostr::{Event, Kind};
use serde::Deserialize;

use crate::credential::CredentialDigest;

const OWNER_GRANT_KIND: Kind = Kind::Custom(27236);
const CHALLENGE_LIFETIME: Duration = Duration::from_secs(30);
const GRANT_LIFETIME: Duration = Duration::from_secs(60);
const MAX_PENDING: usize = 64;

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct GrantRequest {
    pub(crate) owner_event: Event,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantTarget {
    authority: AuthorityIdentity,
    channel_id: String,
    parent_channel_id: String,
    huddle_generation: u64,
}

#[derive(Default)]
pub(crate) struct HuddleGrants {
    inner: Mutex<GrantCache>,
}

#[derive(Default)]
struct GrantCache {
    challenges: HashMap<String, (CredentialDigest, Instant)>,
    grants: HashMap<String, (CredentialDigest, GrantTarget, Instant)>,
}

pub(crate) enum GrantError {
    Invalid,
    Full,
    Unavailable,
}

impl HuddleGrants {
    pub(crate) fn challenge(&self, credential: CredentialDigest) -> Result<String, GrantError> {
        let mut cache = self.inner.lock().map_err(|_| GrantError::Unavailable)?;
        cache.expire();
        if cache.challenges.len() >= MAX_PENDING {
            return Err(GrantError::Full);
        }
        let nonce = uuid::Uuid::new_v4().to_string();
        cache
            .challenges
            .insert(nonce.clone(), (credential, Instant::now()));
        Ok(nonce)
    }

    pub(crate) fn grant(
        &self,
        credential: &CredentialDigest,
        event: &Event,
        identity: &AuthorityIdentity,
        owner: &str,
    ) -> Result<String, GrantError> {
        if event.kind != OWNER_GRANT_KIND
            || event.pubkey.to_hex() != owner
            || event.content.len() > 4096
            || event.verify().is_err()
            || SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| GrantError::Unavailable)?
                .as_secs()
                .abs_diff(event.created_at.as_secs())
                > CHALLENGE_LIFETIME.as_secs()
        {
            return Err(GrantError::Invalid);
        }
        let [tag] = event.tags.as_slice() else {
            return Err(GrantError::Invalid);
        };
        let [kind, nonce] = tag.as_slice() else {
            return Err(GrantError::Invalid);
        };
        if kind != "challenge" {
            return Err(GrantError::Invalid);
        }
        let target: GrantTarget =
            serde_json::from_str(&event.content).map_err(|_| GrantError::Invalid)?;
        if target.authority != *identity
            || target.huddle_generation == 0
            || !canonical_uuid(&target.channel_id)
            || !canonical_uuid(&target.parent_channel_id)
            || target.channel_id == target.parent_channel_id
        {
            return Err(GrantError::Invalid);
        }
        let mut cache = self.inner.lock().map_err(|_| GrantError::Unavailable)?;
        cache.expire();
        let Some((bound, _)) = cache.challenges.remove(nonce) else {
            return Err(GrantError::Invalid);
        };
        if &bound != credential {
            return Err(GrantError::Invalid);
        }
        if cache.grants.len() >= MAX_PENDING {
            return Err(GrantError::Full);
        }
        let id = uuid::Uuid::new_v4().to_string();
        cache
            .grants
            .insert(id.clone(), (bound, target, Instant::now()));
        Ok(id)
    }
}

impl GrantCache {
    fn expire(&mut self) {
        self.challenges
            .retain(|_, (_, issued)| issued.elapsed() < CHALLENGE_LIFETIME);
        self.grants
            .retain(|_, (_, _, issued)| issued.elapsed() < GRANT_LIFETIME);
    }
}

fn canonical_uuid(value: &str) -> bool {
    uuid::Uuid::parse_str(value).is_ok_and(|parsed| parsed.to_string() == value)
}
