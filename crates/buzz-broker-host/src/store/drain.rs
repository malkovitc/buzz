use super::*;

pub(crate) struct PendingEffect {
    pub(crate) credential: CredentialDigest,
    pub(crate) request_id: String,
    pub(crate) event: nostr::Event,
    pub(crate) relay_url: String,
}

impl AuthorityStore {
    pub(crate) async fn pending_message_post(
        &self,
        identity: &AuthorityIdentity,
    ) -> Result<Option<PendingEffect>, StoreError> {
        let identity = identity
            .validated()
            .map_err(|_| StoreError::InvalidIdentity)?;
        let authority_json = canonical_identity(&identity)?;
        let row = sqlx::query(
            "SELECT credential_hash, state, quiescing FROM broker_authorities
             WHERE authority_json = ?",
        )
        .bind(authority_json)
        .fetch_optional(&self.pool)
        .await
        .map_err(|_| StoreError::Unavailable)?
        .ok_or(StoreError::AuthorityNotFound)?;
        let hash: Vec<u8> = row
            .try_get("credential_hash")
            .map_err(|_| StoreError::Integrity)?;
        let hash: [u8; 32] = hash.try_into().map_err(|_| StoreError::Integrity)?;
        let state: String = row.try_get("state").map_err(|_| StoreError::Integrity)?;
        let quiescing: i64 = row
            .try_get("quiescing")
            .map_err(|_| StoreError::Integrity)?;
        let state = parse_state(&state, quiescing)?;
        if state == AuthorityState::Active {
            return Err(StoreError::NotQuiescing);
        }
        let credential = CredentialDigest::from_hash(hash);
        let rows = sqlx::query(
            "SELECT request_id, event_json FROM broker_effects
             WHERE credential_hash = ? AND response_json IS NULL",
        )
        .bind(credential.as_bytes())
        .fetch_all(&self.pool)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if rows.len() > 1 || (state == AuthorityState::Fenced && !rows.is_empty()) {
            return Err(StoreError::Integrity);
        }
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        let request_id: String = row
            .try_get("request_id")
            .map_err(|_| StoreError::Integrity)?;
        let event_json: String = row
            .try_get("event_json")
            .map_err(|_| StoreError::Integrity)?;
        let event = decode_effect_event(&event_json)?;
        if event.kind.as_u16() != 9
            || event.pubkey.to_hex() != identity.executor_agent_pubkey.as_str()
        {
            return Err(StoreError::Integrity);
        }
        Ok(Some(PendingEffect {
            credential,
            request_id,
            event,
            relay_url: identity.community_relay_url,
        }))
    }
}
