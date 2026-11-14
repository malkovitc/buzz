use super::*;
use buzz_sdk::broker::RuntimeLocationKind;

impl AuthorityStore {
    pub(super) async fn migrate_schema_v7(&self) -> Result<(), StoreError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let current: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if current != 6 {
            return Ok(());
        }
        sqlx::query(AUDIO_SESSIONS_TABLE_SQL)
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(AUDIO_ACTIVE_INDEX_SQL)
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("PRAGMA user_version = 7")
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    pub(super) async fn validate_audio_state(&self) -> Result<(), StoreError> {
        let rows = sqlx::query(
            "SELECT s.session_id, s.state, a.state AS authority_state, a.quiescing
             FROM broker_audio_sessions s
             LEFT JOIN broker_authorities a ON a.credential_hash = s.credential_hash",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        for row in rows {
            let session: String = row
                .try_get("session_id")
                .map_err(|_| StoreError::Integrity)?;
            if !uuid::Uuid::parse_str(&session).is_ok_and(|parsed| parsed.to_string() == session) {
                return Err(StoreError::Integrity);
            }
            let state: String = row.try_get("state").map_err(|_| StoreError::Integrity)?;
            let authority_state: Option<String> = row
                .try_get("authority_state")
                .map_err(|_| StoreError::Integrity)?;
            let quiescing: Option<i64> = row
                .try_get("quiescing")
                .map_err(|_| StoreError::Integrity)?;
            let authority = parse_state(
                authority_state.as_deref().ok_or(StoreError::Integrity)?,
                quiescing.ok_or(StoreError::Integrity)?,
            )?;
            if !matches!(state.as_str(), "active" | "closed")
                || (state == "active" && authority == AuthorityState::Fenced)
            {
                return Err(StoreError::Integrity);
            }
        }
        Ok(())
    }

    pub(crate) async fn open_audio_session(
        &self,
        credential: &CredentialDigest,
        identity: &AuthorityIdentity,
        signer: &HostSigner,
    ) -> Result<String, StoreError> {
        let identity = identity
            .validated()
            .map_err(|_| StoreError::InvalidIdentity)?;
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let authority = load_authority(&mut transaction, credential).await?;
        let owner = signer.owner_hex();
        if authority.identity != identity
            || authority.state != AuthorityState::Active
            || identity.location.kind != RuntimeLocationKind::Local
            || !signer.matches_executor(&identity)
            || !owner_matches(&authority, &owner)
        {
            return Err(StoreError::OwnerMismatch);
        }
        bind_owner(&mut transaction, credential, &owner).await?;
        let active: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM broker_audio_sessions
             WHERE credential_hash = ? AND state = 'active')",
        )
        .bind(credential.as_bytes())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if active {
            return Err(StoreError::EffectsPending);
        }
        sqlx::query(
            "DELETE FROM broker_audio_sessions WHERE credential_hash = ? AND state = 'closed'",
        )
        .bind(credential.as_bytes())
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        let session = uuid::Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO broker_audio_sessions (credential_hash, session_id, state)
             VALUES (?, ?, 'active')",
        )
        .bind(credential.as_bytes())
        .bind(&session)
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        Ok(session)
    }

    pub(crate) async fn close_audio_session(
        &self,
        credential: &CredentialDigest,
        session: &str,
    ) -> Result<(), StoreError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let result = sqlx::query(
            "UPDATE broker_audio_sessions SET state = 'closed'
             WHERE credential_hash = ? AND session_id = ? AND state = 'active'",
        )
        .bind(credential.as_bytes())
        .bind(session)
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if result.rows_affected() != 1 {
            return Err(StoreError::AuthorityNotFound);
        }
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    pub(crate) async fn audio_session_can_send(
        &self,
        credential: &CredentialDigest,
        session: &str,
    ) -> Result<bool, StoreError> {
        sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM broker_audio_sessions s
                JOIN broker_authorities a ON a.credential_hash = s.credential_hash
                WHERE s.credential_hash = ? AND s.session_id = ? AND s.state = 'active'
                  AND a.state = 'active' AND a.quiescing = 0
            )",
        )
        .bind(credential.as_bytes())
        .bind(session)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| StoreError::Unavailable)
    }

    pub(super) async fn has_active_audio_sessions(
        &self,
        identity: &AuthorityIdentity,
    ) -> Result<bool, StoreError> {
        let authority_json = canonical_identity(identity)?;
        sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM broker_audio_sessions
                WHERE credential_hash = (
                    SELECT credential_hash FROM broker_authorities WHERE authority_json = ?
                ) AND state = 'active'
            )",
        )
        .bind(authority_json)
        .fetch_one(&self.pool)
        .await
        .map_err(|_| StoreError::Unavailable)
    }
}
