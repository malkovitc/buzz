use super::*;

impl AuthorityStore {
    pub(crate) async fn inspect(
        &self,
        identity: &AuthorityIdentity,
    ) -> Result<Option<AuthorityState>, StoreError> {
        let identity = identity
            .validated()
            .map_err(|_| StoreError::InvalidIdentity)?;
        let authority_json = canonical_identity(&identity)?;
        let row =
            sqlx::query("SELECT state, quiescing FROM broker_authorities WHERE authority_json = ?")
                .bind(authority_json)
                .fetch_optional(&self.pool)
                .await
                .map_err(|_| StoreError::Unavailable)?;
        row.map(|row| {
            let state: String = row.try_get("state").map_err(|_| StoreError::Integrity)?;
            let quiescing: i64 = row
                .try_get("quiescing")
                .map_err(|_| StoreError::Integrity)?;
            parse_state(&state, quiescing)
        })
        .transpose()
    }

    #[cfg(unix)]
    pub(super) async fn migrate_schema_v4(&self) -> Result<(), StoreError> {
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
        if current != 3 {
            return Ok(());
        }
        require_canonical_authority_state(&mut transaction).await?;
        sqlx::query("ALTER TABLE broker_receipts ADD COLUMN owner_pubkey TEXT")
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("PRAGMA user_version = 4")
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    pub(crate) async fn fence(&self, identity: &AuthorityIdentity) -> Result<(), StoreError> {
        let identity = identity
            .validated()
            .map_err(|_| StoreError::InvalidIdentity)?;
        let authority_json = canonical_identity(&identity)?;
        #[cfg(unix)]
        let _publication_guard = self.publication_lock_for_identity(&identity).await?;
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| StoreError::Unavailable)?;
        require_canonical_authority_state(&mut transaction).await?;
        let row =
            sqlx::query("SELECT state, quiescing FROM broker_authorities WHERE authority_json = ?")
                .bind(&authority_json)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| StoreError::Unavailable)?;
        let pending: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM broker_effects
                WHERE credential_hash = (
                    SELECT credential_hash FROM broker_authorities WHERE authority_json = ?
                ) AND response_json IS NULL
            )",
        )
        .bind(&authority_json)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if pending {
            return Err(StoreError::EffectsPending);
        }
        #[cfg(unix)]
        {
            let audio_pending: bool = sqlx::query_scalar(
                "SELECT EXISTS(
                    SELECT 1 FROM broker_audio_sessions
                    WHERE credential_hash = (
                        SELECT credential_hash FROM broker_authorities WHERE authority_json = ?
                    ) AND state = 'active'
                )",
            )
            .bind(&authority_json)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
            if audio_pending {
                return Err(StoreError::EffectsPending);
            }
        }
        let state = row
            .map(|row| {
                let state: String = row.try_get("state").map_err(|_| StoreError::Integrity)?;
                let quiescing: i64 = row
                    .try_get("quiescing")
                    .map_err(|_| StoreError::Integrity)?;
                parse_state(&state, quiescing)
            })
            .transpose()?
            .ok_or(StoreError::AuthorityNotFound)?;
        match state {
            AuthorityState::Fenced => {}
            AuthorityState::Active | AuthorityState::Quiescing => {
                sqlx::query(
                    "UPDATE broker_authorities SET state = 'fenced', quiescing = 0
                     WHERE authority_json = ? AND (state = 'active' OR quiescing = 1)",
                )
                .bind(&authority_json)
                .execute(&mut *transaction)
                .await
                .map_err(|_| StoreError::Unavailable)?;
            }
        }
        invalidate_authority_receipts(&mut transaction, &authority_json).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    #[cfg(unix)]
    pub(super) async fn migrate_schema_v3(&self) -> Result<(), StoreError> {
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
        if current != 2 {
            return Ok(());
        }
        require_canonical_authority_state_v2(&mut transaction).await?;
        sqlx::query(
            "ALTER TABLE broker_authorities ADD COLUMN quiescing INTEGER NOT NULL DEFAULT 0
             CHECK(quiescing IN (0, 1))",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("DROP INDEX one_active_generation_per_scope")
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(
            "CREATE UNIQUE INDEX one_unfenced_generation_per_scope
             ON broker_authorities(authority_scope)
             WHERE state = 'active' OR quiescing = 1",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("PRAGMA user_version = 3")
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    pub(crate) async fn quiesce(
        &self,
        identity: &AuthorityIdentity,
    ) -> Result<AuthorityState, StoreError> {
        let identity = identity
            .validated()
            .map_err(|_| StoreError::InvalidIdentity)?;
        let authority_json = canonical_identity(&identity)?;
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| StoreError::Unavailable)?;
        require_canonical_authority_state(&mut transaction).await?;
        let row =
            sqlx::query("SELECT state, quiescing FROM broker_authorities WHERE authority_json = ?")
                .bind(&authority_json)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| StoreError::Unavailable)?
                .ok_or(StoreError::AuthorityNotFound)?;
        let state: String = row.try_get("state").map_err(|_| StoreError::Integrity)?;
        let quiescing: i64 = row
            .try_get("quiescing")
            .map_err(|_| StoreError::Integrity)?;
        let current = parse_state(&state, quiescing)?;
        if current == AuthorityState::Active {
            sqlx::query(
                "UPDATE broker_authorities SET state = 'fenced', quiescing = 1
                 WHERE authority_json = ? AND state = 'active' AND quiescing = 0",
            )
            .bind(&authority_json)
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        }
        invalidate_authority_receipts(&mut transaction, &authority_json).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        #[cfg(unix)]
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.has_active_audio_sessions(&identity).await? {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
            Ok::<(), StoreError>(())
        })
        .await
        .map_err(|_| StoreError::EffectsPending)??;
        Ok(if current == AuthorityState::Fenced {
            AuthorityState::Fenced
        } else {
            AuthorityState::Quiescing
        })
    }
}

#[cfg(unix)]
pub(super) async fn require_canonical_authority_state_v2(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), StoreError> {
    let rows = sqlx::query(
        "SELECT authority_json, authority_scope, generation, state FROM broker_authorities",
    )
    .fetch_all(&mut **transaction)
    .await
    .map_err(|_| StoreError::Unavailable)?;
    for row in rows {
        let authority_json: String = row
            .try_get("authority_json")
            .map_err(|_| StoreError::Integrity)?;
        let stored_scope: Vec<u8> = row
            .try_get("authority_scope")
            .map_err(|_| StoreError::Integrity)?;
        let generation: String = row
            .try_get("generation")
            .map_err(|_| StoreError::Integrity)?;
        let state: String = row.try_get("state").map_err(|_| StoreError::Integrity)?;
        decode_canonical_identity(&authority_json, &stored_scope, &generation)?;
        parse_state(&state, 0)?;
    }
    Ok(())
}
