use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::time::Duration;

use buzz_sdk::broker::{
    Action, ActionOutcome, AuthorityIdentity, AuthorityState, BrokerError, BrokerResponse,
    BrokerResult, ManagedAcpAuthority, StorageAddress, ValidatedRequest,
};
#[cfg(unix)]
use buzz_sdk::broker::{ActionArgs, EventPublished};
use serde::Serialize;
use sha2::{Digest, Sha256};
#[cfg(unix)]
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Connection as _, Row, SqlitePool};

use crate::credential::CredentialDigest;
#[cfg(unix)]
use crate::signing::HostSigner;

#[cfg(unix)]
mod drain;
#[cfg(unix)]
mod effect_lock;
mod lifecycle;

#[cfg(unix)]
const SCHEMA_VERSION: i64 = 3;
#[cfg(unix)]
const EFFECTS_TABLE_SQL: &str = "CREATE TABLE broker_effects (
    credential_hash BLOB NOT NULL,
    request_id TEXT NOT NULL,
    request_digest BLOB NOT NULL CHECK(length(request_digest) = 32),
    action TEXT NOT NULL CHECK(action = 'message.post'),
    event_json TEXT NOT NULL,
    response_json TEXT,
    PRIMARY KEY (credential_hash, request_id),
    FOREIGN KEY (credential_hash) REFERENCES broker_authorities(credential_hash)
) STRICT";
pub(crate) const MAX_RECEIPTS_PER_CREDENTIAL: i64 = 64;

#[derive(Clone)]
pub(crate) struct AuthorityStore {
    pool: SqlitePool,
    #[cfg(unix)]
    state_dir: PathBuf,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthorityScope<'a> {
    community_relay_url: &'a str,
    logical_agent_pubkey: &'a str,
    task_id: &'a str,
}

struct StoredAuthority {
    identity: AuthorityIdentity,
    state: AuthorityState,
}

enum ReceiptMatch {
    Missing,
    Replay(Box<BrokerResponse>),
    Conflict,
}

#[cfg(unix)]
pub(crate) enum EffectAdmission {
    Pending(nostr::Event, String, bool),
    Completed(BrokerResponse),
    Conflict,
}

impl AuthorityStore {
    #[cfg(not(unix))]
    pub(crate) async fn open(_path: &Path) -> Result<Self, StoreError> {
        Err(StoreError::UnsupportedPlatform)
    }

    #[cfg(unix)]
    pub(crate) async fn open(path: &Path) -> Result<Self, StoreError> {
        validate_state_path(path)?;
        ensure_private_database_file(path)?;
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false)
            .foreign_keys(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Full)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let state_dir = path.parent().ok_or(StoreError::InvalidPath)?.to_path_buf();
        let store = Self { pool, state_dir };
        store.initialize().await?;
        Ok(store)
    }

    #[cfg(unix)]
    async fn initialize(&self) -> Result<(), StoreError> {
        loop {
            let current: i64 = sqlx::query_scalar("PRAGMA user_version")
                .fetch_one(&self.pool)
                .await
                .map_err(|_| StoreError::Unavailable)?;
            match current {
                0 => self.create_schema().await?,
                1 => self.migrate_schema_v2().await?,
                2 => self.migrate_schema_v3().await?,
                SCHEMA_VERSION => break,
                _ => return Err(StoreError::UnsupportedSchema),
            }
        }
        let integrity: String = sqlx::query_scalar("PRAGMA quick_check")
            .fetch_one(&self.pool)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if integrity != "ok" {
            return Err(StoreError::Integrity);
        }
        self.validate_authority_state().await?;
        self.validate_effect_state().await
    }

    #[cfg(unix)]
    async fn create_schema(&self) -> Result<(), StoreError> {
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
        if current != 0 {
            return Ok(());
        }
        sqlx::query(
            "CREATE TABLE broker_authorities (
                credential_hash BLOB PRIMARY KEY NOT NULL CHECK(length(credential_hash) = 32),
                authority_json TEXT NOT NULL UNIQUE,
                authority_scope BLOB NOT NULL CHECK(length(authority_scope) = 32),
                generation TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('active', 'fenced')),
                quiescing INTEGER NOT NULL DEFAULT 0 CHECK(quiescing IN (0, 1)),
                UNIQUE(authority_scope, generation)
            ) STRICT",
        )
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
        sqlx::query(
            "CREATE TABLE broker_receipts (
                receipt_sequence INTEGER PRIMARY KEY,
                credential_hash BLOB NOT NULL,
                request_id TEXT NOT NULL,
                request_digest BLOB NOT NULL CHECK(length(request_digest) = 32),
                response_json TEXT NOT NULL,
                UNIQUE (credential_hash, request_id),
                FOREIGN KEY (credential_hash) REFERENCES broker_authorities(credential_hash)
            ) STRICT",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(EFFECTS_TABLE_SQL)
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

    #[cfg(unix)]
    async fn migrate_schema_v2(&self) -> Result<(), StoreError> {
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
        if current != 1 {
            return Ok(());
        }
        lifecycle::require_canonical_authority_state_v2(&mut transaction).await?;
        sqlx::query(EFFECTS_TABLE_SQL)
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("PRAGMA user_version = 2")
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    pub(crate) async fn issue(
        &self,
        digest: &CredentialDigest,
        identity: &AuthorityIdentity,
    ) -> Result<(), StoreError> {
        let identity = identity
            .validated()
            .map_err(|_| StoreError::InvalidIdentity)?;
        let authority_json = canonical_identity(&identity)?;
        let scope = authority_scope(&identity)?;
        let generation = identity.generation.clone();
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

        let generation_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM broker_authorities
                WHERE authority_scope = ? AND generation = ?
            )",
        )
        .bind(scope.as_slice())
        .bind(&generation)
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if generation_exists {
            return Err(StoreError::GenerationAlreadyIssued);
        }

        let scope_active: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM broker_authorities
                WHERE authority_scope = ? AND (state = 'active' OR quiescing = 1)
            )",
        )
        .bind(scope.as_slice())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if scope_active {
            return Err(StoreError::ScopeAlreadyActive);
        }

        sqlx::query(
            "INSERT INTO broker_authorities
             (credential_hash, authority_json, authority_scope, generation, state)
             VALUES (?, ?, ?, ?, 'active')",
        )
        .bind(digest.as_bytes())
        .bind(authority_json)
        .bind(scope.as_slice())
        .bind(generation)
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Conflict)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    #[cfg(unix)]
    async fn validate_authority_state(&self) -> Result<(), StoreError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut transaction = connection
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        require_canonical_authority_state(&mut transaction).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    #[cfg(unix)]
    async fn validate_effect_state(&self) -> Result<(), StoreError> {
        let rows = sqlx::query(
            "SELECT e.request_id, e.event_json, e.response_json, a.authority_json
             FROM broker_effects e
             JOIN broker_authorities a ON a.credential_hash = e.credential_hash",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        for row in rows {
            let request_id: String = row
                .try_get("request_id")
                .map_err(|_| StoreError::Integrity)?;
            let event_json: String = row
                .try_get("event_json")
                .map_err(|_| StoreError::Integrity)?;
            let authority_json: String = row
                .try_get("authority_json")
                .map_err(|_| StoreError::Integrity)?;
            let identity: AuthorityIdentity =
                serde_json::from_str(&authority_json).map_err(|_| StoreError::Integrity)?;
            let event = decode_effect_event(&event_json)?;
            if event.kind.as_u16() != 9
                || event.pubkey.to_hex() != identity.executor_agent_pubkey.as_str()
            {
                return Err(StoreError::Integrity);
            }
            let response_json: Option<String> = row
                .try_get("response_json")
                .map_err(|_| StoreError::Integrity)?;
            if let Some(response_json) = response_json {
                let response: BrokerResponse =
                    serde_json::from_str(&response_json).map_err(|_| StoreError::Integrity)?;
                if response.request_id != request_id
                    || !matches!(
                        response.result,
                        BrokerResult::Succeeded {
                            outcome: ActionOutcome::MessagePost(ref published)
                        } if published.event_id == event.id.to_hex()
                    )
                {
                    return Err(StoreError::Integrity);
                }
            }
        }
        Ok(())
    }

    pub(crate) async fn execute(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
    ) -> Result<BrokerResponse, StoreError> {
        self.execute_inner(credential, request_bytes, request, None)
            .await
    }

    #[cfg(unix)]
    pub(crate) async fn execute_with_address(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
        address: StorageAddress,
    ) -> Result<BrokerResponse, StoreError> {
        self.execute_inner(credential, request_bytes, request, Some(address))
            .await
    }

    async fn execute_inner(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
        address: Option<StorageAddress>,
    ) -> Result<BrokerResponse, StoreError> {
        let request_digest: [u8; 32] = Sha256::digest(request_bytes).into();
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
        let effect_id_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM broker_effects WHERE credential_hash = ? AND request_id = ?)",
        )
        .bind(credential.as_bytes())
        .bind(request.request_id())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if effect_id_exists {
            return Ok(conflict_response(request.request_id()));
        }
        match load_receipt(
            &mut transaction,
            credential,
            request.request_id(),
            &request_digest,
        )
        .await?
        {
            ReceiptMatch::Replay(response) => return Ok((*response).replayed()),
            ReceiptMatch::Conflict => return Ok(conflict_response(request.request_id())),
            ReceiptMatch::Missing => {}
        }

        let result = match request.action() {
            Action::AuthorityStatus => {
                BrokerResult::succeeded(ActionOutcome::AuthorityStatus(ManagedAcpAuthority {
                    identity: authority.identity,
                    state: authority.state,
                }))
            }
            Action::StorageAddress => match address {
                Some(address)
                    if authority.state == AuthorityState::Active
                        && address.author_pubkey == authority.identity.executor_agent_pubkey =>
                {
                    BrokerResult::succeeded(ActionOutcome::StorageAddress(address))
                }
                Some(_) => BrokerResult::failed(BrokerError::unauthorized(
                    "authority generation is not active or signer does not match executor",
                )),
                None => BrokerResult::failed(BrokerError::unsupported(
                    "storage.address requires a configured host signer",
                )),
            },
            _ => BrokerResult::failed(BrokerError::unsupported(
                "this host does not serve the requested action",
            )),
        };
        let response = BrokerResponse::new(request.request_id(), result);
        let response_json = serde_json::to_string(&response).map_err(|_| StoreError::Integrity)?;
        sqlx::query(
            "INSERT INTO broker_receipts
             (credential_hash, request_id, request_digest, response_json)
             VALUES (?, ?, ?, ?)",
        )
        .bind(credential.as_bytes())
        .bind(request.request_id())
        .bind(request_digest.as_slice())
        .bind(response_json)
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Conflict)?;
        prune_receipts(&mut transaction, credential).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        Ok(response)
    }

    #[cfg(unix)]
    pub(crate) async fn prepare_message_post(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
        signer: &HostSigner,
    ) -> Result<EffectAdmission, StoreError> {
        let ActionArgs::MessagePost(args) = request.args() else {
            return Err(StoreError::InvalidIdentity);
        };
        let request_digest: [u8; 32] = Sha256::digest(request_bytes).into();
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

        let existing = sqlx::query(
            "SELECT request_digest, event_json, response_json FROM broker_effects
             WHERE credential_hash = ? AND request_id = ?",
        )
        .bind(credential.as_bytes())
        .bind(request.request_id())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if let Some(row) = existing {
            let stored_digest: Vec<u8> = row
                .try_get("request_digest")
                .map_err(|_| StoreError::Integrity)?;
            if stored_digest.as_slice() != request_digest {
                return Ok(EffectAdmission::Conflict);
            }
            let response_json: Option<String> = row
                .try_get("response_json")
                .map_err(|_| StoreError::Integrity)?;
            if let Some(response_json) = response_json {
                let response =
                    serde_json::from_str(&response_json).map_err(|_| StoreError::Integrity)?;
                return Ok(EffectAdmission::Completed(response));
            }
            let event_json: String = row
                .try_get("event_json")
                .map_err(|_| StoreError::Integrity)?;
            let event = decode_effect_event(&event_json)?;
            if !signer.matches_executor(&authority.identity)
                || event.pubkey.to_hex() != authority.identity.executor_agent_pubkey.as_str()
            {
                return Err(StoreError::InvalidIdentity);
            }
            return Ok(EffectAdmission::Pending(
                event,
                authority.identity.community_relay_url,
                true,
            ));
        }
        let old_receipt: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM broker_receipts WHERE credential_hash = ? AND request_id = ?)",
        )
        .bind(credential.as_bytes())
        .bind(request.request_id())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if old_receipt {
            return Ok(EffectAdmission::Conflict);
        }
        let pending_effect: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM broker_effects
                WHERE credential_hash = ? AND response_json IS NULL
            )",
        )
        .bind(credential.as_bytes())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if pending_effect {
            return Err(StoreError::EffectsPending);
        }
        if authority.state != AuthorityState::Active {
            return Err(StoreError::AuthorityFenced);
        }
        let event = signer
            .sign_message(&authority.identity, args)
            .map_err(|_| StoreError::InvalidIdentity)?;
        let event_json = serde_json::to_string(&event).map_err(|_| StoreError::Integrity)?;
        sqlx::query(
            "INSERT INTO broker_effects
             (credential_hash, request_id, request_digest, action, event_json)
             VALUES (?, ?, ?, 'message.post', ?)",
        )
        .bind(credential.as_bytes())
        .bind(request.request_id())
        .bind(request_digest.as_slice())
        .bind(event_json)
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Conflict)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        Ok(EffectAdmission::Pending(
            event,
            authority.identity.community_relay_url,
            false,
        ))
    }

    #[cfg(unix)]
    pub(crate) async fn complete_message_post(
        &self,
        credential: &CredentialDigest,
        request_id: &str,
        event_id: &str,
    ) -> Result<BrokerResponse, StoreError> {
        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| StoreError::Unavailable)?;
        load_authority(&mut transaction, credential).await?;
        let row = sqlx::query(
            "SELECT event_json, response_json FROM broker_effects
             WHERE credential_hash = ? AND request_id = ?",
        )
        .bind(credential.as_bytes())
        .bind(request_id)
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?
        .ok_or(StoreError::Conflict)?;
        let event_json: String = row
            .try_get("event_json")
            .map_err(|_| StoreError::Integrity)?;
        let event = decode_effect_event(&event_json)?;
        if event.id.to_hex() != event_id {
            return Err(StoreError::Conflict);
        }
        let response_json: Option<String> = row
            .try_get("response_json")
            .map_err(|_| StoreError::Integrity)?;
        if let Some(response_json) = response_json {
            return serde_json::from_str(&response_json).map_err(|_| StoreError::Integrity);
        }
        let response = BrokerResponse::new(
            request_id,
            BrokerResult::succeeded(ActionOutcome::MessagePost(EventPublished {
                event_id: event.id.to_hex(),
                kind: u32::from(event.kind.as_u16()),
                created_at: event.created_at.as_secs(),
            })),
        );
        let response_json = serde_json::to_string(&response).map_err(|_| StoreError::Integrity)?;
        sqlx::query(
            "UPDATE broker_effects SET response_json = ?
             WHERE credential_hash = ? AND request_id = ? AND response_json IS NULL",
        )
        .bind(response_json)
        .bind(credential.as_bytes())
        .bind(request_id)
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        Ok(response)
    }
}

#[cfg(unix)]
fn decode_effect_event(value: &str) -> Result<nostr::Event, StoreError> {
    let event: nostr::Event = serde_json::from_str(value).map_err(|_| StoreError::Integrity)?;
    event.verify().map_err(|_| StoreError::Integrity)?;
    Ok(event)
}

#[cfg(all(test, not(unix)))]
#[tokio::test]
async fn unsupported_platform_rejects_authority_state() {
    let result = AuthorityStore::open(Path::new("authority.db")).await;
    assert!(matches!(result, Err(StoreError::UnsupportedPlatform)));
}

async fn invalidate_authority_receipts(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    authority_json: &str,
) -> Result<(), StoreError> {
    sqlx::query(
        "DELETE FROM broker_receipts
         WHERE credential_hash = (
             SELECT credential_hash FROM broker_authorities WHERE authority_json = ?
         )",
    )
    .bind(authority_json)
    .execute(&mut **transaction)
    .await
    .map_err(|_| StoreError::Unavailable)?;
    Ok(())
}

async fn prune_receipts(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    credential: &CredentialDigest,
) -> Result<(), StoreError> {
    sqlx::query(
        "DELETE FROM broker_receipts
         WHERE credential_hash = ?
           AND receipt_sequence NOT IN (
               SELECT receipt_sequence FROM broker_receipts
               WHERE credential_hash = ?
               ORDER BY receipt_sequence DESC
               LIMIT ?
           )",
    )
    .bind(credential.as_bytes())
    .bind(credential.as_bytes())
    .bind(MAX_RECEIPTS_PER_CREDENTIAL)
    .execute(&mut **transaction)
    .await
    .map_err(|_| StoreError::Unavailable)?;
    Ok(())
}

async fn require_canonical_authority_state(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), StoreError> {
    let rows = sqlx::query(
        "SELECT authority_json, authority_scope, generation, state, quiescing FROM broker_authorities",
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
        let stored_generation: String = row
            .try_get("generation")
            .map_err(|_| StoreError::Integrity)?;
        let state: String = row.try_get("state").map_err(|_| StoreError::Integrity)?;
        let quiescing: i64 = row
            .try_get("quiescing")
            .map_err(|_| StoreError::Integrity)?;
        decode_canonical_identity(&authority_json, &stored_scope, &stored_generation)?;
        parse_state(&state, quiescing)?;
    }
    Ok(())
}

async fn load_authority(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    credential: &CredentialDigest,
) -> Result<StoredAuthority, StoreError> {
    let row = sqlx::query(
        "SELECT authority_json, authority_scope, generation, state, quiescing
         FROM broker_authorities WHERE credential_hash = ?",
    )
    .bind(credential.as_bytes())
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| StoreError::Unavailable)?
    .ok_or(StoreError::Unauthenticated)?;
    let authority_json: String = row
        .try_get("authority_json")
        .map_err(|_| StoreError::Integrity)?;
    let state: String = row.try_get("state").map_err(|_| StoreError::Integrity)?;
    let quiescing: i64 = row
        .try_get("quiescing")
        .map_err(|_| StoreError::Integrity)?;
    let stored_scope: Vec<u8> = row
        .try_get("authority_scope")
        .map_err(|_| StoreError::Integrity)?;
    let stored_generation: String = row
        .try_get("generation")
        .map_err(|_| StoreError::Integrity)?;
    let identity = decode_canonical_identity(&authority_json, &stored_scope, &stored_generation)?;
    Ok(StoredAuthority {
        identity,
        state: parse_state(&state, quiescing)?,
    })
}

async fn load_receipt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    credential: &CredentialDigest,
    request_id: &str,
    request_digest: &[u8; 32],
) -> Result<ReceiptMatch, StoreError> {
    let row = sqlx::query(
        "SELECT request_digest, response_json FROM broker_receipts
         WHERE credential_hash = ? AND request_id = ?",
    )
    .bind(credential.as_bytes())
    .bind(request_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|_| StoreError::Unavailable)?;
    let Some(row) = row else {
        return Ok(ReceiptMatch::Missing);
    };
    let stored_digest: Vec<u8> = row
        .try_get("request_digest")
        .map_err(|_| StoreError::Integrity)?;
    if stored_digest.as_slice() != request_digest {
        return Ok(ReceiptMatch::Conflict);
    }
    let response_json: String = row
        .try_get("response_json")
        .map_err(|_| StoreError::Integrity)?;
    let response = serde_json::from_str(&response_json).map_err(|_| StoreError::Integrity)?;
    Ok(ReceiptMatch::Replay(Box::new(response)))
}

fn conflict_response(request_id: &str) -> BrokerResponse {
    BrokerResponse::new(
        request_id,
        BrokerResult::failed(BrokerError::new(
            buzz_sdk::broker::BrokerErrorCode::RequestIdConflict,
            "requestId was already used with different request bytes",
        )),
    )
}

fn decode_canonical_identity(
    authority_json: &str,
    stored_scope: &[u8],
    stored_generation: &str,
) -> Result<AuthorityIdentity, StoreError> {
    let parsed: AuthorityIdentity =
        serde_json::from_str(authority_json).map_err(|_| StoreError::Integrity)?;
    let identity = parsed.validated().map_err(|_| StoreError::Integrity)?;
    let canonical = canonical_identity(&identity)?;
    if canonical.as_bytes() != authority_json.as_bytes() {
        return Err(StoreError::Integrity);
    }
    let expected_scope = authority_scope(&identity)?;
    if expected_scope.as_slice() != stored_scope {
        return Err(StoreError::Integrity);
    }
    if identity.generation != stored_generation {
        return Err(StoreError::Integrity);
    }
    Ok(identity)
}

fn parse_state(value: &str, quiescing: i64) -> Result<AuthorityState, StoreError> {
    match (value, quiescing) {
        ("active", 0) => Ok(AuthorityState::Active),
        ("fenced", 1) => Ok(AuthorityState::Quiescing),
        ("fenced", 0) => Ok(AuthorityState::Fenced),
        _ => Err(StoreError::Integrity),
    }
}

fn canonical_identity(identity: &AuthorityIdentity) -> Result<String, StoreError> {
    serde_json::to_string(identity).map_err(|_| StoreError::InvalidIdentity)
}

fn authority_scope(identity: &AuthorityIdentity) -> Result<[u8; 32], StoreError> {
    let scope = AuthorityScope {
        community_relay_url: &identity.community_relay_url,
        logical_agent_pubkey: identity.logical_agent_pubkey.as_str(),
        task_id: &identity.task_id,
    };
    let bytes = serde_json::to_vec(&scope).map_err(|_| StoreError::InvalidIdentity)?;
    Ok(Sha256::digest(bytes).into())
}

#[cfg(unix)]
fn validate_state_path(path: &Path) -> Result<(), StoreError> {
    if !path.is_absolute() {
        return Err(StoreError::InvalidPath);
    }
    let parent = path.parent().ok_or(StoreError::InvalidPath)?;
    let metadata = std::fs::metadata(parent).map_err(|_| StoreError::InvalidPath)?;
    if !metadata.is_dir() {
        return Err(StoreError::InvalidPath);
    }
    require_private_directory(&metadata)?;
    if std::fs::symlink_metadata(path).is_ok_and(|entry| entry.file_type().is_symlink()) {
        return Err(StoreError::InvalidPath);
    }
    Ok(())
}

#[cfg(unix)]
fn require_private_directory(metadata: &std::fs::Metadata) -> Result<(), StoreError> {
    if !is_private_owned_directory(metadata) {
        return Err(StoreError::InsecureDirectory);
    }
    Ok(())
}

#[cfg(unix)]
fn is_private_owned_directory(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    metadata.permissions().mode() & 0o077 == 0 && is_owned_by_effective_user(metadata)
}

#[cfg(unix)]
fn ensure_private_database_file(path: &Path) -> Result<(), StoreError> {
    if path.exists() {
        require_private_file(path)?;
        return Ok(());
    }
    create_private_file(path)
}

#[cfg(unix)]
fn create_private_file(path: &Path) -> Result<(), StoreError> {
    use std::os::unix::fs::OpenOptionsExt as _;
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .map_err(|_| StoreError::Unavailable)?;
    Ok(())
}

#[cfg(unix)]
fn require_private_file(path: &Path) -> Result<(), StoreError> {
    let metadata = std::fs::metadata(path).map_err(|_| StoreError::Unavailable)?;
    if !is_private_regular_file(&metadata) {
        return Err(StoreError::InsecureFile);
    }
    Ok(())
}

#[cfg(unix)]
fn is_private_regular_file(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::PermissionsExt as _;
    metadata.is_file()
        && metadata.permissions().mode() & 0o077 == 0
        && is_owned_by_effective_user(metadata)
}

#[cfg(unix)]
fn is_owned_by_effective_user(metadata: &std::fs::Metadata) -> bool {
    use std::os::unix::fs::MetadataExt as _;
    metadata.uid() == nix::unistd::geteuid().as_raw()
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[cfg(not(unix))]
    #[error("broker authority state is unsupported without Unix owner checks")]
    UnsupportedPlatform,
    #[cfg(unix)]
    #[error("broker state path is invalid")]
    InvalidPath,
    #[cfg(unix)]
    #[error("broker state directory must be owned by this process user and owner-only")]
    InsecureDirectory,
    #[cfg(unix)]
    #[error("broker state file must be owned by this process user and owner-only")]
    InsecureFile,
    #[error("broker state is unavailable")]
    Unavailable,
    #[cfg(unix)]
    #[error("broker state schema is unsupported")]
    UnsupportedSchema,
    #[error("broker state integrity check failed")]
    Integrity,
    #[error("authority identity is invalid")]
    InvalidIdentity,
    #[error("this exact authority generation was already issued")]
    GenerationAlreadyIssued,
    #[error("another generation is already active for this authority scope")]
    ScopeAlreadyActive,
    #[error("authority state changed concurrently")]
    Conflict,
    #[error("authority identity was not issued by this host")]
    AuthorityNotFound,
    #[error("broker credential is not authenticated")]
    Unauthenticated,
    #[error("authority generation is fenced")]
    #[cfg(unix)]
    AuthorityFenced,
    #[error("authority has unresolved effects")]
    EffectsPending,
    #[error("authority must be quiescing before effects can drain")]
    #[cfg(unix)]
    NotQuiescing,
}
