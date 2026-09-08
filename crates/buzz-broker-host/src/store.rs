use std::path::Path;
use std::time::Duration;

use buzz_sdk::broker::{
    Action, ActionOutcome, AuthorityIdentity, AuthorityState, BrokerError, BrokerResponse,
    BrokerResult, ManagedAcpAuthority, ValidatedRequest,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous};
use sqlx::{Connection as _, Row, SqlitePool};

use crate::credential::CredentialDigest;

const SCHEMA_VERSION: i64 = 1;
pub(crate) const MAX_RECEIPTS_PER_CREDENTIAL: i64 = 64;

#[derive(Clone)]
pub(crate) struct AuthorityStore {
    pool: SqlitePool,
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

impl AuthorityStore {
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
        let store = Self { pool };
        store.initialize().await?;
        Ok(store)
    }

    async fn initialize(&self) -> Result<(), StoreError> {
        let current: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&self.pool)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        match current {
            0 => self.create_schema().await?,
            SCHEMA_VERSION => {}
            _ => return Err(StoreError::UnsupportedSchema),
        }
        let integrity: String = sqlx::query_scalar("PRAGMA quick_check")
            .fetch_one(&self.pool)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        if integrity != "ok" {
            return Err(StoreError::Integrity);
        }
        self.validate_authority_state().await
    }

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
        sqlx::query(
            "CREATE TABLE broker_authorities (
                credential_hash BLOB PRIMARY KEY NOT NULL CHECK(length(credential_hash) = 32),
                authority_json TEXT NOT NULL UNIQUE,
                authority_scope BLOB NOT NULL CHECK(length(authority_scope) = 32),
                generation TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('active', 'fenced')),
                UNIQUE(authority_scope, generation)
            ) STRICT",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(
            "CREATE UNIQUE INDEX one_active_generation_per_scope
             ON broker_authorities(authority_scope) WHERE state = 'active'",
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
        sqlx::query("PRAGMA user_version = 1")
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
            "SELECT EXISTS(SELECT 1 FROM broker_authorities WHERE authority_scope = ? AND state = 'active')",
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

    pub(crate) async fn fence(&self, identity: &AuthorityIdentity) -> Result<(), StoreError> {
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
        let state: Option<String> =
            sqlx::query_scalar("SELECT state FROM broker_authorities WHERE authority_json = ?")
                .bind(&authority_json)
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|_| StoreError::Unavailable)?;
        match state.as_deref() {
            Some("fenced") => {}
            Some("active") => {
                sqlx::query(
                    "UPDATE broker_authorities SET state = 'fenced'
                     WHERE authority_json = ? AND state = 'active'",
                )
                .bind(&authority_json)
                .execute(&mut *transaction)
                .await
                .map_err(|_| StoreError::Unavailable)?;
            }
            Some(_) => return Err(StoreError::Integrity),
            None => return Err(StoreError::AuthorityNotFound),
        }
        invalidate_authority_receipts(&mut transaction, &authority_json).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

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

    pub(crate) async fn execute(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
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
            _ => BrokerResult::failed(BrokerError::unsupported(
                "this host currently serves authority.status only",
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
        let stored_generation: String = row
            .try_get("generation")
            .map_err(|_| StoreError::Integrity)?;
        let state: String = row.try_get("state").map_err(|_| StoreError::Integrity)?;
        decode_canonical_identity(&authority_json, &stored_scope, &stored_generation)?;
        parse_state(&state)?;
    }
    Ok(())
}

async fn load_authority(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    credential: &CredentialDigest,
) -> Result<StoredAuthority, StoreError> {
    let row = sqlx::query(
        "SELECT authority_json, authority_scope, generation, state
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
    let stored_scope: Vec<u8> = row
        .try_get("authority_scope")
        .map_err(|_| StoreError::Integrity)?;
    let stored_generation: String = row
        .try_get("generation")
        .map_err(|_| StoreError::Integrity)?;
    let identity = decode_canonical_identity(&authority_json, &stored_scope, &stored_generation)?;
    Ok(StoredAuthority {
        identity,
        state: parse_state(&state)?,
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

fn parse_state(value: &str) -> Result<AuthorityState, StoreError> {
    match value {
        "active" => Ok(AuthorityState::Active),
        "fenced" => Ok(AuthorityState::Fenced),
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

#[cfg(not(unix))]
fn require_private_directory(_metadata: &std::fs::Metadata) -> Result<(), StoreError> {
    Ok(())
}

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

#[cfg(not(unix))]
fn create_private_file(path: &Path) -> Result<(), StoreError> {
    std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
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
    // SAFETY: geteuid(2) has no preconditions and only reads process identity.
    metadata.uid() == unsafe { libc::geteuid() }
}

#[cfg(not(unix))]
fn require_private_file(path: &Path) -> Result<(), StoreError> {
    if !std::fs::metadata(path)
        .map_err(|_| StoreError::Unavailable)?
        .is_file()
    {
        return Err(StoreError::InsecureFile);
    }
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreError {
    #[error("broker state path is invalid")]
    InvalidPath,
    #[cfg(unix)]
    #[error("broker state directory must be owned by this process user and owner-only")]
    InsecureDirectory,
    #[error("broker state file must be owned by this process user and owner-only")]
    InsecureFile,
    #[error("broker state is unavailable")]
    Unavailable,
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
}
