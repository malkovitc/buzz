use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;
#[cfg(unix)]
use std::time::Duration;

use buzz_sdk::broker::{
    Action, ActionOutcome, AuthorityIdentity, AuthorityState, BrokerError, BrokerResponse,
    BrokerResult, ManagedAcpAuthority, MessagePage, StorageAddress, ValidatedRequest,
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
mod audio;
#[cfg(unix)]
mod drain;
#[cfg(unix)]
mod effect_lock;
mod lifecycle;

#[cfg(unix)]
const SCHEMA_VERSION: i64 = 7;
#[cfg(unix)]
const AUDIO_SESSIONS_TABLE_SQL: &str = "CREATE TABLE broker_audio_sessions (
    credential_hash BLOB NOT NULL,
    session_id TEXT NOT NULL CHECK(length(session_id) = 36),
    state TEXT NOT NULL CHECK(state IN ('active', 'closed')),
    PRIMARY KEY (credential_hash, session_id),
    FOREIGN KEY (credential_hash) REFERENCES broker_authorities(credential_hash)
) STRICT";
#[cfg(unix)]
const AUDIO_ACTIVE_INDEX_SQL: &str = "CREATE UNIQUE INDEX one_active_audio_session_per_credential
    ON broker_audio_sessions(credential_hash) WHERE state = 'active'";
#[cfg(unix)]
const EFFECTS_TABLE_SQL: &str = "CREATE TABLE broker_effects (
    credential_hash BLOB NOT NULL,
    request_id TEXT NOT NULL,
    request_digest BLOB NOT NULL CHECK(length(request_digest) = 32),
    action TEXT NOT NULL CHECK(action IN ('message.post', 'message.reply')),
    event_json TEXT NOT NULL,
    response_json TEXT,
    PRIMARY KEY (credential_hash, request_id),
    FOREIGN KEY (credential_hash) REFERENCES broker_authorities(credential_hash)
) STRICT";
pub(crate) const MAX_RECEIPTS_PER_CREDENTIAL: i64 = 64;
const MAX_RECEIPT_BYTES_PER_CREDENTIAL: i64 = 32 * 1024 * 1024;
const MAX_TOTAL_RECEIPT_BYTES: i64 = 128 * 1024 * 1024;

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
    owner_pubkey: Option<String>,
}

enum ReceiptMatch {
    Missing,
    Replay(Box<BrokerResponse>),
    Conflict,
    OwnerChanged,
}

#[cfg(unix)]
pub(crate) enum EffectAdmission {
    Pending(nostr::Event, String, bool),
    Completed(BrokerResponse),
    NeedsReplyTarget(String),
    Conflict,
}

#[cfg(unix)]
pub(crate) enum ChannelReadAdmission {
    Replay(BrokerResponse),
    Query(String),
    Denied,
}

impl AuthorityStore {
    #[cfg(unix)]
    pub(crate) async fn require_active_local_owner(
        &self,
        credential: &CredentialDigest,
        signer: &HostSigner,
    ) -> Result<Option<AuthorityIdentity>, StoreError> {
        use buzz_sdk::broker::RuntimeLocationKind;

        let mut connection = self
            .pool
            .acquire()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let mut transaction = connection
            .begin_with("BEGIN IMMEDIATE")
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let authority = match load_authority(&mut transaction, credential).await {
            Ok(authority) => authority,
            Err(StoreError::Unauthenticated) => return Ok(None),
            Err(error) => return Err(error),
        };
        let owner = signer.owner_hex();
        if authority.state != AuthorityState::Active
            || authority.identity.location.kind != RuntimeLocationKind::Local
            || !signer.matches_executor(&authority.identity)
            || !owner_matches(&authority, &owner)
        {
            return Ok(None);
        }
        bind_owner(&mut transaction, credential, &owner).await?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        Ok(Some(authority.identity))
    }

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
                3 => self.migrate_schema_v4().await?,
                4 => self.migrate_schema_v5().await?,
                5 => self.migrate_schema_v6().await?,
                6 => self.migrate_schema_v7().await?,
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
        self.validate_effect_state().await?;
        self.validate_audio_state().await
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
                owner_pubkey TEXT CHECK(owner_pubkey IS NULL OR length(owner_pubkey) = 64),
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
                owner_pubkey TEXT,
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

    #[cfg(unix)]
    async fn migrate_schema_v5(&self) -> Result<(), StoreError> {
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
        if current != 4 {
            return Ok(());
        }
        sqlx::query(
            "ALTER TABLE broker_authorities ADD COLUMN owner_pubkey TEXT
             CHECK(owner_pubkey IS NULL OR length(owner_pubkey) = 64)",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        // Prior schemas did not bind the owner. Even if receipts were evicted,
        // an active generation might already have used another owner.
        sqlx::query(
            "UPDATE broker_authorities SET state = 'fenced', quiescing = 1
             WHERE state = 'active' AND quiescing = 0",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(
            "DELETE FROM broker_receipts WHERE credential_hash IN (
                SELECT credential_hash FROM broker_authorities WHERE quiescing = 1
            )",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("PRAGMA user_version = 5")
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    #[cfg(unix)]
    async fn migrate_schema_v6(&self) -> Result<(), StoreError> {
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
        if current != 5 {
            return Ok(());
        }
        sqlx::query("ALTER TABLE broker_effects RENAME TO broker_effects_v5")
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(EFFECTS_TABLE_SQL)
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query(
            "INSERT INTO broker_effects
             (credential_hash, request_id, request_digest, action, event_json, response_json)
             SELECT credential_hash, request_id, request_digest, action, event_json, response_json
             FROM broker_effects_v5",
        )
        .execute(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("DROP TABLE broker_effects_v5")
            .execute(&mut *transaction)
            .await
            .map_err(|_| StoreError::Unavailable)?;
        sqlx::query("PRAGMA user_version = 6")
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
        let owners: Vec<Option<String>> =
            sqlx::query_scalar("SELECT owner_pubkey FROM broker_authorities")
                .fetch_all(&mut *transaction)
                .await
                .map_err(|_| StoreError::Unavailable)?;
        if owners
            .iter()
            .flatten()
            .any(|owner| invalid_owner_pubkey(owner))
        {
            return Err(StoreError::Integrity);
        }
        transaction
            .commit()
            .await
            .map_err(|_| StoreError::Unavailable)
    }

    #[cfg(unix)]
    async fn validate_effect_state(&self) -> Result<(), StoreError> {
        let rows = sqlx::query(
            "SELECT e.request_id, e.action, e.event_json, e.response_json, a.authority_json
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
            let action: String = row.try_get("action").map_err(|_| StoreError::Integrity)?;
            if action != "message.post" && action != "message.reply" {
                return Err(StoreError::Integrity);
            }
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
                        BrokerResult::Succeeded { outcome: ActionOutcome::MessagePost(ref published) }
                            if action == "message.post" && published.event_id == event.id.to_hex()
                    ) && !matches!(
                        response.result,
                        BrokerResult::Succeeded { outcome: ActionOutcome::MessageReply(ref published) }
                            if action == "message.reply" && published.event_id == event.id.to_hex()
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
        self.execute_inner(credential, request_bytes, request, None, None)
            .await
    }

    #[cfg(unix)]
    pub(crate) async fn execute_with_address(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
        address: StorageAddress,
        owner_pubkey: String,
    ) -> Result<BrokerResponse, StoreError> {
        self.execute_inner(
            credential,
            request_bytes,
            request,
            Some((address, owner_pubkey)),
            None,
        )
        .await
    }

    #[cfg(unix)]
    pub(crate) async fn prepare_channel_read(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
        signer: &HostSigner,
    ) -> Result<ChannelReadAdmission, StoreError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|_| StoreError::Unavailable)?;
        let authority = load_authority(&mut transaction, credential).await?;
        let request_digest: [u8; 32] = Sha256::digest(request_bytes).into();
        let effect_id_exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM broker_effects WHERE credential_hash = ? AND request_id = ?)",
        )
        .bind(credential.as_bytes())
        .bind(request.request_id())
        .fetch_one(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if effect_id_exists {
            return Ok(ChannelReadAdmission::Replay(conflict_response(
                request.request_id(),
            )));
        }
        match load_receipt(
            &mut transaction,
            credential,
            request.request_id(),
            &request_digest,
            Some(&signer.owner_hex()),
        )
        .await?
        {
            ReceiptMatch::Conflict => {
                return Ok(ChannelReadAdmission::Replay(conflict_response(
                    request.request_id(),
                )))
            }
            ReceiptMatch::OwnerChanged => return Ok(ChannelReadAdmission::Denied),
            ReceiptMatch::Replay(response) => {
                if matches!(response.result, BrokerResult::Succeeded { .. })
                    && (authority.state != AuthorityState::Active
                        || !signer.matches_executor(&authority.identity))
                {
                    return Ok(ChannelReadAdmission::Denied);
                }
                return Ok(ChannelReadAdmission::Replay((*response).replayed()));
            }
            ReceiptMatch::Missing => {}
        }
        if authority.state != AuthorityState::Active
            || !signer.matches_executor(&authority.identity)
            || !owner_matches(&authority, &signer.owner_hex())
        {
            return Ok(ChannelReadAdmission::Denied);
        }
        Ok(ChannelReadAdmission::Query(
            authority.identity.community_relay_url,
        ))
    }

    #[cfg(unix)]
    pub(crate) async fn execute_with_read_page(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
        page: MessagePage,
        signer: &HostSigner,
    ) -> Result<BrokerResponse, StoreError> {
        self.execute_inner(
            credential,
            request_bytes,
            request,
            None,
            Some((Some(page), signer.pubkey_hex(), signer.owner_hex())),
        )
        .await
    }

    #[cfg(unix)]
    pub(crate) async fn execute_read_denial(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
        signer: &HostSigner,
    ) -> Result<BrokerResponse, StoreError> {
        self.execute_inner(
            credential,
            request_bytes,
            request,
            None,
            Some((None, signer.pubkey_hex(), signer.owner_hex())),
        )
        .await
    }

    async fn execute_inner(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
        address: Option<(StorageAddress, String)>,
        read_page: Option<(Option<MessagePage>, String, String)>,
    ) -> Result<BrokerResponse, StoreError> {
        let request_digest: [u8; 32] = Sha256::digest(request_bytes).into();
        let read_owner = read_page.as_ref().map(|(_, _, owner)| owner.clone());
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
            read_owner.as_deref(),
        )
        .await?
        {
            ReceiptMatch::Replay(response) => {
                if request.action() == Action::StorageAddress {
                    if let BrokerResult::Succeeded {
                        outcome: ActionOutcome::StorageAddress(stored),
                    } = &response.result
                    {
                        let still_authorized = authority.state == AuthorityState::Active
                            && address.as_ref().is_some_and(|(current, owner)| {
                                current == stored
                                    && current.author_pubkey
                                        == authority.identity.executor_agent_pubkey
                                    && owner_matches(&authority, owner)
                            });
                        if !still_authorized {
                            return Ok(BrokerResponse::new(
                                request.request_id(),
                                BrokerResult::failed(BrokerError::unauthorized(
                                    "verified signer or owner changed for this generation",
                                )),
                            ));
                        }
                    }
                }
                if request.action() == Action::ChannelRead
                    && matches!(response.result, BrokerResult::Succeeded { .. })
                    && (authority.state != AuthorityState::Active
                        || read_page.as_ref().is_none_or(|(_, signer, _)| {
                            signer != authority.identity.executor_agent_pubkey.as_str()
                        })
                        || read_owner
                            .as_deref()
                            .is_none_or(|owner| !owner_matches(&authority, owner)))
                {
                    return Ok(BrokerResponse::new(
                        request.request_id(),
                        BrokerResult::failed(BrokerError::unauthorized(
                            "active authority and matching host signer are required",
                        )),
                    ));
                }
                return Ok((*response).replayed());
            }
            ReceiptMatch::Conflict => return Ok(conflict_response(request.request_id())),
            ReceiptMatch::OwnerChanged => {
                return Ok(BrokerResponse::new(
                    request.request_id(),
                    BrokerResult::failed(BrokerError::unauthorized(
                        "read receipt belongs to another verified owner",
                    )),
                ));
            }
            ReceiptMatch::Missing => {}
        }

        let result = match request.action() {
            Action::AuthorityStatus => {
                BrokerResult::succeeded(ActionOutcome::AuthorityStatus(ManagedAcpAuthority {
                    identity: authority.identity,
                    state: authority.state,
                }))
            }
            Action::StorageAddress => match address.as_ref() {
                Some((address, owner))
                    if authority.state == AuthorityState::Active
                        && address.author_pubkey == authority.identity.executor_agent_pubkey
                        && owner_matches(&authority, owner) =>
                {
                    BrokerResult::succeeded(ActionOutcome::StorageAddress(address.clone()))
                }
                Some(_) => BrokerResult::failed(BrokerError::unauthorized(
                    "authority generation is not active or signer does not match executor",
                )),
                None => BrokerResult::failed(BrokerError::unsupported(
                    "storage.address requires a configured host signer",
                )),
            },
            Action::ChannelRead => match read_page {
                Some((Some(page), signer_pubkey, _))
                    if authority.state == AuthorityState::Active
                        && signer_pubkey == authority.identity.executor_agent_pubkey.as_str()
                        && read_owner
                            .as_deref()
                            .is_some_and(|owner| owner_matches(&authority, owner)) =>
                {
                    BrokerResult::succeeded(ActionOutcome::ChannelRead(page))
                }
                Some(_) => BrokerResult::failed(BrokerError::unauthorized(
                    "authority generation is not active or signer does not match executor",
                )),
                None => BrokerResult::failed(BrokerError::unsupported(
                    "channel.read requires a configured host signer",
                )),
            },
            _ => BrokerResult::failed(BrokerError::unsupported(
                "this host does not serve the requested action",
            )),
        };
        if matches!(result, BrokerResult::Succeeded { .. }) {
            if let Some(owner) = address
                .as_ref()
                .map(|(_, owner)| owner)
                .or(read_owner.as_ref())
            {
                bind_owner(&mut transaction, credential, owner).await?;
            }
        }
        let response = BrokerResponse::new(request.request_id(), result);
        let response_json = serde_json::to_string(&response).map_err(|_| StoreError::Integrity)?;
        sqlx::query(
            "INSERT INTO broker_receipts
             (credential_hash, request_id, request_digest, response_json, owner_pubkey)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(credential.as_bytes())
        .bind(request.request_id())
        .bind(request_digest.as_slice())
        .bind(response_json)
        .bind(
            if matches!(
                response.result,
                BrokerResult::Succeeded {
                    outcome: ActionOutcome::ChannelRead(_)
                }
            ) {
                read_owner.as_deref()
            } else {
                None
            },
        )
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
        self.prepare_message_effect(credential, request_bytes, request, signer, None)
            .await
    }

    #[cfg(unix)]
    pub(crate) async fn prepare_message_reply(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
        signer: &HostSigner,
        thread: Option<&buzz_sdk::ThreadRef>,
    ) -> Result<EffectAdmission, StoreError> {
        self.prepare_message_effect(credential, request_bytes, request, signer, thread)
            .await
    }

    #[cfg(unix)]
    async fn prepare_message_effect(
        &self,
        credential: &CredentialDigest,
        request_bytes: &[u8],
        request: &ValidatedRequest,
        signer: &HostSigner,
        thread: Option<&buzz_sdk::ThreadRef>,
    ) -> Result<EffectAdmission, StoreError> {
        let action = match request.args() {
            ActionArgs::MessagePost(_) => "message.post",
            ActionArgs::MessageReply(_) => "message.reply",
            _ => return Err(StoreError::InvalidIdentity),
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
        if !owner_matches(&authority, &signer.owner_hex()) {
            return Err(StoreError::OwnerMismatch);
        }

        let existing = sqlx::query(
            "SELECT action, request_digest, event_json, response_json FROM broker_effects
             WHERE credential_hash = ? AND request_id = ?",
        )
        .bind(credential.as_bytes())
        .bind(request.request_id())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|_| StoreError::Unavailable)?;
        if let Some(row) = existing {
            let stored_action: String = row.try_get("action").map_err(|_| StoreError::Integrity)?;
            if stored_action != action {
                return Ok(EffectAdmission::Conflict);
            }
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
        if !signer.matches_executor(&authority.identity) {
            return Err(StoreError::InvalidIdentity);
        }
        if action == "message.reply" && thread.is_none() {
            return Ok(EffectAdmission::NeedsReplyTarget(
                authority.identity.community_relay_url,
            ));
        }
        let event = match request.args() {
            ActionArgs::MessagePost(args) => signer.sign_message(&authority.identity, args),
            ActionArgs::MessageReply(args) => signer.sign_reply(
                &authority.identity,
                args,
                thread.ok_or(StoreError::InvalidIdentity)?,
            ),
            _ => return Err(StoreError::InvalidIdentity),
        }
        .map_err(|_| StoreError::InvalidIdentity)?;
        let event_json = serde_json::to_string(&event).map_err(|_| StoreError::Integrity)?;
        bind_owner(&mut transaction, credential, &signer.owner_hex()).await?;
        sqlx::query(
            "INSERT INTO broker_effects
             (credential_hash, request_id, request_digest, action, event_json)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(credential.as_bytes())
        .bind(request.request_id())
        .bind(request_digest.as_slice())
        .bind(action)
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
    pub(crate) async fn complete_message_effect(
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
            "SELECT action, event_json, response_json FROM broker_effects
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
        let action: String = row.try_get("action").map_err(|_| StoreError::Integrity)?;
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
        let published = EventPublished {
            event_id: event.id.to_hex(),
            kind: u32::from(event.kind.as_u16()),
            created_at: event.created_at.as_secs(),
        };
        let outcome = match action.as_str() {
            "message.post" => ActionOutcome::MessagePost(published),
            "message.reply" => ActionOutcome::MessageReply(published),
            _ => return Err(StoreError::Integrity),
        };
        let response = BrokerResponse::new(request_id, BrokerResult::succeeded(outcome));
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
    prune_receipts_with_limits(
        transaction,
        credential,
        MAX_RECEIPTS_PER_CREDENTIAL,
        MAX_RECEIPT_BYTES_PER_CREDENTIAL,
        MAX_TOTAL_RECEIPT_BYTES,
    )
    .await
}

pub(crate) async fn prune_receipts_with_limits(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    credential: &CredentialDigest,
    max_count: i64,
    max_credential_bytes: i64,
    max_total_bytes: i64,
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
    .bind(max_count)
    .execute(&mut **transaction)
    .await
    .map_err(|_| StoreError::Unavailable)?;
    sqlx::query(
        "DELETE FROM broker_receipts WHERE receipt_sequence IN (
            SELECT receipt_sequence FROM (
                SELECT receipt_sequence,
                    SUM(LENGTH(CAST(response_json AS BLOB))) OVER (
                        ORDER BY receipt_sequence DESC ROWS UNBOUNDED PRECEDING
                    ) AS running_bytes
                FROM broker_receipts WHERE credential_hash = ?
            ) WHERE running_bytes > ?
        )",
    )
    .bind(credential.as_bytes())
    .bind(max_credential_bytes)
    .execute(&mut **transaction)
    .await
    .map_err(|_| StoreError::Unavailable)?;
    sqlx::query(
        "DELETE FROM broker_receipts WHERE receipt_sequence IN (
            SELECT receipt_sequence FROM (
                SELECT receipt_sequence,
                    SUM(LENGTH(CAST(response_json AS BLOB))) OVER (
                        ORDER BY receipt_sequence DESC ROWS UNBOUNDED PRECEDING
                    ) AS running_bytes
                FROM broker_receipts
            ) WHERE running_bytes > ?
        )",
    )
    .bind(max_total_bytes)
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
        "SELECT authority_json, authority_scope, generation, state, quiescing, owner_pubkey
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
    let owner_pubkey: Option<String> = row
        .try_get("owner_pubkey")
        .map_err(|_| StoreError::Integrity)?;
    if owner_pubkey.as_deref().is_some_and(invalid_owner_pubkey) {
        return Err(StoreError::Integrity);
    }
    Ok(StoredAuthority {
        identity,
        state: parse_state(&state, quiescing)?,
        owner_pubkey,
    })
}

fn owner_matches(authority: &StoredAuthority, owner: &str) -> bool {
    authority
        .owner_pubkey
        .as_deref()
        .is_none_or(|bound| bound == owner)
}

fn invalid_owner_pubkey(owner: &str) -> bool {
    owner.len() != 64
        || !owner
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
}

async fn bind_owner(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    credential: &CredentialDigest,
    owner: &str,
) -> Result<(), StoreError> {
    let result = sqlx::query(
        "UPDATE broker_authorities SET owner_pubkey = ?
         WHERE credential_hash = ? AND (owner_pubkey IS NULL OR owner_pubkey = ?)",
    )
    .bind(owner)
    .bind(credential.as_bytes())
    .bind(owner)
    .execute(&mut **transaction)
    .await
    .map_err(|_| StoreError::Unavailable)?;
    if result.rows_affected() != 1 {
        return Err(StoreError::OwnerMismatch);
    }
    Ok(())
}

async fn load_receipt(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    credential: &CredentialDigest,
    request_id: &str,
    request_digest: &[u8; 32],
    expected_owner: Option<&str>,
) -> Result<ReceiptMatch, StoreError> {
    let row = sqlx::query(
        "SELECT request_digest, response_json, owner_pubkey FROM broker_receipts
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
    let response: BrokerResponse =
        serde_json::from_str(&response_json).map_err(|_| StoreError::Integrity)?;
    if matches!(
        response.result,
        BrokerResult::Succeeded {
            outcome: ActionOutcome::ChannelRead(_)
        }
    ) {
        let stored_owner: Option<String> = row
            .try_get("owner_pubkey")
            .map_err(|_| StoreError::Integrity)?;
        if stored_owner.as_deref() != expected_owner {
            return Ok(ReceiptMatch::OwnerChanged);
        }
    }
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
    #[error("authority generation belongs to another verified owner")]
    OwnerMismatch,
    #[error("authority has unresolved effects")]
    EffectsPending,
    #[error("authority must be quiescing before effects can drain")]
    #[cfg(unix)]
    NotQuiescing,
}
