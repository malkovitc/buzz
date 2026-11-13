use super::*;

#[tokio::test]
async fn v3_migration_preserves_receipts_and_adds_read_owner_column() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("create schema");
    let bearer = fixture.issue(&store).await;
    let credential = crate::credential::CredentialDigest::from_bearer(&bearer).unwrap();
    let request = status_request("v3-receipt");
    let validated = serde_json::from_slice::<BrokerRequest>(request.body())
        .unwrap()
        .validated()
        .unwrap();
    store
        .execute(&credential, request.body(), &validated)
        .await
        .expect("persist old receipt");
    drop(store);
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .unwrap();
    sqlx::query("ALTER TABLE broker_receipts DROP COLUMN owner_pubkey")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("PRAGMA user_version = 3")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let reopened = AuthorityStore::open(&fixture.state)
        .await
        .expect("migrate v3");
    let replay = reopened
        .execute(&credential, request.body(), &validated)
        .await
        .expect("old receipt replays");
    assert!(replay.replayed);
    let inspect = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .unwrap();
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&inspect)
        .await
        .unwrap();
    assert_eq!(version, 4);
    inspect.close().await;
}

#[tokio::test]
async fn quiesce_closes_admission_and_survives_restart_before_fence() {
    use buzz_sdk::broker::{AuthorityState, ManagedAcpAuthority};

    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    let credential = fixture.issue(&store).await;
    let (base_url, server) = spawn_host(store.clone()).await;
    let fence = AuthorityFence::new(
        base_url.clone(),
        credential.clone(),
        fixture.identity.clone(),
    )
    .expect("client fence");
    assert_eq!(fence.permit().await, Ok(()));

    let request = status_request("same-status-id");
    let (_, before) = send(&base_url, &credential, request.body()).await;
    let before: BrokerResponse = serde_json::from_slice(&before).expect("status response");
    assert!(!before.replayed);

    crate::issuer::quiesce_from_file(&store, &fixture.authority_file)
        .await
        .expect("quiesce commits through authority file");
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .expect("inspect compatibility state");
    let legacy_state: String =
        sqlx::query_scalar("SELECT state FROM broker_authorities WHERE authority_json = ?")
            .bind(serde_json::to_string(&fixture.identity).unwrap())
            .fetch_one(&pool)
            .await
            .expect("legacy state");
    assert_eq!(legacy_state, "fenced", "older hosts must fail closed");
    let scope: Vec<u8> = sqlx::query_scalar(
        "SELECT authority_scope FROM broker_authorities WHERE authority_json = ?",
    )
    .bind(serde_json::to_string(&fixture.identity).unwrap())
    .fetch_one(&pool)
    .await
    .expect("scope reservation");
    let legacy_issue = sqlx::query(
        "INSERT INTO broker_authorities
         (credential_hash, authority_json, authority_scope, generation, state)
         VALUES (?, '{}', ?, ?, 'active')",
    )
    .bind(vec![1u8; 32])
    .bind(scope)
    .bind(OTHER_GENERATION)
    .execute(&pool)
    .await;
    assert!(legacy_issue.is_err(), "database blocks older host issuance");
    pool.close().await;
    store
        .quiesce(&fixture.identity)
        .await
        .expect("quiesce is idempotent");
    assert_eq!(fence.permit().await, Err(AuthorityError::NotActive));
    let (_, after) = send(&base_url, &credential, request.body()).await;
    let after: BrokerResponse = serde_json::from_slice(&after).expect("fresh status response");
    assert!(!after.replayed, "old active receipt was invalidated");
    assert!(matches!(
        after.result,
        BrokerResult::Succeeded {
            outcome: buzz_sdk::broker::ActionOutcome::AuthorityStatus(ManagedAcpAuthority {
                state: AuthorityState::Quiescing,
                ..
            })
        }
    ));

    let mut successor = fixture.identity.clone();
    successor.generation = OTHER_GENERATION.into();
    successor.location.id = "successor".into();
    let successor_file = fixture.directory.path().join("successor.json");
    let successor_credential = fixture.directory.path().join("successor.credential");
    write_identity(&successor_file, &successor);
    assert!(matches!(
        issue_to_file(&store, &successor_file, &successor_credential).await,
        Err(IssuerError::Store(StoreError::ScopeAlreadyActive))
    ));
    assert!(!successor_credential.exists());
    server.abort();
    drop(store);

    let reopened = AuthorityStore::open(&fixture.state)
        .await
        .expect("state reopens");
    let (restarted_url, restarted_server) = spawn_host(reopened.clone()).await;
    let restarted_fence = AuthorityFence::new(restarted_url, credential, fixture.identity.clone())
        .expect("restarted client fence");
    assert_eq!(
        restarted_fence.permit().await,
        Err(AuthorityError::NotActive)
    );
    reopened
        .fence(&fixture.identity)
        .await
        .expect("fence after quiesce");
    assert_eq!(
        crate::issuer::quiesce_from_file(&reopened, &fixture.authority_file)
            .await
            .expect("quiesce retry reports current state"),
        AuthorityState::Fenced
    );
    issue_to_file(&reopened, &successor_file, &successor_credential)
        .await
        .expect("successor issues only after fence");
    restarted_server.abort();
}

#[tokio::test]
async fn v2_migration_keeps_existing_authority_and_adds_quiescing_state() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("create schema");
    let credential = fixture.issue(&store).await;
    drop(store);
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .expect("fixture database");
    simulate_v2_schema(&pool).await;
    pool.close().await;

    let migrated = AuthorityStore::open(&fixture.state)
        .await
        .expect("v2 state migrates");
    let digest =
        crate::credential::CredentialDigest::from_bearer(&credential).expect("issued credential");
    let request = status_request("after-v2-migration");
    let validated = serde_json::from_slice::<BrokerRequest>(request.body())
        .expect("request parses")
        .validated()
        .expect("request validates");
    let response = migrated
        .execute(&digest, request.body(), &validated)
        .await
        .expect("issued authority remains usable");
    assert!(matches!(response.result, BrokerResult::Succeeded { .. }));
    migrated
        .quiesce(&fixture.identity)
        .await
        .expect("new state works");
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .expect("inspect schema");
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .expect("schema version");
    assert_eq!(version, 4);
    pool.close().await;
}

#[tokio::test]
async fn concurrent_hosts_migrate_v2_once_and_both_open() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("create schema");
    drop(store);
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .expect("fixture database");
    simulate_v2_schema(&pool).await;
    pool.close().await;

    let mut attempts = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let path = fixture.state.clone();
        attempts.spawn(async move { AuthorityStore::open(&path).await });
    }
    while let Some(result) = attempts.join_next().await {
        assert!(
            result.expect("task completes").is_ok(),
            "concurrent host opens"
        );
    }
}
