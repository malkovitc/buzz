use super::*;

use buzz_sdk::broker::{PubkeyHex, RuntimeLocationKind};
use nostr::Keys;
use std::os::unix::fs::PermissionsExt as _;

#[tokio::test]
async fn huddle_quiesce_waits_for_durable_relay_close_receipt() {
    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    let agent_pubkey = PubkeyHex::parse(agent.public_key().to_hex()).unwrap();
    fixture.identity.logical_agent_pubkey = agent_pubkey.clone();
    fixture.identity.executor_agent_pubkey = agent_pubkey;
    fixture.identity.location.kind = RuntimeLocationKind::Local;
    write_identity(&fixture.authority_file, &fixture.identity);

    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).unwrap();
    std::fs::write(
        &tag_path,
        buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9").unwrap(),
    )
    .unwrap();
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let signer = HostSigner::load(&key_path, &tag_path).unwrap();
    let store = AuthorityStore::open(&fixture.state).await.unwrap();
    let bearer = fixture.issue(&store).await;
    let credential = crate::credential::CredentialDigest::from_bearer(&bearer).unwrap();
    let session = store
        .open_audio_session(&credential, &fixture.identity, &signer)
        .await
        .unwrap();
    assert!(matches!(
        store
            .open_audio_session(&credential, &fixture.identity, &signer)
            .await,
        Err(StoreError::EffectsPending)
    ));

    let quiesce_store = store.clone();
    let identity = fixture.identity.clone();
    let quiesce = tokio::spawn(async move { quiesce_store.quiesce(&identity).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !quiesce.is_finished(),
        "quiesce returned before relay close"
    );
    assert!(matches!(
        store.fence(&fixture.identity).await,
        Err(StoreError::EffectsPending)
    ));
    let reopened = AuthorityStore::open(&fixture.state).await.unwrap();
    assert!(matches!(
        reopened.fence(&fixture.identity).await,
        Err(StoreError::EffectsPending)
    ));
    reopened
        .close_audio_session(&credential, &session)
        .await
        .unwrap();
    assert_eq!(
        quiesce.await.unwrap().unwrap(),
        buzz_sdk::broker::AuthorityState::Quiescing
    );
    assert!(matches!(
        reopened
            .open_audio_session(&credential, &fixture.identity, &signer)
            .await,
        Err(StoreError::OwnerMismatch)
    ));
    reopened.fence(&fixture.identity).await.unwrap();
}

#[tokio::test]
async fn huddle_audio_reopen_rejects_malformed_session_receipt() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state).await.unwrap();
    let bearer = fixture.issue(&store).await;
    let credential = crate::credential::CredentialDigest::from_bearer(&bearer).unwrap();
    drop(store);
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO broker_audio_sessions (credential_hash, session_id, state)
         VALUES (?, ?, 'closed')",
    )
    .bind(credential.as_bytes())
    .bind("zzzzzzzz-zzzz-zzzz-zzzz-zzzzzzzzzzzz")
    .execute(&pool)
    .await
    .unwrap();
    pool.close().await;
    assert!(matches!(
        AuthorityStore::open(&fixture.state).await,
        Err(StoreError::Integrity)
    ));
}
