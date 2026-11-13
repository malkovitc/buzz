use super::*;

#[test]
fn host_signer_requires_verified_owner_attestation_for_its_exact_agent_key() {
    use nostr::Keys;
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::tempdir().expect("private fixture directory");
    make_private_directory(directory.path());
    let key_path = directory.path().join("agent.key");
    let tag_path = directory.path().join("owner.auth-tag");
    let agent = Keys::generate();
    let owner = Keys::generate();
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
        .expect("private key fixture");
    let tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
        .expect("valid owner attestation");
    std::fs::write(&tag_path, &tag).expect("tag fixture");
    std::fs::set_permissions(&tag_path, std::fs::Permissions::from_mode(0o600))
        .expect("private tag fixture");

    let signer = HostSigner::load(&key_path, &tag_path).expect("verified signer loads");
    assert!(signer
        .auth_tag_json()
        .contains(&owner.public_key().to_hex()));

    let other_agent = Keys::generate();
    let forged = buzz_sdk::nip_oa::compute_auth_tag(&owner, &other_agent.public_key(), "kind=9")
        .expect("other agent tag");
    std::fs::write(&tag_path, forged).expect("replace tag fixture");
    assert!(matches!(
        HostSigner::load(&key_path, &tag_path),
        Err(SigningError::InvalidAttestation)
    ));
}

#[test]
fn host_signer_rejects_private_files_in_writable_directory() {
    use nostr::Keys;
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::tempdir().expect("fixture directory");
    std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o777))
        .expect("writable directory fixture");
    let key_path = directory.path().join("agent.key");
    let tag_path = directory.path().join("owner.auth-tag");
    let agent = Keys::generate();
    let owner = Keys::generate();
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    let tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
        .expect("owner attestation");
    std::fs::write(&tag_path, tag).expect("tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private fixture file");
    }
    assert!(matches!(
        HostSigner::load(&key_path, &tag_path),
        Err(SigningError::InsecureFile)
    ));
}

#[test]
fn host_signer_only_signs_message_for_bound_executor_identity() {
    use buzz_sdk::broker::{MessagePostArgs, MessageReplyArgs};
    use nostr::{EventBuilder, Keys, Kind, Tag};
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::tempdir().expect("private fixture directory");
    make_private_directory(directory.path());
    let key_path = directory.path().join("agent.key");
    let tag_path = directory.path().join("owner.auth-tag");
    let agent = Keys::generate();
    let owner = Keys::generate();
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    std::fs::set_permissions(&key_path, std::fs::Permissions::from_mode(0o600))
        .expect("private key fixture");
    let tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
        .expect("valid owner attestation");
    std::fs::write(&tag_path, tag).expect("tag fixture");
    std::fs::set_permissions(&tag_path, std::fs::Permissions::from_mode(0o600))
        .expect("private tag fixture");
    let signer = HostSigner::load(&key_path, &tag_path).expect("verified signer loads");

    let mut identity = identity(GENERATION);
    identity.executor_agent_pubkey =
        buzz_sdk::broker::PubkeyHex::parse(agent.public_key().to_hex())
            .expect("valid executor pubkey");
    let args = MessagePostArgs {
        channel_id: CHANNEL_ID.to_owned(),
        content: "hello from host".into(),
        mentions: Vec::new(),
    };
    let event = signer
        .sign_message(&identity, &args)
        .expect("matching executor can sign");
    assert_eq!(event.pubkey, agent.public_key());
    assert_eq!(event.kind.as_u16(), 9);
    assert_eq!(event.content, args.content);
    assert_eq!(
        event
            .tags
            .iter()
            .filter(|tag| tag.as_slice().first().map(String::as_str) == Some("auth"))
            .count(),
        1
    );
    let parent = EventBuilder::new(Kind::Custom(9), "parent")
        .tags([Tag::parse(["h", CHANNEL_ID]).unwrap()])
        .sign_with_keys(&owner)
        .unwrap();
    let reply_args = MessageReplyArgs {
        channel_id: CHANNEL_ID.into(),
        reply_to_event_id: parent.id.to_hex(),
        content: "thread reply".into(),
        mentions: Vec::new(),
    };
    let reply = signer
        .sign_reply(
            &identity,
            &reply_args,
            &buzz_sdk::ThreadRef {
                root_event_id: parent.id,
                parent_event_id: parent.id,
            },
        )
        .expect("matching executor signs reply");
    assert!(reply.verify().is_ok());
    assert!(reply.tags.iter().any(|tag| {
        let parts = tag.as_slice();
        parts.first().map(String::as_str) == Some("e")
            && parts.get(1).map(String::as_str) == Some(parent.id.to_hex().as_str())
            && parts.get(3).map(String::as_str) == Some("reply")
    }));

    identity.executor_agent_pubkey =
        buzz_sdk::broker::PubkeyHex::parse(Keys::generate().public_key().to_hex())
            .expect("other executor pubkey");
    assert!(matches!(
        signer.sign_message(&identity, &args),
        Err(SigningError::ExecutorMismatch)
    ));
}

#[tokio::test]
async fn message_effect_admission_survives_restart_and_blocks_fence_until_terminal() {
    use buzz_sdk::broker::{MessagePostArgs, PubkeyHex};
    use nostr::Keys;
    use std::os::unix::fs::PermissionsExt as _;

    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    fixture.identity.executor_agent_pubkey =
        PubkeyHex::parse(agent.public_key().to_hex()).expect("valid executor");
    write_identity(&fixture.authority_file, &fixture.identity);
    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    let tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
        .expect("owner attestation");
    std::fs::write(&tag_path, tag).expect("tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private fixture file");
    }
    let signer = HostSigner::load(&key_path, &tag_path).expect("host signer");
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("state opens");
    let bearer = fixture.issue(&store).await;
    let credential =
        crate::credential::CredentialDigest::from_bearer(&bearer).expect("issued bearer");

    let args = MessagePostArgs {
        channel_id: CHANNEL_ID.to_owned(),
        content: "exact effect".into(),
        mentions: Vec::new(),
    };
    let request = BrokerRequest::new("effect-1", ActionArgs::MessagePost(args.clone()))
        .and_then(BrokerRequest::prepare)
        .expect("valid request");
    let validated = serde_json::from_slice::<BrokerRequest>(request.body())
        .expect("request parses")
        .validated()
        .expect("request validates");
    let first = store
        .prepare_message_post(&credential, request.body(), &validated, &signer)
        .await
        .expect("active generation admits effect");
    let event_id = match first {
        EffectAdmission::Pending(event, _, false) => event.id.to_hex(),
        _ => panic!("first admission must be pending"),
    };
    assert!(matches!(
        store.fence(&fixture.identity).await,
        Err(StoreError::EffectsPending)
    ));
    let second = BrokerRequest::new("effect-2", ActionArgs::MessagePost(args.clone()))
        .and_then(BrokerRequest::prepare)
        .expect("second request");
    let second_validated = serde_json::from_slice::<BrokerRequest>(second.body())
        .expect("second request parses")
        .validated()
        .expect("second request validates");
    assert!(matches!(
        store
            .prepare_message_post(&credential, second.body(), &second_validated, &signer)
            .await,
        Err(StoreError::EffectsPending)
    ));
    let status_same_id = status_request("effect-1");
    let status_validated = serde_json::from_slice::<BrokerRequest>(status_same_id.body())
        .expect("status request parses")
        .validated()
        .expect("status request validates");
    let status_response = store
        .execute(&credential, status_same_id.body(), &status_validated)
        .await
        .expect("status receives broker verdict");
    assert!(matches!(
        status_response.result,
        BrokerResult::Failed { ref error }
            if error.code == BrokerErrorCode::RequestIdConflict
    ));

    drop(store);
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .unwrap();
    sqlx::query("ALTER TABLE broker_effects RENAME TO broker_effects_v6")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query(
        "CREATE TABLE broker_effects (
            credential_hash BLOB NOT NULL,
            request_id TEXT NOT NULL,
            request_digest BLOB NOT NULL CHECK(length(request_digest) = 32),
            action TEXT NOT NULL CHECK(action = 'message.post'),
            event_json TEXT NOT NULL,
            response_json TEXT,
            PRIMARY KEY (credential_hash, request_id),
            FOREIGN KEY (credential_hash) REFERENCES broker_authorities(credential_hash)
        ) STRICT",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO broker_effects SELECT credential_hash, request_id, request_digest,
             action, event_json, response_json FROM broker_effects_v6",
    )
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query("DROP TABLE broker_effects_v6")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("PRAGMA user_version = 5")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    let reopened = AuthorityStore::open(&fixture.state)
        .await
        .expect("v5 pending journal survives upgrade");
    let duplicate = reopened
        .prepare_message_post(&credential, request.body(), &validated, &signer)
        .await
        .expect("exact retry loads the same effect");
    assert!(
        matches!(duplicate, EffectAdmission::Pending(event, _, true) if event.id.to_hex() == event_id)
    );
    reopened
        .quiesce(&fixture.identity)
        .await
        .expect("quiesce closes new admission while pending effect remains");
    assert!(matches!(
        reopened
            .prepare_message_post(&credential, request.body(), &validated, &signer)
            .await,
        Ok(EffectAdmission::Pending(event, _, true)) if event.id.to_hex() == event_id
    ));

    let other_agent = Keys::generate();
    std::fs::write(&key_path, other_agent.secret_key().to_secret_hex())
        .expect("replacement key fixture");
    let other_tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &other_agent.public_key(), "kind=9")
        .expect("replacement owner attestation");
    std::fs::write(&tag_path, other_tag).expect("replacement tag fixture");
    let wrong_signer = HostSigner::load(&key_path, &tag_path).expect("verified replacement signer");
    assert!(matches!(
        reopened
            .prepare_message_post(&credential, request.body(), &validated, &wrong_signer)
            .await,
        Err(StoreError::InvalidIdentity)
    ));

    let changed = BrokerRequest::new(
        "effect-1",
        ActionArgs::MessagePost(MessagePostArgs {
            content: "changed bytes".into(),
            ..args
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("conflicting request");
    let changed_validated = serde_json::from_slice::<BrokerRequest>(changed.body())
        .expect("changed request parses")
        .validated()
        .expect("changed request validates");
    assert!(matches!(
        reopened
            .prepare_message_post(&credential, changed.body(), &changed_validated, &signer)
            .await,
        Ok(EffectAdmission::Conflict)
    ));

    let outcome = reopened
        .complete_message_effect(&credential, "effect-1", &event_id)
        .await
        .expect("publisher acknowledgement commits outcome");
    assert_eq!(
        serde_json::to_value(&outcome).expect("response JSON")["outcome"]["eventId"],
        event_id
    );
    assert!(matches!(
        reopened
            .prepare_message_post(&credential, second.body(), &second_validated, &signer)
            .await,
        Err(StoreError::AuthorityFenced)
    ));
    reopened
        .fence(&fixture.identity)
        .await
        .expect("fence can commit after effect is terminal");
    assert!(matches!(
        reopened
            .prepare_message_post(&credential, request.body(), &validated, &signer)
            .await,
        Ok(EffectAdmission::Completed(_))
    ));
}

#[tokio::test]
async fn relay_publisher_sends_exact_signed_event_with_fresh_host_auth() {
    use axum::{body::Bytes, http::HeaderMap, routing::post, Router};
    use base64::Engine as _;
    use nostr::{Event, Keys};
    use std::os::unix::fs::PermissionsExt as _;

    let directory = tempfile::tempdir().expect("private fixture directory");
    make_private_directory(directory.path());
    let key_path = directory.path().join("agent.key");
    let tag_path = directory.path().join("owner.auth-tag");
    let agent = Keys::generate();
    let owner = Keys::generate();
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    let tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
        .expect("owner attestation");
    std::fs::write(&tag_path, &tag).expect("tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private fixture file");
    }
    let signer = HostSigner::load(&key_path, &tag_path).expect("host signer");
    let mut identity = identity(GENERATION);
    identity.executor_agent_pubkey =
        buzz_sdk::broker::PubkeyHex::parse(agent.public_key().to_hex()).expect("executor pubkey");
    let event = signer
        .sign_message(
            &identity,
            &buzz_sdk::broker::MessagePostArgs {
                channel_id: CHANNEL_ID.to_owned(),
                content: "publish once".into(),
                mentions: Vec::new(),
            },
        )
        .expect("message signed");
    let expected_id = event.id.to_hex();
    let expected_tag = tag.clone();
    let app = Router::new().route(
        "/events",
        post(move |headers: HeaderMap, body: Bytes| {
            let expected_id = expected_id.clone();
            let expected_tag = expected_tag.clone();
            async move {
                let actual: Event = serde_json::from_slice(&body).expect("exact event JSON");
                assert_eq!(actual.id.to_hex(), expected_id);
                assert_eq!(
                    headers.get("x-auth-tag").unwrap().to_str().unwrap(),
                    expected_tag
                );
                let auth = headers
                    .get("authorization")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .strip_prefix("Nostr ")
                    .expect("NIP-98 header");
                let auth_bytes = base64::engine::general_purpose::STANDARD
                    .decode(auth)
                    .expect("base64 auth");
                let auth_event: Event = serde_json::from_slice(&auth_bytes).expect("auth event");
                assert_eq!(auth_event.pubkey, actual.pubkey);
                assert_eq!(auth_event.kind.as_u16(), 27235);
                axum::Json(serde_json::json!({
                    "accepted": true,
                    "event_id": expected_id,
                    "message": ""
                }))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test relay binds");
    let address = listener.local_addr().expect("test relay address");
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let publisher = RelayPublisher::new().expect("publisher client");
    publisher
        .publish(&signer, &format!("ws://{address}/community/?x=1"), &event)
        .await
        .expect("relay accepted the exact event");
    server.abort();
}

#[tokio::test]
async fn relay_publisher_never_forwards_signed_headers_on_redirect() {
    use axum::{response::Redirect, routing::post, Router};
    use nostr::Keys;
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let directory = tempfile::tempdir().expect("private fixture directory");
    make_private_directory(directory.path());
    let key_path = directory.path().join("agent.key");
    let tag_path = directory.path().join("owner.auth-tag");
    let agent = Keys::generate();
    let owner = Keys::generate();
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    std::fs::write(
        &tag_path,
        buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
            .expect("owner attestation"),
    )
    .expect("tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private fixture file");
    }
    let signer = HostSigner::load(&key_path, &tag_path).expect("host signer");
    let mut identity = identity(GENERATION);
    identity.executor_agent_pubkey =
        buzz_sdk::broker::PubkeyHex::parse(agent.public_key().to_hex()).expect("executor pubkey");
    let event = signer
        .sign_message(
            &identity,
            &buzz_sdk::broker::MessagePostArgs {
                channel_id: CHANNEL_ID.into(),
                content: "redirect probe".into(),
                mentions: Vec::new(),
            },
        )
        .expect("signed event");

    let forwarded = Arc::new(AtomicBool::new(false));
    let captured = forwarded.clone();
    let target = Router::new().route(
        "/steal",
        post(move || {
            let captured = captured.clone();
            async move {
                captured.store(true, Ordering::SeqCst);
                axum::Json(serde_json::json!({"accepted": true, "event_id": "wrong"}))
            }
        }),
    );
    let target_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("target listener");
    let target_url = format!("http://{}/steal", target_listener.local_addr().unwrap());
    let target_task =
        tokio::spawn(async move { axum::serve(target_listener, target).await.unwrap() });
    let relay = Router::new()
        .route(
            "/events",
            post(move || async move { Redirect::temporary(&target_url) }),
        )
        .route(
            "/query",
            post(|| async { axum::Json(Vec::<nostr::Event>::new()) }),
        );
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("relay listener");
    let relay_url = format!("ws://{}", relay_listener.local_addr().unwrap());
    let relay_task = tokio::spawn(async move { axum::serve(relay_listener, relay).await.unwrap() });

    assert!(publisher_is_indeterminate(&signer, &relay_url, &event).await);
    assert!(!forwarded.load(Ordering::SeqCst));
    relay_task.abort();
    target_task.abort();
}

async fn publisher_is_indeterminate(
    signer: &HostSigner,
    relay_url: &str,
    event: &nostr::Event,
) -> bool {
    matches!(
        RelayPublisher::new()
            .expect("publisher client")
            .publish(signer, relay_url, event)
            .await,
        Err(crate::publisher::PublishError::Indeterminate)
    )
}

#[tokio::test]
async fn broker_http_posts_once_and_replays_committed_effect_after_fence() {
    use axum::{body::Bytes, routing::post, Router};
    use buzz_sdk::broker::{MessagePostArgs, PubkeyHex};
    use nostr::{Event, Keys};
    use std::os::unix::fs::PermissionsExt as _;

    let relay_app = Router::new().route(
        "/events",
        post(|body: Bytes| async move {
            let event: Event = serde_json::from_slice(&body).expect("signed relay event");
            axum::Json(serde_json::json!({
                "accepted": true,
                "event_id": event.id.to_hex(),
                "message": ""
            }))
        }),
    );
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("relay listener");
    let relay_address = relay_listener.local_addr().expect("relay address");
    let relay_task = tokio::spawn(async move {
        axum::serve(relay_listener, relay_app).await.unwrap();
    });

    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    fixture.identity.executor_agent_pubkey =
        PubkeyHex::parse(agent.public_key().to_hex()).expect("executor pubkey");
    fixture.identity.community_relay_url = format!("ws://{relay_address}");
    write_identity(&fixture.authority_file, &fixture.identity);
    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    let tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
        .expect("owner attestation");
    std::fs::write(&tag_path, tag).expect("tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private fixture file");
    }
    let signer = HostSigner::load(&key_path, &tag_path).expect("host signer");
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("state opens");
    let bearer = fixture.issue(&store).await;
    let host_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("host listener");
    let host_address = host_listener.local_addr().expect("host address");
    let host_task = tokio::spawn(async move {
        axum::serve(
            host_listener,
            crate::http::app_with_effects(store, signer, RelayPublisher::new().unwrap()),
        )
        .await
        .unwrap();
    });
    let host_url = format!("http://{host_address}");
    let request = BrokerRequest::new(
        "posted-once",
        ActionArgs::MessagePost(MessagePostArgs {
            channel_id: CHANNEL_ID.into(),
            content: "hello from broker".into(),
            mentions: Vec::new(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("post request");
    let (status, first_body) = send(&host_url, &bearer, request.body()).await;
    assert_eq!(status, reqwest::StatusCode::OK);
    let first: BrokerResponse = serde_json::from_slice(&first_body).expect("broker response");
    assert!(matches!(first.result, BrokerResult::Succeeded { .. }));
    assert!(!first.replayed);

    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("state reopens");
    store
        .fence(&fixture.identity)
        .await
        .expect("fence after commit");
    let (_, replay_body) = send(&host_url, &bearer, request.body()).await;
    let replay: BrokerResponse = serde_json::from_slice(&replay_body).expect("replay response");
    assert!(replay.replayed);
    assert_eq!(
        serde_json::to_value(&first).unwrap()["outcome"]["eventId"],
        serde_json::to_value(&replay).unwrap()["outcome"]["eventId"]
    );
    host_task.abort();
    relay_task.abort();
}

#[tokio::test]
async fn unresolved_post_blocks_new_request_ids_and_fence() {
    use buzz_sdk::broker::{MessagePostArgs, PubkeyHex};
    use nostr::Keys;
    use std::os::unix::fs::PermissionsExt as _;

    let unavailable = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("reserve unavailable relay address");
    let relay_address = unavailable.local_addr().expect("relay address");
    drop(unavailable);
    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    fixture.identity.executor_agent_pubkey =
        PubkeyHex::parse(agent.public_key().to_hex()).expect("executor pubkey");
    fixture.identity.community_relay_url = format!("ws://{relay_address}");
    write_identity(&fixture.authority_file, &fixture.identity);
    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    let tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
        .expect("owner attestation");
    std::fs::write(&tag_path, tag).expect("tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private fixture file");
    }
    let signer = HostSigner::load(&key_path, &tag_path).expect("host signer");
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("state opens");
    let bearer = fixture.issue(&store).await;
    let host_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("host listener");
    let host_address = host_listener.local_addr().expect("host address");
    let host_task = tokio::spawn({
        let store = store.clone();
        async move {
            axum::serve(
                host_listener,
                crate::http::app_with_effects(store, signer, RelayPublisher::new().unwrap()),
            )
            .await
            .unwrap();
        }
    });
    let host_url = format!("http://{host_address}");
    let args = MessagePostArgs {
        channel_id: CHANNEL_ID.into(),
        content: "pending effect".into(),
        mentions: Vec::new(),
    };
    let first = BrokerRequest::new("first", ActionArgs::MessagePost(args.clone()))
        .and_then(BrokerRequest::prepare)
        .expect("first request");
    let (_, first_body) = send(&host_url, &bearer, first.body()).await;
    let first_response: BrokerResponse = serde_json::from_slice(&first_body).unwrap();
    assert!(matches!(
        first_response.result,
        BrokerResult::Indeterminate { .. }
    ));
    assert!(matches!(
        store.fence(&fixture.identity).await,
        Err(StoreError::EffectsPending)
    ));

    let second = BrokerRequest::new("second", ActionArgs::MessagePost(args))
        .and_then(BrokerRequest::prepare)
        .expect("second request");
    let (_, second_body) = send(&host_url, &bearer, second.body()).await;
    let second_response: BrokerResponse = serde_json::from_slice(&second_body).unwrap();
    assert!(matches!(
        second_response.result,
        BrokerResult::Failed { ref error }
            if error.code == BrokerErrorCode::ActionFailed
    ));
    host_task.abort();
}

#[tokio::test]
async fn relay_publisher_reconciles_lost_ack_with_exact_event_query() {
    use axum::{body::Bytes, http::StatusCode, routing::post, Router};
    use nostr::{Event, Keys};
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    let directory = tempfile::tempdir().expect("private fixture directory");
    make_private_directory(directory.path());
    let key_path = directory.path().join("agent.key");
    let tag_path = directory.path().join("owner.auth-tag");
    let agent = Keys::generate();
    let owner = Keys::generate();
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    let tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
        .expect("owner attestation");
    std::fs::write(&tag_path, tag).expect("tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private fixture file");
    }
    let signer = HostSigner::load(&key_path, &tag_path).expect("host signer");
    let mut identity = identity(GENERATION);
    identity.executor_agent_pubkey =
        buzz_sdk::broker::PubkeyHex::parse(agent.public_key().to_hex()).expect("executor pubkey");
    let event = signer
        .sign_message(
            &identity,
            &buzz_sdk::broker::MessagePostArgs {
                channel_id: CHANNEL_ID.into(),
                content: "lost acknowledgement".into(),
                mentions: Vec::new(),
            },
        )
        .expect("message signed");
    let saved = Arc::new(Mutex::new(None::<Event>));
    let post_count = Arc::new(AtomicUsize::new(0));
    let post_saved = Arc::clone(&saved);
    let post_counter = Arc::clone(&post_count);
    let query_saved = Arc::clone(&saved);
    let relay = Router::new()
        .route(
            "/events",
            post(move |body: Bytes| {
                let saved = Arc::clone(&post_saved);
                let post_counter = Arc::clone(&post_counter);
                async move {
                    post_counter.fetch_add(1, Ordering::SeqCst);
                    *saved.lock().expect("saved event lock") =
                        Some(serde_json::from_slice(&body).expect("event JSON"));
                    StatusCode::BAD_GATEWAY
                }
            }),
        )
        .route(
            "/query",
            post(move |body: Bytes| {
                let saved = Arc::clone(&query_saved);
                async move {
                    let filters: serde_json::Value =
                        serde_json::from_slice(&body).expect("filter JSON");
                    let event = saved.lock().expect("saved event lock").clone().unwrap();
                    assert_eq!(filters[0]["ids"][0], event.id.to_hex());
                    axum::Json(serde_json::json!([event]))
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("relay listener");
    let address = listener.local_addr().expect("relay address");
    let server = tokio::spawn(async move { axum::serve(listener, relay).await.unwrap() });

    let publisher = RelayPublisher::new().expect("publisher client");
    publisher
        .publish(&signer, &format!("ws://{address}"), &event)
        .await
        .expect("exact event found after lost acknowledgement");
    publisher
        .publish_pending(&signer, &format!("ws://{address}"), &event)
        .await
        .expect("pending retry queries the stored event first");
    assert_eq!(post_count.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn v1_migration_refuses_corrupt_authority_without_upgrading_schema() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("create current schema");
    drop(store);
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .expect("fixture database");
    sqlx::query("DROP TABLE broker_effects")
        .execute(&pool)
        .await
        .expect("simulate v1 schema");
    simulate_v2_schema(&pool).await;
    sqlx::query("PRAGMA user_version = 1")
        .execute(&pool)
        .await
        .expect("v1 schema version");
    sqlx::query(
        "INSERT INTO broker_authorities
         (credential_hash, authority_json, authority_scope, generation, state)
         VALUES (?, '{}', ?, 'corrupt-generation', 'active')",
    )
    .bind(vec![0u8; 32])
    .bind(vec![0u8; 32])
    .execute(&pool)
    .await
    .expect("corrupt legacy authority fixture");
    pool.close().await;

    assert!(matches!(
        AuthorityStore::open(&fixture.state).await,
        Err(StoreError::Integrity)
    ));
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .expect("inspect rejected migration");
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .expect("schema version");
    assert_eq!(version, 1);
    pool.close().await;
}

#[tokio::test]
async fn v1_migration_keeps_issued_authority_and_adds_effect_journal() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("create current schema");
    let bearer = fixture.issue(&store).await;
    drop(store);
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .expect("fixture database");
    sqlx::query("DROP TABLE broker_effects")
        .execute(&pool)
        .await
        .expect("simulate v1 schema");
    simulate_v2_schema(&pool).await;
    sqlx::query("PRAGMA user_version = 1")
        .execute(&pool)
        .await
        .expect("v1 schema version");
    pool.close().await;

    let migrated = AuthorityStore::open(&fixture.state)
        .await
        .expect("valid v1 state migrates");
    let credential =
        crate::credential::CredentialDigest::from_bearer(&bearer).expect("issued bearer");
    let request = status_request("after-v1-migration");
    let validated = serde_json::from_slice::<BrokerRequest>(request.body())
        .expect("status request parses")
        .validated()
        .expect("status request validates");
    let response = migrated
        .execute(&credential, request.body(), &validated)
        .await
        .expect("issued authority remains usable");
    assert!(matches!(response.result, BrokerResult::Succeeded { .. }));
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .expect("inspect migrated schema");
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(&pool)
        .await
        .expect("schema version");
    assert_eq!(version, 6);
    let journal_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'broker_effects')",
    )
    .fetch_one(&pool)
    .await
    .expect("journal table");
    assert!(journal_exists);
    pool.close().await;
}

#[tokio::test]
async fn startup_rejects_effect_event_signed_by_another_executor() {
    use buzz_sdk::broker::{MessagePostArgs, PubkeyHex};
    use nostr::Keys;
    use std::os::unix::fs::PermissionsExt as _;

    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    fixture.identity.executor_agent_pubkey =
        PubkeyHex::parse(agent.public_key().to_hex()).expect("executor pubkey");
    write_identity(&fixture.authority_file, &fixture.identity);
    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    let tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
        .expect("owner attestation");
    std::fs::write(&tag_path, tag).expect("tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private fixture file");
    }
    let signer = HostSigner::load(&key_path, &tag_path).expect("host signer");
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("state opens");
    let bearer = fixture.issue(&store).await;
    let credential =
        crate::credential::CredentialDigest::from_bearer(&bearer).expect("issued bearer");
    let request = BrokerRequest::new(
        "tamper-target",
        ActionArgs::MessagePost(MessagePostArgs {
            channel_id: CHANNEL_ID.into(),
            content: "original".into(),
            mentions: Vec::new(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("post request");
    let validated = serde_json::from_slice::<BrokerRequest>(request.body())
        .expect("request parses")
        .validated()
        .expect("request validates");
    store
        .prepare_message_post(&credential, request.body(), &validated, &signer)
        .await
        .expect("journal effect");

    let wrong_event = buzz_sdk::build_message(
        uuid::Uuid::parse_str(CHANNEL_ID).expect("channel UUID"),
        "different executor",
        None,
        &[],
        false,
        &[],
    )
    .expect("kind 9 builder")
    .sign_with_keys(&Keys::generate())
    .expect("valid but wrong event");
    let pool = sqlx::SqlitePool::connect_with(
        sqlx::sqlite::SqliteConnectOptions::new().filename(&fixture.state),
    )
    .await
    .expect("fixture database");
    sqlx::query("UPDATE broker_effects SET event_json = ? WHERE request_id = 'tamper-target'")
        .bind(serde_json::to_string(&wrong_event).expect("event JSON"))
        .execute(&pool)
        .await
        .expect("tamper fixture");
    pool.close().await;
    assert!(matches!(
        AuthorityStore::open(&fixture.state).await,
        Err(StoreError::Integrity)
    ));
}
