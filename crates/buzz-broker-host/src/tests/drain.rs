use super::*;

#[tokio::test]
#[ignore]
async fn child_acquires_publication_lock() {
    let Some(state_path) = std::env::var_os("BUZZ_EFFECT_LOCK_TEST_STATE") else {
        return;
    };
    let authority_file =
        std::env::var_os("BUZZ_EFFECT_LOCK_TEST_AUTHORITY").expect("child authority path");
    let ready_file = std::env::var_os("BUZZ_EFFECT_LOCK_TEST_READY").expect("child ready path");
    let marker_file = std::env::var_os("BUZZ_EFFECT_LOCK_TEST_MARKER").expect("child marker path");
    let store = AuthorityStore::open(Path::new(&state_path))
        .await
        .expect("child opens shared store");
    let identity =
        crate::issuer::read_identity(Path::new(&authority_file)).expect("child identity");
    std::fs::write(ready_file, b"ready").expect("child signals readiness");
    let _guard = store
        .publication_lock_for_identity(&identity)
        .await
        .expect("child acquires lock after parent releases it");
    std::fs::write(marker_file, b"acquired").expect("child marks acquisition");
}

#[tokio::test]
async fn publication_lock_excludes_another_host_process() {
    let fixture = Fixture::new();
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    fixture.issue(&store).await;
    let guard = store
        .publication_lock_for_identity(&fixture.identity)
        .await
        .expect("parent holds publication lock");
    let ready = fixture.directory.path().join("child-ready");
    let marker = fixture.directory.path().join("child-acquired");
    let mut child = std::process::Command::new(std::env::current_exe().expect("test executable"))
        .args([
            "--ignored",
            "--exact",
            "tests::drain::child_acquires_publication_lock",
        ])
        .env("BUZZ_EFFECT_LOCK_TEST_STATE", &fixture.state)
        .env("BUZZ_EFFECT_LOCK_TEST_AUTHORITY", &fixture.authority_file)
        .env("BUZZ_EFFECT_LOCK_TEST_READY", &ready)
        .env("BUZZ_EFFECT_LOCK_TEST_MARKER", &marker)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("child host starts");

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    while !ready.exists() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "child signals readiness"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !marker.exists(),
        "child cannot acquire lock while parent holds it"
    );
    assert!(child.try_wait().expect("child status").is_none());
    drop(guard);

    let status = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        tokio::task::spawn_blocking(move || child.wait()),
    )
    .await
    .expect("child exits after lock release")
    .expect("child join")
    .expect("child wait");
    assert!(status.success(), "child lock acquisition succeeded");
    assert!(marker.exists(), "child acquired lock after parent release");
}

#[tokio::test]
async fn concurrent_exact_retries_publish_one_event_before_fence() {
    use axum::{body::Bytes, routing::post, Router};
    use buzz_sdk::broker::{MessagePostArgs, PubkeyHex};
    use nostr::{Event, Keys};
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::sync::Notify;

    let posts = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let relay_posts = Arc::clone(&posts);
    let relay_entered = Arc::clone(&entered);
    let relay_release = Arc::clone(&release);
    let relay = Router::new().route(
        "/events",
        post(move |body: Bytes| {
            let posts = Arc::clone(&relay_posts);
            let entered = Arc::clone(&relay_entered);
            let release = Arc::clone(&relay_release);
            async move {
                let event: Event = serde_json::from_slice(&body).expect("signed event");
                if posts.fetch_add(1, Ordering::SeqCst) == 0 {
                    entered.notify_one();
                    release.notified().await;
                }
                axum::Json(serde_json::json!({
                    "accepted": true,
                    "event_id": event.id.to_hex()
                }))
            }
        }),
    );
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("relay listener");
    let relay_url = format!("ws://{}", relay_listener.local_addr().unwrap());
    let relay_task = tokio::spawn(async move { axum::serve(relay_listener, relay).await.unwrap() });

    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    fixture.identity.executor_agent_pubkey =
        PubkeyHex::parse(agent.public_key().to_hex()).expect("executor pubkey");
    fixture.identity.community_relay_url = relay_url;
    write_identity(&fixture.authority_file, &fixture.identity);
    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
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
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    let credential = fixture.issue(&store).await;
    let fence_store = store.clone();
    let fence_identity = fixture.identity.clone();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("host listener");
    let host_url = format!("http://{}", listener.local_addr().unwrap());
    let host_task = tokio::spawn(async move {
        axum::serve(
            listener,
            crate::http::app_with_effects(store, signer, RelayPublisher::new().unwrap()),
        )
        .await
        .unwrap();
    });
    let request = BrokerRequest::new(
        "one-effect-two-clients",
        ActionArgs::MessagePost(MessagePostArgs {
            channel_id: CHANNEL_ID.into(),
            content: "one publication".into(),
            mentions: Vec::new(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("post request");
    let first_url = host_url.clone();
    let first_credential = credential.clone();
    let first_body = request.body().to_vec();
    let first = tokio::spawn(async move { send(&first_url, &first_credential, &first_body).await });
    if tokio::time::timeout(std::time::Duration::from_secs(2), entered.notified())
        .await
        .is_err()
    {
        if first.is_finished() {
            let (_, body) = first.await.expect("first request completed early");
            panic!(
                "first request never reached relay: {}",
                String::from_utf8_lossy(&body)
            );
        }
        panic!("first request did not reach relay within two seconds");
    }
    let second_url = host_url.clone();
    let second_credential = credential.clone();
    let second_body = request.body().to_vec();
    let second =
        tokio::spawn(async move { send(&second_url, &second_credential, &second_body).await });
    let fence = tokio::spawn(async move { fence_store.fence(&fence_identity).await });
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert!(
        !fence.is_finished(),
        "fence waits for in-flight publication"
    );
    release.notify_one();
    let (_, first_response) = first.await.expect("first request completes");
    let (_, second_response) = second.await.expect("second request completes");
    let first: BrokerResponse = serde_json::from_slice(&first_response).expect("first response");
    let second: BrokerResponse = serde_json::from_slice(&second_response).expect("second response");
    assert!(matches!(first.result, BrokerResult::Succeeded { .. }));
    assert!(matches!(second.result, BrokerResult::Succeeded { .. }));
    assert_eq!(posts.load(Ordering::SeqCst), 1, "one relay submission");
    assert!(second.replayed);
    fence
        .await
        .expect("fence task completes")
        .expect("effect completes before fence");
    host_task.abort();
    relay_task.abort();
}

#[tokio::test]
async fn operator_drain_reconciles_crash_after_journal_before_publish() {
    use axum::{body::Bytes, routing::post, Router};
    use buzz_sdk::broker::{MessagePostArgs, PubkeyHex};
    use nostr::{Event, Keys};
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let posts = Arc::new(AtomicUsize::new(0));
    let relay_posts = Arc::clone(&posts);
    let relay = Router::new()
        .route("/query", post(|| async { axum::Json(Vec::<Event>::new()) }))
        .route(
            "/events",
            post(move |body: Bytes| {
                let posts = Arc::clone(&relay_posts);
                async move {
                    posts.fetch_add(1, Ordering::SeqCst);
                    let event: Event = serde_json::from_slice(&body).expect("signed event");
                    axum::Json(serde_json::json!({
                        "accepted": true,
                        "event_id": event.id.to_hex()
                    }))
                }
            }),
        );
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("relay listener");
    let relay_url = format!("ws://{}", relay_listener.local_addr().unwrap());
    let relay_task = tokio::spawn(async move { axum::serve(relay_listener, relay).await.unwrap() });

    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    fixture.identity.executor_agent_pubkey =
        PubkeyHex::parse(agent.public_key().to_hex()).expect("executor pubkey");
    fixture.identity.community_relay_url = relay_url;
    write_identity(&fixture.authority_file, &fixture.identity);
    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
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
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("store opens");
    let bearer = fixture.issue(&store).await;
    let credential =
        crate::credential::CredentialDigest::from_bearer(&bearer).expect("issued credential");
    let request = BrokerRequest::new(
        "crash-after-journal",
        ActionArgs::MessagePost(MessagePostArgs {
            channel_id: CHANNEL_ID.into(),
            content: "drain me".into(),
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
        .expect("effect journaled without publish");
    store
        .quiesce(&fixture.identity)
        .await
        .expect("close admission");
    let publisher = RelayPublisher::new().expect("relay publisher");
    assert!(
        crate::drain::drain_pending(&store, &fixture.identity, &signer, &publisher)
            .await
            .expect("pending effect drains")
    );
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    store
        .fence(&fixture.identity)
        .await
        .expect("drained generation fences");
    assert!(
        !crate::drain::drain_pending(&store, &fixture.identity, &signer, &publisher)
            .await
            .expect("already fenced has no pending effect")
    );
    relay_task.abort();
}

#[tokio::test]
async fn two_hosts_handoff_only_after_pending_effect_drains() {
    use axum::{body::Bytes, routing::post, Router};
    use buzz_sdk::broker::{MessagePostArgs, PubkeyHex};
    use nostr::{Event, Keys};
    use std::os::unix::fs::PermissionsExt as _;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let posts = Arc::new(AtomicUsize::new(0));
    let relay_posts = Arc::clone(&posts);
    let relay = Router::new()
        .route("/query", post(|| async { axum::Json(Vec::<Event>::new()) }))
        .route(
            "/events",
            post(move |body: Bytes| {
                let posts = Arc::clone(&relay_posts);
                async move {
                    let event: Event = serde_json::from_slice(&body).expect("signed event");
                    posts.fetch_add(1, Ordering::SeqCst);
                    axum::Json(serde_json::json!({
                        "accepted": true,
                        "event_id": event.id.to_hex()
                    }))
                }
            }),
        );
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("relay listener");
    let relay_url = format!("ws://{}", relay_listener.local_addr().unwrap());
    let relay_task = tokio::spawn(async move { axum::serve(relay_listener, relay).await.unwrap() });

    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    fixture.identity.executor_agent_pubkey =
        PubkeyHex::parse(agent.public_key().to_hex()).expect("executor pubkey");
    fixture.identity.community_relay_url = relay_url;
    write_identity(&fixture.authority_file, &fixture.identity);
    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
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

    let predecessor = AuthorityStore::open(&fixture.state)
        .await
        .expect("predecessor store");
    let old_bearer = fixture.issue(&predecessor).await;
    let successor = AuthorityStore::open(&fixture.state)
        .await
        .expect("successor store opens independently");
    let old_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("old host listener");
    let old_url = format!("http://{}", old_listener.local_addr().unwrap());
    let old_signer = HostSigner::load(&key_path, &tag_path).expect("old host signer");
    let old_host = tokio::spawn(async move {
        axum::serve(
            old_listener,
            crate::http::app_with_effects(
                predecessor.clone(),
                old_signer,
                RelayPublisher::new().unwrap(),
            ),
        )
        .await
        .unwrap();
    });
    let old_fence = AuthorityFence::new(
        old_url.clone(),
        old_bearer.clone(),
        fixture.identity.clone(),
    )
    .expect("old runtime fence");
    assert_eq!(old_fence.permit().await, Ok(()));

    let pending = BrokerRequest::new(
        "predecessor-pending",
        ActionArgs::MessagePost(MessagePostArgs {
            channel_id: CHANNEL_ID.into(),
            content: "journaled before crash".into(),
            mentions: Vec::new(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("pending request");
    let validated = serde_json::from_slice::<BrokerRequest>(pending.body())
        .expect("request parses")
        .validated()
        .expect("request validates");
    let old_digest = crate::credential::CredentialDigest::from_bearer(&old_bearer)
        .expect("old credential digest");
    let signer = HostSigner::load(&key_path, &tag_path).expect("operator signer");
    successor
        .prepare_message_post(&old_digest, pending.body(), &validated, &signer)
        .await
        .expect("old effect journaled before publication");
    successor
        .quiesce(&fixture.identity)
        .await
        .expect("old generation quiesces");
    assert_eq!(old_fence.permit().await, Err(AuthorityError::NotActive));
    let stale_request = BrokerRequest::new(
        "predecessor-after-quiesce",
        ActionArgs::MessagePost(MessagePostArgs {
            channel_id: CHANNEL_ID.into(),
            content: "must not publish".into(),
            mentions: Vec::new(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("stale request");
    let (_, stale_body) = send(&old_url, &old_bearer, stale_request.body()).await;
    let stale_response: BrokerResponse =
        serde_json::from_slice(&stale_body).expect("quiescing response");
    assert!(matches!(stale_response.result, BrokerResult::Failed { .. }));
    assert_eq!(posts.load(Ordering::SeqCst), 0);

    let mut next_identity = fixture.identity.clone();
    next_identity.generation = OTHER_GENERATION.into();
    next_identity.location.id = "successor".into();
    let next_authority_file = fixture.directory.path().join("successor.json");
    let next_credential_file = fixture.directory.path().join("successor.credential");
    write_identity(&next_authority_file, &next_identity);
    assert!(matches!(
        issue_to_file(&successor, &next_authority_file, &next_credential_file).await,
        Err(IssuerError::Store(StoreError::ScopeAlreadyActive))
    ));
    assert!(matches!(
        successor.fence(&fixture.identity).await,
        Err(StoreError::EffectsPending)
    ));
    assert_eq!(posts.load(Ordering::SeqCst), 0);

    let publisher = RelayPublisher::new().expect("relay publisher");
    assert!(
        crate::drain::drain_pending(&successor, &fixture.identity, &signer, &publisher)
            .await
            .expect("operator drains predecessor")
    );
    assert_eq!(posts.load(Ordering::SeqCst), 1);
    successor
        .fence(&fixture.identity)
        .await
        .expect("predecessor fences after drain");
    issue_to_file(&successor, &next_authority_file, &next_credential_file)
        .await
        .expect("successor issues after fence");
    let next_bearer =
        std::fs::read_to_string(&next_credential_file).expect("successor credential readable");
    let next_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("next host listener");
    let next_url = format!("http://{}", next_listener.local_addr().unwrap());
    let next_signer = HostSigner::load(&key_path, &tag_path).expect("next host signer");
    let next_host = tokio::spawn(async move {
        axum::serve(
            next_listener,
            crate::http::app_with_effects(successor, next_signer, RelayPublisher::new().unwrap()),
        )
        .await
        .unwrap();
    });
    let next_fence = AuthorityFence::new(next_url.clone(), next_bearer.clone(), next_identity)
        .expect("successor runtime fence");
    assert_eq!(next_fence.permit().await, Ok(()));
    let next_request = BrokerRequest::new(
        "successor-post",
        ActionArgs::MessagePost(MessagePostArgs {
            channel_id: CHANNEL_ID.into(),
            content: "one successor publication".into(),
            mentions: Vec::new(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("successor request");
    let (_, next_body) = send(&next_url, &next_bearer, next_request.body()).await;
    let next_response: BrokerResponse =
        serde_json::from_slice(&next_body).expect("successor response");
    assert!(matches!(
        next_response.result,
        BrokerResult::Succeeded { .. }
    ));
    assert_eq!(posts.load(Ordering::SeqCst), 2);

    let (_, stale_body) = send(&old_url, &old_bearer, next_request.body()).await;
    let stale_response: BrokerResponse = serde_json::from_slice(&stale_body).expect("old response");
    assert!(matches!(stale_response.result, BrokerResult::Failed { .. }));
    assert_eq!(posts.load(Ordering::SeqCst), 2, "old host cannot republish");
    old_host.abort();
    next_host.abort();
    relay_task.abort();
}
