use super::*;

use buzz_sdk::broker::{ActionOutcome, MessagePostArgs, PubkeyHex, StorageAddressArgs};
use nostr::Keys;
use std::os::unix::fs::PermissionsExt as _;

#[tokio::test]
async fn storage_address_uses_verified_owner_and_host_key_only_while_active() {
    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    fixture.identity.executor_agent_pubkey =
        PubkeyHex::parse(agent.public_key().to_hex()).expect("executor pubkey");
    write_identity(&fixture.authority_file, &fixture.identity);
    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("agent key fixture");
    std::fs::write(
        &tag_path,
        buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
            .expect("owner attestation"),
    )
    .expect("tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private signer file");
    }
    let signer = HostSigner::load(&key_path, &tag_path).expect("verified signer");
    let store = AuthorityStore::open(&fixture.state)
        .await
        .expect("state opens");
    let bearer = fixture.issue(&store).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("host listener");
    let host_url = format!("http://{}", listener.local_addr().unwrap());
    let host_store = store.clone();
    let host = tokio::spawn(async move {
        axum::serve(
            listener,
            crate::http::app_with_effects(host_store, signer, RelayPublisher::new().unwrap()),
        )
        .await
        .unwrap();
    });
    let request = BrokerRequest::new(
        "address-active",
        ActionArgs::StorageAddress(StorageAddressArgs {
            slug: "core".into(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("address request");
    let (_, body) = send(&host_url, &bearer, request.body()).await;
    let response: BrokerResponse = serde_json::from_slice(&body).expect("address response");
    let expected = buzz_core::engram::d_tag(
        &buzz_core::engram::conversation_key(agent.secret_key(), &owner.public_key()),
        "core",
    );
    assert!(matches!(
        response.result,
        BrokerResult::Succeeded {
            outcome: ActionOutcome::StorageAddress(ref address)
        } if address.author_pubkey == fixture.identity.executor_agent_pubkey
            && address.kind == buzz_core::kind::KIND_AGENT_ENGRAM
            && address.d_tag == expected
    ));
    let (_, replay_body) = send(&host_url, &bearer, request.body()).await;
    let replay: BrokerResponse = serde_json::from_slice(&replay_body).expect("replay response");
    assert!(replay.replayed, "same address request replays its receipt");
    let (status_only_url, status_only_host) = spawn_host(store.clone()).await;
    let (_, status_only_body) = send(&status_only_url, &bearer, request.body()).await;
    let status_only: BrokerResponse =
        serde_json::from_slice(&status_only_body).expect("status-only response");
    assert!(matches!(status_only.result, BrokerResult::Failed { .. }));
    assert!(
        !status_only.replayed,
        "status-only host cannot replay an address"
    );
    let denied_request = BrokerRequest::new(
        "address-denied-first",
        ActionArgs::StorageAddress(StorageAddressArgs {
            slug: "core".into(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("status-only request");
    let (_, denied_body) = send(&status_only_url, &bearer, denied_request.body()).await;
    let denied: BrokerResponse = serde_json::from_slice(&denied_body).expect("denial response");
    assert!(matches!(denied.result, BrokerResult::Failed { .. }));
    assert!(!denied.replayed);
    let (_, denied_retry_body) = send(&host_url, &bearer, denied_request.body()).await;
    let denied_retry: BrokerResponse =
        serde_json::from_slice(&denied_retry_body).expect("denial replay");
    assert!(matches!(denied_retry.result, BrokerResult::Failed { .. }));
    assert!(
        denied_retry.replayed,
        "denial follows the exact retry contract"
    );
    let changed_request = BrokerRequest::new(
        "address-denied-first",
        ActionArgs::StorageAddress(StorageAddressArgs {
            slug: "mem/other".into(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("changed request");
    let (_, conflict_body) = send(&host_url, &bearer, changed_request.body()).await;
    let conflict: BrokerResponse = serde_json::from_slice(&conflict_body).expect("conflict");
    assert!(matches!(
        conflict.result,
        BrokerResult::Failed { ref error } if error.code == BrokerErrorCode::RequestIdConflict
    ));

    let other_agent = Keys::generate();
    let other_key = fixture.directory.path().join("other-agent.key");
    let other_tag = fixture.directory.path().join("other-owner.auth-tag");
    std::fs::write(&other_key, other_agent.secret_key().to_secret_hex()).expect("other key");
    std::fs::write(
        &other_tag,
        buzz_sdk::nip_oa::compute_auth_tag(&owner, &other_agent.public_key(), "kind=9")
            .expect("other attestation"),
    )
    .expect("other tag");
    for path in [&other_key, &other_tag] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private other signer file");
    }
    let wrong_signer = HostSigner::load(&other_key, &other_tag).expect("other verified signer");
    let wrong_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("other host listener");
    let wrong_url = format!("http://{}", wrong_listener.local_addr().unwrap());
    let wrong_store = store.clone();
    let wrong_host = tokio::spawn(async move {
        axum::serve(
            wrong_listener,
            crate::http::app_with_effects(
                wrong_store,
                wrong_signer,
                RelayPublisher::new().unwrap(),
            ),
        )
        .await
        .unwrap();
    });
    let (_, wrong_body) = send(&wrong_url, &bearer, request.body()).await;
    let wrong: BrokerResponse = serde_json::from_slice(&wrong_body).expect("other host response");
    assert!(matches!(wrong.result, BrokerResult::Failed { .. }));
    assert!(!wrong.replayed, "wrong signer cannot replay an address");

    let rotated_owner = Keys::generate();
    let rotated_tag = fixture.directory.path().join("rotated-owner.auth-tag");
    std::fs::write(
        &rotated_tag,
        buzz_sdk::nip_oa::compute_auth_tag(&rotated_owner, &agent.public_key(), "kind=9")
            .expect("rotated owner attestation"),
    )
    .expect("rotated tag");
    std::fs::set_permissions(&rotated_tag, std::fs::Permissions::from_mode(0o600))
        .expect("private rotated tag");
    let rotated_signer = HostSigner::load(&key_path, &rotated_tag).expect("rotated signer");
    let rotated_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("rotated host listener");
    let rotated_url = format!("http://{}", rotated_listener.local_addr().unwrap());
    let rotated_store = store.clone();
    let rotated_host = tokio::spawn(async move {
        axum::serve(
            rotated_listener,
            crate::http::app_with_effects(
                rotated_store,
                rotated_signer,
                RelayPublisher::new().unwrap(),
            ),
        )
        .await
        .unwrap();
    });
    let (_, rotated_body) = send(&rotated_url, &bearer, request.body()).await;
    let rotated: BrokerResponse =
        serde_json::from_slice(&rotated_body).expect("rotated owner response");
    assert!(matches!(rotated.result, BrokerResult::Failed { .. }));
    assert!(
        !rotated.replayed,
        "old owner address cannot replay after rotation"
    );
    let fresh_rotated_request = BrokerRequest::new(
        "address-rotated-fresh",
        ActionArgs::StorageAddress(StorageAddressArgs {
            slug: "core".into(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("fresh rotated request");
    let (_, fresh_rotated_body) = send(&rotated_url, &bearer, fresh_rotated_request.body()).await;
    let fresh_rotated: BrokerResponse =
        serde_json::from_slice(&fresh_rotated_body).expect("fresh rotated owner response");
    assert!(matches!(
        fresh_rotated.result,
        BrokerResult::Failed { ref error } if error.code == BrokerErrorCode::Unauthorized
    ));
    let rotated_read = BrokerRequest::new(
        "read-rotated-fresh",
        ActionArgs::ChannelRead(ChannelReadArgs {
            channel_id: CHANNEL_ID.into(),
            ..ChannelReadArgs::default()
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("rotated read request");
    let (_, rotated_read_body) = send(&rotated_url, &bearer, rotated_read.body()).await;
    let rotated_read_response: BrokerResponse =
        serde_json::from_slice(&rotated_read_body).expect("rotated read response");
    assert!(matches!(
        rotated_read_response.result,
        BrokerResult::Failed { ref error } if error.code == BrokerErrorCode::Unauthorized
    ));
    let rotated_post = BrokerRequest::new(
        "post-rotated-fresh",
        ActionArgs::MessagePost(MessagePostArgs {
            channel_id: CHANNEL_ID.into(),
            content: "must not publish".into(),
            mentions: Vec::new(),
        }),
    )
    .and_then(BrokerRequest::prepare)
    .expect("rotated post request");
    let (_, rotated_post_body) = send(&rotated_url, &bearer, rotated_post.body()).await;
    let rotated_post_response: BrokerResponse =
        serde_json::from_slice(&rotated_post_body).expect("rotated post response");
    assert!(matches!(
        rotated_post_response.result,
        BrokerResult::Failed { ref error } if error.code == BrokerErrorCode::Unauthorized
    ));

    store
        .quiesce(&fixture.identity)
        .await
        .expect("authority quiesces");
    let (_, after_body) = send(&host_url, &bearer, request.body()).await;
    let after: BrokerResponse = serde_json::from_slice(&after_body).expect("quiescing response");
    assert!(matches!(after.result, BrokerResult::Failed { .. }));
    assert!(!after.replayed, "active address receipt was invalidated");
    host.abort();
    status_only_host.abort();
    wrong_host.abort();
    rotated_host.abort();
}
