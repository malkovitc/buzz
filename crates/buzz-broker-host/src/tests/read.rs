use super::*;

use axum::{body::Bytes, http::HeaderMap, response::IntoResponse as _, routing::post, Router};
use base64::Engine as _;
use buzz_sdk::broker::{MessagePostArgs, PubkeyHex};
use nostr::{Event, EventBuilder, Keys, Kind, Tag};
use std::os::unix::fs::PermissionsExt as _;

#[tokio::test]
async fn channel_query_uses_host_nip98_and_returns_verified_channel_event() {
    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    let key_path = fixture.directory.path().join("agent.key");
    let tag_path = fixture.directory.path().join("owner.auth-tag");
    std::fs::write(&key_path, agent.secret_key().to_secret_hex()).expect("key fixture");
    let auth_tag = buzz_sdk::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "kind=9")
        .expect("owner attestation");
    std::fs::write(&tag_path, &auth_tag).expect("tag fixture");
    for path in [&key_path, &tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
            .expect("private signer file");
    }
    let signer = HostSigner::load(&key_path, &tag_path).expect("verified signer");
    let mut identity = fixture.identity.clone();
    identity.executor_agent_pubkey = PubkeyHex::parse(agent.public_key().to_hex()).unwrap();
    let signed_message = signer
        .sign_message(
            &identity,
            &MessagePostArgs {
                channel_id: CHANNEL_ID.into(),
                content: "relay message".into(),
                mentions: Vec::new(),
            },
        )
        .expect("signed channel event");
    let candidate = EventBuilder::new(Kind::Custom(9), "same-timestamp page")
        .tags([
            Tag::parse(["h", CHANNEL_ID]).unwrap(),
            buzz_sdk::nip_oa::parse_auth_tag(&auth_tag).unwrap(),
        ])
        .custom_created_at(signed_message.created_at)
        .sign_with_keys(&agent)
        .unwrap();
    assert_ne!(signed_message.id, candidate.id);
    // Cursor order is by event ID within a timestamp; sort the fixtures
    // instead of hoping one of a bounded set hashes above a random ID.
    let (message, second_message) = if signed_message.id.to_hex() < candidate.id.to_hex() {
        (signed_message, candidate)
    } else {
        (candidate, signed_message)
    };
    let expected_id = message.id;
    let second_id = second_message.id;
    let expected_tag = auth_tag.clone();
    let app = Router::new().route(
        "/query",
        post(move |headers: HeaderMap, body: Bytes| {
            let message = message.clone();
            let second_message = second_message.clone();
            let expected_tag = expected_tag.clone();
            async move {
                assert_eq!(headers.get("x-auth-tag").unwrap(), expected_tag.as_str());
                let auth = headers
                    .get("authorization")
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .strip_prefix("Nostr ")
                    .expect("NIP-98 authorization");
                let auth_bytes = base64::engine::general_purpose::STANDARD
                    .decode(auth)
                    .expect("base64 auth");
                let auth_event: Event = serde_json::from_slice(&auth_bytes).unwrap();
                assert_eq!(auth_event.pubkey, message.pubkey);
                assert_eq!(auth_event.kind.as_u16(), 27235);
                assert!(auth_event.verify().is_ok());
                let filters: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert_eq!(filters[0]["#h"], serde_json::json!([CHANNEL_ID]));
                assert!(filters[0]["limit"] == 1 || filters[0]["limit"] == 16);
                if filters[0].get("#p").is_some() {
                    assert_eq!(
                        filters[0]["#p"],
                        serde_json::json!([message.pubkey.to_hex()])
                    );
                }
                if filters[0].get("#e").is_some() {
                    return vec![b'x'; 16 * 1024 * 1024 + 1].into_response();
                }
                if filters[0].get("before_id").is_some() {
                    assert_eq!(filters[0]["until"], message.created_at.as_secs());
                    if filters[0]["before_id"] == message.id.to_hex() {
                        axum::Json(serde_json::json!([second_message])).into_response()
                    } else {
                        assert_eq!(filters[0]["before_id"], second_message.id.to_hex());
                        axum::Json(serde_json::json!([])).into_response()
                    }
                } else {
                    axum::Json(serde_json::json!([message])).into_response()
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay = format!("ws://{}/community/?q=1", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let publisher = RelayPublisher::new().unwrap();
    let args = ChannelReadArgs {
        channel_id: CHANNEL_ID.into(),
        limit: Some(1),
        ..ChannelReadArgs::default()
    };
    let page = publisher
        .query_channel(&signer, &relay, &args)
        .await
        .expect("signed relay query succeeds");
    let acp_page = publisher
        .query_channel(
            &signer,
            &relay,
            &ChannelReadArgs {
                limit: Some(100),
                ..args.clone()
            },
        )
        .await
        .expect("ACP poller limit is bounded by the host");
    assert_eq!(acp_page.messages.len(), 1);
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].0.id, expected_id);
    assert!(page.next_cursor.is_some(), "full page has a continuation");
    let next_args = ChannelReadArgs {
        cursor: page.next_cursor.clone(),
        ..args.clone()
    };
    let second_page = publisher
        .query_channel(&signer, &relay, &next_args)
        .await
        .expect("opaque cursor reaches same-timestamp successor");
    assert_eq!(second_page.messages.len(), 1);
    assert_eq!(second_page.messages[0].0.id, second_id);
    let terminal = publisher
        .query_channel(
            &signer,
            &relay,
            &ChannelReadArgs {
                cursor: second_page.next_cursor,
                ..args.clone()
            },
        )
        .await
        .expect("opaque cursor reaches terminal page");
    assert!(terminal.messages.is_empty());
    assert!(terminal.next_cursor.is_none());
    let mut wrong_scope = next_args.clone();
    wrong_scope.mentions_only = true;
    assert!(publisher
        .query_channel(&signer, &relay, &wrong_scope)
        .await
        .is_err());
    let mention_args = ChannelReadArgs {
        mentions_only: true,
        ..args.clone()
    };
    assert!(
        publisher
            .query_channel(&signer, &relay, &mention_args)
            .await
            .is_err(),
        "relay cannot inject an event without the requested mention"
    );
    let oversized_args = ChannelReadArgs {
        root_event_id: Some(expected_id.to_hex()),
        ..args.clone()
    };
    assert!(matches!(
        publisher
            .query_channel(&signer, &relay, &oversized_args)
            .await,
        Err(crate::publisher::PublishError::ResponseTooLarge)
    ));
    fixture.identity.executor_agent_pubkey = PubkeyHex::parse(agent.public_key().to_hex()).unwrap();
    fixture.identity.community_relay_url = relay;
    write_identity(&fixture.authority_file, &fixture.identity);
    let store = AuthorityStore::open(&fixture.state).await.unwrap();
    let bearer = fixture.issue(&store).await;
    let status_store = store.clone();
    let wrong_store = store.clone();
    let rotated_store = store.clone();
    let operator_store = store.clone();
    let host_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let host_url = format!("http://{}", host_listener.local_addr().unwrap());
    let host = tokio::spawn(async move {
        axum::serve(
            host_listener,
            crate::http::app_with_effects(store, signer, RelayPublisher::new().unwrap()),
        )
        .await
        .unwrap();
    });
    let request = BrokerRequest::new(
        "channel-read-1",
        ActionArgs::ChannelRead(ChannelReadArgs {
            channel_id: CHANNEL_ID.into(),
            limit: Some(1),
            ..ChannelReadArgs::default()
        }),
    )
    .and_then(BrokerRequest::prepare)
    .unwrap();
    let (_, body) = send(&host_url, &bearer, request.body()).await;
    let response: BrokerResponse = serde_json::from_slice(&body).unwrap();
    assert!(matches!(
        response.result,
        BrokerResult::Succeeded {
            outcome: buzz_sdk::broker::ActionOutcome::ChannelRead(ref page)
        } if page.messages.len() == 1 && page.messages[0].0.id == expected_id
    ));
    let bad_cursor_request = BrokerRequest::new(
        "channel-read-bad-cursor",
        ActionArgs::ChannelRead(ChannelReadArgs {
            mentions_only: true,
            cursor: page.next_cursor.clone(),
            ..args.clone()
        }),
    )
    .and_then(BrokerRequest::prepare)
    .unwrap();
    let (_, bad_cursor_body) = send(&host_url, &bearer, bad_cursor_request.body()).await;
    let bad_cursor: BrokerResponse = serde_json::from_slice(&bad_cursor_body).unwrap();
    assert!(
        matches!(bad_cursor.result, BrokerResult::Failed { ref error }
        if error.code == BrokerErrorCode::InvalidRequest)
    );
    let (status_url, status_host) = spawn_host(status_store).await;
    let denied_request = BrokerRequest::new(
        "channel-read-denied-first",
        ActionArgs::ChannelRead(args.clone()),
    )
    .and_then(BrokerRequest::prepare)
    .unwrap();
    let (_, denied_body) = send(&status_url, &bearer, denied_request.body()).await;
    let denied: BrokerResponse = serde_json::from_slice(&denied_body).unwrap();
    assert!(matches!(denied.result, BrokerResult::Failed { .. }));
    let (_, denied_retry_body) = send(&host_url, &bearer, denied_request.body()).await;
    let denied_retry: BrokerResponse = serde_json::from_slice(&denied_retry_body).unwrap();
    assert!(matches!(denied_retry.result, BrokerResult::Failed { .. }));
    assert!(
        denied_retry.replayed,
        "denied exact retry replays after signer appears"
    );
    let wrong_agent = Keys::generate();
    let wrong_key = fixture.directory.path().join("wrong-agent.key");
    let wrong_tag_path = fixture.directory.path().join("wrong-agent.auth-tag");
    std::fs::write(&wrong_key, wrong_agent.secret_key().to_secret_hex()).unwrap();
    std::fs::write(
        &wrong_tag_path,
        buzz_sdk::nip_oa::compute_auth_tag(&owner, &wrong_agent.public_key(), "kind=9").unwrap(),
    )
    .unwrap();
    for path in [&wrong_key, &wrong_tag_path] {
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
    }
    let wrong_signer = HostSigner::load(&wrong_key, &wrong_tag_path).unwrap();
    let wrong_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let wrong_url = format!("http://{}", wrong_listener.local_addr().unwrap());
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
    let wrong_request = BrokerRequest::new(
        "channel-read-wrong-first",
        ActionArgs::ChannelRead(args.clone()),
    )
    .and_then(BrokerRequest::prepare)
    .unwrap();
    let (_, wrong_body) = send(&wrong_url, &bearer, wrong_request.body()).await;
    let wrong: BrokerResponse = serde_json::from_slice(&wrong_body).unwrap();
    assert!(matches!(wrong.result, BrokerResult::Failed { .. }));
    let (_, wrong_retry_body) = send(&host_url, &bearer, wrong_request.body()).await;
    let wrong_retry: BrokerResponse = serde_json::from_slice(&wrong_retry_body).unwrap();
    assert!(matches!(wrong_retry.result, BrokerResult::Failed { .. }));
    assert!(
        wrong_retry.replayed,
        "wrong-signer denial retains its receipt"
    );
    let rotated_owner = Keys::generate();
    let rotated_tag_path = fixture.directory.path().join("rotated-owner.auth-tag");
    std::fs::write(
        &rotated_tag_path,
        buzz_sdk::nip_oa::compute_auth_tag(&rotated_owner, &agent.public_key(), "kind=9").unwrap(),
    )
    .unwrap();
    std::fs::set_permissions(&rotated_tag_path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let rotated_signer = HostSigner::load(&key_path, &rotated_tag_path).unwrap();
    let rotated_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let rotated_url = format!("http://{}", rotated_listener.local_addr().unwrap());
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
    let rotated: BrokerResponse = serde_json::from_slice(&rotated_body).unwrap();
    assert!(matches!(rotated.result, BrokerResult::Failed { ref error }
        if error.code == BrokerErrorCode::Unauthorized));
    assert!(
        !rotated.replayed,
        "read page cannot replay to another owner"
    );
    let rotated_fresh = BrokerRequest::new(
        "channel-read-rotated-fresh",
        ActionArgs::ChannelRead(args.clone()),
    )
    .and_then(BrokerRequest::prepare)
    .unwrap();
    let (_, rotated_fresh_body) = send(&rotated_url, &bearer, rotated_fresh.body()).await;
    let rotated_fresh_response: BrokerResponse =
        serde_json::from_slice(&rotated_fresh_body).unwrap();
    assert!(
        matches!(rotated_fresh_response.result, BrokerResult::Failed { ref error }
        if error.code == BrokerErrorCode::Unauthorized)
    );
    server.abort();
    let (_, retry_body) = send(&host_url, &bearer, request.body()).await;
    let retry: BrokerResponse = serde_json::from_slice(&retry_body).unwrap();
    assert!(matches!(retry.result, BrokerResult::Succeeded { .. }));
    assert!(
        retry.replayed,
        "exact retry uses the stored page during relay outage"
    );
    let changed = BrokerRequest::new(
        "channel-read-1",
        ActionArgs::ChannelRead(ChannelReadArgs {
            mentions_only: true,
            ..args
        }),
    )
    .and_then(BrokerRequest::prepare)
    .unwrap();
    let (_, changed_body) = send(&host_url, &bearer, changed.body()).await;
    let changed_response: BrokerResponse = serde_json::from_slice(&changed_body).unwrap();
    assert!(
        matches!(changed_response.result, BrokerResult::Failed { ref error }
        if error.code == BrokerErrorCode::RequestIdConflict)
    );
    operator_store.quiesce(&fixture.identity).await.unwrap();
    let (_, quiesced_body) = send(&host_url, &bearer, request.body()).await;
    let quiesced: BrokerResponse = serde_json::from_slice(&quiesced_body).unwrap();
    assert!(matches!(quiesced.result, BrokerResult::Failed { ref error }
        if error.code == BrokerErrorCode::Unauthorized));
    host.abort();
    status_host.abort();
    wrong_host.abort();
    rotated_host.abort();
}
