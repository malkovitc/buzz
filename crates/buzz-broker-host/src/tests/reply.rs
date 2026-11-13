use super::*;

use axum::{body::Bytes, http::HeaderMap, routing::post, Router};
use buzz_sdk::broker::{ActionOutcome, MessageReplyArgs};
use nostr::{Event, EventBuilder, Keys, Kind, Tag};
use std::os::unix::fs::PermissionsExt as _;

#[tokio::test]
async fn reply_target_requires_signed_parent_in_requested_channel() {
    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
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
    let parent = EventBuilder::new(Kind::Custom(9), "parent")
        .tags([Tag::parse(["h", CHANNEL_ID]).unwrap()])
        .sign_with_keys(&owner)
        .unwrap();
    let wrong_channel = EventBuilder::new(Kind::Custom(9), "other channel")
        .tags([Tag::parse(["h", "22222222-2222-4222-8222-222222222222"]).unwrap()])
        .sign_with_keys(&owner)
        .unwrap();
    let nested = EventBuilder::new(Kind::Custom(9), "nested")
        .tags([
            Tag::parse(["h", CHANNEL_ID]).unwrap(),
            Tag::parse(["e", &parent.id.to_hex(), "", "root"]).unwrap(),
            Tag::parse(["e", &parent.id.to_hex(), "", "reply"]).unwrap(),
        ])
        .sign_with_keys(&owner)
        .unwrap();
    let root_only = EventBuilder::new(Kind::Custom(9), "top level despite root tag")
        .tags([
            Tag::parse(["h", CHANNEL_ID]).unwrap(),
            Tag::parse(["e", &"00".repeat(32), "", "root"]).unwrap(),
        ])
        .sign_with_keys(&owner)
        .unwrap();
    let mut forged = EventBuilder::new(Kind::Custom(9), "original")
        .tags([Tag::parse(["h", CHANNEL_ID]).unwrap()])
        .sign_with_keys(&owner)
        .unwrap();
    forged.content = "tampered".into();
    let duplicate_channel = EventBuilder::new(Kind::Custom(9), "ambiguous")
        .tags([
            Tag::parse(["h", CHANNEL_ID]).unwrap(),
            Tag::parse(["h", CHANNEL_ID]).unwrap(),
        ])
        .sign_with_keys(&owner)
        .unwrap();
    let valid_id = parent.id.to_hex();
    let wrong_id = wrong_channel.id.to_hex();
    let nested_id = nested.id.to_hex();
    let root_only_id = root_only.id.to_hex();
    let forged_id = forged.id.to_hex();
    let duplicate_id = duplicate_channel.id.to_hex();
    let relay = Router::new()
        .route(
            "/query",
            post(move |headers: HeaderMap, body: Bytes| {
                let parent = parent.clone();
                let wrong_channel = wrong_channel.clone();
                let nested = nested.clone();
                let root_only = root_only.clone();
                let forged = forged.clone();
                let duplicate_channel = duplicate_channel.clone();
                async move {
                    assert!(headers
                        .get("authorization")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .starts_with("Nostr "));
                    assert!(headers.get("x-auth-tag").is_some());
                    let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let id = request[0]["ids"][0].as_str().unwrap();
                    let events: Vec<Event> = if id == parent.id.to_hex() {
                        vec![parent]
                    } else if id == nested.id.to_hex() {
                        vec![nested]
                    } else if id == root_only.id.to_hex() {
                        vec![root_only]
                    } else if id == wrong_channel.id.to_hex() {
                        vec![wrong_channel]
                    } else if id == duplicate_channel.id.to_hex() {
                        vec![duplicate_channel]
                    } else if id == forged.id.to_hex() {
                        vec![forged]
                    } else {
                        Vec::new()
                    };
                    axum::Json(events)
                }
            }),
        )
        .route(
            "/events",
            post(|body: Bytes| async move {
                let event: Event = serde_json::from_slice(&body).unwrap();
                axum::Json(serde_json::json!({
                    "accepted": true,
                    "event_id": event.id.to_hex()
                }))
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, relay).await.unwrap() });
    let publisher = RelayPublisher::new().unwrap();
    let args = MessageReplyArgs {
        channel_id: CHANNEL_ID.into(),
        reply_to_event_id: valid_id,
        content: "reply".into(),
        mentions: Vec::new(),
    };
    let thread = publisher
        .resolve_reply_target(&signer, &relay_url, &args)
        .await
        .expect("signed same-channel parent is accepted");
    assert_eq!(thread.root_event_id.to_hex(), args.reply_to_event_id);
    assert_eq!(thread.parent_event_id.to_hex(), args.reply_to_event_id);
    let nested_thread = publisher
        .resolve_reply_target(
            &signer,
            &relay_url,
            &MessageReplyArgs {
                reply_to_event_id: nested_id,
                ..args.clone()
            },
        )
        .await
        .expect("nested reply verifies its root in the same channel");
    assert_eq!(nested_thread.root_event_id, thread.root_event_id);
    assert_ne!(nested_thread.parent_event_id, nested_thread.root_event_id);
    let root_only_thread = publisher
        .resolve_reply_target(
            &signer,
            &relay_url,
            &MessageReplyArgs {
                reply_to_event_id: root_only_id,
                ..args.clone()
            },
        )
        .await
        .expect("root-only parent starts a new thread at itself");
    assert_eq!(
        root_only_thread.root_event_id,
        root_only_thread.parent_event_id
    );
    let wrong_args = MessageReplyArgs {
        reply_to_event_id: wrong_id,
        ..args.clone()
    };
    assert!(publisher
        .resolve_reply_target(&signer, &relay_url, &wrong_args)
        .await
        .is_err());
    for bad_id in [duplicate_id, forged_id, "00".repeat(32)] {
        assert!(publisher
            .resolve_reply_target(
                &signer,
                &relay_url,
                &MessageReplyArgs {
                    reply_to_event_id: bad_id,
                    ..args.clone()
                },
            )
            .await
            .is_err());
    }
    fixture.identity.executor_agent_pubkey =
        buzz_sdk::broker::PubkeyHex::parse(agent.public_key().to_hex()).unwrap();
    fixture.identity.community_relay_url = relay_url;
    write_identity(&fixture.authority_file, &fixture.identity);
    let store = AuthorityStore::open(&fixture.state).await.unwrap();
    let bearer = fixture.issue(&store).await;
    let credential = crate::credential::CredentialDigest::from_bearer(&bearer).unwrap();
    let request = BrokerRequest::new("reply-effect", ActionArgs::MessageReply(args.clone()))
        .and_then(BrokerRequest::prepare)
        .unwrap();
    let validated = serde_json::from_slice::<BrokerRequest>(request.body())
        .unwrap()
        .validated()
        .unwrap();
    let first = store
        .prepare_message_reply(
            &credential,
            request.body(),
            &validated,
            &signer,
            Some(&thread),
        )
        .await
        .expect("reply admitted after target proof");
    let event_id = match first {
        EffectAdmission::Pending(event, _, false) => {
            assert!(event.verify().is_ok());
            assert!(event.tags.iter().any(|tag| {
                let parts = tag.as_slice();
                parts.first().map(String::as_str) == Some("e")
                    && parts.get(1).map(String::as_str) == Some(args.reply_to_event_id.as_str())
                    && parts.get(3).map(String::as_str) == Some("reply")
            }));
            event.id.to_hex()
        }
        _ => panic!("first reply admission must journal one event"),
    };
    assert!(matches!(
        store
            .prepare_message_reply(&credential, request.body(), &validated, &signer, None)
            .await,
        Ok(EffectAdmission::Pending(event, _, true)) if event.id.to_hex() == event_id
    ));
    store.quiesce(&fixture.identity).await.unwrap();
    assert!(
        crate::drain::drain_pending(&store, &fixture.identity, &signer, &publisher)
            .await
            .expect("operator drain resolves the journaled reply")
    );
    let response = store
        .complete_message_effect(&credential, "reply-effect", &event_id)
        .await
        .unwrap();
    assert!(matches!(
        response.result,
        BrokerResult::Succeeded {
            outcome: ActionOutcome::MessageReply(_)
        }
    ));
    store.fence(&fixture.identity).await.unwrap();
    assert!(matches!(
        store
            .prepare_message_reply(&credential, request.body(), &validated, &signer, None)
            .await,
        Ok(EffectAdmission::Completed(_))
    ));
    server.abort();
}
