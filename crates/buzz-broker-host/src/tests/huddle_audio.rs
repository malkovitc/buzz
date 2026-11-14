use super::*;

use axum::body::{to_bytes, Body};
use axum::extract::{
    ws::{Message as ServerMessage, WebSocket},
    Path as AxumPath, WebSocketUpgrade,
};
use axum::http::{header::AUTHORIZATION, Request, StatusCode};
use axum::Router;
use futures_util::{SinkExt as _, StreamExt as _};
use nostr::{EventBuilder, Keys, Kind, Tag};
use std::os::unix::fs::PermissionsExt as _;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;
use tower::ServiceExt as _;

#[tokio::test]
async fn huddle_audio_host_signer_joins_v2_and_sends_one_frame() {
    let fixture = Fixture::new();
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
    let app = Router::new().route(
        "/huddle/{channel}/audio",
        axum::routing::get(
            |AxumPath(channel): AxumPath<String>, ws: WebSocketUpgrade| async move {
                assert_eq!(channel, CHANNEL_ID);
                ws.on_upgrade(move |mut socket: WebSocket| async move {
                    socket
                        .send(ServerMessage::Text(
                            serde_json::json!({"type":"challenge","challenge":"fresh"})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                    let Some(Ok(ServerMessage::Text(auth))) = socket.recv().await else {
                        panic!("auth message");
                    };
                    let auth: serde_json::Value = serde_json::from_str(&auth).unwrap();
                    let event: nostr::Event =
                        serde_json::from_value(auth["event"].clone()).unwrap();
                    assert!(event.verify().is_ok());
                    assert_eq!(event.pubkey, agent.public_key());
                    assert_eq!(event.kind.as_u16(), 22242);
                    assert_eq!(auth["protocol_version"], 2);
                    assert_eq!(
                        auth["parent_channel_id"],
                        "11111111-1111-4111-8111-111111111111"
                    );
                    assert!(event
                        .tags
                        .iter()
                        .any(|tag| tag.kind().to_string() == "challenge"
                            && tag.content() == Some("fresh")));
                    assert!(event
                        .tags
                        .iter()
                        .any(|tag| tag.as_slice() == ["broker_audio", "local_v1"]));
                    socket
                        .send(ServerMessage::Text(
                        serde_json::json!({"type":"joined","peer_index":1,"peers":[],"broker_audio_local_only":true})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                    let Some(Ok(ServerMessage::Binary(frame))) = socket.recv().await else {
                        panic!("audio frame");
                    };
                    assert_eq!(frame.len(), 9);
                    assert_eq!(frame[8], 0xF8);
                    socket.send(ServerMessage::Close(None)).await.unwrap();
                })
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut relay = crate::huddle::relay::AudioRelay::connect(
        &signer,
        &relay_url,
        CHANNEL_ID,
        "11111111-1111-4111-8111-111111111111",
    )
    .await
    .unwrap();
    relay
        .sink
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            vec![0, 1, 0, 0, 3, 192, 0, 0, 0xF8].into(),
        ))
        .await
        .unwrap();
    assert!(matches!(
        relay.stream.next().await,
        Some(Ok(tokio_tungstenite::tungstenite::Message::Close(_)))
    ));
    server.abort();
}

#[tokio::test]
async fn huddle_audio_consumes_owner_grant_and_closes_before_fence() {
    let mut fixture = Fixture::new();
    let agent = Keys::generate();
    let owner = Keys::generate();
    let agent_pubkey = agent.public_key().to_hex();
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1);
    let close_gate = std::sync::Arc::new(tokio::sync::Notify::new());
    let relay_close_gate = std::sync::Arc::clone(&close_gate);
    let relay_app = Router::new()
        .route(
            "/huddle/{channel}/audio",
            axum::routing::get(move |ws: WebSocketUpgrade| {
                let frame_tx = frame_tx.clone();
                let close_gate = std::sync::Arc::clone(&relay_close_gate);
                async move {
                    ws.on_upgrade(move |mut socket| async move {
                        socket
                            .send(ServerMessage::Text(
                                serde_json::json!({"type":"challenge","challenge":"fresh"})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .unwrap();
                        let Some(Ok(ServerMessage::Text(_))) = socket.recv().await else {
                            panic!("relay auth");
                        };
                        socket
                            .send(ServerMessage::Text(
                                serde_json::json!({"type":"joined","peer_index":1,"peers":[],"broker_audio_local_only":true})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .unwrap();
                        loop {
                            match socket.recv().await {
                                Some(Ok(ServerMessage::Binary(frame))) => {
                                    frame_tx.send(frame.to_vec()).await.unwrap();
                                }
                                Some(Ok(ServerMessage::Close(_))) => {
                                    close_gate.notified().await;
                                    socket.flush().await.unwrap();
                                    break;
                                }
                                _ => break,
                            }
                        }
                    })
                }
            }),
        );
    let relay_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    fixture.identity.community_relay_url = format!("ws://{}", relay_listener.local_addr().unwrap());
    fixture.identity.location.kind = buzz_sdk::broker::RuntimeLocationKind::Local;
    fixture.identity.logical_agent_pubkey =
        buzz_sdk::broker::PubkeyHex::parse(&agent_pubkey).unwrap();
    fixture.identity.executor_agent_pubkey = fixture.identity.logical_agent_pubkey.clone();
    write_identity(&fixture.authority_file, &fixture.identity);
    let relay_server =
        tokio::spawn(async move { axum::serve(relay_listener, relay_app).await.unwrap() });

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
    let app = crate::http::app_with_effects(store.clone(), signer, RelayPublisher::new().unwrap());
    let grant_id = issue_huddle_grant(&app, &bearer, &owner, &fixture.identity).await;
    let old_grant_id = issue_huddle_grant(&app, &bearer, &owner, &fixture.identity).await;

    let host_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let host_address = host_listener.local_addr().unwrap();
    let host_url = format!("ws://{host_address}/v1/huddle/audio?grantId={grant_id}");
    let host_server = tokio::spawn(async move { axum::serve(host_listener, app).await.unwrap() });
    let mut request = host_url.into_client_request().unwrap();
    request
        .headers_mut()
        .insert(AUTHORIZATION, format!("Bearer {bearer}").parse().unwrap());
    let (mut client, _) = tokio_tungstenite::connect_async(request).await.unwrap();
    let joined = client.next().await.unwrap().unwrap().into_text().unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&joined).unwrap()["type"],
        "joined"
    );
    client
        .send(tokio_tungstenite::tungstenite::Message::Binary(
            vec![0, 1, 0, 0, 3, 192, 0, 0, 0xF8].into(),
        ))
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(std::time::Duration::from_secs(2), frame_rx.recv())
            .await
            .unwrap()
            .unwrap()
            .len(),
        9
    );
    client.close(None).await.unwrap();
    let quiesce_store = store.clone();
    let quiesce_identity = fixture.identity.clone();
    let quiesce = tokio::spawn(async move { quiesce_store.quiesce(&quiesce_identity).await });
    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    assert!(
        !quiesce.is_finished(),
        "quiesce returned before relay close reply"
    );
    close_gate.notify_one();
    quiesce.await.unwrap().unwrap();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), client.next()).await;
    let old_url = format!("ws://{host_address}/v1/huddle/audio?grantId={old_grant_id}");
    let mut old_request = old_url.into_client_request().unwrap();
    old_request
        .headers_mut()
        .insert(AUTHORIZATION, format!("Bearer {bearer}").parse().unwrap());
    let old_connect = tokio_tungstenite::connect_async(old_request).await;
    assert!(matches!(
        old_connect,
        Err(tokio_tungstenite::tungstenite::Error::Http(ref response))
            if response.status() == StatusCode::FORBIDDEN
    ));
    store.fence(&fixture.identity).await.unwrap();
    host_server.abort();
    relay_server.abort();
}

async fn issue_huddle_grant(
    app: &Router,
    bearer: &str,
    owner: &Keys,
    identity: &AuthorityIdentity,
) -> String {
    let challenge = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/huddle/challenge")
                .header(AUTHORIZATION, format!("Bearer {bearer}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(challenge.status(), StatusCode::OK);
    let body = to_bytes(challenge.into_body(), 4096).await.unwrap();
    let challenge: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let event = EventBuilder::new(
        Kind::Custom(27236),
        serde_json::json!({
            "authority": identity,
            "channelId": CHANNEL_ID,
            "parentChannelId": "11111111-1111-4111-8111-111111111111",
            "huddleGeneration": 1,
        })
        .to_string(),
    )
    .tags([Tag::parse(["challenge", challenge["nonce"].as_str().unwrap()]).unwrap()])
    .sign_with_keys(owner)
    .unwrap();
    let grant = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/v1/huddle/grant")
                .header(AUTHORIZATION, format!("Bearer {bearer}"))
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({"ownerEvent": event}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(grant.status(), StatusCode::OK);
    let body = to_bytes(grant.into_body(), 4096).await.unwrap();
    let grant: serde_json::Value = serde_json::from_slice(&body).unwrap();
    grant["grantId"].as_str().unwrap().to_owned()
}

#[tokio::test]
async fn huddle_audio_rejects_join_without_local_only_relay_proof() {
    let fixture = Fixture::new();
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
    let app = Router::new().route(
        "/huddle/{channel}/audio",
        axum::routing::get(|ws: WebSocketUpgrade| async move {
            ws.on_upgrade(|mut socket| async move {
                socket.send(ServerMessage::Text(serde_json::json!({"type":"challenge","challenge":"fresh"}).to_string().into())).await.unwrap();
                let _ = socket.recv().await;
                socket.send(ServerMessage::Text(serde_json::json!({"type":"joined","peer_index":1,"peers":[],"broker_audio_local_only":false}).to_string().into())).await.unwrap();
            })
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_url = format!("ws://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let result = crate::huddle::relay::AudioRelay::connect(
        &signer,
        &relay_url,
        CHANNEL_ID,
        "11111111-1111-4111-8111-111111111111",
    )
    .await;
    assert!(result.is_err());
    server.abort();
}
