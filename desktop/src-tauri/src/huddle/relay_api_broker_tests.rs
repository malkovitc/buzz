use super::*;
use axum::{
    extract::ws::WebSocketUpgrade,
    http::HeaderMap,
    routing::{get, post},
    Json, Router,
};
use nostr::{Keys, Kind};
use std::sync::{Arc, Mutex};

const CHANNEL: &str = "d48aefc0-66d9-4eec-a5d2-9a2b0b5f4710";
const PARENT: &str = "11111111-1111-4111-8111-111111111111";

#[tokio::test]
async fn blocked_broker_audio_send_exits_when_publisher_is_cancelled() {
    let mut blocked = Box::pin(futures_util::sink::unfold((), |(), _: WsMsg| async move {
        std::future::pending::<Result<(), std::io::Error>>()
            .await
            .map(|()| ())
    }));
    let cancel = tokio_util::sync::CancellationToken::new();
    let send = send_tts_ws_message(&mut blocked, WsMsg::Binary(vec![1].into()), &cancel);
    tokio::pin!(send);
    tokio::select! {
        result = &mut send => panic!("send completed before cancellation: {result:?}"),
        _ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
    }
    cancel.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), send)
        .await
        .expect("cancelled send must not remain blocked");
    assert!(result.unwrap_err().contains("cancelled"));
}

#[tokio::test]
async fn broker_audio_connection_uses_owner_grant_and_relay_peer_index() {
    let owner = Keys::generate();
    let captured = Arc::new(Mutex::new(None::<nostr::Event>));
    let grant_capture = Arc::clone(&captured);
    let app = Router::new()
        .route("/v1/huddle/challenge", post(|headers: HeaderMap| async move {
            assert_eq!(headers["authorization"], "Bearer test-bearer");
            Json(serde_json::json!({"nonce":"test-nonce"}))
        }))
        .route("/v1/huddle/grant", post(move |headers: HeaderMap, Json(body): Json<serde_json::Value>| {
            let grant_capture = Arc::clone(&grant_capture);
            async move {
                assert_eq!(headers["authorization"], "Bearer test-bearer");
                let event: nostr::Event = serde_json::from_value(body["ownerEvent"].clone()).unwrap();
                assert!(event.verify().is_ok());
                *grant_capture.lock().unwrap() = Some(event);
                Json(serde_json::json!({"grantId":"22222222-2222-4222-8222-222222222222"}))
            }
        }))
        .route("/v1/huddle/audio", get(|headers: HeaderMap, ws: WebSocketUpgrade| async move {
            assert_eq!(headers["authorization"], "Bearer test-bearer");
            ws.on_upgrade(|mut socket| async move {
                socket.send(axum::extract::ws::Message::Text(
                    serde_json::json!({"type":"joined","channelId":CHANNEL,"huddleGeneration":7,"peerIndex":9}).to_string().into()
                )).await.unwrap();
                let _ = socket.recv().await;
            })
        }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let snapshot = crate::managed_agents::BrokerMediaSnapshot {
        url: format!("http://{address}"),
        credential: zeroize::Zeroizing::new("test-bearer".to_string()),
        authority_json: serde_json::json!({"generation":"33333333-3333-4333-8333-333333333333"})
            .to_string(),
    };
    let (mut sink, _stream, peer_index) =
        connect_broker_tts_audio_socket(&snapshot, &owner, CHANNEL, PARENT, 7)
            .await
            .unwrap();
    assert_eq!(peer_index, 9);
    let event = captured.lock().unwrap().clone().unwrap();
    assert_eq!(event.pubkey, owner.public_key());
    assert_eq!(event.kind, Kind::Custom(27236));
    let target: serde_json::Value = serde_json::from_str(&event.content).unwrap();
    assert_eq!(target["channelId"], CHANNEL);
    assert_eq!(target["parentChannelId"], PARENT);
    assert_eq!(target["huddleGeneration"], 7);
    assert_eq!(
        target["authority"]["generation"],
        "33333333-3333-4333-8333-333333333333"
    );
    sink.send(WsMsg::Close(None)).await.unwrap();
    server.abort();
}
