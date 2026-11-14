//! Live ingress proof for the broker-owned Huddle audio role gate.
//!
//! Run against one local relay with mesh disabled, a migrated Postgres writer,
//! and `BUZZ_REQUIRE_RELAY_MEMBERSHIP=false`:
//! `RELAY_WS_URL=ws://127.0.0.1:3100 BUZZ_TEST_DATABASE_URL=... cargo test -p buzz-test-client --test broker_huddle_role_live -- --ignored`

use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use nostr::{EventBuilder, Keys, Kind, Tag};
use serde_json::{json, Value};
use sqlx::PgPool;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};
use uuid::Uuid;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

const TIMEOUT: Duration = Duration::from_secs(5);

fn relay_url() -> String {
    std::env::var("RELAY_WS_URL").unwrap_or_else(|_| "ws://127.0.0.1:3100".into())
}

async fn next_json(socket: &mut Socket) -> Value {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Text(text))) => {
                    return serde_json::from_str(&text).expect("relay control JSON");
                }
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                other => panic!("relay closed before control message: {other:?}"),
            }
        }
    })
    .await
    .expect("relay control timeout")
}

async fn join(channel: Uuid, keys: &Keys, broker: bool) -> Socket {
    let origin = relay_url();
    let (mut socket, _) = connect_async(format!("{origin}/huddle/{channel}/audio"))
        .await
        .expect("connect Huddle audio");
    let challenge = next_json(&mut socket).await;
    assert_eq!(challenge["type"], "challenge");
    let nonce = challenge["challenge"].as_str().expect("challenge nonce");
    let mut tags = vec![
        Tag::parse(["relay", &origin]).expect("relay tag"),
        Tag::parse(["challenge", nonce]).expect("challenge tag"),
    ];
    if broker {
        tags.push(Tag::parse(["broker_audio", "local_v1"]).expect("broker marker"));
    }
    let event = EventBuilder::new(Kind::Custom(22242), "")
        .tags(tags)
        .sign_with_keys(keys)
        .expect("sign NIP-42 event");
    socket
        .send(Message::Text(
            json!({"type":"auth","event":event,"protocol_version":2})
                .to_string()
                .into(),
        ))
        .await
        .expect("send audio auth");
    let joined = next_json(&mut socket).await;
    assert_eq!(joined["type"], "joined", "audio join rejected: {joined}");
    if broker {
        assert_eq!(joined["broker_audio_local_only"], true);
    }
    socket
}

async fn expect_audio(socket: &mut Socket, expected: &[u8]) {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Binary(frame))) => {
                    assert_eq!(&frame[1..], expected, "relay changed the Opus frame");
                    break;
                }
                Some(Ok(Message::Text(_)))
                | Some(Ok(Message::Ping(_)))
                | Some(Ok(Message::Pong(_))) => {}
                other => panic!("observer lost Huddle audio: {other:?}"),
            }
        }
    })
    .await
    .expect("audio delivery timeout");
}

async fn expect_close(socket: &mut Socket) {
    tokio::time::timeout(TIMEOUT, async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(_)) => {}
                Some(Err(error)) => panic!("broker socket failed before Close: {error}"),
            }
        }
    })
    .await
    .expect("broker socket did not close after role loss");
}

async fn assert_no_audio(socket: &mut Socket) {
    let result = tokio::time::timeout(Duration::from_millis(300), async {
        loop {
            match socket.next().await {
                Some(Ok(Message::Binary(frame))) => panic!("revoked audio delivered: {frame:?}"),
                Some(Ok(Message::Text(_)))
                | Some(Ok(Message::Ping(_)))
                | Some(Ok(Message::Pong(_))) => {}
                other => panic!("observer closed unexpectedly: {other:?}"),
            }
        }
    })
    .await;
    assert!(
        result.is_err(),
        "observer must remain open without revoked audio"
    );
}

#[tokio::test]
#[ignore = "requires one running local relay and migrated Postgres"]
async fn marked_bot_audio_stops_on_live_downgrade_and_removal() {
    let database_url = std::env::var("BUZZ_TEST_DATABASE_URL").expect("test database URL");
    let pool = PgPool::connect(&database_url).await.expect("test Postgres");
    let origin = relay_url();
    let host = origin
        .trim_start_matches("ws://")
        .trim_start_matches("wss://");
    let proposed_community = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO communities (id, host) VALUES ($1, $2) ON CONFLICT (lower(host)) DO NOTHING",
    )
    .bind(proposed_community)
    .bind(host)
    .execute(&pool)
    .await
    .expect("seed test community");
    let community: Uuid =
        sqlx::query_scalar("SELECT id FROM communities WHERE lower(host) = lower($1)")
            .bind(host)
            .fetch_one(&pool)
            .await
            .expect("lookup test community");
    let channel = Uuid::new_v4();
    let owner = Keys::generate();
    let bot = Keys::generate();
    let observer = Keys::generate();
    sqlx::query("INSERT INTO channels (id, community_id, name, channel_type, visibility, created_by) VALUES ($1, $2, $3, 'stream', 'private', $4)")
        .bind(channel)
        .bind(community)
        .bind(format!("broker-role-live-{channel}"))
        .bind(owner.public_key().to_bytes().to_vec())
        .execute(&pool)
        .await
        .expect("create test channel");
    for (keys, role) in [(&bot, "bot"), (&observer, "member")] {
        sqlx::query("INSERT INTO channel_members (community_id, channel_id, pubkey, role) VALUES ($1, $2, $3, $4::member_role)")
            .bind(community)
            .bind(channel)
            .bind(keys.public_key().to_bytes().to_vec())
            .bind(role)
            .execute(&pool)
            .await
            .expect("seed channel member");
    }

    let mut receiving = join(channel, &observer, false).await;
    let mut sending = join(channel, &bot, true).await;
    let frame = |payload| vec![0, 1, 0, 0, 3, 192, 255, 0, payload];
    sending
        .send(Message::Binary(frame(0xa1).into()))
        .await
        .expect("send first frame");
    expect_audio(&mut receiving, &frame(0xa1)).await;

    sqlx::query("UPDATE channel_members SET role = 'member' WHERE community_id = $1 AND channel_id = $2 AND pubkey = $3")
        .bind(community)
        .bind(channel)
        .bind(bot.public_key().to_bytes().to_vec())
        .execute(&pool)
        .await
        .expect("downgrade bot");
    sending
        .send(Message::Binary(frame(0xa2).into()))
        .await
        .expect("send after downgrade");
    expect_close(&mut sending).await;
    assert_no_audio(&mut receiving).await;

    sqlx::query("UPDATE channel_members SET role = 'bot' WHERE community_id = $1 AND channel_id = $2 AND pubkey = $3")
        .bind(community)
        .bind(channel)
        .bind(bot.public_key().to_bytes().to_vec())
        .execute(&pool)
        .await
        .expect("restore bot role");
    let mut sending = join(channel, &bot, true).await;
    sending
        .send(Message::Binary(frame(0xb1).into()))
        .await
        .expect("send restored frame");
    expect_audio(&mut receiving, &frame(0xb1)).await;
    sqlx::query("UPDATE channel_members SET removed_at = NOW() WHERE community_id = $1 AND channel_id = $2 AND pubkey = $3")
        .bind(community)
        .bind(channel)
        .bind(bot.public_key().to_bytes().to_vec())
        .execute(&pool)
        .await
        .expect("remove bot");
    sending
        .send(Message::Binary(frame(0xb2).into()))
        .await
        .expect("send after removal");
    expect_close(&mut sending).await;
    assert_no_audio(&mut receiving).await;
}
