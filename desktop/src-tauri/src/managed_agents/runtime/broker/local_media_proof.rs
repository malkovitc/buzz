use super::*;
use axum::{
    extract::{ws::Message, Path as AxumPath, WebSocketUpgrade},
    routing::get,
    Router,
};
use futures_util::SinkExt as _;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

const CHANNEL: &str = "d48aefc0-66d9-4eec-a5d2-9a2b0b5f4710";
const PARENT: &str = "11111111-1111-4111-8111-111111111111";

struct RuntimeCleanup {
    state: Arc<crate::app_state::AppState>,
    key: ManagedAgentRuntimeKey,
    close_gate: Arc<tokio::sync::Notify>,
}

impl Drop for RuntimeCleanup {
    fn drop(&mut self) {
        self.close_gate.notify_one();
        let mut runtimes = self
            .state
            .managed_agent_processes
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if let Some(mut tracked) = runtimes.remove(&self.key) {
            let _ = tracked.child.kill();
            let _ = tracked.child.wait();
        }
    }
}

#[tokio::test]
#[ignore = "requires BUZZ_TEST_BROKER_HOST_BIN pointing to the current built host"]
async fn desktop_audio_reaches_local_relay_and_stops_before_fence() {
    let binary = PathBuf::from(std::env::var("BUZZ_TEST_BROKER_HOST_BIN").unwrap());
    let owner = nostr::Keys::generate();
    let agent = nostr::Keys::generate();
    let agent_pubkey = agent.public_key();
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let (close_tx, mut close_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let (terminal_tx, mut terminal_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
    let close_gate = Arc::new(tokio::sync::Notify::new());
    let app = Router::new().route(
        "/huddle/{channel}/audio",
        get({
            let close_gate = Arc::clone(&close_gate);
            move |AxumPath(channel): AxumPath<String>, ws: WebSocketUpgrade| {
                let frame_tx = frame_tx.clone();
                let close_tx = close_tx.clone();
                let terminal_tx = terminal_tx.clone();
                let close_gate = Arc::clone(&close_gate);
                async move {
                    assert_eq!(channel, CHANNEL);
                    ws.on_upgrade(move |mut socket| async move {
                        socket
                            .send(Message::Text(
                                serde_json::json!({"type":"challenge","challenge":"fresh"})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .unwrap();
                        let Some(Ok(Message::Text(auth))) = socket.recv().await else {
                            panic!("broker relay authentication");
                        };
                        let auth: serde_json::Value = serde_json::from_str(&auth).unwrap();
                        let event: nostr::Event =
                            serde_json::from_value(auth["event"].clone()).unwrap();
                        assert!(event.verify().is_ok());
                        assert_eq!(event.pubkey, agent_pubkey);
                        assert_eq!(auth["protocol_version"], 2);
                        assert_eq!(auth["parent_channel_id"], PARENT);
                        assert!(event
                            .tags
                            .iter()
                            .any(|tag| tag.as_slice() == ["broker_audio", "local_v1"]));
                        socket
                            .send(Message::Text(
                                serde_json::json!({
                                    "type":"joined", "peer_index":3, "peers":[],
                                    "broker_audio_local_only":true
                                })
                                .to_string()
                                .into(),
                            ))
                            .await
                            .unwrap();
                        loop {
                            match socket.recv().await {
                                Some(Ok(Message::Binary(frame))) => {
                                    frame_tx.send(frame.to_vec()).unwrap();
                                }
                                Some(Ok(Message::Close(_))) => {
                                    close_tx.send(()).unwrap();
                                    close_gate.notified().await;
                                    socket.flush().await.unwrap();
                                    break;
                                }
                                _ => break,
                            }
                        }
                        drop(socket);
                        terminal_tx.send(()).unwrap();
                    })
                }
            }
        }),
    );
    let relay = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let relay_url = format!("ws://{}", relay.local_addr().unwrap());
    let relay_server = tokio::spawn(async move { axum::serve(relay, app).await.unwrap() });

    let directory = tempfile::tempdir().unwrap();
    let mut record = super::super::test_fixtures::fixture(
        super::super::super::RespondTo::OwnerOnly,
        Vec::new(),
        None,
    );
    record.pubkey = agent_pubkey.to_hex();
    record.private_key_nsec = agent.secret_key().to_secret_hex();
    record.auth_tag =
        Some(buzz_sdk_pkg::nip_oa::compute_auth_tag(&owner, &agent_pubkey, "kind=9").unwrap());
    record
        .env_vars
        .insert("BUZZ_ACP_CHANNELS".into(), CHANNEL.into());
    let key = ManagedAgentRuntimeKey::new(&record.pubkey, &relay_url).unwrap();
    let base = directory.path().join("agents");
    ensure_private_dir(&base).unwrap();
    let log = directory.path().join("agent.log");
    let mut launch = BrokerHost::launch(
        &base,
        &binary,
        &record,
        &key,
        &owner.public_key().to_hex(),
        "desktop-local-proof",
        &log,
    )
    .unwrap();
    let snapshot = launch.host.media_snapshot().unwrap();
    let state = Arc::new(crate::app_state::build_app_state());
    *state.relay_url_override.lock().unwrap() = Some(relay_url.clone());
    let process = crate::managed_agents::ManagedAgentProcess {
        child: std::process::Command::new("/bin/sleep")
            .arg("30")
            .spawn()
            .unwrap(),
        log_path: log,
        spawn_config: crate::managed_agents::spawn_snapshot::SpawnConfigSnapshot {
            acp_command: "test".into(),
            command: "test".into(),
            args: Vec::new(),
            mcp_command: String::new(),
            env: Default::default(),
            relay_url: key.relay_url.clone(),
            team_instructions: None,
            system_prompt: None,
            model: None,
            provider: None,
            session_title: None,
            auth_tag: None,
            respond_to: "owner-only".into(),
            respond_to_allowlist: None,
            idle_timeout_seconds: None,
            max_turn_duration_seconds: None,
            parallelism: 1,
            effort_level: None,
        },
        setup_mode: false,
        adapter_availability: None,
        start_nonce: uuid::Uuid::new_v4().to_string(),
        broker_host: Some(launch.host),
    };
    state.managed_agent_processes.lock().unwrap().insert(
        key.clone(),
        crate::managed_agents::ManagedAgentPairRuntime::starting(process),
    );
    let _cleanup = RuntimeCleanup {
        state: Arc::clone(&state),
        key: key.clone(),
        close_gate: Arc::clone(&close_gate),
    };
    let local_publishers = Arc::new(Mutex::new(std::collections::HashMap::new()));
    let publisher = crate::huddle::relay_api::connect_broker_tts_audio_publisher(
        &snapshot,
        &owner,
        CHANNEL,
        PARENT,
        7,
        Arc::clone(&local_publishers),
    )
    .await
    .unwrap();
    assert_eq!(local_publishers.lock().unwrap().get(&3), Some(&1));
    publisher.test_publish_pcm(vec![0.2; 480]);
    let frame = tokio::time::timeout(Duration::from_secs(5), frame_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let (header, opus) = crate::huddle::wire::FrameHeader::parse(&frame).unwrap();
    assert_eq!(header.seq, 0);
    assert!(!opus.is_empty(), "Desktop must send an encoded v2 frame");
    let mut decoder = opus::Decoder::new(48_000, opus::Channels::Mono).unwrap();
    let mut decoded = vec![0.0_f32; 960];
    assert!(decoder.decode_float(opus, &mut decoded, false).unwrap() > 0);

    let stop_state = Arc::clone(&state);
    let stop_key = key.clone();
    let stop = tokio::task::spawn_blocking(move || {
        let mut runtimes = stop_state.managed_agent_processes.lock().unwrap();
        runtimes.get_mut(&stop_key).unwrap().close_broker()
    });
    tokio::time::timeout(Duration::from_secs(5), close_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        state.managed_agent_processes.try_lock().is_err(),
        "stop must hold the runtime map through relay Close"
    );
    assert!(
        !stop.is_finished(),
        "quiesce returned before relay Close reply"
    );
    let registered = Arc::new(AtomicBool::new(false));
    let registration_state = Arc::clone(&state);
    let registration_flag = Arc::clone(&registered);
    let registration_pubkey = key.pubkey.clone();
    let registration_authority = snapshot.authority_json.clone();
    let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
    let registration = tokio::task::spawn_blocking(move || {
        entered_tx.send(()).unwrap();
        crate::huddle::test_with_current_broker_media(
            &registration_state,
            &registration_pubkey,
            &registration_authority,
            || registration_flag.store(true, Ordering::SeqCst),
        )
    });
    entered_rx.await.unwrap();
    assert!(
        !registration.is_finished(),
        "registration passed a quiescing runtime"
    );
    publisher.test_publish_pcm(vec![0.2; 480]);
    close_gate.notify_one();
    tokio::time::timeout(Duration::from_secs(5), stop)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(registration.await.unwrap().is_err());
    assert!(!registered.load(Ordering::SeqCst));
    tokio::time::timeout(Duration::from_secs(5), terminal_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        frame_rx.try_recv().is_err(),
        "relay observed an old frame before terminal Close"
    );
    tokio::time::timeout(Duration::from_secs(2), publisher.test_wait_closed())
        .await
        .expect("broker stop must close the publisher receiver while its handle is held");
    tokio::time::timeout(Duration::from_secs(2), async {
        while !local_publishers.lock().unwrap().is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("publisher socket lease must end while its handle is held");
    drop(publisher);
    relay_server.abort();
}
