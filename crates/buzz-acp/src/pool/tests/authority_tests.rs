use super::*;

use axum::body::{Body, Bytes};
use axum::http::StatusCode;
use axum::response::Response;
use axum::routing::post;
use axum::Router;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::net::TcpListener;

async fn authority_fence_then_fenced(active_permits: usize) -> AuthorityFence {
    let pubkey = Keys::generate().public_key().to_hex();
    let identity: buzz_sdk::broker::AuthorityIdentity = serde_json::from_value(json!({
        "communityRelayUrl": "wss://relay.example.invalid",
        "logicalAgentPubkey": pubkey,
        "executorAgentPubkey": pubkey,
        "taskId": "40b68c08-ed45-4c4b-a1d8-e46d6478d642",
        "generation": "8d86e776-0f6a-418b-b7fb-4f87be556591",
        "location": {"kind": "cloud", "id": "runtime-b"},
        "runtimeSupport": "portable"
    }))
    .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let host_identity = identity.clone();
    let app = Router::new().route(
        "/v1/action",
        post(move |body: Bytes| {
            let calls = Arc::clone(&calls);
            let identity = host_identity.clone();
            async move {
                let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let state = if calls.fetch_add(1, Ordering::SeqCst) < active_permits {
                    "active"
                } else {
                    "fenced"
                };
                let response = json!({
                    "type": "broker_result",
                    "protocolVersion": 1,
                    "requestId": request["requestId"],
                    "status": "succeeded",
                    "action": "authority.status",
                    "outcome": {"identity": identity, "state": state},
                });
                Response::builder()
                    .status(StatusCode::OK)
                    .body(Body::from(response.to_string()))
                    .unwrap()
            }
        }),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    AuthorityFence::new(format!("http://{address}"), "credential".into(), identity).unwrap()
}

#[tokio::test]
async fn authority_cutover_during_setup_blocks_the_initial_acp_prompt() {
    let capture = std::env::temp_dir().join(format!(
        "buzz-acp-authority-prompt-fence-{}.ndjson",
        Uuid::new_v4()
    ));
    let quoted_capture = capture.to_string_lossy().replace('\'', "'\\''");
    let script = format!(
        r#"while IFS= read -r line; do
  printf '%s\n' "$line" >> '{quoted_capture}'
  printf '%s\n' '{{"jsonrpc":"2.0","id":0,"result":{{"stopReason":"end_turn"}}}}'
done"#
    );
    let acp = AcpClient::spawn("bash", &["-c".to_string(), script], &[], false)
        .await
        .expect("spawn authority-fence ACP script");
    let mut agent = OwnedAgent {
        index: 0,
        acp,
        state: SessionState::default(),
        model_capabilities: None,
        desired_model: None,
        model_overridden: false,
        desired_model_request_id: None,
        desired_model_pending_ack: false,
        startup_effort: None,
        agent_name: "legacy-test-agent".into(),
        goose_system_prompt_supported: None,
        protocol_version: 1,
    };
    agent.state.heartbeat_session = Some("live-session".into());
    let mut ctx = make_prompt_context_no_owner();
    ctx.authority_fence = Some(authority_fence_then_fenced(1).await);
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();

    run_prompt_task(
        agent,
        None,
        Some("initial heartbeat".into()),
        Arc::new(ctx),
        result_tx,
        None,
        "turn-authority-cutover".into(),
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;

    let mut result = result_rx.recv().await.expect("prompt result");
    assert!(matches!(result.outcome, PromptOutcome::Error(_)));
    result.agent.acp.shutdown().await;
    assert!(!capture.exists(), "ACP prompt must not cross a stale fence");
}

#[tokio::test]
async fn authority_cutover_after_session_new_blocks_initial_message() {
    let capture = std::env::temp_dir().join(format!(
        "buzz-acp-authority-initial-message-fence-{}.ndjson",
        Uuid::new_v4()
    ));
    let quoted_capture = capture.to_string_lossy().replace('\'', "'\\''");
    let script = format!(
        r#"count=0
while IFS= read -r line; do
  printf '%s\n' "$line" >> '{quoted_capture}'
  count=$((count + 1))
  id=$((count - 1))
  if [ "$count" -eq 1 ]; then
    printf '%s\n' '{{"jsonrpc":"2.0","id":0,"result":{{"sessionId":"new-session"}}}}'
  else
    printf '%s\n' '{{"jsonrpc":"2.0","id":'"$id"',"result":{{"stopReason":"end_turn"}}}}'
  fi
done"#
    );
    let acp = AcpClient::spawn("bash", &["-c".to_string(), script], &[], false)
        .await
        .expect("spawn initial-message ACP script");
    let agent = OwnedAgent {
        index: 0,
        acp,
        state: SessionState::default(),
        model_capabilities: None,
        desired_model: None,
        model_overridden: false,
        desired_model_request_id: None,
        desired_model_pending_ack: false,
        startup_effort: None,
        agent_name: "legacy-test-agent".into(),
        goose_system_prompt_supported: None,
        protocol_version: 1,
    };
    let channel_id = Uuid::new_v4();
    let event = EventBuilder::new(Kind::Custom(9), "start")
        .sign_with_keys(&Keys::generate())
        .unwrap();
    let batch = FlushBatch {
        channel_id,
        events: vec![crate::queue::BatchEvent {
            event,
            prompt_tag: "test".into(),
            received_at: std::time::Instant::now(),
        }],
        cancelled_events: Vec::new(),
        cancel_reason: None,
    };
    let mut ctx = make_prompt_context_no_owner();
    ctx.initial_message = Some("bootstrap".into());
    ctx.authority_fence = Some(authority_fence_then_fenced(2).await);
    let (result_tx, mut result_rx) = mpsc::unbounded_channel();

    run_prompt_task(
        agent,
        Some(batch),
        None,
        Arc::new(ctx),
        result_tx,
        None,
        "turn-initial-message-cutover".into(),
        Arc::new(Mutex::new(Vec::new())),
    )
    .await;

    let mut result = result_rx.recv().await.expect("prompt result");
    assert!(matches!(result.outcome, PromptOutcome::Error(_)));
    assert!(!result.agent.state.sessions.contains_key(&channel_id));
    result.agent.acp.shutdown().await;
    let requests: Vec<serde_json::Value> = std::fs::read_to_string(&capture)
        .expect("session/new request was captured")
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    std::fs::remove_file(capture).unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["method"], "session/new");
}
