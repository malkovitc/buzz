//! Tests for the OpenAI Realtime WebSocket adapter.

use super::*;

#[test]
fn session_configuration_disables_tools_and_pins_pcm() {
    let update = session_update(false);
    assert_eq!(update["session"]["tools"], serde_json::json!([]));
    assert_eq!(update["session"]["tool_choice"], "none");
    assert_eq!(
        update["session"]["audio"]["input"]["format"]["rate"],
        OPENAI_PCM_RATE
    );
    assert_eq!(
        update["session"]["audio"]["output"]["format"]["rate"],
        OPENAI_PCM_RATE
    );
    assert_eq!(
        update["session"]["audio"]["input"]["turn_detection"]["type"],
        "server_vad"
    );
    assert!(session_update(true)["session"]["audio"]["input"]["turn_detection"].is_null());
}

#[test]
fn credential_header_failure_does_not_expose_credential() {
    let credential = ProviderCredential::for_test("canary-secret\ninvalid");
    let error = authenticated_request(OPENAI_REALTIME_URL, &credential)
        .expect_err("invalid credential header");
    assert_eq!(
        error,
        "OpenAI Realtime credential cannot be used as a header"
    );
    assert!(!error.contains("canary-secret"));
}

#[tokio::test]
async fn fake_provider_exercises_auth_audio_and_barge_in_cancellation() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind fake provider");
    let address = listener.local_addr().expect("fake provider address");
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("provider accept");
        let mut socket = tokio_tungstenite::accept_async(stream)
            .await
            .expect("provider websocket");
        socket
            .send(Message::Text(
                serde_json::json!({"type": "session.created"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("session created");
        let update = socket
            .next()
            .await
            .expect("session update")
            .expect("valid update");
        let Message::Text(update) = update else {
            panic!("text session update")
        };
        let update: serde_json::Value =
            serde_json::from_slice(update.as_bytes()).expect("update json");
        assert_eq!(update["session"]["tool_choice"], "none");
        socket
            .send(Message::Text(
                serde_json::json!({"type": "session.updated"})
                    .to_string()
                    .into(),
            ))
            .await
            .expect("session updated");
        let input = socket
            .next()
            .await
            .expect("audio input")
            .expect("valid input");
        let Message::Text(input) = input else {
            panic!("text audio input")
        };
        let input: serde_json::Value =
            serde_json::from_slice(input.as_bytes()).expect("input json");
        assert_eq!(input["type"], "input_audio_buffer.append");
        for expected in ["input_audio_buffer.commit", "response.create"] {
            let turn = socket
                .next()
                .await
                .expect("input turn event")
                .expect("valid input turn event");
            let Message::Text(turn) = turn else {
                panic!("text input turn event")
            };
            let turn: serde_json::Value =
                serde_json::from_slice(turn.as_bytes()).expect("input turn json");
            assert_eq!(turn["type"], expected);
        }
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type": "response.created",
                    "response": {"id": "response-1", "status": "in_progress"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("response started");
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type": "response.output_audio.delta",
                    "delta": STANDARD.encode([1_u8, 0, 2, 0]),
                    "response_id": "response-1",
                    "item_id": "item-1",
                    "content_index": 0,
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("audio output");
        let cancel = socket
            .next()
            .await
            .expect("barge-in cancel")
            .expect("valid cancel");
        let Message::Text(cancel) = cancel else {
            panic!("text cancel")
        };
        let cancel: serde_json::Value =
            serde_json::from_slice(cancel.as_bytes()).expect("cancel json");
        assert_eq!(cancel["type"], "response.cancel");
        let truncate = socket
            .next()
            .await
            .expect("truncate")
            .expect("valid truncate");
        let Message::Text(truncate) = truncate else {
            panic!("text truncate")
        };
        let truncate: serde_json::Value =
            serde_json::from_slice(truncate.as_bytes()).expect("truncate json");
        assert_eq!(truncate["type"], "conversation.item.truncate");
        assert_eq!(truncate["item_id"], "item-1");
        assert_eq!(truncate["audio_end_ms"], 0);
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type": "response.output_audio.delta",
                    "delta": STANDARD.encode([3_u8, 0, 4, 0]),
                    "response_id": "response-1",
                    "item_id": "item-1",
                    "content_index": 0,
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("late cancelled audio");
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type": "response.done",
                    "response": {"id": "response-1", "status": "cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("cancelled response terminal");
        for expected in [
            "input_audio_buffer.append",
            "input_audio_buffer.commit",
            "response.create",
        ] {
            let message = socket
                .next()
                .await
                .expect("second turn event")
                .expect("valid second turn event");
            let Message::Text(message) = message else {
                panic!("text second turn event")
            };
            let message: serde_json::Value =
                serde_json::from_slice(message.as_bytes()).expect("second turn JSON");
            assert_eq!(message["type"], expected);
        }
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type": "response.created",
                    "response": {"id": "response-2", "status": "in_progress"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("second response started");
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type": "response.output_audio.delta",
                    "delta": STANDARD.encode([5_u8, 0, 6, 0]),
                    "response_id": "response-2",
                    "item_id": "item-2",
                    "content_index": 0,
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("second response audio");
        for event in [
            serde_json::json!({
                "type": "response.output_audio.done",
                "response_id": "response-2"
            }),
            serde_json::json!({
                "type": "response.done",
                "response": {"id": "response-2", "status": "completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("second response completion");
        }
        for expected in [
            "input_audio_buffer.append",
            "input_audio_buffer.commit",
            "response.create",
            "response.cancel",
        ] {
            let message = socket
                .next()
                .await
                .expect("third turn event")
                .expect("valid third turn event");
            let Message::Text(message) = message else {
                panic!("text third turn event")
            };
            let message: serde_json::Value =
                serde_json::from_slice(message.as_bytes()).expect("third turn JSON");
            assert_eq!(message["type"], expected);
        }
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type": "response.created",
                    "response": {"id": "response-3", "status": "in_progress"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("third response started");
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type": "response.output_audio.delta",
                    "delta": STANDARD.encode([7_u8, 0, 8, 0]),
                    "response_id": "response-3",
                    "item_id": "item-3",
                    "content_index": 0,
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("late third response audio");
        let deferred_input = socket
            .next()
            .await
            .expect("deferred audio input")
            .expect("valid deferred audio input");
        let Message::Text(deferred_input) = deferred_input else {
            panic!("text deferred audio input")
        };
        let deferred_input: serde_json::Value =
            serde_json::from_slice(deferred_input.as_bytes()).expect("deferred input JSON");
        assert_eq!(deferred_input["type"], "input_audio_buffer.append");
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(50), socket.next())
                .await
                .is_err()
        );
        socket
            .send(Message::Text(
                serde_json::json!({
                    "type": "response.done",
                    "response": {"id": "response-3", "status": "cancelled"}
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("third response terminal");
        for expected in ["input_audio_buffer.commit", "response.create"] {
            let message = socket
                .next()
                .await
                .expect("deferred turn event")
                .expect("valid deferred turn event");
            let Message::Text(message) = message else {
                panic!("text deferred turn event")
            };
            let message: serde_json::Value =
                serde_json::from_slice(message.as_bytes()).expect("deferred turn JSON");
            assert_eq!(message["type"], expected);
        }
        for event in [
            serde_json::json!({
                "type": "response.created",
                "response": {"id": "response-4", "status": "in_progress"}
            }),
            serde_json::json!({
                "type": "response.done",
                "response": {"id": "response-4", "status": "completed"}
            }),
        ] {
            socket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("deferred response lifecycle");
        }
        for expected in [
            "input_audio_buffer.append",
            "input_audio_buffer.commit",
            "response.create",
        ] {
            let message = socket
                .next()
                .await
                .expect("post-terminal turn event")
                .expect("valid post-terminal turn event");
            let Message::Text(message) = message else {
                panic!("text post-terminal turn event")
            };
            let message: serde_json::Value =
                serde_json::from_slice(message.as_bytes()).expect("post-terminal turn JSON");
            assert_eq!(message["type"], expected);
        }
    });

    let mut session = connect_to(
        &format!("ws://{address}"),
        ProviderCredential::for_test("fake-provider-key"),
        true,
        ProviderOutputFence::shared(),
        CancellationToken::new(),
    )
    .await
    .expect("connect fake provider");
    session
        .interrupt_output()
        .expect("first-turn idle PTT interruption");
    let first_turn_generation = session.output_fence().generation();
    session
        .input
        .send(ProviderInputFrame {
            samples_24k: vec![1, 2],
        })
        .await
        .expect("send provider audio");
    session.end_input_turn().expect("end provider input turn");
    assert!(matches!(
        session.events.recv().await,
        Some(ProviderEvent::OutputStarted { .. })
    ));
    let queued_generation = match session.events.recv().await {
        Some(ProviderEvent::Audio(frame)) => frame.output_generation,
        other => panic!("expected provider audio, got {other:?}"),
    };
    assert_eq!(queued_generation, first_turn_generation);
    session.interrupt_output().expect("PTT press interruption");
    assert!(!session.output_fence().admits(queued_generation));
    let terminal = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match session.events.recv().await {
                Some(ProviderEvent::Audio(_)) => panic!("cancelled response audio escaped"),
                Some(ProviderEvent::ResponseDone {
                    response_id,
                    completed: false,
                }) if response_id == "response-1" => break,
                Some(_) => {}
                None => panic!("provider events closed before cancellation terminal"),
            }
        }
    });
    terminal.await.expect("cancelled response terminal timeout");
    session
        .interrupt_output()
        .expect("between-turn idle PTT interruption");
    let second_turn_generation = session.output_fence().generation();
    assert!(second_turn_generation > first_turn_generation);
    session
        .input
        .send(ProviderInputFrame {
            samples_24k: vec![3, 4],
        })
        .await
        .expect("second provider input");
    session
        .end_input_turn()
        .expect("second provider input turn");
    assert!(matches!(
        session.events.recv().await,
        Some(ProviderEvent::OutputStarted { response_id }) if response_id == "response-2"
    ));
    let second_audio_generation = match session.events.recv().await {
        Some(ProviderEvent::Audio(frame)) => frame.output_generation,
        other => panic!("expected second provider audio, got {other:?}"),
    };
    assert_eq!(second_audio_generation, second_turn_generation);
    assert!(matches!(
        session.events.recv().await,
        Some(ProviderEvent::OutputDone { response_id }) if response_id == "response-2"
    ));
    assert!(matches!(
        session.events.recv().await,
        Some(ProviderEvent::ResponseDone {
            response_id,
            completed: true,
        }) if response_id == "response-2"
    ));
    session
        .input
        .send(ProviderInputFrame {
            samples_24k: vec![5, 6],
        })
        .await
        .expect("third provider input");
    session.end_input_turn().expect("third provider input turn");
    session
        .interrupt_output()
        .expect("awaiting-response interruption");
    let third_start = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match session.events.recv().await {
                Some(ProviderEvent::OutputStarted { response_id })
                    if response_id == "response-3" =>
                {
                    break;
                }
                Some(ProviderEvent::Audio(frame)) if frame.response_id == "response-3" => {
                    panic!("awaiting-response cancelled audio escaped")
                }
                Some(_) => {}
                None => panic!("provider events closed before third response start"),
            }
        }
    });
    third_start.await.expect("third response start timeout");
    session
        .input
        .send(ProviderInputFrame {
            samples_24k: vec![7, 8],
        })
        .await
        .expect("deferred provider input");
    session
        .end_input_turn()
        .expect("defer provider turn while cancellation is pending");
    let third_terminal = tokio::time::timeout(std::time::Duration::from_secs(2), async {
        loop {
            match session.events.recv().await {
                Some(ProviderEvent::Audio(frame)) if frame.response_id == "response-3" => {
                    panic!("awaiting-response cancelled audio escaped")
                }
                Some(ProviderEvent::ResponseDone {
                    response_id,
                    completed: false,
                }) if response_id == "response-3" => break,
                Some(_) => {}
                None => panic!("provider events closed before third terminal"),
            }
        }
    });
    third_terminal
        .await
        .expect("third response terminal timeout");
    assert!(matches!(
        session.events.recv().await,
        Some(ProviderEvent::OutputStarted { response_id }) if response_id == "response-4"
    ));
    assert!(matches!(
        session.events.recv().await,
        Some(ProviderEvent::ResponseDone {
            response_id,
            completed: true,
        }) if response_id == "response-4"
    ));
    session
        .interrupt_output()
        .expect("post-terminal idle interruption");
    session
        .input
        .send(ProviderInputFrame {
            samples_24k: vec![9, 10],
        })
        .await
        .expect("post-terminal provider input");
    session
        .end_input_turn()
        .expect("post-terminal provider input turn");
    tokio::time::timeout(std::time::Duration::from_secs(2), server)
        .await
        .expect("PTT interruption timeout")
        .expect("fake provider task");
    let _ = session.close().await;
}

#[tokio::test]
#[ignore = "requires an explicitly supplied spend-capped OPENAI_API_KEY"]
async fn credentialed_session_update_smoke() {
    let mut session = OpenAiRealtimeSession::connect_with_output_fence(
        false,
        ProviderOutputFence::shared(),
        CancellationToken::new(),
    )
    .await
    .expect("credentialed realtime session");
    session.close().await.expect("clean provider close");
}
