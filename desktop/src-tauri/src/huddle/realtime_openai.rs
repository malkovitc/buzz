//! OpenAI Realtime WebSocket adapter.
//!
//! This is the only module that knows OpenAI event names or authentication.
//! It emits only the bounded provider-neutral events validated by
//! `realtime_voice`.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{
        client::IntoClientRequest,
        http::{header::AUTHORIZATION, HeaderValue, Request},
        protocol::WebSocketConfig,
        Message,
    },
};
use tokio_util::sync::CancellationToken;

use super::realtime_voice::{
    validate_openai_event, ProviderCredential, ProviderEvent, ProviderInputFrame,
    ProviderOutputFence, MAX_PROVIDER_MESSAGE_BYTES, OPENAI_PCM_RATE,
};

const OPENAI_REALTIME_URL: &str = "wss://api.openai.com/v1/realtime?model=gpt-realtime-2.1";
const PROVIDER_READY_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
const PROVIDER_CLOSE_FRAMES_TIMEOUT: std::time::Duration = std::time::Duration::from_millis(500);
const PROVIDER_INPUT_QUEUE_DEPTH: usize = 8;
const PROVIDER_EVENT_QUEUE_DEPTH: usize = 16;

#[derive(Clone, Copy)]
enum ProviderControl {
    EndInputTurn,
    InterruptOutput,
}

/// One bounded OpenAI session owned by the Tauri host process.
pub(crate) struct OpenAiRealtimeSession {
    pub(crate) input: mpsc::Sender<ProviderInputFrame>,
    controls: mpsc::Sender<ProviderControl>,
    output_fence: std::sync::Arc<ProviderOutputFence>,
    pub(crate) events: mpsc::Receiver<ProviderEvent>,
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl OpenAiRealtimeSession {
    pub(crate) async fn connect_with_output_fence(
        manual_input_turns: bool,
        output_fence: std::sync::Arc<ProviderOutputFence>,
        cancel: CancellationToken,
    ) -> Result<Self, String> {
        connect_to(
            OPENAI_REALTIME_URL,
            ProviderCredential::from_env()?,
            manual_input_turns,
            output_fence,
            cancel,
        )
        .await
    }

    pub(crate) fn end_input_turn(&self) -> Result<(), String> {
        self.send_control(ProviderControl::EndInputTurn)
    }

    #[cfg(test)]
    pub(crate) fn interrupt_output(&self) -> Result<(), String> {
        self.output_fence.advance();
        self.interrupt_output_after_fence()
    }

    pub(crate) fn interrupt_output_after_fence(&self) -> Result<(), String> {
        self.send_control(ProviderControl::InterruptOutput)
    }

    pub(crate) fn output_fence(&self) -> std::sync::Arc<ProviderOutputFence> {
        std::sync::Arc::clone(&self.output_fence)
    }

    fn send_control(&self, control: ProviderControl) -> Result<(), String> {
        match self.controls.try_send(control) {
            Ok(()) => Ok(()),
            Err(mpsc::error::TrySendError::Full(_)) => {
                Err("provider control queue is full".to_string())
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                Err("provider control queue is closed".to_string())
            }
        }
    }

    pub(crate) fn cancel(&self) {
        self.cancel.cancel();
    }

    pub(crate) async fn close(&mut self) -> Result<(), String> {
        self.cancel();
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match tokio::time::timeout(std::time::Duration::from_secs(2), &mut task).await {
            Ok(result) => {
                result.map_err(|_| "provider session task did not stop cleanly".to_string())?
            }
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err("provider session close timed out".to_string())
            }
        }
    }
}

impl Drop for OpenAiRealtimeSession {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn provider_socket_config() -> WebSocketConfig {
    WebSocketConfig::default()
        .read_buffer_size(MAX_PROVIDER_MESSAGE_BYTES)
        .write_buffer_size(16 * 1024)
        .max_write_buffer_size(128 * 1024)
        .max_message_size(Some(MAX_PROVIDER_MESSAGE_BYTES))
        .max_frame_size(Some(MAX_PROVIDER_MESSAGE_BYTES))
}

fn authenticated_request(
    url: &str,
    credential: &ProviderCredential,
) -> Result<Request<()>, String> {
    let mut request = url
        .into_client_request()
        .map_err(|_| "OpenAI Realtime endpoint is invalid".to_string())?;
    let authorization = HeaderValue::from_str(&format!("Bearer {}", credential.expose()))
        .map_err(|_| "OpenAI Realtime credential cannot be used as a header".to_string())?;
    request.headers_mut().insert(AUTHORIZATION, authorization);
    Ok(request)
}

async fn connect_to(
    url: &str,
    credential: ProviderCredential,
    manual_input_turns: bool,
    output_fence: std::sync::Arc<ProviderOutputFence>,
    cancel: CancellationToken,
) -> Result<OpenAiRealtimeSession, String> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let request = authenticated_request(url, &credential)?;
    let (mut socket, _) = tokio::time::timeout(
        PROVIDER_READY_TIMEOUT,
        connect_async_with_config(request, Some(provider_socket_config()), true),
    )
    .await
    .map_err(|_| "OpenAI Realtime connection timed out".to_string())?
    .map_err(|_| "OpenAI Realtime connection failed".to_string())?;
    drop(credential);

    await_ready_event(&mut socket, "session.created").await?;
    socket
        .send(Message::Text(
            session_update(manual_input_turns).to_string().into(),
        ))
        .await
        .map_err(|_| "OpenAI Realtime session configuration failed".to_string())?;
    await_ready_event(&mut socket, "session.updated").await?;

    let (input, input_rx) = mpsc::channel(PROVIDER_INPUT_QUEUE_DEPTH);
    let (controls, control_rx) = mpsc::channel(2);
    let (event_tx, events) = mpsc::channel(PROVIDER_EVENT_QUEUE_DEPTH);
    let task_cancel = cancel.clone();
    let task = tokio::spawn(run_socket(
        socket,
        input_rx,
        control_rx,
        event_tx,
        manual_input_turns,
        std::sync::Arc::clone(&output_fence),
        task_cancel,
    ));
    Ok(OpenAiRealtimeSession {
        input,
        controls,
        output_fence,
        events,
        cancel,
        task: Some(task),
    })
}

fn session_update(manual_input_turns: bool) -> serde_json::Value {
    let turn_detection = if manual_input_turns {
        serde_json::Value::Null
    } else {
        serde_json::json!({ "type": "server_vad" })
    };
    serde_json::json!({
        "type": "session.update",
        "session": {
            "type": "realtime",
            "model": "gpt-realtime-2.1",
            "instructions": "Participate in this voice conversation. Be concise and do not call tools.",
            "output_modalities": ["audio"],
            "audio": {
                "input": {
                    "format": { "type": "audio/pcm", "rate": OPENAI_PCM_RATE },
                    "turn_detection": turn_detection
                },
                "output": {
                    "format": { "type": "audio/pcm", "rate": OPENAI_PCM_RATE },
                    "voice": "marin"
                }
            },
            "tools": [],
            "tool_choice": "none"
        }
    })
}

async fn await_ready_event(
    socket: &mut super::relay_api::WsStream,
    expected_type: &str,
) -> Result<(), String> {
    tokio::time::timeout(PROVIDER_READY_TIMEOUT, async {
        loop {
            let message = socket
                .next()
                .await
                .ok_or_else(|| "OpenAI Realtime closed during startup".to_string())?
                .map_err(|_| "OpenAI Realtime startup read failed".to_string())?;
            let Message::Text(text) = message else {
                continue;
            };
            let event: serde_json::Value = serde_json::from_slice(text.as_bytes())
                .map_err(|_| "OpenAI Realtime startup event is malformed".to_string())?;
            let event_type = event.get("type").and_then(serde_json::Value::as_str);
            if event_type == Some(expected_type) {
                return Ok(());
            }
            if event_type == Some("error") {
                return Err("OpenAI Realtime rejected session startup".to_string());
            }
        }
    })
    .await
    .map_err(|_| "OpenAI Realtime startup timed out".to_string())?
}

fn encode_input(frame: ProviderInputFrame) -> Message {
    let pcm: Vec<u8> = frame
        .samples_24k
        .into_iter()
        .flat_map(i16::to_le_bytes)
        .collect();
    Message::Text(
        serde_json::json!({
            "type": "input_audio_buffer.append",
            "audio": STANDARD.encode(pcm),
        })
        .to_string()
        .into(),
    )
}

fn interrupts_active_output(event: &ProviderEvent, output_active: bool) -> bool {
    output_active && matches!(event, ProviderEvent::InputSpeechStarted)
}

fn should_commit_input_turn(manual_input_turns: bool, has_uncommitted_input: bool) -> bool {
    manual_input_turns && has_uncommitted_input
}

async fn send_provider_control(
    socket: &mut futures_util::stream::SplitSink<super::relay_api::WsStream, Message>,
    message: Message,
    cancel: &CancellationToken,
    failure: &str,
) -> Result<(), String> {
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err("provider session cancelled".to_string()),
        result = socket.send(message) => result.map_err(|_| failure.to_string()),
    }
}

async fn cancel_active_output(
    socket: &mut futures_util::stream::SplitSink<super::relay_api::WsStream, Message>,
    output_item: Option<&(String, u32)>,
    cancel: &CancellationToken,
) -> Result<(), String> {
    send_provider_control(
        socket,
        Message::Text(
            serde_json::json!({"type": "response.cancel"})
                .to_string()
                .into(),
        ),
        cancel,
        "OpenAI Realtime response cancellation failed",
    )
    .await?;
    if let Some((item_id, content_index)) = output_item {
        send_provider_control(
            socket,
            Message::Text(
                serde_json::json!({
                    "type": "conversation.item.truncate",
                    "item_id": item_id,
                    "content_index": content_index,
                    "audio_end_ms": 0
                })
                .to_string()
                .into(),
            ),
            cancel,
            "OpenAI Realtime output truncation failed",
        )
        .await?;
    }
    Ok(())
}

fn response_cancellation_pending(
    blocked_response: &Option<String>,
    block_next_response: bool,
) -> bool {
    blocked_response.is_some() || block_next_response
}

fn should_start_deferred_turn(pending_manual_turn: bool, has_uncommitted_input: bool) -> bool {
    pending_manual_turn && has_uncommitted_input
}

async fn commit_and_start_response(
    socket: &mut futures_util::stream::SplitSink<super::relay_api::WsStream, Message>,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let commit = Message::Text(
        serde_json::json!({"type": "input_audio_buffer.commit"})
            .to_string()
            .into(),
    );
    send_provider_control(
        socket,
        commit,
        cancel,
        "OpenAI Realtime input commit failed",
    )
    .await?;
    let create = Message::Text(
        serde_json::json!({"type": "response.create"})
            .to_string()
            .into(),
    );
    send_provider_control(
        socket,
        create,
        cancel,
        "OpenAI Realtime response start failed",
    )
    .await
}

async fn run_socket(
    socket: super::relay_api::WsStream,
    mut input_rx: mpsc::Receiver<ProviderInputFrame>,
    mut control_rx: mpsc::Receiver<ProviderControl>,
    event_tx: mpsc::Sender<ProviderEvent>,
    manual_input_turns: bool,
    output_fence: std::sync::Arc<ProviderOutputFence>,
    cancel: CancellationToken,
) -> Result<(), String> {
    let (mut socket_tx, mut socket_rx) = socket.split();
    let mut output_active = false;
    let mut output_item: Option<(String, u32)> = None;
    let mut active_response: Option<String> = None;
    let mut blocked_response: Option<String> = None;
    let mut block_next_response = false;
    let mut accepted_output_generation = output_fence.generation();
    let mut has_uncommitted_input = false;
    let mut pending_manual_turn = false;
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => {
                let close_frames = async {
                    let _ = socket_tx.send(Message::Text(
                        serde_json::json!({"type": "response.cancel"}).to_string().into()
                    )).await;
                    let _ = socket_tx.send(Message::Close(None)).await;
                };
                let _ = tokio::time::timeout(PROVIDER_CLOSE_FRAMES_TIMEOUT, close_frames).await;
                return Ok(());
            }
            input = input_rx.recv() => {
                let Some(input) = input else { return Ok(()) };
                tokio::select! {
                    biased;
                    _ = cancel.cancelled() => return Ok(()),
                    result = socket_tx.send(encode_input(input)) => {
                        result.map_err(|_| "OpenAI Realtime audio send failed".to_string())?;
                    }
                }
                has_uncommitted_input = true;
            }
            control = control_rx.recv() => {
                let Some(control) = control else { return Ok(()) };
                match control {
                    ProviderControl::EndInputTurn => {
                        if !should_commit_input_turn(manual_input_turns, has_uncommitted_input) {
                            continue;
                        }
                        if response_cancellation_pending(&blocked_response, block_next_response) {
                            pending_manual_turn = true;
                            continue;
                        }
                        commit_and_start_response(&mut socket_tx, &cancel).await?;
                        output_active = true;
                        active_response = None;
                        output_item = None;
                        has_uncommitted_input = false;
                    }
                    ProviderControl::InterruptOutput => {
                        accepted_output_generation = output_fence.generation();
                        if output_active {
                            blocked_response = active_response.take();
                            block_next_response = blocked_response.is_none();
                            cancel_active_output(&mut socket_tx, output_item.as_ref(), &cancel).await?;
                            output_active = false;
                            output_item = None;
                        }
                    }
                }
            }
            message = socket_rx.next() => {
                let message = message
                    .ok_or_else(|| "OpenAI Realtime connection closed".to_string())?
                    .map_err(|_| "OpenAI Realtime receive failed".to_string())?;
                match message {
                    Message::Text(text) => {
                        if let Some(mut event) = validate_openai_event(text.as_bytes())? {
                            let mut start_deferred_turn = false;
                            if interrupts_active_output(&event, output_active) {
                                output_fence.advance();
                                accepted_output_generation = output_fence.generation();
                                blocked_response = active_response.take();
                                block_next_response = blocked_response.is_none();
                                cancel_active_output(&mut socket_tx, output_item.as_ref(), &cancel).await?;
                            }
                            if block_next_response {
                                if let ProviderEvent::OutputStarted { response_id } = &event {
                                    blocked_response = Some(response_id.clone());
                                    block_next_response = false;
                                }
                            }
                            let blocked_audio = matches!(
                                &event,
                                ProviderEvent::Audio(frame)
                                    if blocked_response.as_ref() == Some(&frame.response_id)
                            );
                            if blocked_audio {
                                continue;
                            }
                            match &mut event {
                                ProviderEvent::InputSpeechStopped => {
                                    has_uncommitted_input = false;
                                }
                                ProviderEvent::OutputStarted { response_id } => {
                                    let response_is_blocked =
                                        blocked_response.as_ref() == Some(response_id);
                                    if response_is_blocked {
                                        output_active = false;
                                        output_item = None;
                                    } else {
                                        active_response = Some(response_id.clone());
                                        output_active = true;
                                    }
                                }
                                ProviderEvent::Audio(frame) => {
                                    frame.output_generation = accepted_output_generation;
                                    output_active = true;
                                    output_item = Some((frame.item_id.clone(), frame.content_index));
                                }
                                ProviderEvent::OutputDone { .. } => {
                                    output_active = false;
                                    output_item = None;
                                }
                                ProviderEvent::ResponseDone { response_id, .. }
                                    if blocked_response.as_ref() == Some(response_id) =>
                                {
                                    blocked_response = None;
                                    let terminal_is_active =
                                        active_response.as_ref() == Some(response_id);
                                    if terminal_is_active {
                                        active_response = None;
                                        output_active = false;
                                        output_item = None;
                                    }
                                    start_deferred_turn = should_start_deferred_turn(
                                        pending_manual_turn,
                                        has_uncommitted_input,
                                    );
                                }
                                ProviderEvent::ResponseDone { response_id, .. }
                                    if active_response.as_ref() == Some(response_id) =>
                                {
                                    active_response = None;
                                    output_active = false;
                                    output_item = None;
                                }
                                _ => {}
                            }
                            if start_deferred_turn {
                                commit_and_start_response(&mut socket_tx, &cancel).await?;
                                output_active = true;
                                active_response = None;
                                output_item = None;
                                has_uncommitted_input = false;
                                pending_manual_turn = false;
                            }
                            event_tx.send(event).await
                                .map_err(|_| "provider event consumer closed".to_string())?;
                        }
                    }
                    Message::Ping(payload) => {
                        socket_tx.send(Message::Pong(payload)).await
                            .map_err(|_| "OpenAI Realtime pong failed".to_string())?;
                    }
                    Message::Close(_) => return Err("OpenAI Realtime connection closed".to_string()),
                    _ => {}
                }
            }
        }
    }
}

#[cfg(test)]
#[path = "realtime_openai_tests.rs"]
mod tests;
