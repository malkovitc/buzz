//! Freshness-bounded managed-agent Huddle audio publisher.

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use futures_util::{SinkExt, StreamExt};
use tokio_tungstenite::tungstenite::Message as WsMsg;
use tokio_util::sync::CancellationToken;

use crate::app_state::AppState;

use super::realtime_voice::{ProviderOutputFence, ProviderOutputSendPermit};
use super::relay_api::{
    connect_authenticated_audio_socket, upsample_tts_24k_to_48k, HuddleOpusEncoder, WsReceiver,
    WsSink,
};

const REALTIME_BROADCAST_MAX_FRAMES: usize = 10; // Keep at most 200 ms.

/// Result of attempting to enqueue one provider audio delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub(super) enum RealtimePublishOutcome {
    Enqueued,
    Cancelled,
    StaleGeneration,
}

/// Send-only Huddle peer for provider-returned audio.
pub(crate) struct RealtimeAudioPublisher {
    queue: Arc<Mutex<RealtimeFrameQueue>>,
    output_fence: Arc<ProviderOutputFence>,
    cancel: CancellationToken,
    task: Option<tokio::task::JoinHandle<Result<(), String>>>,
}

impl RealtimeAudioPublisher {
    pub(crate) fn publish(
        &self,
        output_generation: u64,
        samples_24k: &[i16],
    ) -> RealtimePublishOutcome {
        if self.cancel.is_cancelled() {
            return RealtimePublishOutcome::Cancelled;
        }
        if !self.output_fence.admits(output_generation) {
            return RealtimePublishOutcome::StaleGeneration;
        }
        let normalized_24k: Vec<f32> = samples_24k
            .iter()
            .map(|sample| *sample as f32 / 32_768.0)
            .collect();
        let samples_48k = upsample_tts_24k_to_48k(&normalized_24k);
        self.queue
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .append(output_generation, &samples_48k);
        RealtimePublishOutcome::Enqueued
    }

    pub(crate) fn finish_output(&self) {
        self.queue
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .finish_output();
    }

    pub(crate) fn clear(&self) {
        self.queue
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clear();
    }

    pub(crate) fn failure_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    pub(crate) async fn wait_for_output_quiescence(&self) {
        self.output_fence.wait_for_quiescence().await;
    }

    pub(crate) async fn close(&mut self) -> Result<(), String> {
        self.cancel.cancel();
        self.clear();
        let Some(mut task) = self.task.take() else {
            return Ok(());
        };
        match tokio::time::timeout(std::time::Duration::from_secs(2), &mut task).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("realtime publisher task failed".to_string()),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err("realtime publisher close timed out".to_string())
            }
        }
    }
}

impl Drop for RealtimeAudioPublisher {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

struct QueuedAudioFrame {
    output_generation: u64,
    samples: Vec<f32>,
}

#[derive(Default)]
struct RealtimeFrameQueue {
    frames: VecDeque<QueuedAudioFrame>,
    partial: Vec<f32>,
    partial_generation: Option<u64>,
}

impl RealtimeFrameQueue {
    fn push_frame(&mut self, output_generation: u64, samples: Vec<f32>) {
        while self.frames.len() >= REALTIME_BROADCAST_MAX_FRAMES {
            self.frames.pop_front();
        }
        self.frames.push_back(QueuedAudioFrame {
            output_generation,
            samples,
        });
    }

    fn append(&mut self, output_generation: u64, mut samples: &[f32]) {
        if self.partial_generation != Some(output_generation) {
            self.partial.clear();
            self.partial_generation = Some(output_generation);
        }
        if !self.partial.is_empty() {
            let needed = 960 - self.partial.len();
            let take = needed.min(samples.len());
            self.partial.extend_from_slice(&samples[..take]);
            samples = &samples[take..];
            if self.partial.len() == 960 {
                let frame = std::mem::take(&mut self.partial);
                self.push_frame(output_generation, frame);
            }
        }
        let mut chunks = samples.chunks_exact(960);
        for chunk in &mut chunks {
            self.push_frame(output_generation, chunk.to_vec());
        }
        self.partial.extend_from_slice(chunks.remainder());
    }

    fn finish_output(&mut self) {
        if self.partial.is_empty() {
            return;
        }
        let mut frame = std::mem::take(&mut self.partial);
        frame.resize(960, 0.0);
        let output_generation = self.partial_generation.take().unwrap_or_default();
        self.push_frame(output_generation, frame);
    }

    fn clear(&mut self) {
        self.frames.clear();
        self.partial.clear();
        self.partial_generation = None;
    }

    fn pop_front(&mut self) -> Option<QueuedAudioFrame> {
        self.frames.pop_front()
    }
}

pub(crate) async fn connect_realtime_audio_publisher(
    channel_id: &str,
    parent_channel_id: Option<&str>,
    state: &AppState,
    keys: &nostr::Keys,
    auth_tag_json: Option<&str>,
    output_fence: Arc<ProviderOutputFence>,
    cancel: CancellationToken,
) -> Result<RealtimeAudioPublisher, String> {
    let relay_url = crate::relay::relay_ws_url_with_override(state);
    connect_realtime_audio_publisher_at(
        channel_id,
        parent_channel_id,
        &relay_url,
        keys,
        auth_tag_json,
        output_fence,
        cancel,
    )
    .await
}

pub(super) async fn connect_realtime_audio_publisher_at(
    channel_id: &str,
    parent_channel_id: Option<&str>,
    relay_url: &str,
    keys: &nostr::Keys,
    auth_tag_json: Option<&str>,
    output_fence: Arc<ProviderOutputFence>,
    cancel: CancellationToken,
) -> Result<RealtimeAudioPublisher, String> {
    let (ws_tx, ws_rx, _, _) = connect_authenticated_audio_socket(
        channel_id,
        parent_channel_id,
        relay_url,
        keys,
        auth_tag_json,
    )
    .await?;
    let task_cancel = cancel.clone();
    let queue = Arc::new(Mutex::new(RealtimeFrameQueue::default()));
    let task_queue = Arc::clone(&queue);
    let task_output_fence = Arc::clone(&output_fence);
    let worker_cancel = task_cancel.clone();
    let worker = tokio::spawn(async move {
        run_realtime_audio_publisher(ws_tx, ws_rx, task_queue, task_output_fence, &worker_cancel)
            .await
    });
    let task = tokio::spawn(supervise_publisher_task(worker, task_cancel));
    Ok(RealtimeAudioPublisher {
        queue,
        output_fence,
        cancel,
        task: Some(task),
    })
}

async fn supervise_publisher_task(
    worker: tokio::task::JoinHandle<Result<(), String>>,
    failure_token: CancellationToken,
) -> Result<(), String> {
    let result = worker
        .await
        .map_err(|_| "realtime publisher worker failed".to_string());
    failure_token.cancel();
    result?
}

#[derive(Debug, PartialEq, Eq)]
enum AuthorizedSendOutcome {
    Sent,
    TransportDiscardRequired,
}

async fn send_authorized_frame<S>(
    socket: &mut S,
    payload: Vec<u8>,
    _permit: ProviderOutputSendPermit,
    cancel: &CancellationToken,
) -> Result<AuthorizedSendOutcome, String>
where
    S: futures_util::Sink<WsMsg> + Unpin,
{
    tokio::select! {
        biased;
        _ = cancel.cancelled() => Ok(AuthorizedSendOutcome::TransportDiscardRequired),
        result = socket.send(WsMsg::Binary(payload.into())) => {
            result
                .map(|_| AuthorizedSendOutcome::Sent)
                .map_err(|_| "agent audio socket send failed".to_string())
        }
    }
}

async fn run_realtime_audio_publisher(
    mut ws_tx: WsSink,
    mut ws_rx: WsReceiver,
    queue: Arc<Mutex<RealtimeFrameQueue>>,
    output_fence: Arc<ProviderOutputFence>,
    cancel: &CancellationToken,
) -> Result<(), String> {
    let mut encoder = HuddleOpusEncoder::new("realtime")?;
    let mut send_tick = tokio::time::interval(std::time::Duration::from_millis(20));
    send_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = send_tick.tick() => {
                let frame = queue.lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .pop_front();
                let Some(frame) = frame else { continue };
                let Some(payload) = encoder.encode(&frame.samples)? else { continue };
                let Some(send_permit) = output_fence.begin_send(frame.output_generation) else {
                    continue;
                };
                match send_authorized_frame(
                    &mut ws_tx,
                    payload,
                    send_permit,
                    cancel,
                )
                .await?
                {
                    AuthorizedSendOutcome::Sent => {}
                    AuthorizedSendOutcome::TransportDiscardRequired => return Ok(()),
                }
            }
            message = ws_rx.next() => match message {
                Some(Ok(WsMsg::Ping(data))) => {
                    ws_tx.send(WsMsg::Pong(data)).await
                        .map_err(|_| "agent audio socket pong failed".to_string())?;
                }
                Some(Ok(WsMsg::Close(_))) | None => {
                    return Err("agent audio socket closed".to_string());
                }
                Some(Err(_)) => return Err("agent audio socket receive failed".to_string()),
                Some(Ok(_)) => {}
            }
        }
    }
    let _ = ws_tx.send(WsMsg::Close(None)).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct PendingSinkState {
        buffered: std::sync::atomic::AtomicBool,
        close_polled: std::sync::atomic::AtomicBool,
    }

    struct PendingFlushSink(std::sync::Arc<PendingSinkState>);

    impl futures_util::Sink<WsMsg> for PendingFlushSink {
        type Error = ();

        fn poll_ready(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }

        fn start_send(self: std::pin::Pin<&mut Self>, _item: WsMsg) -> Result<(), Self::Error> {
            self.0
                .buffered
                .store(true, std::sync::atomic::Ordering::Release);
            Ok(())
        }

        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Pending
        }

        fn poll_close(
            self: std::pin::Pin<&mut Self>,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            self.0
                .close_polled
                .store(true, std::sync::atomic::Ordering::Release);
            std::task::Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn cancelled_pending_send_requires_transport_discard_without_close_flush() {
        let state = std::sync::Arc::new(PendingSinkState::default());
        let sink_state = std::sync::Arc::clone(&state);
        let fence = ProviderOutputFence::shared();
        let permit = fence.begin_send(0).expect("send permit");
        let cancel = CancellationToken::new();
        let task_cancel = cancel.clone();
        let send = tokio::spawn(async move {
            let mut sink = PendingFlushSink(sink_state);
            send_authorized_frame(&mut sink, vec![1], permit, &task_cancel).await
        });
        while !state.buffered.load(std::sync::atomic::Ordering::Acquire) {
            tokio::task::yield_now().await;
        }

        cancel.cancel();

        assert_eq!(
            send.await.expect("send task").expect("send outcome"),
            AuthorizedSendOutcome::TransportDiscardRequired
        );
        assert!(!state
            .close_polled
            .load(std::sync::atomic::Ordering::Acquire));
        fence.wait_for_quiescence().await;
    }

    #[tokio::test]
    async fn close_propagates_publisher_failure_and_join_error() {
        let publisher_with_task = |task| RealtimeAudioPublisher {
            queue: Arc::new(Mutex::new(RealtimeFrameQueue::default())),
            output_fence: ProviderOutputFence::shared(),
            cancel: CancellationToken::new(),
            task: Some(task),
        };

        let failed_task = tokio::spawn(async { Err("socket failed".to_string()) });
        let mut failed = publisher_with_task(failed_task);
        assert_eq!(failed.close().await, Err("socket failed".to_string()));

        let failure_token = CancellationToken::new();
        let observed_failure = failure_token.clone();
        let panicked_worker = tokio::spawn(async {
            panic!("publisher panic");
            #[allow(unreachable_code)]
            Ok(())
        });
        let supervised_task =
            tokio::spawn(supervise_publisher_task(panicked_worker, failure_token));
        let mut panicked = publisher_with_task(supervised_task);
        assert_eq!(
            panicked.close().await,
            Err("realtime publisher worker failed".to_string())
        );
        assert!(observed_failure.is_cancelled());
    }

    #[test]
    fn publish_reports_enqueued_cancelled_and_stale_outcomes() {
        let publisher = |output_fence, cancel| RealtimeAudioPublisher {
            queue: Arc::new(Mutex::new(RealtimeFrameQueue::default())),
            output_fence,
            cancel,
            task: None,
        };

        let accepted = publisher(ProviderOutputFence::shared(), CancellationToken::new());
        assert_eq!(
            accepted.publish(0, &[1; 480]),
            RealtimePublishOutcome::Enqueued
        );

        let cancelled_token = CancellationToken::new();
        cancelled_token.cancel();
        let cancelled = publisher(ProviderOutputFence::shared(), cancelled_token);
        assert_eq!(
            cancelled.publish(0, &[1; 480]),
            RealtimePublishOutcome::Cancelled
        );

        let stale_fence = ProviderOutputFence::shared();
        stale_fence.advance();
        let stale = publisher(stale_fence, CancellationToken::new());
        assert_eq!(
            stale.publish(0, &[1; 480]),
            RealtimePublishOutcome::StaleGeneration
        );
    }

    #[test]
    fn queue_keeps_only_the_freshest_two_hundred_ms() {
        let mut queue = RealtimeFrameQueue::default();
        let samples: Vec<f32> = (0..12)
            .flat_map(|frame| std::iter::repeat_n(frame as f32, 960))
            .collect();
        queue.append(0, &samples);
        assert_eq!(queue.frames.len(), REALTIME_BROADCAST_MAX_FRAMES);
        assert_eq!(
            queue.frames.front().and_then(|frame| frame.samples.first()),
            Some(&2.0)
        );
        assert_eq!(
            queue.frames.back().and_then(|frame| frame.samples.first()),
            Some(&11.0)
        );
    }

    #[test]
    fn preserves_partial_audio_across_provider_deltas() {
        let mut queue = RealtimeFrameQueue::default();
        queue.append(0, &vec![1.0; 500]);
        queue.append(0, &vec![2.0; 500]);

        assert_eq!(queue.frames.len(), 1);
        assert_eq!(queue.partial.len(), 40);
        let frame = queue.frames.front().expect("complete frame");
        assert!(frame.samples[..500].iter().all(|sample| *sample == 1.0));
        assert!(frame.samples[500..].iter().all(|sample| *sample == 2.0));

        queue.finish_output();
        assert_eq!(queue.frames.len(), 2);
        assert!(queue.partial.is_empty());
    }

    #[tokio::test]
    async fn output_fence_rejects_stale_frames_and_acknowledges_in_flight_send() {
        let fence = ProviderOutputFence::shared();
        let permit = fence.begin_send(0).expect("initial send permit");
        fence.advance();
        assert!(fence.begin_send(0).is_none());
        assert!(fence.begin_send(1).is_some());
        assert!(tokio::time::timeout(
            std::time::Duration::from_millis(10),
            fence.wait_for_quiescence()
        )
        .await
        .is_err());

        drop(permit);

        tokio::time::timeout(
            std::time::Duration::from_millis(100),
            fence.wait_for_quiescence(),
        )
        .await
        .expect("in-flight send quiescence");
    }

    #[tokio::test]
    async fn authenticates_and_sends_v2_opus_to_an_isolated_relay() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind isolated relay");
        let address = listener.local_addr().expect("relay address");
        let relay_url = format!("ws://{address}");
        let keys = nostr::Keys::generate();
        let expected_pubkey = keys.public_key().to_hex();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("relay accept");
            let mut socket = tokio_tungstenite::accept_async(stream)
                .await
                .expect("relay websocket");
            socket
                .send(WsMsg::Text(
                    serde_json::json!({"type": "challenge", "challenge": "isolated"})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("relay challenge");
            let auth = socket
                .next()
                .await
                .expect("auth message")
                .expect("valid auth");
            let WsMsg::Text(auth) = auth else {
                panic!("text auth")
            };
            let auth: serde_json::Value =
                serde_json::from_slice(auth.as_bytes()).expect("auth json");
            assert_eq!(auth["event"]["pubkey"], expected_pubkey);
            assert_eq!(
                auth["protocol_version"],
                super::super::wire::PROTOCOL_VERSION
            );
            socket
                .send(WsMsg::Text(
                    serde_json::json!({"type": "joined", "peer_index": 1, "peers": []})
                        .to_string()
                        .into(),
                ))
                .await
                .expect("relay joined");
            let audio = tokio::time::timeout(std::time::Duration::from_secs(2), socket.next())
                .await
                .expect("audio deadline")
                .expect("audio message")
                .expect("valid audio");
            let WsMsg::Binary(audio) = audio else {
                panic!("binary audio")
            };
            let (header, opus) =
                super::super::wire::FrameHeader::parse(&audio).expect("v2 frame header");
            assert_eq!(header.seq, 0);
            assert_eq!(header.ts_48k, 0);
            assert!(!opus.is_empty());
        });

        let output_fence = ProviderOutputFence::shared();
        let mut publisher = connect_realtime_audio_publisher_at(
            &uuid::Uuid::new_v4().to_string(),
            None,
            &relay_url,
            &keys,
            None,
            output_fence,
            CancellationToken::new(),
        )
        .await
        .expect("publisher connection");
        assert_eq!(
            publisher.publish(0, &vec![1_000; 480]),
            RealtimePublishOutcome::Enqueued
        );
        server.await.expect("isolated relay task");
        publisher.close().await.expect("publisher close");
    }
}
