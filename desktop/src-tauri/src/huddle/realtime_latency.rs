//! Credentialed full-path latency acceptance for VOICE 2.
//! Each attempt owns a fresh provider session and a fresh two-peer Huddle
//! transport, so no response, WebSocket, jitter, or playout state crosses an
//! attempt boundary.

use std::time::{Duration, Instant};

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use super::{
    realtime_openai::OpenAiRealtimeSession,
    realtime_publisher::{
        connect_realtime_audio_publisher_at, RealtimeAudioPublisher, RealtimePublishOutcome,
    },
    realtime_voice::{ProviderEvent, ProviderInputFrame, ProviderOutputFence},
};

const ATTEMPTS: usize = 20;
const COMPLETION_PERCENT_MIN: usize = 95;
const FULL_PATH_MEDIAN_MAX_MS: u64 = 1_500;
const FULL_PATH_P95_MAX_MS: u64 = 3_000;
const BUZZ_LOCAL_P95_MAX_MS: u64 = 100;
const WHOLE_ATTEMPT_TIMEOUT: Duration = Duration::from_secs(20);
const ROUTE_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
const SPEECH_FIXTURE: &str = "https://raw.githubusercontent.com/openai/whisper/86098128c0b4f24f0e2aa2994de830614b474227/tests/jfk.flac";

#[derive(Clone)]
struct PlayoutMarker {
    monotonic: Instant,
    wall_clock: String,
}

#[derive(Default)]
struct AttemptObservations {
    speech_end: Option<PlayoutMarker>,
    provider_first_audio: Option<PlayoutMarker>,
    publisher_accept: Option<PlayoutMarker>,
    relay_receive: Option<PlayoutMarker>,
    first_playout: Option<PlayoutMarker>,
}

impl AttemptObservations {
    fn record_speech_end(&mut self, marker: PlayoutMarker) {
        self.speech_end = Some(marker);
    }

    fn record_provider_first_audio(&mut self, marker: PlayoutMarker) {
        Self::record_once(&mut self.provider_first_audio, marker);
    }

    fn record_publisher_accept(&mut self, marker: PlayoutMarker) {
        Self::record_once(&mut self.publisher_accept, marker);
    }

    fn record_relay_receive(&mut self, marker: PlayoutMarker) {
        Self::record_once(&mut self.relay_receive, marker);
    }

    fn record_first_playout(&mut self, marker: PlayoutMarker) {
        Self::record_once(&mut self.first_playout, marker);
    }

    fn record_once(slot: &mut Option<PlayoutMarker>, marker: PlayoutMarker) {
        if slot.is_none() {
            *slot = Some(marker);
        }
    }

    fn elapsed(start: &Option<PlayoutMarker>, end: &Option<PlayoutMarker>) -> Option<Duration> {
        end.as_ref()?
            .monotonic
            .checked_duration_since(start.as_ref()?.monotonic)
    }

    fn latency(&self) -> Option<Duration> {
        Self::elapsed(&self.speech_end, &self.first_playout)
    }
}

fn observation_marker() -> PlayoutMarker {
    PlayoutMarker {
        monotonic: Instant::now(),
        wall_clock: Utc::now().to_rfc3339(),
    }
}

#[derive(Default)]
struct AttemptResponseCorrelation {
    response_id: Option<String>,
}

impl AttemptResponseCorrelation {
    fn start(&mut self, response_id: String) -> Result<(), &'static str> {
        if self.response_id.is_some() {
            return Err("multiple_provider_responses");
        }
        self.response_id = Some(response_id);
        Ok(())
    }

    fn require(&self, response_id: &str) -> Result<(), &'static str> {
        match self.response_id.as_deref() {
            Some(expected) if expected == response_id => Ok(()),
            Some(_) => Err("provider_response_identity_mismatch"),
            None => Err("provider_response_before_start"),
        }
    }
}

#[test]
fn attempt_observations_retain_partial_and_complete_timestamps() {
    let speech_end = Instant::now();
    let provider_audio = speech_end + Duration::from_millis(20);
    let publisher_accept = speech_end + Duration::from_millis(21);
    let relay_receive = speech_end + Duration::from_millis(23);
    let first_playout = speech_end + Duration::from_millis(25);
    let mut observations = AttemptObservations::default();
    observations.record_speech_end(PlayoutMarker {
        monotonic: speech_end,
        wall_clock: "speech-end".to_string(),
    });
    assert_eq!(
        observations
            .speech_end
            .as_ref()
            .map(|marker| marker.wall_clock.as_str()),
        Some("speech-end")
    );
    assert!(observations.first_playout.is_none());
    assert!(observations.latency().is_none());

    observations.record_provider_first_audio(PlayoutMarker {
        monotonic: provider_audio,
        wall_clock: "provider-audio".to_string(),
    });
    observations.record_publisher_accept(PlayoutMarker {
        monotonic: publisher_accept,
        wall_clock: "publisher-accept".to_string(),
    });
    observations.record_relay_receive(PlayoutMarker {
        monotonic: relay_receive,
        wall_clock: "relay-receive".to_string(),
    });
    observations.record_first_playout(PlayoutMarker {
        monotonic: first_playout,
        wall_clock: "first-playout".to_string(),
    });
    assert_eq!(
        AttemptObservations::elapsed(&observations.speech_end, &observations.provider_first_audio),
        Some(Duration::from_millis(20))
    );
    assert_eq!(
        AttemptObservations::elapsed(&observations.publisher_accept, &observations.relay_receive),
        Some(Duration::from_millis(2))
    );
    assert_eq!(observations.latency(), Some(Duration::from_millis(25)));
    assert_eq!(
        observations
            .first_playout
            .as_ref()
            .map(|marker| marker.wall_clock.as_str()),
        Some("first-playout")
    );
}

#[test]
fn response_correlation_rejects_every_mismatched_terminal_identity() {
    let mut correlation = AttemptResponseCorrelation::default();
    correlation
        .start("response-1".to_string())
        .expect("response start");
    assert!(correlation.require("response-1").is_ok());
    assert_eq!(
        correlation.require("response-2"),
        Err("provider_response_identity_mismatch")
    );
}

struct IsolatedRoute {
    publisher: RealtimeAudioPublisher,
    output_fence: std::sync::Arc<ProviderOutputFence>,
    playout: mpsc::Receiver<PlayoutMarker>,
    media_cancel: CancellationToken,
    observations: std::sync::Arc<std::sync::Mutex<AttemptObservations>>,
    human_playout: tokio::task::JoinHandle<Result<(), &'static str>>,
    relay: tokio::task::JoinHandle<Result<(), &'static str>>,
}

async fn accept_latency_peer(
    listener: &tokio::net::TcpListener,
    relay_url: &str,
    expected_pubkey: &nostr::PublicKey,
    peer_index: u8,
    peers: serde_json::Value,
) -> tokio_tungstenite::WebSocketStream<tokio::net::TcpStream> {
    let (stream, _) = listener.accept().await.expect("latency peer accept");
    let mut socket = tokio_tungstenite::accept_async(stream)
        .await
        .expect("latency peer WebSocket");
    let challenge = format!("latency-{peer_index}");
    socket
        .send(Message::Text(
            serde_json::json!({"type":"challenge","challenge":challenge})
                .to_string()
                .into(),
        ))
        .await
        .expect("latency challenge");
    let auth = socket
        .next()
        .await
        .expect("latency auth")
        .expect("valid latency auth");
    let Message::Text(auth) = auth else {
        panic!("latency auth must be text")
    };
    let auth: serde_json::Value = serde_json::from_slice(auth.as_bytes()).expect("auth JSON");
    assert_eq!(auth["type"], "auth");
    assert_eq!(auth["protocol_version"], super::wire::PROTOCOL_VERSION);
    let event: nostr::Event = serde_json::from_value(auth["event"].clone()).expect("NIP-42 event");
    event.verify().expect("valid NIP-42 signature");
    assert_eq!(&event.pubkey, expected_pubkey, "admitted member identity");
    let tags = auth["event"]["tags"].as_array().expect("NIP-42 tags");
    let required_tags = [("challenge", challenge.as_str()), ("relay", relay_url)];
    for (name, value) in required_tags {
        assert!(tags
            .iter()
            .any(|tag| tag == &serde_json::json!([name, value])));
    }
    socket
        .send(Message::Text(
            serde_json::json!({"type":"joined","peer_index":peer_index,"peers":peers})
                .to_string()
                .into(),
        ))
        .await
        .expect("admit latency peer");
    socket
}

async fn settle_route_task(
    mut task: tokio::task::JoinHandle<Result<(), &'static str>>,
    join_failure: &'static str,
    close_timeout: &'static str,
) -> Result<(), &'static str> {
    match tokio::time::timeout(ROUTE_CLOSE_TIMEOUT, &mut task).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err(join_failure),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err(close_timeout)
        }
    }
}

fn should_probe_playout(received_non_dtx: bool, first_playout_reported: bool) -> bool {
    received_non_dtx && !first_playout_reported
}

fn latency_turn_is_complete(response_completed: bool, first_playout_observed: bool) -> bool {
    response_completed && first_playout_observed
}

fn resolve_attempt_terminal_reason(
    turn_reason: &'static str,
    provider_close_reason: Option<&'static str>,
    route_close_reason: Option<&'static str>,
) -> &'static str {
    if turn_reason != "completed" {
        return turn_reason;
    }
    provider_close_reason
        .or(route_close_reason)
        .unwrap_or("completed")
}

#[test]
fn attempt_terminal_reason_retains_the_primary_failure() {
    let cases = [
        ("turn", Some("provider"), Some("route"), "turn"),
        ("completed", Some("provider"), None, "provider"),
        ("completed", None, Some("route"), "route"),
    ];
    for (turn, provider, route, expected) in cases {
        assert_eq!(
            resolve_attempt_terminal_reason(turn, provider, route),
            expected
        );
    }
}

struct CompletedLatencyEvidence {
    full_path_ms: Vec<u64>,
    buzz_local_ms: Vec<u64>,
}

impl CompletedLatencyEvidence {
    fn from_outcomes(outcomes: &[serde_json::Value]) -> Result<Self, &'static str> {
        let samples: Vec<(u64, u64)> = outcomes
            .iter()
            .filter(|outcome| outcome["terminal_reason"] == "completed")
            .map(|outcome| {
                let full_path_ms = outcome["latency_ms"]
                    .as_u64()
                    .ok_or("completed_full_path_latency_missing")?;
                let buzz_local_ms = outcome["buzz_local_ms"]
                    .as_u64()
                    .ok_or("completed_buzz_local_latency_missing")?;
                Ok::<(u64, u64), &'static str>((full_path_ms, buzz_local_ms))
            })
            .collect::<Result<_, _>>()?;
        let (full_path_ms, buzz_local_ms) = samples.into_iter().unzip();
        Ok(Self {
            full_path_ms,
            buzz_local_ms,
        })
    }
}

#[test]
fn completed_latency_evidence_requires_every_metric() {
    let complete = serde_json::json!({
        "terminal_reason": "completed",
        "latency_ms": 25,
        "buzz_local_ms": 5,
    });
    let failed = serde_json::json!({
        "terminal_reason": "provider_close_failed",
        "latency_ms": null,
        "buzz_local_ms": null,
    });
    let evidence =
        CompletedLatencyEvidence::from_outcomes(&[complete, failed]).expect("complete evidence");
    assert_eq!(evidence.full_path_ms, vec![25]);
    assert_eq!(evidence.buzz_local_ms, vec![5]);

    let missing_full_path = serde_json::json!({
        "terminal_reason": "completed",
        "latency_ms": null,
        "buzz_local_ms": 5,
    });
    assert_eq!(
        CompletedLatencyEvidence::from_outcomes(&[missing_full_path]).err(),
        Some("completed_full_path_latency_missing")
    );

    let missing_buzz_local = serde_json::json!({
        "terminal_reason": "completed",
        "latency_ms": 25,
        "buzz_local_ms": null,
    });
    assert_eq!(
        CompletedLatencyEvidence::from_outcomes(&[missing_buzz_local]).err(),
        Some("completed_buzz_local_latency_missing")
    );
}

#[tokio::test]
async fn route_task_terminal_error_is_not_filtered() {
    let task = tokio::spawn(async { Err("human_receive_closed") });
    assert_eq!(
        settle_route_task(task, "join_failed", "close_timeout").await,
        Err("human_receive_closed")
    );
}

impl IsolatedRoute {
    async fn start() -> Result<Self, &'static str> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| "route_bind_failed")?;
        let relay_url = format!("ws://{}", listener.local_addr().expect("relay address"));
        let channel_id = uuid::Uuid::new_v4().to_string();
        let agent_keys = nostr::Keys::generate();
        let human_keys = nostr::Keys::generate();
        let expected_agent = agent_keys.public_key();
        let expected_human = human_keys.public_key();
        let observations =
            std::sync::Arc::new(std::sync::Mutex::new(AttemptObservations::default()));
        let relay_observations = std::sync::Arc::clone(&observations);
        let relay_endpoint = relay_url.clone();
        let relay = tokio::spawn(async move {
            let mut agent = accept_latency_peer(
                &listener,
                &relay_endpoint,
                &expected_agent,
                1,
                serde_json::json!([]),
            )
            .await;
            let mut human = accept_latency_peer(
                &listener,
                &relay_endpoint,
                &expected_human,
                2,
                serde_json::json!([{
                    "peer_index": 1,
                    "pubkey": expected_agent.to_hex(),
                    "epoch": 0
                }]),
            )
            .await;
            loop {
                let message = match agent.next().await {
                    Some(Ok(message)) => message,
                    Some(Err(_)) => return Err("relay_receive_failed"),
                    None => return Ok(()),
                };
                let Message::Binary(audio) = message else {
                    continue;
                };
                let (header, _) =
                    super::wire::FrameHeader::parse(&audio).ok_or("relay_agent_frame_invalid")?;
                if !header.is_dtx() {
                    relay_observations
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .record_relay_receive(observation_marker());
                }
                let mut relay_frame = Vec::with_capacity(audio.len() + 1);
                relay_frame.push(1);
                relay_frame.extend_from_slice(&audio);
                human
                    .send(Message::Binary(relay_frame.into()))
                    .await
                    .map_err(|_| "relay_fanout_failed")?;
            }
        });

        let media_cancel = CancellationToken::new();
        let output_fence = ProviderOutputFence::shared();
        let publisher = connect_realtime_audio_publisher_at(
            &channel_id,
            None,
            &relay_url,
            &agent_keys,
            None,
            std::sync::Arc::clone(&output_fence),
            media_cancel.clone(),
        )
        .await;
        let mut publisher = match publisher {
            Ok(publisher) => publisher,
            Err(_) => {
                relay.abort();
                let _ = relay.await;
                return Err("agent_publisher_connect_failed");
            }
        };
        let human_connection = super::relay_api::connect_authenticated_audio_socket(
            &channel_id,
            None,
            &relay_url,
            &human_keys,
            None,
        )
        .await;
        let (human_tx, mut human_rx, _, _) = match human_connection {
            Ok(connection) => connection,
            Err(_) => {
                let _ = publisher.close().await;
                relay.abort();
                let _ = relay.await;
                return Err("human_peer_connect_failed");
            }
        };
        let (playout_tx, playout) = mpsc::channel(64);
        let playout_observations = std::sync::Arc::clone(&observations);
        let human_cancel = media_cancel.clone();
        let human_playout = tokio::spawn(async move {
            let _human_tx = human_tx;
            let mut jitter =
                super::jitter::PeerJitterBuffer::new(1).map_err(|_| "human_jitter_init_failed")?;
            let mut received_non_dtx = false;
            let mut first_playout_reported = false;
            let mut playout_tick = tokio::time::interval(Duration::from_millis(10));
            playout_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = human_cancel.cancelled() => return Ok(()),
                    _ = playout_tick.tick(), if should_probe_playout(received_non_dtx, first_playout_reported) => {
                        let (_, voice_active) = jitter
                            .get_audio()
                            .map_err(|_| "human_playout_decode_failed")?;
                        if voice_active {
                            let marker = observation_marker();
                            playout_observations
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .record_first_playout(marker.clone());
                            playout_tx
                                .send(marker)
                                .await
                                .map_err(|_| "playout_observer_closed")?;
                            first_playout_reported = true;
                        }
                    }
                    message = human_rx.next() => {
                        let message = match message {
                            Some(Ok(message)) => message,
                            Some(Err(_)) => return Err("human_receive_failed"),
                            None => return Err("human_receive_closed"),
                        };
                        let Message::Binary(audio) = message else {
                            continue;
                        };
                        let Some((_, header, opus)) = super::wire::parse_relay_frame(&audio) else {
                            return Err("human_relay_frame_invalid");
                        };
                        if header.is_dtx() {
                            continue;
                        }
                        jitter
                            .insert_packet(header.seq, header.ts_48k, opus)
                            .map_err(|_| "human_jitter_insert_failed")?;
                        received_non_dtx = true;
                    }
                }
            }
        });

        Ok(Self {
            publisher,
            output_fence,
            playout,
            media_cancel,
            observations,
            human_playout,
            relay,
        })
    }

    async fn close(mut self) -> Result<(), &'static str> {
        self.output_fence.advance();
        self.publisher.clear();
        let output_quiescent = tokio::time::timeout(
            ROUTE_CLOSE_TIMEOUT,
            self.publisher.wait_for_output_quiescence(),
        )
        .await
        .is_ok();
        self.media_cancel.cancel();
        let publisher_result = self
            .publisher
            .close()
            .await
            .map_err(|_| "publisher_close_failed");
        let human_result = settle_route_task(
            self.human_playout,
            "human_playout_task_failed",
            "human_playout_close_timeout",
        )
        .await;
        let relay_result =
            settle_route_task(self.relay, "relay_task_failed", "relay_close_timeout").await;
        [
            output_quiescent
                .then_some(())
                .ok_or("output_quiescence_timeout"),
            publisher_result,
            human_result,
            relay_result,
        ]
        .into_iter()
        .find_map(Result::err)
        .map_or(Ok(()), Err)
    }
}

async fn run_latency_turn(
    session: &mut OpenAiRealtimeSession,
    route: &mut IsolatedRoute,
    speech_frames: &[Vec<i16>],
) -> Result<(), &'static str> {
    for samples_24k in speech_frames {
        session
            .input
            .send(ProviderInputFrame {
                samples_24k: samples_24k.clone(),
            })
            .await
            .map_err(|_| "provider_input_closed")?;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Local PTT speech end is captured before provider commit/create.
    let speech_end = Instant::now();
    route
        .observations
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .record_speech_end(PlayoutMarker {
            monotonic: speech_end,
            wall_clock: Utc::now().to_rfc3339(),
        });
    session
        .end_input_turn()
        .map_err(|_| "provider_turn_control_closed")?;
    let mut correlation = AttemptResponseCorrelation::default();
    let mut first_playout = None;
    let mut response_completed = false;
    loop {
        tokio::select! {
            accepted = route.playout.recv(), if first_playout.is_none() => {
                let accepted = accepted.ok_or("human_playout_closed")?;
                if accepted.monotonic < speech_end {
                    return Err("playout_precedes_speech_end");
                }
                first_playout = Some(accepted);
            }
            event = session.events.recv() => match event.ok_or("provider_events_closed")? {
                ProviderEvent::OutputStarted { response_id } => {
                    correlation.start(response_id)?;
                }
                ProviderEvent::Audio(frame) => {
                    correlation.require(&frame.response_id)?;
                    route
                        .observations
                        .lock()
                        .unwrap_or_else(|error| error.into_inner())
                        .record_provider_first_audio(observation_marker());
                    match route
                        .publisher
                        .publish(frame.output_generation, &frame.samples_24k)
                    {
                        RealtimePublishOutcome::Enqueued => {
                            route
                                .observations
                                .lock()
                                .unwrap_or_else(|error| error.into_inner())
                                .record_publisher_accept(observation_marker());
                        }
                        RealtimePublishOutcome::Cancelled => {
                            return Err("publisher_cancelled");
                        }
                        RealtimePublishOutcome::StaleGeneration => {
                            return Err("publisher_output_generation_stale");
                        }
                    }
                }
                ProviderEvent::OutputDone { response_id } => {
                    correlation.require(&response_id)?;
                    route.publisher.finish_output();
                }
                ProviderEvent::ResponseDone {
                    response_id,
                    completed,
                } => {
                    correlation.require(&response_id)?;
                    if !completed {
                        return Err("provider_response_cancelled");
                    }
                    response_completed = true;
                }
                ProviderEvent::InputSpeechStarted
                | ProviderEvent::InputSpeechStopped
                | ProviderEvent::SessionReady => {}
            }
        }
        if latency_turn_is_complete(response_completed, first_playout.is_some()) {
            return Ok(());
        }
    }
}

async fn execute_isolated_attempt(attempt: usize, speech_frames: &[Vec<i16>]) -> serde_json::Value {
    let started_at = Utc::now().to_rfc3339();
    let route = IsolatedRoute::start().await;
    let mut route = match route {
        Ok(route) => route,
        Err(reason) => {
            return serde_json::json!({
                "attempt": attempt,
                "started_at": started_at,
                "ended_at": Utc::now().to_rfc3339(),
                "speech_end_at": null,
                "provider_first_audio_at": null,
                "publisher_accept_at": null,
                "relay_receive_at": null,
                "first_playout_at": null,
                "latency_ms": null,
                "buzz_local_ms": null,
                "segments_ms": null,
                "turn_terminal_reason": reason,
                "provider_close_reason": null,
                "route_close_reason": null,
                "terminal_reason": reason,
            });
        }
    };
    let session_cancel = CancellationToken::new();
    let session = OpenAiRealtimeSession::connect_with_output_fence(
        true,
        std::sync::Arc::clone(&route.output_fence),
        session_cancel.clone(),
    )
    .await;

    let (turn_terminal_reason, provider_close_reason) = match session {
        Ok(mut session) => {
            let turn_result = tokio::time::timeout(
                WHOLE_ATTEMPT_TIMEOUT,
                run_latency_turn(&mut session, &mut route, speech_frames),
            )
            .await;
            let outcome = match turn_result {
                Ok(Ok(())) => "completed",
                Ok(Err(reason)) => reason,
                Err(_) => "whole_attempt_timeout",
            };
            route.output_fence.advance();
            session_cancel.cancel();
            let close_reason = session.close().await.err().map(|_| "provider_close_failed");
            (outcome, close_reason)
        }
        Err(_) => ("provider_connect_failed", None),
    };
    let observations = std::sync::Arc::clone(&route.observations);
    let route_close_reason = route.close().await.err();
    let terminal_reason = resolve_attempt_terminal_reason(
        turn_terminal_reason,
        provider_close_reason,
        route_close_reason,
    );
    let observations = observations
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let latency_ms = observations.latency().map(|latency| latency.as_millis());
    let buzz_local_ms =
        AttemptObservations::elapsed(&observations.publisher_accept, &observations.first_playout)
            .map(|latency| latency.as_millis());
    let speech_end_at = observations
        .speech_end
        .as_ref()
        .map(|marker| marker.wall_clock.as_str());
    let provider_first_audio_at = observations
        .provider_first_audio
        .as_ref()
        .map(|marker| marker.wall_clock.as_str());
    let publisher_accept_at = observations
        .publisher_accept
        .as_ref()
        .map(|marker| marker.wall_clock.as_str());
    let relay_receive_at = observations
        .relay_receive
        .as_ref()
        .map(|marker| marker.wall_clock.as_str());
    let first_playout_at = observations
        .first_playout
        .as_ref()
        .map(|marker| marker.wall_clock.as_str());
    let segment_ms =
        |start, end| AttemptObservations::elapsed(start, end).map(|duration| duration.as_millis());
    let segments_ms = serde_json::json!({
        "speech_end_to_provider_audio": segment_ms(
            &observations.speech_end,
            &observations.provider_first_audio,
        ),
        "provider_audio_to_publisher_accept": segment_ms(
            &observations.provider_first_audio,
            &observations.publisher_accept,
        ),
        "publisher_accept_to_relay_receive": segment_ms(
            &observations.publisher_accept,
            &observations.relay_receive,
        ),
        "relay_receive_to_playout": segment_ms(
            &observations.relay_receive,
            &observations.first_playout,
        ),
    });
    serde_json::json!({
        "attempt": attempt,
        "started_at": started_at,
        "ended_at": Utc::now().to_rfc3339(),
        "speech_end_at": speech_end_at,
        "provider_first_audio_at": provider_first_audio_at,
        "publisher_accept_at": publisher_accept_at,
        "relay_receive_at": relay_receive_at,
        "first_playout_at": first_playout_at,
        "latency_ms": latency_ms,
        "buzz_local_ms": buzz_local_ms,
        "segments_ms": segments_ms,
        "turn_terminal_reason": turn_terminal_reason,
        "provider_close_reason": provider_close_reason,
        "route_close_reason": route_close_reason,
        "terminal_reason": terminal_reason,
    })
}

fn decode_fixture(compressed: &[u8]) -> Vec<Vec<i16>> {
    use std::io::Write as _;

    let mut child = std::process::Command::new("ffmpeg")
        .args([
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            "pipe:0",
            "-t",
            "3",
            "-f",
            "s16le",
            "-ac",
            "1",
            "-ar",
            "24000",
            "pipe:1",
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("start in-memory fixture decoder");
    let mut decoder_input = child.stdin.take().expect("ffmpeg stdin");
    let compressed = compressed.to_vec();
    let writer = std::thread::spawn(move || decoder_input.write_all(&compressed));
    let decoded = child.wait_with_output().expect("decode fixture in memory");
    let _ = writer.join().expect("fixture writer");
    assert!(decoded.status.success());
    let samples: Vec<i16> = decoded
        .stdout
        .chunks_exact(2)
        .map(|bytes| i16::from_le_bytes([bytes[0], bytes[1]]))
        .collect();
    samples.chunks(2_400).map(<[i16]>::to_vec).collect()
}

fn exact_clean_head() -> String {
    let head = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .expect("read evidence commit");
    assert!(head.status.success());
    let status = std::process::Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .expect("verify evidence worktree");
    assert!(status.status.success());
    assert!(
        status.stdout.is_empty(),
        "latency evidence requires an exact clean commit"
    );
    String::from_utf8(head.stdout)
        .expect("commit UTF-8")
        .trim()
        .to_string()
}

fn optional_command(command: &str, args: &[&str]) -> Option<String> {
    let output = std::process::Command::new(command)
        .args(args)
        .output()
        .ok()?;
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn active_network_interface() -> Option<String> {
    optional_command("/sbin/route", &["-n", "get", "default"])?
        .lines()
        .find_map(|line| {
            let (field, value) = line.split_once(':')?;
            (field.trim() == "interface").then(|| value.trim().to_string())
        })
}

fn network_hardware_port(interface: &str) -> Option<String> {
    let inventory = optional_command("networksetup", &["-listallhardwareports"])?;
    let mut hardware_port = None;
    for line in inventory.lines() {
        let Some((field, value)) = line.split_once(':') else {
            continue;
        };
        match field.trim() {
            "Hardware Port" => hardware_port = Some(value.trim().to_string()),
            "Device" if value.trim() == interface => return hardware_port,
            _ => {}
        }
    }
    None
}

#[tokio::test]
#[ignore = "requires explicit OPENAI_API_KEY, VOICE2_LATENCY_MANIFEST, network, and ffmpeg; incurs provider spend"]
async fn fixed_twenty_attempt_full_huddle_playout_latency_gate() {
    let manifest_path = std::env::var("VOICE2_LATENCY_MANIFEST")
        .expect("VOICE2_LATENCY_MANIFEST must name the retained evidence artifact");
    let evidence_commit = exact_clean_head();
    let network_interface = active_network_interface();
    let network_type = network_interface.as_deref().and_then(network_hardware_port);
    let compressed = reqwest::get(SPEECH_FIXTURE)
        .await
        .expect("download pinned public speech fixture")
        .bytes()
        .await
        .expect("read fixture in memory");
    let fixture_sha256 = hex::encode(Sha256::digest(&compressed));
    let speech_frames = decode_fixture(&compressed);

    let warm_up = execute_isolated_attempt(0, &speech_frames).await;
    let mut outcomes = Vec::with_capacity(ATTEMPTS);
    for attempt in 1..=ATTEMPTS {
        outcomes.push(execute_isolated_attempt(attempt, &speech_frames).await);
    }

    let evidence = CompletedLatencyEvidence::from_outcomes(&outcomes)
        .expect("every completed attempt must retain complete latency evidence");
    let mut completed_ms = evidence.full_path_ms;
    completed_ms.sort_unstable();
    let mut completed_buzz_local_ms = evidence.buzz_local_ms;
    completed_buzz_local_ms.sort_unstable();
    let median_ms = completed_ms
        .get(ATTEMPTS / 2 - 1)
        .zip(completed_ms.get(ATTEMPTS / 2))
        .map(|(lower, upper)| (lower + upper) / 2);
    let p95_index = (ATTEMPTS * 95).div_ceil(100) - 1;
    let p95_ms = completed_ms.get(p95_index).copied();
    let buzz_local_p95_ms = completed_buzz_local_ms.get(p95_index).copied();
    let manifest = serde_json::json!({
        "schema": "buzz.voice2.full-path-latency.v2",
        "commit": evidence_commit,
        "provider": {
            "endpoint": "wss://api.openai.com/v1/realtime?model=gpt-realtime-2.1",
            "api_contract": "OpenAI Realtime v1 WebSocket",
            "api_revision": "model-pinned:gpt-realtime-2.1",
            "model": "gpt-realtime-2.1",
            "voice": "marin",
            "tools": "disabled",
            "turn_detection": "manual-push-to-talk"
        },
        "media": {
            "provider_input": "pcm16le-mono-24000hz",
            "provider_output": "pcm16le-mono-24000hz",
            "huddle_output": "opus-v2-mono-48000hz-20ms",
            "t0": "local-ptt-speech-end-before-provider-commit",
            "provider_first_audio": "first-correlated-provider-audio-received-by-host",
            "publisher_accept": "first-provider-audio-passed-to-fenced-huddle-publisher",
            "relay_receive": "first-non-dtx-agent-frame-received-by-isolated-relay",
            "t1": "first-voice-active-frame-returned-by-human-peer-jitter-get-audio"
        },
        "route": "fresh-isolated-nip42-two-peer-huddle-per-attempt",
        "host": {
            "os": std::env::consts::OS,
            "arch": std::env::consts::ARCH,
            "os_version": optional_command("sw_vers", &["-productVersion"]),
            "cpu": optional_command("sysctl", &["-n", "machdep.cpu.brand_string"]),
            "provider_network_type": network_type,
            "provider_network_interface": network_interface,
            "huddle_network": "IPv4 loopback"
        },
        "fixture_url": SPEECH_FIXTURE,
        "fixture_sha256": fixture_sha256,
        "warm_up": warm_up,
        "warm_up_attempts": 1,
        "measured_attempts": ATTEMPTS,
        "retries": 0,
        "whole_attempt_timeout_ms": WHOLE_ATTEMPT_TIMEOUT.as_millis(),
        "attempts": outcomes,
        "completed": completed_ms.len(),
        "median_ms": median_ms,
        "nearest_rank_p95_ms": p95_ms,
        "nearest_rank_buzz_local_p95_ms": buzz_local_p95_ms,
        "thresholds": {
            "completion_percent": COMPLETION_PERCENT_MIN,
            "median_ms": FULL_PATH_MEDIAN_MAX_MS,
            "p95_ms": FULL_PATH_P95_MAX_MS,
            "buzz_local_p95_ms": BUZZ_LOCAL_P95_MAX_MS
        }
    });
    std::fs::write(
        &manifest_path,
        serde_json::to_vec_pretty(&manifest).expect("serialize latency manifest"),
    )
    .expect("retain latency manifest");
    eprintln!("VOICE2_LATENCY_MANIFEST={manifest_path}");

    assert_eq!(warm_up["terminal_reason"], "completed", "warm-up gate");
    assert!(
        completed_ms.len() * 100 >= ATTEMPTS * COMPLETION_PERCENT_MIN,
        "completion gate"
    );
    assert!(median_ms.is_some_and(|value| value <= FULL_PATH_MEDIAN_MAX_MS));
    assert!(p95_ms.is_some_and(|value| value <= FULL_PATH_P95_MAX_MS));
    assert!(buzz_local_p95_ms.is_some_and(|value| value <= BUZZ_LOCAL_P95_MAX_MS));
}
