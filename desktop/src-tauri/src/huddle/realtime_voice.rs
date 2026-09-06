//! Fail-closed contracts for a Huddle-owned realtime voice session.
//!
//! Provider wire shapes stop in this module. Callers receive only validated,
//! bounded PCM and state events; credentials and provider response bodies are
//! never part of diagnostics.

use base64::{engine::general_purpose::STANDARD, Engine as _};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use uuid::Uuid;

const CAPTURE_ENVELOPE_MAGIC: &[u8; 4] = b"BZPC";
const CAPTURE_ENVELOPE_VERSION: u8 = 1;
const CAPTURE_ENVELOPE_HEADER_BYTES: usize = 88;
pub(super) const OPENAI_PCM_RATE: u32 = 24_000;
pub(super) const MAX_PROVIDER_MESSAGE_BYTES: usize = 64 * 1024;
const MAX_ENCODED_AUDIO_BYTES: usize = 48 * 1024;
const MAX_DECODED_AUDIO_BYTES: usize = 36 * 1024;
const MAX_PROVIDER_AUDIO_SAMPLES: usize = MAX_DECODED_AUDIO_BYTES / 2;
const WALL_CLOCK_LIMIT: Duration = Duration::from_secs(20 * 60);
const INPUT_SAMPLE_LIMIT: u64 = 15 * 60 * 48_000;
const OUTPUT_SAMPLE_LIMIT: u64 = 5 * 60 * OPENAI_PCM_RATE as u64;

#[derive(Debug)]
struct ProviderOutputFenceState {
    generation: u64,
    active_sends: usize,
}

/// One linearized host-owned generation and in-flight-send contract for queued
/// provider audio and the publisher's actual socket-send boundary.
#[derive(Debug)]
pub(crate) struct ProviderOutputFence {
    state: Mutex<ProviderOutputFenceState>,
    quiescence: tokio::sync::Notify,
}

pub(crate) struct ProviderOutputSendPermit(Arc<ProviderOutputFence>);

impl Drop for ProviderOutputSendPermit {
    fn drop(&mut self) {
        let mut state = self
            .0
            .state
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        state.active_sends = state.active_sends.saturating_sub(1);
        let became_quiescent = state.active_sends == 0;
        drop(state);
        if became_quiescent {
            self.0.quiescence.notify_waiters();
        }
    }
}

impl ProviderOutputFence {
    pub(crate) fn shared() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(ProviderOutputFenceState {
                generation: 0,
                active_sends: 0,
            }),
            quiescence: tokio::sync::Notify::new(),
        })
    }

    pub(crate) fn generation(&self) -> u64 {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .generation
    }

    pub(crate) fn advance(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.generation = state.generation.wrapping_add(1);
    }

    pub(crate) fn admits(&self, generation: u64) -> bool {
        self.generation() == generation
    }

    pub(crate) fn begin_send(
        self: &Arc<Self>,
        generation: u64,
    ) -> Option<ProviderOutputSendPermit> {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.generation != generation {
            return None;
        }
        state.active_sends += 1;
        Some(ProviderOutputSendPermit(Arc::clone(self)))
    }

    pub(crate) async fn wait_for_quiescence(&self) {
        loop {
            let notified = self.quiescence.notified();
            let active_sends = self
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .active_sends;
            if active_sends == 0 {
                return;
            }
            notified.await;
        }
    }
}

/// Exact identity of one realtime voice lifetime.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RealtimeVoiceSessionKey {
    pub(crate) channel_id: Uuid,
    pub(crate) huddle_generation: u64,
    pub(crate) agent_pubkey: String,
}

impl RealtimeVoiceSessionKey {
    pub(crate) fn validated(
        channel_id: &str,
        huddle_generation: u64,
        agent_pubkey: &str,
    ) -> Result<Self, String> {
        let channel_id = super::relay_api::parse_channel_uuid(channel_id)?;
        super::relay_api::validate_pubkey_hex(agent_pubkey)?;
        Ok(Self {
            channel_id,
            huddle_generation,
            agent_pubkey: agent_pubkey.to_ascii_lowercase(),
        })
    }
}

/// Explicit local-user authorization for external audio processing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ExternalAudioEgressGrant {
    pub(crate) session: RealtimeVoiceSessionKey,
    pub(crate) local_human_pubkey: String,
    pub(crate) capture_generation: u64,
    pub(crate) source_id: Uuid,
}

impl ExternalAudioEgressGrant {
    pub(crate) fn validated(
        session: RealtimeVoiceSessionKey,
        capture: &CaptureLease,
    ) -> Result<Self, String> {
        let belongs_to_session = capture.session_channel_id == session.channel_id
            && capture.huddle_generation == session.huddle_generation;
        if !belongs_to_session {
            return Err("microphone capture does not belong to this realtime session".to_string());
        }
        super::relay_api::validate_pubkey_hex(&capture.local_human_pubkey)?;
        Ok(Self {
            session,
            local_human_pubkey: capture.local_human_pubkey.to_ascii_lowercase(),
            capture_generation: capture.capture_generation,
            source_id: capture.source_id,
        })
    }

    pub(crate) fn admits(&self, capture: &CapturedHuddlePcm) -> bool {
        capture.capture_generation == self.capture_generation
            && capture.source_id == self.source_id
            && capture.local_human_pubkey == self.local_human_pubkey
    }
}

/// Lease identifying the one browser capture source currently allowed to send.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CaptureLease {
    pub(crate) session_channel_id: Uuid,
    pub(crate) huddle_generation: u64,
    pub(crate) capture_generation: u64,
    pub(crate) source_id: Uuid,
    pub(crate) local_human_pubkey: String,
}

/// PCM accepted from the browser only after exact capture-lease validation.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct CapturedHuddlePcm {
    pub(crate) capture_generation: u64,
    pub(crate) source_id: Uuid,
    pub(crate) local_human_pubkey: String,
    pub(crate) samples_48k: Vec<f32>,
}

#[derive(Debug, PartialEq, Eq)]
struct CaptureEnvelopeHeader {
    channel_id: Uuid,
    huddle_generation: u64,
    capture_generation: u64,
    source_id: Uuid,
    local_human_pubkey: String,
}

fn valid_capture_envelope_length(bytes: &[u8]) -> bool {
    let pcm_length = bytes.len().saturating_sub(CAPTURE_ENVELOPE_HEADER_BYTES);
    bytes.len() > CAPTURE_ENVELOPE_HEADER_BYTES
        && pcm_length <= super::capture::MAX_AUDIO_BATCH_BYTES
        && pcm_length.is_multiple_of(4)
}

fn read_u64(bytes: &[u8], start: usize) -> Result<u64, String> {
    let value: [u8; 8] = bytes
        .get(start..start + 8)
        .ok_or_else(|| "captured PCM envelope is truncated".to_string())?
        .try_into()
        .map_err(|_| "captured PCM envelope is truncated".to_string())?;
    Ok(u64::from_be_bytes(value))
}

fn decode_capture_header(bytes: &[u8]) -> Result<CaptureEnvelopeHeader, String> {
    if !valid_capture_envelope_length(bytes) {
        return Err("captured PCM envelope length is invalid".to_string());
    }
    if bytes.get(..4) != Some(CAPTURE_ENVELOPE_MAGIC) {
        return Err("captured PCM envelope magic is invalid".to_string());
    }
    if bytes.get(4).copied() != Some(CAPTURE_ENVELOPE_VERSION) {
        return Err("captured PCM envelope version is unsupported".to_string());
    }
    if bytes.get(5..8) != Some(&[0_u8; 3]) {
        return Err("captured PCM envelope reserved bytes are invalid".to_string());
    }
    let channel_id = Uuid::from_slice(&bytes[8..24])
        .map_err(|_| "captured PCM channel identity is invalid".to_string())?;
    let huddle_generation = read_u64(bytes, 24)?;
    let capture_generation = read_u64(bytes, 32)?;
    let source_id = Uuid::from_slice(&bytes[40..56])
        .map_err(|_| "capture source identity is invalid".to_string())?;
    let local_human_pubkey = hex::encode(&bytes[56..88]);
    Ok(CaptureEnvelopeHeader {
        channel_id,
        huddle_generation,
        capture_generation,
        source_id,
        local_human_pubkey,
    })
}

fn capture_header_matches_lease(header: &CaptureEnvelopeHeader, lease: &CaptureLease) -> bool {
    header.channel_id == lease.session_channel_id
        && header.huddle_generation == lease.huddle_generation
        && header.capture_generation == lease.capture_generation
        && header.source_id == lease.source_id
        && header.local_human_pubkey == lease.local_human_pubkey
}

impl CapturedHuddlePcm {
    pub(crate) fn decode(bytes: &[u8], lease: &CaptureLease) -> Result<Self, String> {
        let header = decode_capture_header(bytes)?;
        if !capture_header_matches_lease(&header, lease) {
            return Err("captured PCM does not match the current capture lease".to_string());
        }
        let samples_48k: Vec<f32> = bytes[CAPTURE_ENVELOPE_HEADER_BYTES..]
            .chunks_exact(4)
            .map(|sample| f32::from_le_bytes([sample[0], sample[1], sample[2], sample[3]]))
            .collect();
        if !samples_48k.iter().all(|sample| sample.is_finite()) {
            return Err("captured PCM contains a non-finite sample".to_string());
        }
        Ok(Self {
            capture_generation: header.capture_generation,
            source_id: header.source_id,
            local_human_pubkey: header.local_human_pubkey,
            samples_48k,
        })
    }
}

/// Provider input PCM normalized once from admitted 48 kHz capture.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ProviderInputFrame {
    pub(crate) samples_24k: Vec<i16>,
}

impl ProviderInputFrame {
    pub(crate) fn from_capture(capture: &CapturedHuddlePcm) -> Result<Self, String> {
        if !capture.samples_48k.len().is_multiple_of(2) {
            return Err("captured PCM does not align to the provider rate".to_string());
        }
        let samples_24k = capture
            .samples_48k
            .chunks_exact(2)
            .map(|pair| {
                let sample = ((pair[0] + pair[1]) * 0.5).clamp(-1.0, 1.0);
                (sample * i16::MAX as f32).round() as i16
            })
            .collect();
        Ok(Self { samples_24k })
    }
}

/// Provider PCM after fixed-rate and allocation bounds have been checked.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct NormalizedAudioFrame {
    pub(crate) samples_24k: Vec<i16>,
    /// Host-owned output fence, assigned by the adapter after validation.
    pub(crate) output_generation: u64,
    pub(crate) response_id: String,
    pub(crate) item_id: String,
    pub(crate) content_index: u32,
}

#[derive(Deserialize)]
struct ProviderEventEnvelope<'a> {
    #[serde(rename = "type")]
    event_type: &'a str,
    #[serde(default)]
    delta: Option<&'a str>,
    #[serde(default)]
    response_id: Option<&'a str>,
    #[serde(default)]
    item_id: Option<&'a str>,
    #[serde(default)]
    response: Option<ProviderResponseEnvelope<'a>>,
    #[serde(default)]
    content_index: Option<u64>,
}

#[derive(Deserialize)]
struct ProviderResponseEnvelope<'a> {
    id: &'a str,
    status: &'a str,
}

/// Provider events that are safe to expose outside the OpenAI adapter.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum ProviderEvent {
    Audio(NormalizedAudioFrame),
    OutputStarted {
        response_id: String,
    },
    OutputDone {
        response_id: String,
    },
    ResponseDone {
        response_id: String,
        completed: bool,
    },
    InputSpeechStarted,
    InputSpeechStopped,
    SessionReady,
}

fn encoded_audio_within_limit(encoded: &str) -> bool {
    encoded.len() <= MAX_ENCODED_AUDIO_BYTES
}

fn decoded_audio_within_limit(decoded: &[u8]) -> bool {
    decoded.len() <= MAX_DECODED_AUDIO_BYTES && decoded.len().is_multiple_of(2)
}

fn valid_provider_item_id(item_id: &str) -> bool {
    !item_id.is_empty()
        && item_id.len() <= 128
        && item_id.chars().all(|character| !character.is_control())
}

fn decode_provider_audio(
    encoded: &str,
    response_id: &str,
    item_id: &str,
    content_index: u64,
) -> Result<NormalizedAudioFrame, String> {
    if !encoded_audio_within_limit(encoded) {
        return Err("provider audio field exceeds encoded limit".to_string());
    }
    let decoded = STANDARD
        .decode(encoded)
        .map_err(|_| "provider audio field is not valid base64".to_string())?;
    if !decoded_audio_within_limit(&decoded) {
        return Err("provider audio exceeds decoded limit".to_string());
    }
    let samples_24k: Vec<i16> = decoded
        .chunks_exact(2)
        .map(|bytes| i16::from_le_bytes([bytes[0], bytes[1]]))
        .collect();
    if samples_24k.len() > MAX_PROVIDER_AUDIO_SAMPLES {
        return Err("provider audio exceeds sample limit".to_string());
    }
    if !valid_provider_item_id(response_id) {
        return Err("provider audio response identity is invalid".to_string());
    }
    if !valid_provider_item_id(item_id) {
        return Err("provider audio item identity is invalid".to_string());
    }
    let content_index = u32::try_from(content_index)
        .map_err(|_| "provider audio content index is invalid".to_string())?;
    Ok(NormalizedAudioFrame {
        samples_24k,
        output_generation: 0,
        response_id: response_id.to_string(),
        item_id: item_id.to_string(),
        content_index,
    })
}

pub(crate) fn validate_openai_event(raw: &[u8]) -> Result<Option<ProviderEvent>, String> {
    if raw.len() > MAX_PROVIDER_MESSAGE_BYTES {
        return Err("provider message exceeds raw limit".to_string());
    }
    let envelope: ProviderEventEnvelope<'_> =
        serde_json::from_slice(raw).map_err(|_| "provider message is malformed".to_string())?;
    let event = match envelope.event_type {
        "response.output_audio.delta" => {
            let encoded = envelope
                .delta
                .ok_or_else(|| "provider audio event is missing delta".to_string())?;
            let response_id = envelope
                .response_id
                .ok_or_else(|| "provider audio event is missing response identity".to_string())?;
            let item_id = envelope
                .item_id
                .ok_or_else(|| "provider audio event is missing item identity".to_string())?;
            let content_index = envelope
                .content_index
                .ok_or_else(|| "provider audio event is missing content index".to_string())?;
            ProviderEvent::Audio(decode_provider_audio(
                encoded,
                response_id,
                item_id,
                content_index,
            )?)
        }
        "response.created" => ProviderEvent::OutputStarted {
            response_id: envelope
                .response
                .filter(|response| valid_provider_item_id(response.id))
                .ok_or_else(|| "provider response start identity is invalid".to_string())?
                .id
                .to_string(),
        },
        "response.output_audio.done" => ProviderEvent::OutputDone {
            response_id: envelope
                .response_id
                .filter(|id| valid_provider_item_id(id))
                .ok_or_else(|| "provider output completion identity is invalid".to_string())?
                .to_string(),
        },
        "input_audio_buffer.speech_started" => ProviderEvent::InputSpeechStarted,
        "input_audio_buffer.speech_stopped" => ProviderEvent::InputSpeechStopped,
        "session.created" | "session.updated" => ProviderEvent::SessionReady,
        "response.done" => {
            let response = envelope
                .response
                .ok_or_else(|| "provider response completion is missing identity".to_string())?;
            if !valid_provider_item_id(response.id) {
                return Err("provider response completion identity is invalid".to_string());
            }
            let completed = match response.status {
                "completed" => true,
                "cancelled" => false,
                _ => return Err("provider response did not complete successfully".to_string()),
            };
            ProviderEvent::ResponseDone {
                response_id: response.id.to_string(),
                completed,
            }
        }
        "error" => return Err("provider session reported an error".to_string()),
        "response.function_call_arguments.delta" | "response.function_call_arguments.done" => {
            return Err("provider tool event rejected".to_string());
        }
        _ => return Ok(None),
    };
    Ok(Some(event))
}

/// Fixed, sample-count-based provider resource budget.
#[derive(Debug)]
pub(crate) struct ProviderSessionBudget {
    admitted_input_samples_48k: u64,
    decoded_output_samples_24k: u64,
}

impl ProviderSessionBudget {
    pub(crate) fn new() -> Self {
        Self {
            admitted_input_samples_48k: 0,
            decoded_output_samples_24k: 0,
        }
    }

    pub(crate) fn wall_clock_limit() -> Duration {
        WALL_CLOCK_LIMIT
    }

    pub(crate) fn charge_input(&mut self, sample_count: usize) -> Result<(), String> {
        self.admitted_input_samples_48k = charge_samples(
            self.admitted_input_samples_48k,
            sample_count,
            INPUT_SAMPLE_LIMIT,
        )?;
        Ok(())
    }

    pub(crate) fn charge_output(&mut self, sample_count: usize) -> Result<(), String> {
        self.decoded_output_samples_24k = charge_samples(
            self.decoded_output_samples_24k,
            sample_count,
            OUTPUT_SAMPLE_LIMIT,
        )?;
        Ok(())
    }
}

fn charge_samples(current: u64, additional: usize, limit: u64) -> Result<u64, String> {
    let next = current
        .checked_add(additional as u64)
        .ok_or_else(|| "budget_exhausted".to_string())?;
    if next > limit {
        return Err("budget_exhausted".to_string());
    }
    Ok(next)
}

/// Provider credential whose diagnostics never expose secret bytes.
pub(crate) struct ProviderCredential(String);

impl ProviderCredential {
    pub(crate) fn from_env() -> Result<Self, String> {
        let value = std::env::var("OPENAI_API_KEY")
            .map_err(|_| "OpenAI Realtime credential is not configured".to_string())?;
        if value.trim().is_empty() {
            return Err("OpenAI Realtime credential is not configured".to_string());
        }
        Ok(Self(value.trim().to_string()))
    }

    pub(crate) fn expose(&self) -> &str {
        &self.0
    }

    #[cfg(test)]
    pub(super) fn for_test(value: &str) -> Self {
        Self(value.to_string())
    }
}

impl fmt::Debug for ProviderCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProviderCredential([REDACTED])")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pubkey(byte: char) -> String {
        std::iter::repeat_n(byte, 64).collect()
    }

    fn capture_lease() -> CaptureLease {
        CaptureLease {
            session_channel_id: Uuid::nil(),
            huddle_generation: 7,
            capture_generation: 11,
            source_id: Uuid::from_u128(9),
            local_human_pubkey: pubkey('a'),
        }
    }

    fn capture_envelope(sample: f32) -> Vec<u8> {
        let lease = capture_lease();
        let mut bytes = Vec::with_capacity(CAPTURE_ENVELOPE_HEADER_BYTES + 4);
        bytes.extend_from_slice(CAPTURE_ENVELOPE_MAGIC);
        bytes.push(CAPTURE_ENVELOPE_VERSION);
        bytes.extend_from_slice(&[0_u8; 3]);
        bytes.extend_from_slice(lease.session_channel_id.as_bytes());
        bytes.extend_from_slice(&lease.huddle_generation.to_be_bytes());
        bytes.extend_from_slice(&lease.capture_generation.to_be_bytes());
        bytes.extend_from_slice(lease.source_id.as_bytes());
        bytes.extend_from_slice(&hex::decode(&lease.local_human_pubkey).expect("pubkey bytes"));
        bytes.extend_from_slice(&sample.to_le_bytes());
        bytes
    }

    #[test]
    fn captured_pcm_requires_the_complete_exact_lease() {
        let bytes = capture_envelope(0.25);
        let captured = CapturedHuddlePcm::decode(&bytes, &capture_lease()).expect("valid capture");
        assert_eq!(captured.samples_48k, vec![0.25]);
        assert_eq!(captured.local_human_pubkey, pubkey('a'));

        let mismatched_leases = [
            (
                "channel",
                CaptureLease {
                    session_channel_id: Uuid::from_u128(3),
                    ..capture_lease()
                },
            ),
            (
                "huddle generation",
                CaptureLease {
                    huddle_generation: 8,
                    ..capture_lease()
                },
            ),
            (
                "capture generation",
                CaptureLease {
                    capture_generation: 12,
                    ..capture_lease()
                },
            ),
            (
                "source",
                CaptureLease {
                    source_id: Uuid::from_u128(4),
                    ..capture_lease()
                },
            ),
            (
                "human identity",
                CaptureLease {
                    local_human_pubkey: pubkey('b'),
                    ..capture_lease()
                },
            ),
        ];
        for (field, lease) in mismatched_leases {
            assert!(
                CapturedHuddlePcm::decode(&bytes, &lease).is_err(),
                "mismatched {field} must fail closed"
            );
        }
    }

    #[test]
    fn anonymous_unknown_version_and_non_finite_capture_are_rejected() {
        assert!(CapturedHuddlePcm::decode(&0.25_f32.to_le_bytes(), &capture_lease()).is_err());

        let mut unknown = capture_envelope(0.25);
        unknown[4] = CAPTURE_ENVELOPE_VERSION + 1;
        assert_eq!(
            CapturedHuddlePcm::decode(&unknown, &capture_lease()).expect_err("unknown version"),
            "captured PCM envelope version is unsupported"
        );

        let non_finite = capture_envelope(f32::NAN);
        assert_eq!(
            CapturedHuddlePcm::decode(&non_finite, &capture_lease())
                .expect_err("non-finite sample"),
            "captured PCM contains a non-finite sample"
        );
    }

    #[test]
    fn provider_input_is_normalized_once_to_fixed_rate_pcm16() {
        let capture = CapturedHuddlePcm {
            capture_generation: 1,
            source_id: Uuid::nil(),
            local_human_pubkey: pubkey('a'),
            samples_48k: vec![0.5, 0.5, -1.0, -1.0],
        };
        assert_eq!(
            ProviderInputFrame::from_capture(&capture).expect("provider input"),
            ProviderInputFrame {
                samples_24k: vec![16_384, -32_767]
            }
        );
    }

    #[test]
    fn provider_audio_limits_are_checked_before_decode_and_samples_escape() {
        let max_decoded = vec![0_u8; MAX_DECODED_AUDIO_BYTES];
        let event = serde_json::json!({
            "type": "response.output_audio.delta",
            "delta": STANDARD.encode(&max_decoded),
            "response_id": "response-1",
            "item_id": "item-1",
            "content_index": 0,
        });
        let parsed = validate_openai_event(event.to_string().as_bytes()).expect("exact maximum");
        assert!(matches!(parsed, Some(ProviderEvent::Audio(_))));

        let oversized_encoded = "A".repeat(MAX_ENCODED_AUDIO_BYTES + 1);
        let event = format!(
            "{{\"type\":\"response.output_audio.delta\",\"delta\":\"{oversized_encoded}\",\"response_id\":\"response-1\",\"item_id\":\"item-1\",\"content_index\":0}}"
        );
        assert_eq!(
            validate_openai_event(event.as_bytes()).expect_err("encoded max plus one"),
            "provider audio field exceeds encoded limit"
        );
    }

    #[test]
    fn provider_response_completion_requires_identity_and_closed_status() {
        let completed = serde_json::json!({
            "type": "response.done",
            "response": {"id": "response-1", "status": "completed"}
        });
        assert_eq!(
            validate_openai_event(completed.to_string().as_bytes()).expect("completion"),
            Some(ProviderEvent::ResponseDone {
                response_id: "response-1".to_string(),
                completed: true,
            })
        );

        let cancelled = serde_json::json!({
            "type": "response.done",
            "response": {"id": "response-2", "status": "cancelled"}
        });
        assert_eq!(
            validate_openai_event(cancelled.to_string().as_bytes()).expect("cancellation"),
            Some(ProviderEvent::ResponseDone {
                response_id: "response-2".to_string(),
                completed: false,
            })
        );

        let failed = serde_json::json!({
            "type": "response.done",
            "response": {"id": "response-3", "status": "failed"}
        });
        assert!(validate_openai_event(failed.to_string().as_bytes()).is_err());
    }

    #[test]
    fn provider_raw_max_plus_one_is_rejected_before_json_decode() {
        let oversized = vec![b' '; MAX_PROVIDER_MESSAGE_BYTES + 1];
        assert_eq!(
            validate_openai_event(&oversized).expect_err("raw max plus one"),
            "provider message exceeds raw limit"
        );
    }

    #[test]
    fn tool_arguments_do_not_escape_the_adapter() {
        let event = br#"{"type":"response.function_call_arguments.delta","delta":"canary-secret"}"#;
        let error = validate_openai_event(event).expect_err("tool event must fail closed");
        assert_eq!(error, "provider tool event rejected");
        assert!(!error.contains("canary-secret"));
    }

    #[test]
    fn all_budgets_accept_exact_limit_and_reject_one_more() {
        let mut input = ProviderSessionBudget::new();
        assert_eq!(input.charge_input(INPUT_SAMPLE_LIMIT as usize), Ok(()));
        assert_eq!(input.charge_input(1), Err("budget_exhausted".to_string()));

        let mut output = ProviderSessionBudget::new();
        assert_eq!(output.charge_output(OUTPUT_SAMPLE_LIMIT as usize), Ok(()));
        assert_eq!(output.charge_output(1), Err("budget_exhausted".to_string()));

        let wall_limit = ProviderSessionBudget::wall_clock_limit();
        assert_eq!(wall_limit, Duration::from_secs(1_200));
        assert!(wall_limit <= ProviderSessionBudget::wall_clock_limit());
        assert!(wall_limit + Duration::from_nanos(1) > ProviderSessionBudget::wall_clock_limit());
    }

    #[test]
    fn credential_debug_is_redacted() {
        let credential = ProviderCredential("canary-secret".to_string());
        assert_eq!(format!("{credential:?}"), "ProviderCredential([REDACTED])");
        assert!(!format!("{credential:?}").contains(credential.expose()));
    }
}
