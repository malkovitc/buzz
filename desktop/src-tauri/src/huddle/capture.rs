//! Versioned local microphone capture IPC boundary.

use std::sync::atomic::Ordering;

use tauri::State;
use uuid::Uuid;

use crate::app_state::AppState;

use super::{
    realtime_voice::{CaptureLease, CapturedHuddlePcm},
    relay_api::parse_channel_uuid,
    state::{HuddlePhase, HuddleState, VoiceInputMode},
};

/// Maximum decoded PCM payload: 100 KB. The fixed envelope is additional.
pub(super) const MAX_AUDIO_BATCH_BYTES: usize = 100 * 1024;

/// Bind one browser microphone source to the current Huddle lifetime.
#[tauri::command]
pub fn begin_huddle_capture(
    source_id: String,
    state: State<'_, AppState>,
) -> Result<CaptureLease, String> {
    let source_id = Uuid::parse_str(&source_id)
        .map_err(|_| "capture source identity is invalid".to_string())?;
    let local_human_pubkey = state
        .keys
        .lock()
        .map_err(|error| error.to_string())?
        .public_key()
        .to_hex();
    let mut huddle = state.huddle()?;
    if !matches!(huddle.phase, HuddlePhase::Connected | HuddlePhase::Active) {
        return Err("no active Huddle for microphone capture".to_string());
    }
    let channel_id = huddle
        .ephemeral_channel_id
        .as_deref()
        .ok_or("active Huddle has no backing channel")?;
    let lease = CaptureLease {
        session_channel_id: parse_channel_uuid(channel_id)?,
        huddle_generation: huddle.huddle_generation,
        capture_generation: 0,
        source_id,
        local_human_pubkey,
    };
    let replaced_capture = huddle.capture_lease.is_some();
    if replaced_capture {
        huddle.end_realtime_voice();
    }
    let lease = huddle.begin_capture(lease);
    drop(huddle);
    if replaced_capture {
        state.emit_huddle_state_changed();
    }
    Ok(lease)
}

/// End only the browser capture source named by the caller.
#[tauri::command]
pub fn end_huddle_capture(source_id: String, state: State<'_, AppState>) -> Result<(), String> {
    let source_id = Uuid::parse_str(&source_id)
        .map_err(|_| "capture source identity is invalid".to_string())?;
    let mut huddle = state.huddle()?;
    let owns_current_capture = huddle
        .capture_lease
        .as_ref()
        .is_some_and(|lease| lease.source_id == source_id);
    if owns_current_capture {
        huddle.end_capture();
        huddle.end_realtime_voice();
    }
    drop(huddle);
    if owns_current_capture {
        state.emit_huddle_state_changed();
    }
    Ok(())
}

fn capture_transmission_open(huddle: &HuddleState) -> bool {
    let manual = huddle.manual_mic_unmuted.load(Ordering::Acquire);
    let shortcut = huddle.ptt_active.load(Ordering::Acquire);
    manual || (huddle.voice_input_mode == VoiceInputMode::PushToTalk && shortcut)
}

fn human_audio_transport_available(huddle: &HuddleState) -> bool {
    let socket_open = huddle
        .audio_ws_cancel
        .as_ref()
        .is_some_and(|cancel| !cancel.is_cancelled());
    let sender_open = huddle
        .audio_relay_pcm_tx
        .as_ref()
        .is_some_and(|sender| !sender.is_closed());
    socket_open && sender_open
}

fn pcm_f32_le(samples: &[f32]) -> Vec<u8> {
    samples
        .iter()
        .flat_map(|sample| sample.to_le_bytes())
        .collect()
}

/// Validate capture identity before feeding microphone PCM to any consumer.
#[tauri::command]
pub fn push_audio_pcm(
    request: tauri::ipc::Request<'_>,
    state: State<'_, AppState>,
) -> Result<(), String> {
    let tauri::ipc::InvokeBody::Raw(bytes) = request.body() else {
        return Err("expected raw binary body".to_string());
    };
    let huddle = state.huddle()?;
    if !capture_transmission_open(&huddle) {
        return Err("microphone transmission is closed".to_string());
    }
    if !human_audio_transport_available(&huddle) {
        return Err("human Huddle audio transport is unavailable".to_string());
    }
    let lease = huddle
        .capture_lease
        .as_ref()
        .ok_or("microphone capture has no active lease")?;
    let captured = CapturedHuddlePcm::decode(bytes, lease)?;
    let pcm_bytes = pcm_f32_le(&captured.samples_48k);
    if let Some(ref provider_tx) = huddle.realtime_voice_pcm_tx {
        provider_tx.send_replace(Some(captured));
    }
    if let Some(ref pipeline) = huddle.stt_pipeline {
        pipeline.push_audio(pcm_bytes.clone())?;
    }
    if let Some(ref pcm_tx) = huddle.audio_relay_pcm_tx {
        let _ = pcm_tx.try_send(pcm_bytes);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_egress_requires_the_human_audio_transport() {
        let mut huddle = HuddleState::default();
        assert!(!human_audio_transport_available(&huddle));

        let cancel = tokio_util::sync::CancellationToken::new();
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        huddle.audio_ws_cancel = Some(cancel.clone());
        huddle.audio_relay_pcm_tx = Some(sender);
        assert!(human_audio_transport_available(&huddle));

        drop(receiver);
        assert!(!human_audio_transport_available(&huddle));
        cancel.cancel();
        assert!(!human_audio_transport_available(&huddle));
    }
}
