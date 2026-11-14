//! Establishes agent-authenticated publishers for locally synthesized speech.

use std::sync::Arc;

use super::agent_tts_routing::{classify_agent_tts_publisher, AgentTtsPublisherMode};
use super::{relay_api, tts};
use crate::app_state::AppState;

#[cfg(unix)]
fn broker_snapshot(
    state: &AppState,
    speaker_pubkey: &str,
) -> Result<Option<crate::managed_agents::BrokerMediaSnapshot>, String> {
    let relay_url = crate::relay::relay_ws_url_with_override(state);
    let key = crate::managed_agents::ManagedAgentRuntimeKey::new(speaker_pubkey, &relay_url)?;
    let mut runtimes = state
        .managed_agent_processes
        .lock()
        .map_err(|error| format!("managed runtime lock poisoned: {error}"))?;
    runtimes
        .get_mut(&key)
        .and_then(|runtime| runtime.broker_host.as_mut())
        .map(|host| host.media_snapshot())
        .transpose()
}

#[cfg(unix)]
fn with_current_broker_media<R>(
    state: &AppState,
    speaker_pubkey: &str,
    authority_json: &str,
    action: impl FnOnce() -> R,
) -> Result<R, String> {
    let relay_url = crate::relay::relay_ws_url_with_override(state);
    let key = crate::managed_agents::ManagedAgentRuntimeKey::new(speaker_pubkey, &relay_url)?;
    let mut runtimes = state
        .managed_agent_processes
        .lock()
        .map_err(|error| format!("managed runtime lock poisoned: {error}"))?;
    let host = runtimes
        .get_mut(&key)
        .and_then(|runtime| runtime.broker_host.as_mut())
        .ok_or("selected broker generation is no longer tracked")?;
    host.with_current_media(authority_json, action)
}

pub(super) async fn ensure(
    app: &tauri::AppHandle,
    state: &AppState,
    pipeline: &tts::TtsPipeline,
    speaker_pubkey: &str,
) -> Result<bool, String> {
    let selector = match std::env::var("BUZZ_DESKTOP_BROKER_AGENT_PUBKEY") {
        Ok(selector) => Some(selector),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err("BUZZ_DESKTOP_BROKER_AGENT_PUBKEY is not valid text".into());
        }
    };
    #[cfg(unix)]
    let broker_selected = selector
        .as_deref()
        .is_some_and(|selector| selector.eq_ignore_ascii_case(speaker_pubkey));
    #[cfg(unix)]
    let broker = if broker_selected {
        broker_snapshot(state, speaker_pubkey)?
    } else {
        None
    };
    #[cfg(not(unix))]
    let broker: Option<()> = None;
    let mode = classify_agent_tts_publisher(selector.as_deref(), speaker_pubkey, broker.is_some())?;

    let (ephemeral_channel_id, parent_channel_id, huddle_generation, local_tts_publishers) = {
        let huddle = state.huddle()?;
        if !huddle.tts_enabled
            || !matches!(
                huddle.phase,
                super::HuddlePhase::Connected | super::HuddlePhase::Active
            )
        {
            return Err("agent TTS is not enabled in an active Huddle".into());
        }
        (
            huddle
                .ephemeral_channel_id
                .clone()
                .ok_or("active Huddle has no backing channel")?,
            huddle.parent_channel_id.clone(),
            huddle.huddle_generation,
            Arc::clone(&huddle.local_tts_publishers),
        )
    };
    match mode {
        AgentTtsPublisherMode::Direct if pipeline.has_audio_publisher(speaker_pubkey) => {
            return Ok(true);
        }
        _ => {}
    }
    let has_bot_membership =
        relay_api::fetch_channel_members_with_roles(&ephemeral_channel_id, state)
            .await?
            .into_iter()
            .any(|(pubkey, role)| {
                pubkey.eq_ignore_ascii_case(speaker_pubkey) && role.as_deref() == Some("bot")
            });
    if !has_bot_membership {
        return Err("agent is not an active bot member of the Huddle".to_string());
    }
    with_current_huddle(
        state,
        pipeline,
        speaker_pubkey,
        &ephemeral_channel_id,
        parent_channel_id.as_deref(),
        huddle_generation,
        || (),
    )?;
    #[cfg(unix)]
    if let (AgentTtsPublisherMode::Broker, Some(snapshot), Some(parent)) =
        (mode, broker.as_ref(), parent_channel_id.as_deref())
    {
        if pipeline.has_broker_audio_publisher(
            speaker_pubkey,
            &snapshot.authority_json,
            &ephemeral_channel_id,
            parent,
            huddle_generation,
        ) {
            return with_current_broker_media(
                state,
                speaker_pubkey,
                &snapshot.authority_json,
                || {
                    with_current_huddle(
                        state,
                        pipeline,
                        speaker_pubkey,
                        &ephemeral_channel_id,
                        parent_channel_id.as_deref(),
                        huddle_generation,
                        || true,
                    )
                },
            )?;
        }
    }
    let publisher = match mode {
        AgentTtsPublisherMode::Direct => {
            let app_for_load = app.clone();
            let speaker_for_load = speaker_pubkey.to_ascii_lowercase();
            let record = tokio::task::spawn_blocking(move || {
                crate::managed_agents::load_managed_agents(&app_for_load).map(|agents| {
                    agents.into_iter().find(|agent| {
                        agent.pubkey.eq_ignore_ascii_case(&speaker_for_load)
                            && !agent.private_key_nsec.trim().is_empty()
                    })
                })
            })
            .await
            .map_err(|error| format!("managed-agent identity task failed: {error}"))??;
            let Some(record) = record else {
                return Ok(false);
            };
            let keys = nostr::Keys::parse(record.private_key_nsec.trim())
                .map_err(|error| format!("managed-agent identity is unavailable: {error}"))?;
            if !keys
                .public_key()
                .to_hex()
                .eq_ignore_ascii_case(speaker_pubkey)
            {
                return Err("managed-agent identity does not match the Huddle speaker".into());
            }
            relay_api::connect_tts_audio_publisher(
                &ephemeral_channel_id,
                parent_channel_id.as_deref(),
                state,
                &keys,
                record.auth_tag.as_deref(),
                local_tts_publishers,
            )
            .await?
        }
        AgentTtsPublisherMode::Broker => {
            #[cfg(unix)]
            {
                let snapshot = broker
                    .as_ref()
                    .ok_or("selected broker speaker is unavailable")?;
                let parent = parent_channel_id
                    .as_deref()
                    .ok_or("active Huddle has no parent channel")?;
                let owner = state.signing_keys()?;
                relay_api::connect_broker_tts_audio_publisher(
                    snapshot,
                    &owner,
                    &ephemeral_channel_id,
                    parent,
                    huddle_generation,
                    local_tts_publishers,
                )
                .await?
            }
            #[cfg(not(unix))]
            {
                return Err("broker media is unavailable on this platform".into());
            }
        }
    };
    // Runtime → Huddle is the only nested lock order here. Stop/start paths
    // never acquire the Huddle lock while holding the runtime map lock.
    // Keeping the runtime lock through registration orders it before a
    // concurrent quiesce; the host closes any admitted socket on stop.
    #[cfg(unix)]
    if let Some(snapshot) = broker.as_ref() {
        return with_current_broker_media(state, speaker_pubkey, &snapshot.authority_json, || {
            register_if_current_huddle(
                state,
                pipeline,
                speaker_pubkey,
                publisher,
                &ephemeral_channel_id,
                parent_channel_id.as_deref(),
                huddle_generation,
            )
        })?;
    }
    register_if_current_huddle(
        state,
        pipeline,
        speaker_pubkey,
        publisher,
        &ephemeral_channel_id,
        parent_channel_id.as_deref(),
        huddle_generation,
    )
}

fn register_if_current_huddle(
    state: &AppState,
    pipeline: &tts::TtsPipeline,
    speaker_pubkey: &str,
    publisher: tts::TtsAudioPublisher,
    ephemeral_channel_id: &str,
    parent_channel_id: Option<&str>,
    huddle_generation: u64,
) -> Result<bool, String> {
    with_current_huddle(
        state,
        pipeline,
        speaker_pubkey,
        ephemeral_channel_id,
        parent_channel_id,
        huddle_generation,
        || {
            pipeline.register_audio_publisher(speaker_pubkey, publisher);
            true
        },
    )
}

fn with_current_huddle<R>(
    state: &AppState,
    pipeline: &tts::TtsPipeline,
    speaker_pubkey: &str,
    ephemeral_channel_id: &str,
    parent_channel_id: Option<&str>,
    huddle_generation: u64,
    action: impl FnOnce() -> R,
) -> Result<R, String> {
    let huddle = state.huddle()?;
    if !huddle.tts_enabled
        || !super::agent_voice::agent_tts_enabled(&huddle, speaker_pubkey)
        || !huddle.is_current_huddle(ephemeral_channel_id, huddle_generation)
        || huddle.parent_channel_id.as_deref() != parent_channel_id
        || !huddle
            .tts_pipeline
            .as_ref()
            .is_some_and(|active| std::ptr::eq(active.as_ref(), pipeline))
    {
        return Err("Huddle changed during agent TTS publisher setup".into());
    }
    Ok(action())
}
