//! Host-owned lifecycle for one explicitly enabled Huddle realtime agent.

use std::time::Duration;

use tauri::{Manager, State};
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::{
    app_state::AppState,
    commands::fetch_relay_self,
    managed_agents::{self, ManagedAgentRecord, RespondTo},
    native_relay_client::{HuddleRelaySignal, NativeRelayClient, RelaySession, Subscription},
};

use super::{
    pipeline::maybe_start_tts_pipeline,
    realtime_openai::OpenAiRealtimeSession,
    realtime_publisher::{connect_realtime_audio_publisher, RealtimeAudioPublisher},
    realtime_voice::{
        ExternalAudioEgressGrant, ProviderEvent, ProviderInputFrame, ProviderSessionBudget,
        RealtimeVoiceSessionKey,
    },
    relay_api::channel_members_with_roles_from_event,
    state::{HuddlePhase, VoiceInputMode},
};

const AUTHORITY_READY_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);
const OUTPUT_QUIESCENCE_TIMEOUT: Duration = Duration::from_secs(2);

async fn shutdown_local_tts_output(
    pipeline: Option<std::sync::Arc<super::tts::TtsPipeline>>,
    publishers: &super::tts::LocalTtsPublishers,
) -> Result<(), String> {
    tokio::time::timeout(OUTPUT_QUIESCENCE_TIMEOUT, async move {
        if let Some(pipeline) = pipeline {
            pipeline.shutdown();
            let mut worker_exit = pipeline.worker_exit_receiver();
            if !*worker_exit.borrow() {
                worker_exit
                    .changed()
                    .await
                    .map_err(|_| "local TTS worker acknowledgement closed".to_string())?;
            }
            tokio::task::spawn_blocking(move || drop(pipeline))
                .await
                .map_err(|_| "local TTS worker disposal failed".to_string())?;
        }
        loop {
            let is_empty = publishers
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .is_empty();
            if is_empty {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .map_err(|_| "local TTS output did not quiesce".to_string())?
}

struct Admission {
    egress_grant: ExternalAudioEgressGrant,
    agent_keys: nostr::Keys,
    auth_tag: Option<String>,
    parent_channel_id: Option<String>,
    manual_input_turns: bool,
}

fn verified_agent_owner(auth_tag: Option<&str>, keys: &nostr::Keys) -> Result<String, String> {
    let auth_tag = auth_tag
        .ok_or_else(|| "realtime voice requires an owner-attested managed agent".to_string())?;
    let agent_pubkey = nostr::PublicKey::from_hex(&keys.public_key().to_hex())
        .map_err(|_| "managed agent pubkey conversion failed".to_string())?;
    buzz_sdk_pkg::nip_oa::verify_unrestricted_auth_tag(auth_tag, &agent_pubkey)
        .map(|owner| owner.to_hex())
        .map_err(|_| "managed agent owner attestation is invalid".to_string())
}

fn owner_can_direct_agent(record: &ManagedAgentRecord, owner_pubkey: &str) -> Result<bool, String> {
    let allowlist = managed_agents::validate_respond_to_allowlist(&record.respond_to_allowlist)?;
    let allowed = match managed_agents::projected_access_with_policy(
        record,
        managed_agents::owner_only(),
    )
    .0
    {
        RespondTo::OwnerOnly | RespondTo::Anyone => true,
        RespondTo::Allowlist => allowlist.iter().any(|pubkey| pubkey == owner_pubkey),
    };
    Ok(allowed)
}

fn validate_managed_agent_authority(
    records: Vec<ManagedAgentRecord>,
    agent_pubkey: &str,
    owner_pubkey: &str,
) -> Result<(ManagedAgentRecord, nostr::Keys), String> {
    let record = records
        .into_iter()
        .find(|record| record.pubkey == agent_pubkey)
        .ok_or_else(|| "realtime voice agent is not locally managed".to_string())?;
    if !record.is_active {
        return Err("realtime voice agent is archived".to_string());
    }
    let keys = nostr::Keys::parse(&record.private_key_nsec)
        .map_err(|_| "managed agent signing identity is unavailable".to_string())?;
    if keys.public_key().to_hex() != record.pubkey {
        return Err("managed agent signing identity does not match its record".to_string());
    }
    if verified_agent_owner(record.auth_tag.as_deref(), &keys)? != owner_pubkey {
        return Err("managed agent belongs to a different owner".to_string());
    }
    if !owner_can_direct_agent(&record, owner_pubkey)? {
        return Err("local user is not authorized to direct this agent".to_string());
    }
    Ok((record, keys))
}

async fn await_watcher_ready(
    events: &mut tokio::sync::mpsc::Receiver<HuddleRelaySignal>,
) -> Result<(), String> {
    tokio::time::timeout(AUTHORITY_READY_TIMEOUT, async {
        match events.recv().await {
            Some(HuddleRelaySignal::Ready) => Ok(()),
            Some(HuddleRelaySignal::Event(_)) => {
                Err("Huddle authority changed during admission".to_string())
            }
            Some(HuddleRelaySignal::Unavailable) | None => {
                Err("Huddle authority watcher is unavailable".to_string())
            }
        }
    })
    .await
    .map_err(|_| "Huddle authority watcher timed out".to_string())?
}

fn watcher_remains_quiet(events: &mut tokio::sync::mpsc::Receiver<HuddleRelaySignal>) -> bool {
    matches!(
        events.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    )
}

struct AdmissionScope<'a> {
    channel_id: &'a str,
    agent_pubkey: &'a str,
    owner_pubkey: &'a str,
    relay_pubkey: &'a str,
    huddle_generation: u64,
}

async fn admission_snapshot(
    app: &tauri::AppHandle,
    state: &AppState,
    session: &RelaySession,
    scope: AdmissionScope<'_>,
) -> Result<Admission, String> {
    let membership_filter = serde_json::json!({
        "kinds": [39002],
        "authors": [scope.relay_pubkey],
        "#p": [scope.owner_pubkey],
        "#d": [scope.channel_id],
        "limit": 1,
    });
    let membership = session
        .fetch_events(membership_filter, AUTHORITY_READY_TIMEOUT)
        .await?;
    let membership = membership
        .first()
        .filter(|event| event.pubkey.to_hex() == scope.relay_pubkey)
        .ok_or_else(|| "authoritative Huddle membership is unavailable".to_string())?;
    let members = channel_members_with_roles_from_event(membership);
    let is_bot_member = members
        .iter()
        .any(|(pubkey, role)| pubkey == scope.agent_pubkey && role.as_deref() == Some("bot"));
    if !is_bot_member {
        return Err("managed agent is not an authoritative bot member of this Huddle".to_string());
    }
    let is_local_human_member = members
        .iter()
        .any(|(pubkey, role)| pubkey == scope.owner_pubkey && role.as_deref() != Some("bot"));
    if !is_local_human_member {
        return Err("local user is not an authoritative human Huddle member".to_string());
    }

    let records = {
        let _store = state
            .managed_agents_store_lock
            .lock()
            .map_err(|error| error.to_string())?;
        managed_agents::storage::load_managed_agents(app)?
    };
    let (record, agent_keys) =
        validate_managed_agent_authority(records, scope.agent_pubkey, scope.owner_pubkey)?;
    let expected_relay = crate::relay::relay_ws_url_with_override(state);
    if record.relay_url != expected_relay {
        return Err("managed agent belongs to a different relay scope".to_string());
    }

    let (capture, parent_channel_id, manual_input_turns) = {
        let huddle = state.huddle()?;
        if !huddle.is_current_huddle(scope.channel_id, scope.huddle_generation) {
            return Err("Huddle changed during realtime voice admission".to_string());
        }
        let capture = huddle
            .capture_lease
            .clone()
            .ok_or_else(|| "local microphone capture is not active".to_string())?;
        (
            capture,
            huddle.parent_channel_id.clone(),
            huddle.voice_input_mode == VoiceInputMode::PushToTalk,
        )
    };
    let session_key = RealtimeVoiceSessionKey::validated(
        scope.channel_id,
        scope.huddle_generation,
        scope.agent_pubkey,
    )?;
    let egress_grant = ExternalAudioEgressGrant::validated(session_key.clone(), &capture)?;
    if egress_grant.local_human_pubkey != scope.owner_pubkey {
        return Err("microphone capture owner does not match the local identity".to_string());
    }

    Ok(Admission {
        egress_grant,
        agent_keys,
        auth_tag: record.auth_tag,
        parent_channel_id,
        manual_input_turns,
    })
}

fn admitted_provider_input(
    grant: &ExternalAudioEgressGrant,
    budget: &mut ProviderSessionBudget,
    capture: &super::realtime_voice::CapturedHuddlePcm,
) -> Result<ProviderInputFrame, String> {
    if !grant.admits(capture) {
        return Err("microphone capture authority changed".to_string());
    }
    budget.charge_input(capture.samples_48k.len())?;
    ProviderInputFrame::from_capture(capture)
}

struct RuntimeResources {
    provider: OpenAiRealtimeSession,
    publisher: RealtimeAudioPublisher,
    input_rx: watch::Receiver<Option<super::realtime_voice::CapturedHuddlePcm>>,
    input_turns: watch::Receiver<u64>,
    output_interrupts: watch::Receiver<u64>,
    watcher_events: tokio::sync::mpsc::Receiver<HuddleRelaySignal>,
    store_changes: watch::Receiver<u64>,
    admission: Admission,
    subscription_id: String,
}

async fn run_runtime(
    mut resources: RuntimeResources,
    cancel: CancellationToken,
    session: std::sync::Arc<RelaySession>,
) {
    let mut budget = ProviderSessionBudget::new();
    let wall_deadline = tokio::time::sleep(ProviderSessionBudget::wall_clock_limit());
    tokio::pin!(wall_deadline);
    let publisher_failure = resources.publisher.failure_token();

    loop {
        let outcome: Result<(), String> = tokio::select! {
            biased;
            _ = cancel.cancelled() => break,
            _ = publisher_failure.cancelled() => Err("agent audio publisher became unavailable".to_string()),
            _ = &mut wall_deadline => Err("budget_exhausted".to_string()),
            changed = resources.store_changes.changed() => {
                let _ = changed;
                Err("managed agent authority changed".to_string())
            },
            signal = resources.watcher_events.recv() => match signal {
                Some(HuddleRelaySignal::Ready) => Ok(()),
                Some(HuddleRelaySignal::Event(event)) => {
                    let _ = event;
                    Err("Huddle authority changed".to_string())
                }
                Some(HuddleRelaySignal::Unavailable) | None => {
                    Err("Huddle authority watcher became unavailable".to_string())
                }
            },
            changed = resources.input_rx.changed() => match changed {
                Err(_) => Err("microphone capture closed".to_string()),
                Ok(()) => {
                    let capture = resources.input_rx.borrow_and_update().clone();
                    let Some(capture) = capture else { continue };
                    admitted_provider_input(
                        &resources.admission.egress_grant,
                        &mut budget,
                        &capture,
                    ).and_then(|input| {
                        resources.provider.input.try_send(input)
                            .map_err(|_| "provider input backpressure limit reached".to_string())
                    })
                }
            },
            changed = resources.input_turns.changed() => match changed {
                Err(_) => Err("microphone input control closed".to_string()),
                Ok(()) => resources.provider.end_input_turn(),
            },
            changed = resources.output_interrupts.changed() => match changed {
                Err(_) => Err("microphone interruption control closed".to_string()),
                Ok(()) => {
                    resources.publisher.clear();
                    match resources.provider.interrupt_output_after_fence() {
                        Err(error) => Err(error),
                        Ok(()) => tokio::time::timeout(
                            SHUTDOWN_TIMEOUT,
                            resources.publisher.wait_for_output_quiescence(),
                        )
                        .await
                        .map(|_| ())
                        .map_err(|_| "provider output send did not quiesce".to_string()),
                    }
                }
            },
            event = resources.provider.events.recv() => match event {
                Some(ProviderEvent::Audio(frame)) => {
                    if !resources
                        .provider
                        .output_fence()
                        .admits(frame.output_generation)
                    {
                        continue;
                    }
                    match budget.charge_output(frame.samples_24k.len()) {
                        Ok(()) => {
                            let _ = resources
                                .publisher
                                .publish(frame.output_generation, &frame.samples_24k);
                            Ok(())
                        }
                        Err(error) => Err(error),
                    }
                },
                Some(ProviderEvent::InputSpeechStarted) => {
                    resources.publisher.clear();
                    Ok(())
                },
                Some(ProviderEvent::OutputDone { .. }) => {
                    resources.publisher.finish_output();
                    Ok(())
                },
                Some(ProviderEvent::InputSpeechStopped | ProviderEvent::OutputStarted { .. } | ProviderEvent::ResponseDone { .. } | ProviderEvent::SessionReady) => Ok(()),
                None => Err("realtime provider became unavailable".to_string())
            }
        };
        if let Err(reason) = outcome {
            eprintln!("buzz-desktop: realtime voice stopped: {reason}");
            break;
        }
    }

    resources.provider.cancel();
    resources.publisher.clear();
    let subscription_id = resources.subscription_id;
    let _ = resources.publisher.close().await;
    let _ = resources.provider.close().await;
    session.clear_huddle_subscription(&subscription_id).await;
}

#[tauri::command]
pub(crate) async fn enable_realtime_voice(
    agent_pubkey: String,
    app: tauri::AppHandle,
    state: State<'_, AppState>,
    native_relay: State<'_, NativeRelayClient>,
) -> Result<(), String> {
    super::relay_api::validate_pubkey_hex(&agent_pubkey)?;
    let (done_tx, done_rx) = watch::channel(false);
    let output_fence = super::realtime_voice::ProviderOutputFence::shared();
    let (channel_id, huddle_generation, generation, cancel, owner_keys) = {
        let owner_keys = state
            .keys
            .lock()
            .map_err(|error| error.to_string())?
            .clone();
        let mut huddle = state.huddle()?;
        if huddle.realtime_voice_active {
            return Err("realtime voice is already active".to_string());
        }
        if huddle.realtime_voice_is_stopping() {
            return Err("realtime voice is still shutting down".to_string());
        }
        if !matches!(huddle.phase, HuddlePhase::Connected | HuddlePhase::Active) {
            return Err("realtime voice requires an active Huddle".to_string());
        }
        let channel_id = huddle
            .ephemeral_channel_id
            .clone()
            .ok_or_else(|| "active Huddle has no backing channel".to_string())?;
        let huddle_generation = huddle.huddle_generation;
        let human_audio = huddle
            .audio_ws_cancel
            .as_ref()
            .filter(|cancel| !cancel.is_cancelled())
            .ok_or_else(|| "human Huddle audio transport is unavailable".to_string())?;
        let cancel = human_audio.child_token();
        let (generation, cancel) = huddle.begin_realtime_voice(
            agent_pubkey.clone(),
            std::sync::Arc::clone(&output_fence),
            cancel,
        );
        huddle.realtime_voice_done = Some(done_rx);
        (
            channel_id,
            huddle_generation,
            generation,
            cancel,
            owner_keys,
        )
    };

    let relay_pubkey = tokio::select! {
        biased;
        _ = cancel.cancelled() => Err("realtime voice activation was cancelled".to_string()),
        value = fetch_relay_self(&state) => value.and_then(|value| {
            value.ok_or_else(|| "relay membership authority is unavailable".to_string())
        }),
    };
    let relay_pubkey = match relay_pubkey {
        Ok(pubkey) => pubkey,
        Err(error) => {
            state.huddle()?.finish_realtime_voice(generation);
            done_tx.send_replace(true);
            return Err(error);
        }
    };
    let relay_url = crate::relay::relay_ws_url_with_override(&state);
    let (session, mut watcher_events) = match native_relay
        .huddle_session(relay_url, owner_keys.clone(), cancel.clone())
        .await
    {
        Ok(value) => value,
        Err(error) => {
            state.huddle()?.finish_realtime_voice(generation);
            done_tx.send_replace(true);
            return Err(error);
        }
    };
    let subscription_id = format!("huddle:{channel_id}:{generation}");
    let subscription = Subscription {
        id: subscription_id.clone(),
        filter: serde_json::json!({
            "kinds": [39002],
            "authors": [relay_pubkey],
            "#h": [channel_id],
            "#d": [channel_id],
            "limit": 0,
        }),
    };
    session.set_huddle_subscription(Some(subscription)).await;

    let store_changes = managed_agents::storage::subscribe_managed_agent_store_changes();
    managed_agents::storage::fence_managed_agent_authority_with(cancel.clone());
    let activation = tokio::select! {
        biased;
        _ = cancel.cancelled() => Err("realtime voice activation was cancelled".to_string()),
        result = async {
        await_watcher_ready(&mut watcher_events).await?;
        let owner_pubkey = owner_keys.public_key().to_hex();
        let admission = admission_snapshot(
            &app,
            &state,
            &session,
            AdmissionScope {
                channel_id: &channel_id,
                agent_pubkey: &agent_pubkey,
                owner_pubkey: &owner_pubkey,
                relay_pubkey: &relay_pubkey,
                huddle_generation,
            },
        )
        .await?;
        if store_changes.has_changed().unwrap_or(true) {
            return Err("managed agent authority changed during admission".to_string());
        }
        if !watcher_remains_quiet(&mut watcher_events) {
            return Err("Huddle authority changed during admission".to_string());
        }

        let mut provider = OpenAiRealtimeSession::connect_with_output_fence(
            admission.manual_input_turns,
            std::sync::Arc::clone(&output_fence),
            cancel.clone(),
        ).await?;
        let publisher = match connect_realtime_audio_publisher(
            &channel_id,
            admission.parent_channel_id.as_deref(),
            &state,
            &admission.agent_keys,
            admission.auth_tag.as_deref(),
            std::sync::Arc::clone(&output_fence),
            cancel.clone(),
        )
        .await
        {
            Ok(publisher) => publisher,
            Err(error) => {
                let _ = provider.close().await;
                return Err(error);
            }
        };
        Ok::<_, String>((admission, provider, publisher))
        } => result,
    };

    let (admission, mut provider, mut publisher) = match activation {
        Ok(resources) => resources,
        Err(error) => {
            session.clear_huddle_subscription(&subscription_id).await;
            state.huddle()?.finish_realtime_voice(generation);
            done_tx.send_replace(true);
            return Err(error);
        }
    };

    let authority_remains_current =
        !store_changes.has_changed().unwrap_or(true) && watcher_remains_quiet(&mut watcher_events);
    if !authority_remains_current {
        provider.cancel();
        let _ = publisher.close().await;
        let _ = provider.close().await;
        session.clear_huddle_subscription(&subscription_id).await;
        state.huddle()?.finish_realtime_voice(generation);
        done_tx.send_replace(true);
        return Err("realtime voice authority changed before activation".to_string());
    }

    let (input_tx, input_rx) = watch::channel(None);
    let (input_turn_tx, input_turns) = watch::channel(0);
    let (output_interrupt_tx, output_interrupts) = watch::channel(0);
    let local_tts = (|| {
        let mut huddle = state.huddle()?;
        let still_current = huddle.realtime_voice_generation == generation
            && huddle.is_current_huddle(&channel_id, huddle_generation)
            && !cancel.is_cancelled();
        if !still_current {
            return Err("Huddle changed before realtime voice activation".to_string());
        }
        huddle.realtime_voice_pcm_tx = Some(input_tx);
        huddle.realtime_voice_turn_tx = Some(input_turn_tx);
        huddle.realtime_voice_interrupt_tx = Some(output_interrupt_tx);
        Ok((
            huddle.tts_pipeline.take(),
            std::sync::Arc::clone(&huddle.local_tts_publishers),
        ))
    })();
    let (local_tts, local_tts_publishers) = match local_tts {
        Ok(resources) => resources,
        Err(error) => {
            provider.cancel();
            let _ = publisher.close().await;
            let _ = provider.close().await;
            session.clear_huddle_subscription(&subscription_id).await;
            state.huddle()?.finish_realtime_voice(generation);
            done_tx.send_replace(true);
            return Err(error);
        }
    };
    if let Err(error) = shutdown_local_tts_output(local_tts, &local_tts_publishers).await {
        provider.cancel();
        let _ = publisher.close().await;
        let _ = provider.close().await;
        session.clear_huddle_subscription(&subscription_id).await;
        state.huddle()?.finish_realtime_voice(generation);
        done_tx.send_replace(true);
        let _ = maybe_start_tts_pipeline(&state).await;
        return Err(error);
    }

    let task_app = app.clone();
    tokio::spawn(async move {
        run_runtime(
            RuntimeResources {
                provider,
                publisher,
                input_rx,
                input_turns,
                output_interrupts,
                watcher_events,
                store_changes,
                admission,
                subscription_id,
            },
            cancel,
            session,
        )
        .await;
        let app_state = task_app.state::<AppState>();
        if let Ok(mut huddle) = app_state.huddle() {
            huddle.finish_realtime_voice(generation);
        }
        app_state.emit_huddle_state_changed();
        done_tx.send_replace(true);
    });
    state.emit_huddle_state_changed();
    Ok(())
}

#[tauri::command]
pub(crate) async fn disable_realtime_voice(state: State<'_, AppState>) -> Result<(), String> {
    let (mut done, channel_id) = {
        let mut huddle = state.huddle()?;
        let done = huddle.realtime_voice_done.clone();
        let channel_id = huddle.ephemeral_channel_id.clone();
        huddle.end_realtime_voice();
        (done, channel_id)
    };
    if let Some(receiver) = done.as_mut() {
        if !*receiver.borrow() {
            tokio::time::timeout(SHUTDOWN_TIMEOUT, receiver.changed())
                .await
                .map_err(|_| "realtime voice shutdown timed out".to_string())?
                .map_err(|_| "realtime voice shutdown acknowledgement closed".to_string())?;
        }
    }
    state.emit_huddle_state_changed();
    if channel_id.is_some() {
        let _ = maybe_start_tts_pipeline(&state).await;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{shutdown_local_tts_output, verified_agent_owner};

    #[test]
    fn managed_agent_owner_requires_a_valid_attestation() {
        let owner = nostr::Keys::generate();
        let other_owner = nostr::Keys::generate();
        let agent = nostr::Keys::generate();
        let auth_tag = buzz_sdk_pkg::nip_oa::compute_auth_tag(&owner, &agent.public_key(), "")
            .expect("owner attestation");

        let verified = verified_agent_owner(Some(&auth_tag), &agent).unwrap();
        assert_eq!(verified, owner.public_key().to_hex());
        assert_ne!(verified, other_owner.public_key().to_hex());
        assert!(verified_agent_owner(None, &agent).is_err());

        let restricted = buzz_sdk_pkg::nip_oa::compute_auth_tag(
            &owner,
            &agent.public_key(),
            "created_at<4294967295",
        )
        .expect("signed restricted attestation");
        assert!(verified_agent_owner(Some(&restricted), &agent).is_err());
    }

    #[tokio::test]
    async fn realtime_output_waits_for_local_tts_socket_release() {
        let publishers = super::super::tts::LocalTtsPublishers::default();
        let lease =
            super::super::tts::LocalTtsPublisherLease::new(7, std::sync::Arc::clone(&publishers));
        let release = tokio::spawn(async move {
            tokio::task::yield_now().await;
            drop(lease);
        });

        shutdown_local_tts_output(None, &publishers)
            .await
            .expect("local TTS quiescence");
        release.await.expect("release task");
    }
}
