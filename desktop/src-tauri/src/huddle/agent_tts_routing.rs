use super::HuddlePhase;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum AgentTtsPublisherMode {
    Direct,
    Broker,
}

pub(super) fn is_broker_selected_agent(speaker_pubkey: &str) -> bool {
    std::env::var("BUZZ_DESKTOP_BROKER_AGENT_PUBKEY")
        .is_ok_and(|selector| selector.eq_ignore_ascii_case(speaker_pubkey))
}

pub(super) fn should_queue_agent_tts(
    broker_selected: bool,
    publisher: &Result<bool, String>,
) -> bool {
    !broker_selected || matches!(publisher, Ok(true))
}

pub(super) fn classify_agent_tts_publisher(
    selector: Option<&str>,
    speaker_pubkey: &str,
    broker_live: bool,
) -> Result<AgentTtsPublisherMode, String> {
    let Some(selector) = selector else {
        return Ok(AgentTtsPublisherMode::Direct);
    };
    if selector.len() != 64 || !selector.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("BUZZ_DESKTOP_BROKER_AGENT_PUBKEY must be one 64-hex agent pubkey".into());
    }
    if !selector.eq_ignore_ascii_case(speaker_pubkey) {
        return Ok(AgentTtsPublisherMode::Direct);
    }
    if !broker_live {
        return Err("selected broker speaker has no active broker generation".into());
    }
    Ok(AgentTtsPublisherMode::Broker)
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum AgentTtsRuntimeGate {
    Disabled,
    Inactive,
    NeedsPipeline,
    Ready,
}

pub(super) fn classify_agent_tts_runtime(
    enabled: bool,
    phase: &HuddlePhase,
    has_pipeline: bool,
) -> AgentTtsRuntimeGate {
    if !enabled {
        AgentTtsRuntimeGate::Disabled
    } else if !matches!(phase, HuddlePhase::Connected | HuddlePhase::Active) {
        AgentTtsRuntimeGate::Inactive
    } else if has_pipeline {
        AgentTtsRuntimeGate::Ready
    } else {
        AgentTtsRuntimeGate::NeedsPipeline
    }
}

/// Maximum text length accepted for TTS synthesis.
/// This high safety cap keeps unexpectedly large events bounded while allowing
/// normal long-form huddle replies to play in full.
pub(super) const MAX_TTS_TEXT_LEN: usize = 8_096;

pub(super) fn normalize_agent_tts_text(text: String) -> String {
    if text.chars().count() > MAX_TTS_TEXT_LEN {
        let mut truncated: String = text.chars().take(MAX_TTS_TEXT_LEN).collect();
        truncated.push_str("... message truncated.");
        truncated
    } else {
        text
    }
}

pub(super) async fn enqueue_agent_tts_text<F>(
    route_id: u64,
    text: String,
    enqueue: F,
) -> Result<(), String>
where
    F: FnOnce(u64, String) -> Result<(), String> + Send + 'static,
{
    tokio::task::spawn_blocking(move || enqueue(route_id, text))
        .await
        .map_err(|error| format!("TTS enqueue task failed: {error}"))?
}

#[cfg(test)]
#[path = "agent_tts_routing_tests.rs"]
mod tests;
