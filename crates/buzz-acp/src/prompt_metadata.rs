use crate::acp::BuzzPromptMetadata;
use crate::queue::{
    parse_thread_tags, resolve_reply_anchor, FlushBatch, PromptChannelInfo, PromptProfileLookup,
};

pub(crate) fn is_authenticated_cloud_control_event(
    event: &nostr::Event,
    owner_pubkey: Option<&nostr::PublicKey>,
    agent_pubkey: &nostr::PublicKey,
) -> bool {
    let Some(owner) = owner_pubkey else {
        return false;
    };
    let agent_hex = agent_pubkey.to_hex();
    event.kind.as_u16() == buzz_core::kind::KIND_STREAM_MESSAGE as u16
        && event.pubkey == *owner
        && event.tags.iter().any(|tag| {
            tag.as_slice().first().map(String::as_str) == Some("p")
                && tag.as_slice().get(1).map(String::as_str) == Some(agent_hex.as_str())
        })
}

pub(crate) fn cloud_control_for_event(
    event: &nostr::Event,
    known_names: &[&str],
    owner_pubkey: Option<&nostr::PublicKey>,
    agent_pubkey: &nostr::PublicKey,
) -> Option<String> {
    is_authenticated_cloud_control_event(event, owner_pubkey, agent_pubkey)
        .then(|| crate::queue::extract_cloud_control_command(&event.content, known_names))
        .flatten()
}

pub(crate) fn cloud_control_for_batch(
    batch: &FlushBatch,
    known_names: &[&str],
    owner_pubkey: Option<&nostr::PublicKey>,
    agent_pubkey: &nostr::PublicKey,
) -> Option<String> {
    if batch.events.len() != 1 || !batch.cancelled_events.is_empty() {
        return None;
    }
    let event = &batch.events.first()?.event;
    if !is_authenticated_cloud_control_event(event, owner_pubkey, agent_pubkey) {
        return None;
    }
    if let Some(command) = batch.events[0].prompt_tag.strip_prefix("__buzz_control:") {
        if matches!(command, "-status" | "-cloud" | "-local") {
            return Some(command.to_string());
        }
    }
    cloud_control_for_event(event, known_names, owner_pubkey, agent_pubkey)
}

pub(crate) fn for_batch(
    batch: &FlushBatch,
    channel_info: Option<&PromptChannelInfo>,
    profile_lookup: Option<&PromptProfileLookup>,
) -> Option<BuzzPromptMetadata> {
    let last = batch
        .events
        .last()
        .or_else(|| batch.cancelled_events.last())?;
    let triggering_event_ids = batch
        .cancelled_events
        .iter()
        .chain(batch.events.iter())
        .map(|event| event.event.id.to_hex())
        .collect();
    let triggering_id = last.event.id.to_hex();
    let is_dm = channel_info.is_some_and(|info| info.channel_type == "dm");
    let reply_to = if is_dm {
        triggering_id.clone()
    } else {
        resolve_reply_anchor(
            &last.event.pubkey.to_hex(),
            &parse_thread_tags(&last.event),
            &triggering_id,
            profile_lookup,
        )
        .unwrap_or_else(|| triggering_id.clone())
    };
    Some(BuzzPromptMetadata {
        channel_id: batch.channel_id.to_string(),
        triggering_event_ids,
        allowed_reply_event_ids: vec![reply_to.clone()],
        reply_to,
        control_command: None,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Instant;

    use nostr::{EventBuilder, Keys, Kind, Tag};
    use uuid::Uuid;

    use super::*;
    use crate::queue::BatchEvent;

    fn batch(tags: Vec<Tag>) -> FlushBatch {
        let event = EventBuilder::new(Kind::Custom(9), "reply")
            .tags(tags)
            .sign_with_keys(&Keys::generate())
            .unwrap();
        FlushBatch {
            channel_id: Uuid::new_v4(),
            events: vec![BatchEvent {
                event,
                prompt_tag: "test".into(),
                received_at: Instant::now(),
            }],
            cancelled_events: Vec::new(),
            cancel_reason: None,
        }
    }

    #[test]
    fn cloud_control_requires_exact_owner_stream_event_and_real_agent_mention() {
        let owner = Keys::generate();
        let agent = Keys::generate();
        let mentioned = EventBuilder::new(Kind::Custom(9), "@Caliper -status")
            .tags([Tag::public_key(agent.public_key())])
            .sign_with_keys(&owner)
            .unwrap();
        let control_batch = FlushBatch {
            channel_id: Uuid::new_v4(),
            events: vec![BatchEvent {
                event: mentioned,
                prompt_tag: "test".into(),
                received_at: Instant::now(),
            }],
            cancelled_events: Vec::new(),
            cancel_reason: None,
        };
        assert_eq!(
            cloud_control_for_batch(
                &control_batch,
                &["Caliper"],
                Some(&owner.public_key()),
                &agent.public_key(),
            ),
            Some("-status".into())
        );
        assert_eq!(
            cloud_control_for_batch(
                &control_batch,
                &["Caliper"],
                Some(&Keys::generate().public_key()),
                &agent.public_key(),
            ),
            None,
            "a non-owner event must never gain control semantics"
        );
        assert_eq!(
            cloud_control_for_batch(
                &control_batch,
                &["Caliper"],
                Some(&owner.public_key()),
                &Keys::generate().public_key(),
            ),
            None,
            "the event must mention this exact agent"
        );

        let pre_admitted = EventBuilder::new(Kind::Custom(9), "@Unknown Multi Alias -status")
            .tags([Tag::public_key(agent.public_key())])
            .sign_with_keys(&owner)
            .unwrap();
        let pre_admitted_batch = FlushBatch {
            channel_id: Uuid::new_v4(),
            events: vec![BatchEvent {
                event: pre_admitted,
                prompt_tag: "__buzz_control:-status".into(),
                received_at: Instant::now(),
            }],
            cancelled_events: Vec::new(),
            cancel_reason: None,
        };
        assert_eq!(
            cloud_control_for_batch(
                &pre_admitted_batch,
                &[],
                Some(&owner.public_key()),
                &agent.public_key(),
            ),
            Some("-status".into())
        );

        let malformed = EventBuilder::new(Kind::Custom(9), "@Caliper -status check")
            .tags([Tag::public_key(agent.public_key())])
            .sign_with_keys(&owner)
            .unwrap();
        assert_eq!(
            cloud_control_for_event(
                &malformed,
                &["Caliper"],
                Some(&owner.public_key()),
                &agent.public_key(),
            ),
            Some(crate::queue::REJECTED_CLOUD_CONTROL_COMMAND.into())
        );
    }

    #[test]
    fn human_thread_publication_uses_the_root_anchor() {
        let root = "a".repeat(64);
        let parent = "b".repeat(64);
        let tags = vec![
            Tag::parse(["e", &root, "", "root"]).unwrap(),
            Tag::parse(["e", &parent, "", "reply"]).unwrap(),
        ];
        let metadata = for_batch(&batch(tags), None, None).unwrap();
        assert_eq!(metadata.reply_to, root);
        assert_ne!(metadata.reply_to, metadata.triggering_event_ids[0]);
        assert_eq!(metadata.allowed_reply_event_ids, vec![metadata.reply_to]);
    }

    #[test]
    fn top_level_publication_uses_the_triggering_event() {
        let metadata = for_batch(&batch(Vec::new()), None, None).unwrap();
        assert_eq!(metadata.reply_to, metadata.triggering_event_ids[0]);
    }
}
