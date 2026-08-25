use crate::acp::BuzzPromptMetadata;
use crate::queue::{
    parse_thread_tags, resolve_reply_anchor, FlushBatch, PromptChannelInfo, PromptProfileLookup,
};

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
