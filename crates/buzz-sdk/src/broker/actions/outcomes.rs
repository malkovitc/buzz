//! Outcome types — the success payload of each [`Action`], and the tagged union
//! that pairs a wire action name with its outcome. Outcomes are shared where
//! actions agree on what success means: the four event-publishing actions all
//! return [`EventPublished`].

use serde::{Deserialize, Serialize};

use super::{
    absent_or_valued, authority_uuid, authority_uuid_field, channel, channel_id,
    community_relay_url, community_relay_url_field, cursor, event_id, hex64_field, required,
    Action, PubkeyHex, MAX_CONTENT_BYTES, MAX_ENCODED_MESSAGE_BYTES, MAX_NAME_CHARS,
    MAX_PAGE_LIMIT, MAX_SCALAR_CHARS,
};
use crate::SdkError;
use nostr::{Event, EventId, Kind, PublicKey, Tags, Timestamp};

/// The seven canonical members of a Nostr event object, and nothing else.
///
/// `nostr`'s own `Event` deserializer accepts and *discards* unknown members,
/// which would put read results outside the contract's strict-wire rule.
/// Routing through a `deny_unknown_fields` intermediary restores the rule at
/// the one place the contract does not own the type.
///
/// Field names are the wire names from NIP-01 (`created_at`, not `createdAt`) —
/// this is the event's own encoding, not ours to rename.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StrictEvent {
    id: EventId,
    pubkey: PublicKey,
    created_at: Timestamp,
    kind: Kind,
    tags: Tags,
    content: String,
    sig: nostr::secp256k1::schnorr::Signature,
}

/// One message returned by a read: the signed Nostr event, verbatim.
///
/// The event is carried whole — signature and tags included — rather than
/// reduced to a projection, because Schnorr verification is local (see
/// [`Self::verify`]): a keyless agent gets independently verifiable authorship
/// and content, and only trusts the host for *completeness* and authorization.
/// Ancestry and mentions are derived accessors rather than sibling fields, so
/// nothing can disagree with the signed bytes.
///
/// Deserialization is **strict**, via a private `deny_unknown_fields`
/// intermediary; serialization is the event's own, so the wire form is
/// unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(transparent)]
pub struct BrokerMessage(pub Event);

impl<'de> Deserialize<'de> for BrokerMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let strict = StrictEvent::deserialize(deserializer)?;
        Ok(Self(Event::new(
            strict.id,
            strict.pubkey,
            strict.created_at,
            strict.kind,
            strict.tags,
            strict.content,
            strict.sig,
        )))
    }
}

impl BrokerMessage {
    /// The signed event.
    #[must_use]
    pub fn event(&self) -> &Event {
        &self.0
    }

    /// Verify the event's id and Schnorr signature — entirely local; a host
    /// that fabricated or altered a message fails here regardless of what it
    /// claims. Deliberately *not* called by
    /// [`crate::broker::BrokerResponse::validate_for`]: whether to pay for
    /// verification, and what to do when it fails, is the caller's policy.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::InvalidInput`] when the id does not match the
    /// content or the signature does not match the author.
    pub fn verify(&self) -> Result<(), SdkError> {
        self.0.verify().map_err(|e| {
            SdkError::InvalidInput(format!("broker returned an unverifiable event: {e}"))
        })
    }

    /// The author's pubkey, in this contract's identity type.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::InvalidInput`] if the event's author is not
    /// expressible as 64 hex characters.
    pub fn author(&self) -> Result<PubkeyHex, SdkError> {
        PubkeyHex::parse(self.0.pubkey.to_hex())
    }

    /// NIP-10 `root`/`reply` ancestry, parsed from the signed tags.
    #[must_use]
    pub fn thread(&self) -> buzz_core::nip10::ThreadMarkers {
        buzz_core::nip10::parse_thread_markers(&self.0.tags)
    }

    /// Pubkeys this message mentions, from the signed `p` tags.
    #[must_use]
    pub fn mentions(&self) -> Vec<String> {
        self.0
            .tags
            .public_keys()
            .map(nostr::PublicKey::to_hex)
            .collect()
    }
}

/// Outcome of any read action.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct MessagePage {
    /// Messages in the host's declared order.
    pub messages: Vec<BrokerMessage>,
    /// Opaque cursor to pass as [`super::args::ChannelReadArgs::cursor`] on the next call.
    ///
    /// Absent when the host has nothing further, which is how a caller learns
    /// to stop rather than by comparing lengths against a limit it may not have
    /// set.
    #[serde(
        default,
        deserialize_with = "absent_or_valued",
        skip_serializing_if = "Option::is_none"
    )]
    pub next_cursor: Option<String>,
}

/// Outcome of an action that published one event.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct EventPublished {
    /// The published event's id (hex).
    #[serde(deserialize_with = "hex64_field")]
    pub event_id: String,
    /// The published event's kind.
    pub kind: u32,
    /// Creation time the host stamped, Unix seconds.
    pub created_at: u64,
}

/// Outcome of `storage.address`.
///
/// Addressing material only. A `d` tag is a keyed hash of the slug, so it
/// identifies a record without revealing the slug or the key that derived it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StorageAddress {
    /// Author the record is addressed under.
    pub author_pubkey: PubkeyHex,
    /// Event kind holding the record.
    pub kind: u32,
    /// Derived `d` tag (64 hex characters).
    #[serde(deserialize_with = "hex64_field")]
    pub d_tag: String,
}

/// Outcome of `storage.get`.
///
/// Carries the record's plaintext, decrypted host-side. `value` is absent when
/// no record exists at the slug — a normal first-read state, not a failure, so
/// an agent tells "empty memory" from "call failed" without reading an error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StorageRecord {
    /// Decrypted record body; absent when the slug holds no record.
    #[serde(
        default,
        deserialize_with = "absent_or_valued",
        skip_serializing_if = "Option::is_none"
    )]
    pub value: Option<String>,
}

/// Outcome of `observer.emit`.
///
/// A batch acknowledgement, not per-frame receipts: the host re-batches and
/// paces publication, so individual frames have no stable published id to echo.
/// `accepted` is how many frames the host took for publication.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ObserverReceipt {
    /// Frames accepted for publication from this batch.
    pub accepted: u32,
}

/// Whether a runtime can restore vendor-private state, consume only portable
/// continuation context, or cannot safely participate in handoff.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeSupport {
    /// Exact runtime/session continuation is supported.
    Exact,
    /// Only runtime-neutral semantic continuation is supported.
    Portable,
    /// The runtime must fail closed before handoff.
    Unsupported,
}

/// Coarse execution substrate. The opaque id distinguishes concrete hosts
/// without coupling the contract to a provider, process name, or filesystem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeLocationKind {
    /// An owner-controlled local host.
    Local,
    /// A remotely hosted execution target.
    Cloud,
}

/// Exact destination of one managed ACP generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RuntimeLocation {
    /// Local or cloud substrate class.
    pub kind: RuntimeLocationKind,
    /// Host-owned opaque destination identifier.
    pub id: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeLocationWire {
    kind: RuntimeLocationKind,
    id: String,
}

impl<'de> Deserialize<'de> for RuntimeLocation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = RuntimeLocationWire::deserialize(deserializer)?;
        let id = required(&wire.id, "location id", MAX_SCALAR_CHARS)
            .map_err(serde::de::Error::custom)?;
        Ok(Self {
            kind: wire.kind,
            id,
        })
    }
}

impl RuntimeLocation {
    fn validated(&self) -> Result<Self, SdkError> {
        Ok(Self {
            kind: self.kind,
            id: required(&self.id, "location id", MAX_SCALAR_CHARS)?,
        })
    }
}

/// Canonical identity bound to one broker credential by the host.
///
/// The community is identified by its normalized relay URL, matching Buzz's
/// URL-is-the-community product contract. No field is accepted in
/// `authority.status` arguments, so this value can only be asserted by the
/// authenticated host response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AuthorityIdentity {
    /// Canonical relay URL selecting the Buzz community.
    #[serde(deserialize_with = "community_relay_url_field")]
    pub community_relay_url: String,
    /// Stable communication identity seen by existing clients.
    pub logical_agent_pubkey: PubkeyHex,
    /// Managed ACP executor represented by this credential.
    pub executor_agent_pubkey: PubkeyHex,
    /// Host-issued task binding.
    #[serde(deserialize_with = "authority_uuid_field")]
    pub task_id: String,
    /// Host-issued generation binding.
    #[serde(deserialize_with = "authority_uuid_field")]
    pub generation: String,
    /// Exact execution destination.
    pub location: RuntimeLocation,
    /// Continuation mode supported by the provisioned runtime.
    pub runtime_support: RuntimeSupport,
}

impl AuthorityIdentity {
    /// Return the single normalized authority identity used for all comparisons.
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::InvalidInput`] for any malformed or non-usable
    /// boundary identifier.
    pub fn validated(&self) -> Result<Self, SdkError> {
        Ok(Self {
            community_relay_url: community_relay_url(&self.community_relay_url)?,
            logical_agent_pubkey: PubkeyHex::parse(self.logical_agent_pubkey.as_str())?,
            executor_agent_pubkey: PubkeyHex::parse(self.executor_agent_pubkey.as_str())?,
            task_id: authority_uuid(&self.task_id, "task id")?,
            generation: authority_uuid(&self.generation, "generation")?,
            location: self.location.validated()?,
            runtime_support: self.runtime_support,
        })
    }
}

/// Current lifecycle verdict for an authenticated managed-ACP generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthorityState {
    /// New intake and mutating turns may proceed.
    Active,
    /// Intake is closed while the host waits for bounded drain.
    Quiescing,
    /// The generation is terminal and can never be admitted again.
    Fenced,
}

/// Success outcome of `authority.status`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ManagedAcpAuthority {
    /// Exact host-derived identity bound to this credential.
    pub identity: AuthorityIdentity,
    /// Current lifecycle state.
    pub state: AuthorityState,
}

/// Outcome of a successful `agents.create`.
///
/// Carries the new agent's **public** identity only — there is no field for
/// the minted secret, and `deny_unknown_fields` plus the key-set test is what
/// enforces that rather than a comment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentsCreateOutcome {
    /// The new agent's pubkey.
    pub agent_pubkey: PubkeyHex,
    /// The new agent's name as stored.
    pub display_name: String,
    /// Channel the agent was attached to.
    #[serde(deserialize_with = "channel_id")]
    pub channel_id: String,
}

/// Outcome of a successful `agents.update`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentsUpdateOutcome {
    /// The patched agent's pubkey.
    pub agent_pubkey: PubkeyHex,
    /// The agent's name after the update.
    pub display_name: String,
    /// Names of the fields the host actually changed, sorted.
    pub updated_fields: Vec<String>,
}

/// Outcome of a successful `agents.delete`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AgentsDeleteOutcome {
    /// The removed agent's pubkey.
    pub agent_pubkey: PubkeyHex,
    /// The removed agent's name.
    pub display_name: String,
}

/// An action-specific success payload.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "action", content = "outcome", deny_unknown_fields)]
pub enum ActionOutcome {
    /// `authority.status` succeeded.
    #[serde(rename = "authority.status")]
    AuthorityStatus(ManagedAcpAuthority),
    /// `channel.read` succeeded.
    #[serde(rename = "channel.read")]
    ChannelRead(MessagePage),
    /// `message.post` succeeded.
    #[serde(rename = "message.post")]
    MessagePost(EventPublished),
    /// `message.reply` succeeded.
    #[serde(rename = "message.reply")]
    MessageReply(EventPublished),
    /// `reaction.add` succeeded.
    #[serde(rename = "reaction.add")]
    ReactionAdd(EventPublished),
    /// `profile.set` succeeded.
    #[serde(rename = "profile.set")]
    ProfileSet(EventPublished),
    /// `storage.address` succeeded.
    #[serde(rename = "storage.address")]
    StorageAddress(StorageAddress),
    /// `storage.get` succeeded.
    #[serde(rename = "storage.get")]
    StorageGet(StorageRecord),
    /// `storage.put` succeeded.
    #[serde(rename = "storage.put")]
    StoragePut(EventPublished),
    /// `presence.set` succeeded.
    #[serde(rename = "presence.set")]
    PresenceSet(EventPublished),
    /// `typing.set` succeeded.
    #[serde(rename = "typing.set")]
    TypingSet(EventPublished),
    /// `observer.emit` succeeded.
    #[serde(rename = "observer.emit")]
    ObserverEmit(ObserverReceipt),
    /// `liveness.ping` succeeded.
    #[serde(rename = "liveness.ping")]
    LivenessPing(EventPublished),
    /// `agents.create` succeeded.
    #[serde(rename = "agents.create")]
    AgentsCreate(AgentsCreateOutcome),
    /// `agents.update` succeeded.
    #[serde(rename = "agents.update")]
    AgentsUpdate(AgentsUpdateOutcome),
    /// `agents.delete` succeeded.
    #[serde(rename = "agents.delete")]
    AgentsDelete(AgentsDeleteOutcome),
}

impl ActionOutcome {
    /// The action that produced this outcome.
    #[must_use]
    pub fn action(&self) -> Action {
        match self {
            Self::AuthorityStatus(_) => Action::AuthorityStatus,
            Self::ChannelRead(_) => Action::ChannelRead,
            Self::MessagePost(_) => Action::MessagePost,
            Self::MessageReply(_) => Action::MessageReply,
            Self::ReactionAdd(_) => Action::ReactionAdd,
            Self::ProfileSet(_) => Action::ProfileSet,
            Self::StorageAddress(_) => Action::StorageAddress,
            Self::StorageGet(_) => Action::StorageGet,
            Self::StoragePut(_) => Action::StoragePut,
            Self::PresenceSet(_) => Action::PresenceSet,
            Self::TypingSet(_) => Action::TypingSet,
            Self::ObserverEmit(_) => Action::ObserverEmit,
            Self::LivenessPing(_) => Action::LivenessPing,
            Self::AgentsCreate(_) => Action::AgentsCreate,
            Self::AgentsUpdate(_) => Action::AgentsUpdate,
            Self::AgentsDelete(_) => Action::AgentsDelete,
        }
    }

    /// Validate the identifiers and cursors this outcome asserts.
    ///
    /// A well-typed outcome can still carry a malformed id or an unusable
    /// cursor. Signature verification is deliberately *not* here — see
    /// [`BrokerMessage::verify`].
    ///
    /// # Errors
    ///
    /// Returns [`SdkError::InvalidInput`] for a malformed event id, `d` tag,
    /// channel UUID, or cursor, an empty name, or an over-long page.
    pub fn validate(&self) -> Result<(), SdkError> {
        match self {
            Self::AuthorityStatus(authority) => {
                authority.identity.validated()?;
            }
            Self::ChannelRead(page) => {
                if page.messages.len() > MAX_PAGE_LIMIT as usize {
                    return Err(SdkError::InvalidInput(format!(
                        "page holds {} messages, over the {MAX_PAGE_LIMIT} cap",
                        page.messages.len()
                    )));
                }
                for message in &page.messages {
                    if message.0.content.len() > MAX_CONTENT_BYTES {
                        return Err(SdkError::ContentTooLarge {
                            max: MAX_CONTENT_BYTES,
                            got: message.0.content.len(),
                        });
                    }
                    let encoded_len = serde_json::to_vec(message)
                        .map_err(|error| SdkError::InvalidInput(error.to_string()))?
                        .len();
                    if encoded_len > MAX_ENCODED_MESSAGE_BYTES {
                        return Err(SdkError::InvalidInput(format!(
                            "encoded channel message is {encoded_len} bytes, over the {MAX_ENCODED_MESSAGE_BYTES}-byte cap"
                        )));
                    }
                }
                page.next_cursor.as_deref().map(cursor).transpose()?;
            }
            Self::MessagePost(published)
            | Self::MessageReply(published)
            | Self::ReactionAdd(published)
            | Self::ProfileSet(published)
            | Self::StoragePut(published)
            | Self::PresenceSet(published)
            | Self::TypingSet(published)
            | Self::LivenessPing(published) => {
                event_id(&published.event_id, "eventId")?;
            }
            Self::StorageAddress(address) => {
                event_id(&address.d_tag, "dTag")?;
            }
            // Neither a record body nor a batch receipt carries an identifier or
            // cursor to check.
            Self::StorageGet(_) | Self::ObserverEmit(_) => {}
            Self::AgentsCreate(outcome) => {
                channel(&outcome.channel_id)?;
                required(&outcome.display_name, "display name", MAX_NAME_CHARS)?;
            }
            Self::AgentsUpdate(AgentsUpdateOutcome { display_name, .. })
            | Self::AgentsDelete(AgentsDeleteOutcome { display_name, .. }) => {
                required(display_name, "display name", MAX_NAME_CHARS)?;
            }
        }
        Ok(())
    }
}
