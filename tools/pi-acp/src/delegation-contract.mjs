import crypto from "node:crypto";
import { canonicalJson } from "./continuation-canonical.mjs";
import { validateEnvelope } from "./continuation-capsule.mjs";
import { canonicalRelayUrl } from "./task-session.mjs";

export const DELEGATION_SCHEMA_VERSION = 1;
export const OFFER_MARKER = "[PI DELEGATION OFFER v1]";
export const DECISION_MARKER = "[PI DELEGATION DECISION v1]";

const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const GIT_OID = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const CHANNEL_ID = UUID;
const CAPABILITY = /^[a-z][a-z0-9:_-]{0,63}$/;
const BRANCH =
  /^(?!-)(?!.*(?:\.\.|@\{|\\|\s|~|\^|:|\?|\*|\[))(?!.*\/$)(?!.*\.lock$).{1,512}$/;
const DECISIONS = new Set(["accept", "reject", "cancel"]);
const LOCATIONS = new Set(["local", "cloud"]);
const SAFE_REMOTE_PROTOCOLS = new Set(["https:", "ssh:"]);
const MAX_EVENT_AGE_MS = 24 * 60 * 60 * 1000;
const MAX_PROTOCOL_BYTES = 64 * 1024;

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactObject(value, fields, label) {
  if (!isPlainObject(value)) throw new Error(`${label} must be an object`);
  const actual = Object.keys(value).sort();
  const expected = [...fields].sort();
  if (canonicalJson(actual) !== canonicalJson(expected)) {
    throw new Error(`${label} has unknown or missing fields`);
  }
  return value;
}

function requiredString(value, pattern, label) {
  if (typeof value !== "string") throw new Error(`${label} is invalid`);
  if (!pattern.test(value)) throw new Error(`${label} is invalid`);
  return value;
}

function requiredPubkey(value, label) {
  return requiredString(value, HEX64, label);
}

function canonicalIso(value, label) {
  if (typeof value !== "string") throw new Error(`${label} is invalid`);
  const timestamp = Date.parse(value);
  if (!Number.isFinite(timestamp)) throw new Error(`${label} is invalid`);
  if (new Date(timestamp).toISOString() !== value) {
    throw new Error(`${label} must be canonical ISO-8601`);
  }
  return timestamp;
}

function delegationLocationsAreDistinct(value) {
  return value.source.location !== value.target.location;
}

function hasBoundedCapabilities(value) {
  return Array.isArray(value) && value.length > 0 && value.length <= 32;
}

function uniqueCapabilities(value, label) {
  if (!hasBoundedCapabilities(value)) {
    throw new Error(`${label} must contain 1..32 entries`);
  }
  const normalized = value.map((item, index) =>
    requiredString(item, CAPABILITY, `${label}[${index}]`),
  );
  if (new Set(normalized).size !== normalized.length) {
    throw new Error(`${label} contains duplicates`);
  }
  return [...normalized].sort();
}

function isBoundedRemoteUrl(value) {
  return typeof value === "string" && value.length > 0 && value.length <= 2048;
}

function isCredentialFreeRemote(remote) {
  const httpsUsername = remote.protocol === "https:" && remote.username;
  return !httpsUsername && !remote.password && !remote.search && !remote.hash;
}

function safeRemoteUrl(value) {
  if (!isBoundedRemoteUrl(value)) {
    throw new Error("offer.git.remoteUrl is invalid");
  }
  if (/^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+:[^\s]+$/.test(value)) return value;
  let remote;
  try {
    remote = new URL(value);
  } catch {
    throw new Error("offer.git.remoteUrl is invalid");
  }
  if (!SAFE_REMOTE_PROTOCOLS.has(remote.protocol)) {
    throw new Error("offer.git.remoteUrl is invalid or contains credentials");
  }
  if (!isCredentialFreeRemote(remote)) {
    throw new Error("offer.git.remoteUrl is invalid or contains credentials");
  }
  return value;
}

function boundedProtocol(value, label) {
  const bytes = Buffer.byteLength(canonicalJson(value), "utf8");
  if (bytes > MAX_PROTOCOL_BYTES) {
    throw new Error(`${label} exceeds ${MAX_PROTOCOL_BYTES} bytes`);
  }
  return value;
}

function canonicalTask(value) {
  exactObject(
    value,
    ["relayUrl", "agentPubkey", "channelId", "threadRoot"],
    "offer.task",
  );
  const relayUrl = canonicalRelayUrl(value.relayUrl);
  if (relayUrl !== value.relayUrl) {
    throw new Error("offer.task.relayUrl must be canonical");
  }
  return {
    relayUrl,
    agentPubkey: requiredPubkey(value.agentPubkey, "offer.task.agentPubkey"),
    channelId: requiredString(
      value.channelId,
      CHANNEL_ID,
      "offer.task.channelId",
    ),
    threadRoot: requiredString(
      value.threadRoot,
      HEX64,
      "offer.task.threadRoot",
    ),
  };
}

function canonicalGit(value) {
  exactObject(value, ["remoteUrl", "branch", "commit", "tree"], "offer.git");
  return {
    remoteUrl: safeRemoteUrl(value.remoteUrl),
    branch: requiredString(value.branch, BRANCH, "offer.git.branch"),
    commit: requiredString(value.commit, GIT_OID, "offer.git.commit"),
    tree: requiredString(value.tree, GIT_OID, "offer.git.tree"),
  };
}

function isRepositoryCapability(value) {
  return (
    typeof value === "string" &&
    value.length > 0 &&
    value.length <= 512 &&
    !value.includes("\0")
  );
}

function canonicalCapabilities(value) {
  exactObject(
    value,
    ["repository", "branch", "tools", "effects"],
    "offer.capabilities",
  );
  if (!isRepositoryCapability(value.repository)) {
    throw new Error("offer.capabilities.repository is invalid");
  }
  return {
    repository: value.repository,
    branch: requiredString(value.branch, BRANCH, "offer.capabilities.branch"),
    tools: uniqueCapabilities(value.tools, "offer.capabilities.tools"),
    effects: uniqueCapabilities(value.effects, "offer.capabilities.effects"),
  };
}

function offerCapsuleBinding(offer) {
  return {
    task: offer.task,
    git: offer.git,
    generation: offer.source.generation,
    sourceLocation: offer.source.location,
    targetLocation: offer.target.location,
    capsuleDigest: offer.capsuleDigest,
  };
}

function capsuleOfferBinding(envelope) {
  const capsule = envelope.capsule;
  return {
    task: capsule.task,
    git: {
      remoteUrl: capsule.git.remoteUrl,
      branch: capsule.git.branch,
      commit: capsule.git.commit,
      tree: capsule.git.tree,
    },
    generation: capsule.ownership.generation,
    sourceLocation: capsule.ownership.sourceLocation,
    targetLocation: capsule.ownership.targetLocation,
    capsuleDigest: envelope.digest,
  };
}

function assertOfferCapsuleBinding(offer, envelope) {
  if (
    canonicalJson(offerCapsuleBinding(offer)) !==
    canonicalJson(capsuleOfferBinding(envelope))
  ) {
    throw new Error("delegation offer does not match its continuation capsule");
  }
  const capsule = envelope.capsule;
  const lifetimeWithinCapsule =
    Date.parse(offer.createdAt) >= Date.parse(capsule.createdAt) &&
    Date.parse(offer.expiresAt) <= Date.parse(capsule.expiresAt);
  if (!lifetimeWithinCapsule) {
    throw new Error(
      "delegation offer lifetime exceeds its continuation capsule",
    );
  }
}

function canonicalOwnershipEvidence(value) {
  exactObject(
    value,
    ["relayUrl", "agentPubkey", "ownerPubkey", "generation", "location"],
    "ownership evidence",
  );
  if (!LOCATIONS.has(value.location)) {
    throw new Error("ownership evidence location is invalid");
  }
  const relayUrl = canonicalRelayUrl(value.relayUrl);
  if (relayUrl !== value.relayUrl) {
    throw new Error("ownership evidence relayUrl must be canonical");
  }
  return {
    relayUrl,
    agentPubkey: requiredPubkey(
      value.agentPubkey,
      "ownership evidence agentPubkey",
    ),
    ownerPubkey: requiredPubkey(
      value.ownerPubkey,
      "ownership evidence ownerPubkey",
    ),
    generation: requiredString(
      value.generation,
      UUID,
      "ownership evidence generation",
    ),
    location: value.location,
  };
}

function assertCurrentOwnership(offer, value) {
  const evidence = canonicalOwnershipEvidence(value);
  const expected = {
    relayUrl: offer.task.relayUrl,
    agentPubkey: offer.task.agentPubkey,
    ownerPubkey: offer.source.ownerPubkey,
    generation: offer.source.generation,
    location: offer.source.location,
  };
  if (canonicalJson(evidence) !== canonicalJson(expected)) {
    throw new Error("delegation offer does not match authoritative ownership");
  }
}

export function activationGeneration(offerDigest) {
  requiredString(offerDigest, HEX64, "offerDigest");
  const bytes = Buffer.from(offerDigest, "hex").subarray(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

export function validateDelegationOffer(
  value,
  { capsuleEnvelope, ownershipEvidence, now = Date.now() } = {},
) {
  exactObject(
    value,
    [
      "schemaVersion",
      "offerId",
      "createdAt",
      "expiresAt",
      "capsuleDigest",
      "source",
      "target",
      "task",
      "git",
      "capabilities",
    ],
    "offer",
  );
  if (value.schemaVersion !== DELEGATION_SCHEMA_VERSION) {
    throw new Error("unsupported delegation schemaVersion");
  }
  const createdAt = canonicalIso(value.createdAt, "offer.createdAt");
  const expiresAt = canonicalIso(value.expiresAt, "offer.expiresAt");
  if (createdAt > now + 5 * 60 * 1000) {
    throw new Error("delegation offer creation time is in the future");
  }
  const validExpiryWindow =
    expiresAt > createdAt && expiresAt - createdAt <= MAX_EVENT_AGE_MS;
  if (!validExpiryWindow) {
    throw new Error("delegation offer expiry window is invalid");
  }
  if (now > expiresAt) throw new Error("delegation offer is expired");
  exactObject(
    value.source,
    ["ownerPubkey", "generation", "location"],
    "offer.source",
  );
  exactObject(value.target, ["ownerPubkey", "location"], "offer.target");
  if (!LOCATIONS.has(value.source.location)) {
    throw new Error("offer.source.location is invalid");
  }
  if (value.target.location !== "cloud") {
    throw new Error("offer.target.location must be cloud");
  }
  if (!delegationLocationsAreDistinct(value)) {
    throw new Error("delegation source and target locations must differ");
  }
  const offer = {
    schemaVersion: DELEGATION_SCHEMA_VERSION,
    offerId: requiredString(value.offerId, UUID, "offer.offerId"),
    createdAt: value.createdAt,
    expiresAt: value.expiresAt,
    capsuleDigest: requiredString(
      value.capsuleDigest,
      HEX64,
      "offer.capsuleDigest",
    ),
    source: {
      ownerPubkey: requiredPubkey(
        value.source.ownerPubkey,
        "offer.source.ownerPubkey",
      ),
      generation: requiredString(
        value.source.generation,
        UUID,
        "offer.source.generation",
      ),
      location: value.source.location,
    },
    target: {
      ownerPubkey: requiredPubkey(
        value.target.ownerPubkey,
        "offer.target.ownerPubkey",
      ),
      location: value.target.location,
    },
    task: canonicalTask(value.task),
    git: canonicalGit(value.git),
    capabilities: canonicalCapabilities(value.capabilities),
  };
  const distinctPrincipals = new Set([
    offer.source.ownerPubkey,
    offer.target.ownerPubkey,
    offer.task.agentPubkey,
  ]);
  if (distinctPrincipals.size !== 3) {
    throw new Error("delegation source, target, and agent keys must differ");
  }
  if (offer.capabilities.repository !== offer.git.remoteUrl) {
    throw new Error("delegation capability repository does not match Git");
  }
  if (offer.capabilities.branch !== offer.git.branch) {
    throw new Error("delegation capability branch does not match Git");
  }
  if (capsuleEnvelope !== undefined) {
    validateEnvelope(capsuleEnvelope, { now });
    assertOfferCapsuleBinding(offer, capsuleEnvelope);
  }
  if (ownershipEvidence !== undefined) {
    assertCurrentOwnership(offer, ownershipEvidence);
  }
  return boundedProtocol(offer, "delegation offer");
}

export function delegationOfferEnvelope(offer, options = {}) {
  const normalized = validateDelegationOffer(offer, options);
  const digest = crypto
    .createHash("sha256")
    .update(canonicalJson(normalized))
    .digest("hex");
  return {
    offer: normalized,
    digest,
    activationGeneration: activationGeneration(digest),
  };
}

export function renderDelegationOffer(envelope, options = {}) {
  exactObject(
    envelope,
    ["offer", "digest", "activationGeneration"],
    "offer envelope",
  );
  const validated = delegationOfferEnvelope(envelope.offer, options);
  if (validated.digest !== envelope.digest) {
    throw new Error("delegation offer digest mismatch");
  }
  if (validated.activationGeneration !== envelope.activationGeneration) {
    throw new Error("delegation activation generation mismatch");
  }
  return `${OFFER_MARKER}\n${canonicalJson(envelope)}`;
}

export function validateDelegationDecision(
  value,
  offerEnvelope,
  { now = Date.now() } = {},
) {
  exactObject(
    value,
    [
      "schemaVersion",
      "offerId",
      "offerEventId",
      "offerDigest",
      "sourceGeneration",
      "activationGeneration",
      "decision",
      "decidedAt",
    ],
    "decision",
  );
  if (value.schemaVersion !== DELEGATION_SCHEMA_VERSION) {
    throw new Error("unsupported delegation decision schemaVersion");
  }
  if (!DECISIONS.has(value.decision)) {
    throw new Error("delegation decision is invalid");
  }
  const offer = delegationOfferEnvelope(offerEnvelope.offer, { now });
  const expected = {
    offerId: offer.offer.offerId,
    offerDigest: offer.digest,
    sourceGeneration: offer.offer.source.generation,
    activationGeneration: offer.activationGeneration,
  };
  const actual = {
    offerId: value.offerId,
    offerDigest: value.offerDigest,
    sourceGeneration: value.sourceGeneration,
    activationGeneration: value.activationGeneration,
  };
  if (canonicalJson(actual) !== canonicalJson(expected)) {
    throw new Error("delegation decision does not match its offer");
  }
  const decidedAt = canonicalIso(value.decidedAt, "decision.decidedAt");
  const validDecisionTime =
    decidedAt >= Date.parse(offer.offer.createdAt) &&
    decidedAt <= Date.parse(offer.offer.expiresAt) &&
    decidedAt <= now;
  if (!validDecisionTime) {
    throw new Error("delegation decision time is invalid");
  }
  return boundedProtocol(
    {
      schemaVersion: DELEGATION_SCHEMA_VERSION,
      ...actual,
      offerEventId: requiredString(
        value.offerEventId,
        HEX64,
        "decision.offerEventId",
      ),
      decision: value.decision,
      decidedAt: value.decidedAt,
    },
    "delegation decision",
  );
}

export function renderDelegationDecision(
  decision,
  offerEnvelope,
  options = {},
) {
  return `${DECISION_MARKER}\n${canonicalJson(
    validateDelegationDecision(decision, offerEnvelope, options),
  )}`;
}

function parseProtocolContent(content, marker, label) {
  if (typeof content !== "string")
    throw new Error(`${label} content is invalid`);
  const prefix = `${marker}\n`;
  if (!content.startsWith(prefix))
    throw new Error(`${label} marker is invalid`);
  const value = JSON.parse(content.slice(prefix.length));
  if (content !== `${marker}\n${canonicalJson(value)}`) {
    throw new Error(`${label} content is not canonical`);
  }
  return value;
}

export function parseDelegationOffer(content, options = {}) {
  const value = parseProtocolContent(content, OFFER_MARKER, "offer");
  exactObject(
    value,
    ["offer", "digest", "activationGeneration"],
    "offer envelope",
  );
  const envelope = delegationOfferEnvelope(value.offer, options);
  if (canonicalJson(value) !== canonicalJson(envelope)) {
    throw new Error("delegation offer envelope is invalid");
  }
  return envelope;
}

export function parseDelegationDecision(content, offerEnvelope, options = {}) {
  return validateDelegationDecision(
    parseProtocolContent(content, DECISION_MARKER, "decision"),
    offerEnvelope,
    options,
  );
}

function relevantTags(event, name) {
  return event.tags.filter((tag) => tag[0] === name);
}

function assertCanonicalRouting(
  event,
  { channelId, eventTags, mentionPubkeys },
  label,
) {
  const actualChannelTags = relevantTags(event, "h");
  const actualEventTags = relevantTags(event, "e");
  const actualMentionTags = relevantTags(event, "p")
    .map((tag) => canonicalJson(tag))
    .sort();
  const expectedMentionTags = mentionPubkeys
    .map((pubkey) => canonicalJson(["p", pubkey]))
    .sort();
  const validRouting =
    canonicalJson(actualChannelTags) === canonicalJson([["h", channelId]]) &&
    canonicalJson(actualEventTags) === canonicalJson(eventTags) &&
    canonicalJson(actualMentionTags) === canonicalJson(expectedMentionTags);
  if (!validRouting) {
    throw new Error(`${label} routing is invalid`);
  }
}

function hasCanonicalTagShape(tags) {
  return (
    Array.isArray(tags) &&
    tags.every(
      (tag) =>
        Array.isArray(tag) &&
        tag.length >= 2 &&
        tag.every((value) => typeof value === "string"),
    )
  );
}

function canonicalVerifiedEvent(event, label) {
  exactObject(
    event,
    ["id", "pubkey", "kind", "content", "tags", "signatureVerified"],
    label,
  );
  if (event.signatureVerified !== true) {
    throw new Error(`${label} signature is not verified`);
  }
  if (event.kind !== 9) throw new Error(`${label} kind is invalid`);
  if (!hasCanonicalTagShape(event.tags)) {
    throw new Error(`${label} tags are invalid`);
  }
  return {
    id: requiredString(event.id, HEX64, `${label}.id`),
    pubkey: requiredPubkey(event.pubkey, `${label}.pubkey`),
    kind: event.kind,
    content: event.content,
    tags: event.tags,
    signatureVerified: true,
  };
}

export function validateDelegationOfferEvent(event, options = {}) {
  canonicalOwnershipEvidence(options.ownershipEvidence);
  const verified = canonicalVerifiedEvent(event, "offer event");
  const envelope = parseDelegationOffer(verified.content, options);
  const offer = envelope.offer;
  if (verified.pubkey !== offer.source.ownerPubkey) {
    throw new Error("delegation offer signer is not the source owner");
  }
  assertCanonicalRouting(
    verified,
    {
      channelId: offer.task.channelId,
      eventTags: [["e", offer.task.threadRoot, "", "reply"]],
      mentionPubkeys: [offer.target.ownerPubkey, offer.task.agentPubkey],
    },
    "delegation offer event",
  );
  return { event: verified, envelope };
}

function decisionSigner(offer, decision) {
  return decision === "cancel"
    ? offer.source.ownerPubkey
    : offer.target.ownerPubkey;
}

export function validateDelegationDecisionEvent(
  event,
  offerEvent,
  options = {},
) {
  const verified = canonicalVerifiedEvent(event, "decision event");
  const admittedOffer = validateDelegationOfferEvent(offerEvent, options);
  const decision = parseDelegationDecision(
    verified.content,
    admittedOffer.envelope,
    options,
  );
  if (decision.offerEventId !== admittedOffer.event.id) {
    throw new Error("delegation decision references the wrong offer event");
  }
  if (
    verified.pubkey !==
    decisionSigner(admittedOffer.envelope.offer, decision.decision)
  ) {
    throw new Error("delegation decision signer is not authorized");
  }
  const offer = admittedOffer.envelope.offer;
  assertCanonicalRouting(
    verified,
    {
      channelId: offer.task.channelId,
      eventTags: [
        ["e", offer.task.threadRoot, "", "root"],
        ["e", admittedOffer.event.id, "", "reply"],
      ],
      mentionPubkeys: [
        offer.source.ownerPubkey,
        offer.target.ownerPubkey,
        offer.task.agentPubkey,
      ],
    },
    "delegation decision event",
  );
  return { offer: admittedOffer, event: verified, decision };
}

function isBoundedDecisionSet(events) {
  return Array.isArray(events) && events.length > 0 && events.length <= 32;
}

export function resolveAcceptedDelegationDecision(
  events,
  offerEvent,
  options = {},
) {
  if (!isBoundedDecisionSet(events)) {
    throw new Error("delegation decision set must contain 1..32 events");
  }
  const admitted = events.map((event) =>
    validateDelegationDecisionEvent(event, offerEvent, options),
  );
  const uniqueIds = new Set(admitted.map((entry) => entry.event.id));
  if (uniqueIds.size !== admitted.length) {
    throw new Error("delegation decision set contains duplicate events");
  }
  const terminal = admitted.find(
    (entry) => entry.decision.decision !== "accept",
  );
  if (terminal) {
    throw new Error(`delegation offer is ${terminal.decision.decision}ed`);
  }
  if (admitted.length !== 1) {
    throw new Error("delegation decision set has concurrent acceptances");
  }
  return admitted[0];
}

export const delegationValidation = {
  exactObject,
  requiredString,
  canonicalIso,
  boundedProtocol,
  parseProtocolContent,
  canonicalVerifiedEvent,
  assertCanonicalRouting,
};
