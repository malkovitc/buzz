import crypto from "node:crypto";
import { canonicalJson } from "./continuation-canonical.mjs";
import {
  DELEGATION_SCHEMA_VERSION,
  delegationValidation,
  validateDelegationDecisionEvent,
} from "./delegation-contract.mjs";

export const READY_MARKER = "[PI DELEGATION READY v2]";
export const GRANT_MARKER = "[PI DELEGATION GRANT v2]";

const HEX64 = /^[0-9a-f]{64}$/;
const {
  exactObject,
  requiredString,
  canonicalIso,
  boundedProtocol,
  parseProtocolContent,
  canonicalVerifiedEvent,
  assertCanonicalRouting,
  mentionPubkeysForOffer,
} = delegationValidation;

function lineageBinding(admittedDecision) {
  const envelope = admittedDecision.offer.envelope;
  return {
    offerEventId: admittedDecision.offer.event.id,
    decisionEventId: admittedDecision.event.id,
    offerDigest: envelope.digest,
    capsuleDigest: envelope.offer.capsuleDigest,
    sourceGeneration: envelope.offer.source.generation,
    activationGeneration: envelope.activationGeneration,
    sourceAgentPubkey: envelope.offer.task.agentPubkey,
    targetAgentPubkey: envelope.offer.target.agentPubkey,
  };
}

export function validateDelegationReady(
  value,
  admittedDecision,
  { now = Date.now() } = {},
) {
  exactObject(
    value,
    [
      "schemaVersion",
      "offerEventId",
      "decisionEventId",
      "offerDigest",
      "capsuleDigest",
      "sourceGeneration",
      "activationGeneration",
      "sourceAgentPubkey",
      "targetAgentPubkey",
      "importReceiptDigest",
      "readyAt",
    ],
    "ready",
  );
  if (value.schemaVersion !== DELEGATION_SCHEMA_VERSION) {
    throw new Error("unsupported delegation ready schemaVersion");
  }
  if (admittedDecision.decision.decision !== "accept") {
    throw new Error("delegation readiness requires an accepted decision");
  }
  const expected = lineageBinding(admittedDecision);
  const actual = Object.fromEntries(
    Object.keys(expected).map((field) => [field, value[field]]),
  );
  if (canonicalJson(actual) !== canonicalJson(expected)) {
    throw new Error("delegation readiness does not match accepted lineage");
  }
  const readyAt = canonicalIso(value.readyAt, "ready.readyAt");
  const offer = admittedDecision.offer.envelope.offer;
  const validReadyTime =
    readyAt >= Date.parse(admittedDecision.decision.decidedAt) &&
    readyAt <= now &&
    now <= Date.parse(offer.expiresAt);
  if (!validReadyTime) {
    throw new Error("delegation readiness is expired or has an invalid time");
  }
  return boundedProtocol(
    {
      schemaVersion: DELEGATION_SCHEMA_VERSION,
      ...actual,
      importReceiptDigest: requiredString(
        value.importReceiptDigest,
        HEX64,
        "ready.importReceiptDigest",
      ),
      readyAt: value.readyAt,
    },
    "delegation readiness",
  );
}

export function renderDelegationReady(ready, admittedDecision, options = {}) {
  return `${READY_MARKER}\n${canonicalJson(
    validateDelegationReady(ready, admittedDecision, options),
  )}`;
}

export function validateDelegationReadyEvent(
  event,
  decisionEvent,
  offerEvent,
  options = {},
) {
  const admittedDecision = validateDelegationDecisionEvent(
    decisionEvent,
    offerEvent,
    options,
  );
  const verified = canonicalVerifiedEvent(event, "ready event");
  const ready = validateDelegationReady(
    parseProtocolContent(verified.content, READY_MARKER, "ready"),
    admittedDecision,
    options,
  );
  const offer = admittedDecision.offer.envelope.offer;
  if (verified.pubkey !== offer.target.ownerPubkey) {
    throw new Error("delegation readiness signer is not the target owner");
  }
  assertCanonicalRouting(
    verified,
    {
      channelId: offer.task.channelId,
      eventTags: [
        ["e", offer.task.threadRoot, "", "root"],
        ["e", admittedDecision.event.id, "", "reply"],
      ],
      mentionPubkeys: mentionPubkeysForOffer(offer),
    },
    "delegation ready event",
  );
  return { decision: admittedDecision, event: verified, ready };
}

function grantBinding(admittedReady) {
  return {
    ...lineageBinding(admittedReady.decision),
    readyEventId: admittedReady.event.id,
    readyDigest: protocolDigest(admittedReady.ready),
  };
}

export function protocolDigest(value) {
  return crypto.createHash("sha256").update(canonicalJson(value)).digest("hex");
}

function fenceProofPayload(grant) {
  return {
    offerEventId: grant.offerEventId,
    decisionEventId: grant.decisionEventId,
    readyEventId: grant.readyEventId,
    offerDigest: grant.offerDigest,
    readyDigest: grant.readyDigest,
    capsuleDigest: grant.capsuleDigest,
    sourceGeneration: grant.sourceGeneration,
    activationGeneration: grant.activationGeneration,
    sourceAgentPubkey: grant.sourceAgentPubkey,
    targetAgentPubkey: grant.targetAgentPubkey,
    fencedStateDigest: grant.fencedStateDigest,
    ownershipDigest: grant.ownershipDigest,
    readyObservedAt: grant.readyObservedAt,
    grantedAt: grant.grantedAt,
  };
}

export function createFenceProof(grant, proofKey) {
  const key = requiredString(proofKey, HEX64, "fence proof key");
  return crypto
    .createHmac("sha256", Buffer.from(key, "hex"))
    .update(canonicalJson(fenceProofPayload(grant)))
    .digest("hex");
}

export function validateDelegationGrant(
  value,
  admittedReady,
  fenceEvidence,
  proofKey,
  ownershipEvidence,
) {
  exactObject(
    value,
    [
      "schemaVersion",
      "offerEventId",
      "decisionEventId",
      "readyEventId",
      "offerDigest",
      "readyDigest",
      "capsuleDigest",
      "sourceGeneration",
      "activationGeneration",
      "sourceAgentPubkey",
      "targetAgentPubkey",
      "fencedStateDigest",
      "fenceProof",
      "ownershipDigest",
      "readyObservedAt",
      "grantedAt",
    ],
    "grant",
  );
  const expected = grantBinding(admittedReady);
  const actual = Object.fromEntries(
    Object.keys(expected).map((field) => [field, value[field]]),
  );
  if (value.schemaVersion !== DELEGATION_SCHEMA_VERSION) {
    throw new Error("unsupported delegation grant schemaVersion");
  }
  if (canonicalJson(actual) !== canonicalJson(expected)) {
    throw new Error("delegation grant does not match ready lineage");
  }
  exactObject(
    fenceEvidence,
    [
      "sourceGeneration",
      "activationGeneration",
      "stateDigest",
      "readyObservedAt",
    ],
    "fence evidence",
  );
  const expectedFence = {
    sourceGeneration: expected.sourceGeneration,
    activationGeneration: expected.activationGeneration,
    stateDigest: requiredString(
      value.fencedStateDigest,
      HEX64,
      "grant.fencedStateDigest",
    ),
    readyObservedAt: value.readyObservedAt,
  };
  if (canonicalJson(fenceEvidence) !== canonicalJson(expectedFence)) {
    throw new Error("delegation grant does not match durable fence evidence");
  }
  const readyObservedAt = canonicalIso(
    value.readyObservedAt,
    "grant.readyObservedAt",
  );
  const grantedAt = canonicalIso(value.grantedAt, "grant.grantedAt");
  const offerExpiresAt = Date.parse(
    admittedReady.decision.offer.envelope.offer.expiresAt,
  );
  const validGrantTime =
    readyObservedAt >= Date.parse(admittedReady.ready.readyAt) &&
    readyObservedAt <= offerExpiresAt &&
    grantedAt >= readyObservedAt;
  if (!validGrantTime) {
    throw new Error("delegation grant time is invalid");
  }
  const ownershipDigest = requiredString(
    value.ownershipDigest,
    HEX64,
    "grant.ownershipDigest",
  );
  if (ownershipDigest !== protocolDigest(ownershipEvidence)) {
    throw new Error("delegation grant ownership snapshot is invalid");
  }
  const fenceProof = requiredString(
    value.fenceProof,
    HEX64,
    "grant.fenceProof",
  );
  const expectedProof = createFenceProof(value, proofKey);
  const validFenceProof = crypto.timingSafeEqual(
    Buffer.from(fenceProof, "hex"),
    Buffer.from(expectedProof, "hex"),
  );
  if (!validFenceProof) {
    throw new Error("delegation grant fence proof is invalid");
  }
  return boundedProtocol(
    {
      schemaVersion: DELEGATION_SCHEMA_VERSION,
      ...actual,
      fencedStateDigest: value.fencedStateDigest,
      fenceProof,
      ownershipDigest,
      readyObservedAt: value.readyObservedAt,
      grantedAt: value.grantedAt,
    },
    "delegation grant",
  );
}

export function renderDelegationGrant(
  grant,
  admittedReady,
  fenceEvidence,
  proofKey,
  ownershipEvidence,
) {
  return `${GRANT_MARKER}\n${canonicalJson(
    validateDelegationGrant(
      grant,
      admittedReady,
      fenceEvidence,
      proofKey,
      ownershipEvidence,
    ),
  )}`;
}

export function validateDelegationGrantEvent(
  event,
  readyEvent,
  decisionEvent,
  offerEvent,
  proofKey,
  options = {},
) {
  const verified = canonicalVerifiedEvent(event, "grant event");
  const claimedGrant = parseProtocolContent(
    verified.content,
    GRANT_MARKER,
    "grant",
  );
  const readyObservedAt = canonicalIso(
    claimedGrant.readyObservedAt,
    "grant.readyObservedAt",
  );
  const admittedReady = validateDelegationReadyEvent(
    readyEvent,
    decisionEvent,
    offerEvent,
    { ...options, now: readyObservedAt },
  );
  const grant = validateDelegationGrant(
    claimedGrant,
    admittedReady,
    {
      sourceGeneration: claimedGrant.sourceGeneration,
      activationGeneration: claimedGrant.activationGeneration,
      stateDigest: claimedGrant.fencedStateDigest,
      readyObservedAt: claimedGrant.readyObservedAt,
    },
    proofKey,
    options.ownershipEvidence,
  );
  const offer = admittedReady.decision.offer.envelope.offer;
  if (verified.pubkey !== offer.task.agentPubkey) {
    throw new Error("delegation grant signer is not the fenced source agent");
  }
  assertCanonicalRouting(
    verified,
    {
      channelId: offer.task.channelId,
      eventTags: [
        ["e", offer.task.threadRoot, "", "root"],
        ["e", admittedReady.event.id, "", "reply"],
      ],
      mentionPubkeys: mentionPubkeysForOffer(offer),
    },
    "delegation grant event",
  );
  return { ready: admittedReady, event: verified, grant };
}
