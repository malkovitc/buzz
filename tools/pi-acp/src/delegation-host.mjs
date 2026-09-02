#!/usr/bin/env node

import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { canonicalJson } from "./continuation-canonical.mjs";
import {
  acquireOperationLock,
  atomicJson,
  ensureStateDirectory,
  processIdentityFor,
  processOwnsIdentity,
  readBoundedInput,
  readDurableJson,
  readJson,
  reserveStateCapacity,
  runVector,
} from "./delegation-host-runtime.mjs";
import { resolveAcceptedDelegationDecision } from "./delegation-contract.mjs";
import {
  createFenceProof,
  protocolDigest,
  renderDelegationGrant,
  validateDelegationGrantEvent,
  validateDelegationReadyEvent,
} from "./delegation-grant-contract.mjs";

const HEX64 = /^[0-9a-f]{64}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const ROLES = new Set(["source", "target"]);

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

function matchesRequiredString(value, pattern) {
  return typeof value === "string" && pattern.test(value);
}

function requiredString(value, pattern, label) {
  if (!matchesRequiredString(value, pattern)) {
    throw new Error(`${label} is invalid`);
  }
  return value;
}

function assertCommandVector(value, label, required) {
  const optionalCommandIsOmitted = required === false && value === null;
  if (optionalCommandIsOmitted) return;
  const valid =
    Array.isArray(value) &&
    value.length > 0 &&
    value.length <= 16 &&
    value.every(
      (part, index) =>
        typeof part === "string" &&
        part.length > 0 &&
        part.length <= 1024 &&
        !part.includes("\0") &&
        (index !== 0 || path.isAbsolute(part)),
    );
  if (!valid) throw new Error(`${label} must be a bounded command vector`);
}

function validateConfig(value) {
  exactObject(
    value,
    [
      "schemaVersion",
      "role",
      "stateDirectory",
      "fenceProofKeyCommand",
      "decisionReadCommand",
      "sourceFenceCommand",
      "targetActivateCommand",
    ],
    "delegation host config",
  );
  const supportedConfigIdentity =
    value.schemaVersion === 1 && ROLES.has(value.role);
  if (!supportedConfigIdentity) {
    throw new Error("delegation host config values are invalid");
  }
  if (!path.isAbsolute(value.stateDirectory)) {
    throw new Error("delegation stateDirectory must be absolute");
  }
  assertCommandVector(
    value.decisionReadCommand,
    "decisionReadCommand",
    value.role === "source",
  );
  assertCommandVector(value.fenceProofKeyCommand, "fenceProofKeyCommand", true);
  assertCommandVector(
    value.sourceFenceCommand,
    "sourceFenceCommand",
    value.role === "source",
  );
  assertCommandVector(
    value.targetActivateCommand,
    "targetActivateCommand",
    value.role === "target",
  );
  return value;
}

function operationIdForEvents(offerEvent, readyEvent) {
  const offerEventId = requiredString(
    offerEvent?.id,
    HEX64,
    "operation offerEventId",
  );
  const readyEventId = requiredString(
    readyEvent?.id,
    HEX64,
    "operation readyEventId",
  );
  const seed = `${offerEventId}\0${readyEventId}`;
  const bytes = crypto
    .createHash("sha256")
    .update(seed)
    .digest()
    .subarray(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

function operationIdFor(admittedReady) {
  return operationIdForEvents(
    admittedReady.decision.offer.event,
    admittedReady.event,
  );
}

function lineageRecord(admittedReady, operationId) {
  return {
    operationId,
    offerEventId: admittedReady.decision.offer.event.id,
    decisionEventId: admittedReady.decision.event.id,
    readyEventId: admittedReady.event.id,
    offerDigest: admittedReady.ready.offerDigest,
    readyDigest: protocolDigest(admittedReady.ready),
    capsuleDigest: admittedReady.ready.capsuleDigest,
    sourceGeneration: admittedReady.ready.sourceGeneration,
    activationGeneration: admittedReady.ready.activationGeneration,
  };
}

function assertStoredLineage(state, expected) {
  const actual = Object.fromEntries(
    Object.keys(expected).map((field) => [field, state[field]]),
  );
  if (canonicalJson(actual) !== canonicalJson(expected)) {
    throw new Error("durable delegation state belongs to another lineage");
  }
}

function storedTimestamp(value, label) {
  const timestamp = Date.parse(value);
  const validTimestamp =
    Number.isFinite(timestamp) && new Date(timestamp).toISOString() === value;
  if (!validTimestamp) throw new Error(`${label} is invalid`);
  return timestamp;
}

function validateFencingState(
  state,
  admittedReady,
  lineage,
  ownershipEvidence,
) {
  exactObject(
    state,
    [
      "schemaVersion",
      "phase",
      ...Object.keys(lineage),
      "readyObservedAt",
      "grantedAt",
      "decisionEvent",
      "ownershipEvidence",
    ],
    "source fencing state",
  );
  const validIdentity = state.schemaVersion === 1 && state.phase === "fencing";
  if (!validIdentity) throw new Error("source fencing state is invalid");
  assertStoredLineage(state, lineage);
  if (
    canonicalJson(state.decisionEvent) !==
    canonicalJson(admittedReady.decision.event)
  ) {
    throw new Error("source fencing decision snapshot is corrupt");
  }
  if (
    canonicalJson(state.ownershipEvidence) !== canonicalJson(ownershipEvidence)
  ) {
    throw new Error("source fencing ownership snapshot is corrupt");
  }
  storedTimestamp(state.readyObservedAt, "source fencing readyObservedAt");
  storedTimestamp(state.grantedAt, "source fencing grantedAt");
  return state;
}

function validateSourceState(
  state,
  admittedReady,
  lineage,
  proofKey,
  ownershipEvidence,
) {
  exactObject(
    state,
    [
      "schemaVersion",
      "phase",
      ...Object.keys(lineage),
      "fenceEvidence",
      "decisionEvent",
      "ownershipEvidence",
      "grant",
      "grantContent",
      "grantEventId",
    ],
    "source delegation state",
  );
  const validPhase = state.phase === "fenced" || state.phase === "published";
  if (state.schemaVersion !== 1) {
    throw new Error("source delegation state schemaVersion is invalid");
  }
  if (!validPhase) throw new Error("source delegation state phase is invalid");
  assertStoredLineage(state, lineage);
  if (
    canonicalJson(state.decisionEvent) !==
    canonicalJson(admittedReady.decision.event)
  ) {
    throw new Error("source delegation decision snapshot is corrupt");
  }
  if (
    canonicalJson(state.ownershipEvidence) !== canonicalJson(ownershipEvidence)
  ) {
    throw new Error("source delegation ownership snapshot is corrupt");
  }
  const expectedContent = renderDelegationGrant(
    state.grant,
    admittedReady,
    state.fenceEvidence,
    proofKey,
    ownershipEvidence,
  );
  if (state.grantContent !== expectedContent) {
    throw new Error("source delegation grant content is corrupt");
  }
  const validReceipt =
    (state.phase === "fenced" && state.grantEventId === null) ||
    (state.phase === "published" && HEX64.test(state.grantEventId));
  if (!validReceipt) {
    throw new Error("source delegation grant receipt is corrupt");
  }
  return state;
}

function validateTargetState(state, lineage, ownershipEvidence, grantContent) {
  exactObject(
    state,
    [
      "schemaVersion",
      "phase",
      ...Object.keys(lineage),
      "grantEventId",
      "grantContent",
      "ownershipEvidence",
    ],
    "target delegation state",
  );
  const validPhase = state.phase === "activating" || state.phase === "active";
  const validIdentity = state.schemaVersion === 1 && validPhase;
  if (!validIdentity) throw new Error("target delegation state is invalid");
  assertStoredLineage(state, lineage);
  requiredString(state.grantEventId, HEX64, "target grantEventId");
  if (state.grantContent !== grantContent) {
    throw new Error("target delegation grant content is corrupt");
  }
  if (
    canonicalJson(state.ownershipEvidence) !== canonicalJson(ownershipEvidence)
  ) {
    throw new Error("target delegation ownership snapshot is corrupt");
  }
  return state;
}

function grantFor(
  admittedReady,
  fenceEvidence,
  grantedAt,
  proofKey,
  ownershipEvidence,
) {
  const lineage = lineageRecord(admittedReady, operationIdFor(admittedReady));
  const grant = {
    schemaVersion: 1,
    offerEventId: lineage.offerEventId,
    decisionEventId: lineage.decisionEventId,
    readyEventId: lineage.readyEventId,
    offerDigest: lineage.offerDigest,
    readyDigest: lineage.readyDigest,
    capsuleDigest: lineage.capsuleDigest,
    sourceGeneration: lineage.sourceGeneration,
    activationGeneration: lineage.activationGeneration,
    fencedStateDigest: fenceEvidence.stateDigest,
    ownershipDigest: protocolDigest(ownershipEvidence),
    readyObservedAt: fenceEvidence.readyObservedAt,
    grantedAt,
  };
  return { ...grant, fenceProof: createFenceProof(grant, proofKey) };
}

async function loadProofKey(config, commandRunner) {
  const result = await commandRunner(config.fenceProofKeyCommand, {
    schemaVersion: 1,
    purpose: "delegation-fence-proof-v1",
  });
  exactObject(result, ["key"], "fence proof key result");
  return requiredString(result.key, HEX64, "fence proof key result");
}

async function loadDecisionEvents(request, config, commandRunner) {
  const result = await commandRunner(config.decisionReadCommand, {
    schemaVersion: 1,
    relayUrl: request.ownershipEvidence.relayUrl,
    offerEventId: request.offerEvent.id,
    readyEventId: request.readyEvent.id,
  });
  exactObject(result, ["events"], "decision read result");
  if (!Array.isArray(result.events)) {
    throw new Error("decision read events are invalid");
  }
  return result.events;
}

async function admitReady(request, config, commandRunner, now) {
  const events = await loadDecisionEvents(request, config, commandRunner);
  const decision = resolveAcceptedDelegationDecision(
    events,
    request.offerEvent,
    { ownershipEvidence: request.ownershipEvidence, now },
  );
  return validateDelegationReadyEvent(
    request.readyEvent,
    decision.event,
    request.offerEvent,
    { ownershipEvidence: request.ownershipEvidence, now },
  );
}

function publishResponse(state, admittedReady) {
  if (state.phase === "published") {
    return {
      status: "noop",
      operationId: state.operationId,
      grantEventId: state.grantEventId,
    };
  }
  const offer = admittedReady.decision.offer.envelope.offer;
  return {
    status: "publish",
    operationId: state.operationId,
    content: state.grantContent,
    channelId: offer.task.channelId,
    replyTo: admittedReady.event.id,
    mentionPubkeys: [
      offer.source.ownerPubkey,
      offer.target.ownerPubkey,
      offer.task.agentPubkey,
    ],
  };
}

function admittedReadyFromState(request, state, operationId) {
  const observedAt =
    state.phase === "fencing"
      ? state.readyObservedAt
      : state.fenceEvidence?.readyObservedAt;
  const recoveryNow = storedTimestamp(
    observedAt,
    "durable delegation recovery clock",
  );
  const admittedReady = validateDelegationReadyEvent(
    request.readyEvent,
    state.decisionEvent,
    request.offerEvent,
    { ownershipEvidence: state.ownershipEvidence, now: recoveryNow },
  );
  return {
    admittedReady,
    lineage: lineageRecord(admittedReady, operationId),
  };
}

async function completeSourceFence(
  intent,
  admittedReady,
  lineage,
  file,
  config,
  proofKey,
  commandRunner,
  fenceLease,
) {
  validateFencingState(
    intent,
    admittedReady,
    lineage,
    intent.ownershipEvidence,
  );
  const fenceRequest = {
    schemaVersion: 1,
    ...lineage,
    readyObservedAt: intent.readyObservedAt,
  };
  const result = await commandRunner(config.sourceFenceCommand, fenceRequest, {
    lease: fenceLease,
  });
  exactObject(
    result,
    ["status", "sourceGeneration", "activationGeneration", "stateDigest"],
    "source fence result",
  );
  const fenceEvidence = {
    sourceGeneration: requiredString(
      result.sourceGeneration,
      UUID,
      "source fence generation",
    ),
    activationGeneration: requiredString(
      result.activationGeneration,
      UUID,
      "source fence activationGeneration",
    ),
    stateDigest: requiredString(
      result.stateDigest,
      HEX64,
      "source fence stateDigest",
    ),
    readyObservedAt: intent.readyObservedAt,
  };
  const validFence =
    result.status === "fenced" &&
    fenceEvidence.sourceGeneration === lineage.sourceGeneration &&
    fenceEvidence.activationGeneration === lineage.activationGeneration;
  if (!validFence) {
    throw new Error("source fence command did not fence the exact lineage");
  }
  const grant = grantFor(
    admittedReady,
    fenceEvidence,
    intent.grantedAt,
    proofKey,
    intent.ownershipEvidence,
  );
  const state = {
    schemaVersion: 1,
    phase: "fenced",
    ...lineage,
    fenceEvidence,
    decisionEvent: admittedReady.decision.event,
    ownershipEvidence: intent.ownershipEvidence,
    grant,
    grantContent: renderDelegationGrant(
      grant,
      admittedReady,
      fenceEvidence,
      proofKey,
      intent.ownershipEvidence,
    ),
    grantEventId: null,
  };
  atomicJson(file, state);
  return state;
}

async function recoverOrPublishSource(
  request,
  stored,
  operationId,
  file,
  config,
  proofKey,
  commandRunner,
  fenceLease,
) {
  const { admittedReady, lineage } = admittedReadyFromState(
    request,
    stored,
    operationId,
  );
  const state =
    stored.phase === "fencing"
      ? await completeSourceFence(
          stored,
          admittedReady,
          lineage,
          file,
          config,
          proofKey,
          commandRunner,
          fenceLease,
        )
      : validateSourceState(
          stored,
          admittedReady,
          lineage,
          proofKey,
          stored.ownershipEvidence,
        );
  return publishResponse(state, admittedReady);
}

export async function prepareGrant(
  request,
  configValue,
  { now = Date.now(), commandRunner = runVector } = {},
) {
  const config = validateConfig(configValue);
  if (config.role !== "source") {
    throw new Error("only the source host can prepare a delegation grant");
  }
  exactObject(
    request,
    ["offerEvent", "readyEvent", "ownershipEvidence"],
    "prepare grant request",
  );
  const operationId = operationIdForEvents(
    request.offerEvent,
    request.readyEvent,
  );
  const proofKey = await loadProofKey(config, commandRunner);
  const file = path.join(config.stateDirectory, `${operationId}.json`);
  const stored = fs.existsSync(file) ? readDurableJson(file) : null;
  const admittedReady = stored
    ? null
    : await admitReady(request, config, commandRunner, now);
  const releaseCapacity = stored
    ? null
    : reserveStateCapacity(config.stateDirectory);
  try {
    const fenceLease = acquireOperationLock(config.stateDirectory, "fence");
    try {
      const operationLease = acquireOperationLock(
        config.stateDirectory,
        operationId,
      );
      try {
        if (fs.existsSync(file)) {
          return await recoverOrPublishSource(
            request,
            readDurableJson(file),
            operationId,
            file,
            config,
            proofKey,
            commandRunner,
            fenceLease,
          );
        }
        const lineage = lineageRecord(admittedReady, operationId);
        const readyObservedAt = new Date(now).toISOString();
        const intent = {
          schemaVersion: 1,
          phase: "fencing",
          ...lineage,
          readyObservedAt,
          grantedAt: readyObservedAt,
          decisionEvent: admittedReady.decision.event,
          ownershipEvidence: request.ownershipEvidence,
        };
        atomicJson(file, intent);
        const state = await completeSourceFence(
          intent,
          admittedReady,
          lineage,
          file,
          config,
          proofKey,
          commandRunner,
          fenceLease,
        );
        return publishResponse(state, admittedReady);
      } finally {
        operationLease.release();
      }
    } finally {
      fenceLease.release();
    }
  } finally {
    releaseCapacity?.release();
  }
}

export async function commitGrant(
  request,
  configValue,
  { commandRunner = runVector } = {},
) {
  const config = validateConfig(configValue);
  if (config.role !== "source") {
    throw new Error("only the source host can commit a delegation grant");
  }
  exactObject(
    request,
    [
      "operationId",
      "offerEvent",
      "decisionEvent",
      "readyEvent",
      "grantEvent",
      "ownershipEvidence",
    ],
    "commit grant request",
  );
  const operationId = requiredString(
    request.operationId,
    UUID,
    "commit operationId",
  );
  const file = path.join(config.stateDirectory, `${operationId}.json`);
  const proofKey = await loadProofKey(config, commandRunner);
  const release = acquireOperationLock(config.stateDirectory, operationId);
  try {
    const persisted = readDurableJson(file);
    const admittedGrant = validateDelegationGrantEvent(
      request.grantEvent,
      request.readyEvent,
      request.decisionEvent,
      request.offerEvent,
      proofKey,
      { ownershipEvidence: persisted.ownershipEvidence },
    );
    const lineage = lineageRecord(admittedGrant.ready, operationId);
    const state = validateSourceState(
      persisted,
      admittedGrant.ready,
      lineage,
      proofKey,
      persisted.ownershipEvidence,
    );
    if (request.grantEvent.content !== state.grantContent) {
      throw new Error(
        "published grant content differs from durable fence state",
      );
    }
    if (state.phase === "published") {
      if (state.grantEventId !== admittedGrant.event.id) {
        throw new Error("another delegation grant is already published");
      }
      return { status: "noop", operationId };
    }
    const published = {
      ...state,
      phase: "published",
      grantEventId: admittedGrant.event.id,
    };
    atomicJson(file, published);
    return { status: "published", operationId };
  } finally {
    release.release();
  }
}

export async function activateTarget(
  request,
  configValue,
  { commandRunner = runVector } = {},
) {
  const config = validateConfig(configValue);
  if (config.role !== "target") {
    throw new Error("only the target host can activate a delegation grant");
  }
  exactObject(
    request,
    [
      "offerEvent",
      "decisionEvent",
      "readyEvent",
      "grantEvent",
      "ownershipEvidence",
    ],
    "activate target request",
  );
  const operationId = operationIdForEvents(
    request.offerEvent,
    request.readyEvent,
  );
  const file = path.join(config.stateDirectory, `${operationId}.json`);
  const proofKey = await loadProofKey(config, commandRunner);
  const stored = fs.existsSync(file) ? readDurableJson(file) : null;
  const ownershipEvidence =
    stored?.ownershipEvidence ?? request.ownershipEvidence;
  const admittedGrant = validateDelegationGrantEvent(
    request.grantEvent,
    request.readyEvent,
    request.decisionEvent,
    request.offerEvent,
    proofKey,
    { ownershipEvidence },
  );
  const lineage = lineageRecord(admittedGrant.ready, operationId);
  const releaseCapacity = stored
    ? null
    : reserveStateCapacity(config.stateDirectory);
  try {
    const releaseActivation = acquireOperationLock(
      config.stateDirectory,
      "activation",
    );
    try {
      const release = acquireOperationLock(config.stateDirectory, operationId);
      try {
        const current = fs.existsSync(file)
          ? validateTargetState(
              readDurableJson(file),
              lineage,
              ownershipEvidence,
              admittedGrant.event.content,
            )
          : {
              schemaVersion: 1,
              phase: "activating",
              ...lineage,
              grantEventId: admittedGrant.event.id,
              grantContent: admittedGrant.event.content,
              ownershipEvidence,
            };
        if (!fs.existsSync(file)) atomicJson(file, current);
        if (current.phase === "active") {
          return { status: "noop", operationId };
        }
        const activationRequest = {
          schemaVersion: 1,
          ...lineage,
          grantEventId: current.grantEventId,
          fencedStateDigest: admittedGrant.grant.fencedStateDigest,
        };
        const result = await commandRunner(
          config.targetActivateCommand,
          activationRequest,
          { lease: releaseActivation },
        );
        exactObject(
          result,
          ["status", "activationGeneration"],
          "target activation result",
        );
        const validActivation =
          result.status === "active" &&
          result.activationGeneration === lineage.activationGeneration;
        if (!validActivation) {
          throw new Error(
            "target command did not activate the exact generation",
          );
        }
        atomicJson(file, { ...current, phase: "active" });
        return { status: "active", operationId };
      } finally {
        release.release();
      }
    } finally {
      releaseActivation.release();
    }
  } finally {
    releaseCapacity?.release();
  }
}

function loadConfig(env = process.env) {
  const file =
    env.PI_DELEGATION_CONFIG ||
    path.join(os.homedir(), ".buzz", "delegation", "config.json");
  if (!path.isAbsolute(file)) {
    throw new Error("delegation config path must be absolute");
  }
  return validateConfig(readJson(file));
}

function readRequest() {
  return JSON.parse(readBoundedInput().toString("utf8"));
}

export const testOnly = {
  acquireOperationLock,
  atomicJson,
  ensureStateDirectory,
  processIdentityFor,
  processOwnsIdentity,
  readBoundedInput,
  readDurableJson,
  reserveStateCapacity,
  runVector,
};

if (
  process.argv[1] &&
  fs.realpathSync(fileURLToPath(import.meta.url)) ===
    fs.realpathSync(process.argv[1])
) {
  try {
    const mode = process.argv[2];
    const request = readRequest();
    const config = loadConfig();
    const handlers = {
      "prepare-grant": prepareGrant,
      "commit-grant": commitGrant,
      "activate-target": activateTarget,
    };
    const handler = handlers[mode];
    if (!handler) throw new Error("unsupported delegation host mode");
    process.stdout.write(`${canonicalJson(await handler(request, config))}\n`);
  } catch (error) {
    process.stderr.write(`delegation blocked: ${error.message}\n`);
    process.exitCode = 75;
  }
}
