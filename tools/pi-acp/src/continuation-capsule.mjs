import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import {
  canonicalJson,
  capsuleDigest,
  normalizedContinuation,
  renderContinuationContext,
} from "./continuation-canonical.mjs";
import { verifyGitBinding } from "./continuation-git.mjs";
import {
  acquireTaskLease,
  assertTaskSessionByteBudget,
  assertTaskSessionByteCapacity,
  canonicalRelayUrl,
  taskSessionIdentity,
  toolResultLeavesAmbiguousEffect,
} from "./task-session.mjs";

export {
  canonicalJson,
  capsuleDigest,
  renderContinuationContext,
} from "./continuation-canonical.mjs";
export { verifyGitBinding } from "./continuation-git.mjs";

export const CAPSULE_SCHEMA_VERSION = 2;
const SUPPORTED_CAPSULE_SCHEMAS = new Set([1, CAPSULE_SCHEMA_VERSION]);
export const MAX_CAPSULE_BYTES = 64 * 1024;
const MAX_TEXT = 8 * 1024;
const MAX_ITEM_TEXT = 2 * 1024;
const MAX_ITEMS = 64;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX40_OR_64 = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const HEX64 = /^[0-9a-f]{64}$/;
const PI_ENTRY = /^[0-9a-f]{8}$/;
const RUNTIME_ID = /^[a-z][a-z0-9_-]{0,31}$/;
const CONTINUATION_MODES = new Set(["semantic", "exact"]);
const LOCATIONS = new Set(["local", "cloud"]);
const CONTINUATION_LINEAGE_TYPES = new Set([
  "buzz.continuation.lineage.v1",
  "buzz.continuation.lineage.v2",
]);
const SUPPORTED_ADAPTERS = new Set(["pi@1"]);
const FORBIDDEN_KEYS =
  /(?:secret|token|password|credential|api[_-]?key|private[_-]?key|authorization|cookie|environment|rawToolOutput|thinking|reasoning)/i;
const FORBIDDEN_CONTEXT_TEXT =
  /\b(?:passwords?|passphrases?|credentials?|secrets?|tokens?|authorization|cookies?|environments?|private[_ -]?keys?|api[_ -]?keys?|access[_ -]?keys?|recovery[_ -]?phrases?|mnemonics?|wallet[_ -]?seeds?|seed[_ -]?phrases?|one[_ -]?time[_ -]?(?:passwords?|codes?)|otps?|pins?)\b/i;
const FORBIDDEN_TEXT = [
  /\bnsec1[023456789acdefghjklmnpqrstuvwxyz]{20,}\b/i,
  /-----BEGIN [A-Z ]*PRIVATE KEY-----/,
  /\b(?:sk-[A-Za-z0-9_-]{16,}|ghp_[A-Za-z0-9_-]{12,}|github_pat_[A-Za-z0-9_-]{12,}|glpat-[A-Za-z0-9_-]{12,}|npm_[A-Za-z0-9_-]{12,}|pypi-[A-Za-z0-9_-]{12,}|xox[baprs]-[A-Za-z0-9-]{20,}|AIza[A-Za-z0-9_-]{30,}|SG\.[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}|(?:sk|rk)_(?:live|test)_[A-Za-z0-9]{12,})/,
  /\beyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\b/,
  /\b(?:[A-Z][A-Z0-9_]*_)?(?:API_KEY|ACCESS_KEY|PRIVATE_KEY|TOKEN|SECRET|PASSWORD)\s*[:=]\s*\S+/i,
  /\b(?:password|passphrase|credential|private[_ -]?key|api[_ -]?key|(?:secret[_ -]+)?access[_ -]?key|secret|token)\s*(?:is|was|equals?|[:=])\s*\S+/i,
  /\b(?:aws[_ -]+)?(?:secret[_ -]+)?access[_ -]+key\b/i,
  /\b(?:AKIA|ASIA)[A-Z0-9]{16}\b/,
  /\b(?=[A-Za-z0-9/+]{32,}={0,2}\b)(?=[A-Za-z0-9/+]*[A-Z])(?=[A-Za-z0-9/+]*[a-z])(?=[A-Za-z0-9/+]*\d)[A-Za-z0-9/+]{32,}={0,2}\b/,
  /\bAuthorization\s*:\s*(?:Bearer|Basic)\s+\S+/i,
  /\b[a-z][a-z0-9+.-]*:\/\/[^\s:/@]+:[^\s/@]+@/i,
];

function plainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactKeys(value, keys, label) {
  if (!plainObject(value)) throw new Error(`${label} must be an object`);
  const actual = Object.keys(value).sort();
  const expected = [...keys].sort();
  if (JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error(`${label} has unknown or missing fields`);
  }
}

function text(value, label, max = MAX_ITEM_TEXT, { empty = false } = {}) {
  if (
    typeof value !== "string" ||
    (!empty && value.trim().length === 0) ||
    Buffer.byteLength(value, "utf8") > max
  ) {
    throw new Error(`${label} is invalid or exceeds ${max} bytes`);
  }
  return value;
}

function stringList(value, label, max = MAX_ITEMS) {
  if (!Array.isArray(value) || value.length > max) {
    throw new Error(`${label} must be a bounded array`);
  }
  return value.map((item, index) => text(item, `${label}[${index}]`));
}

function iso(value, label) {
  text(value, label, 64);
  const timestamp = Date.parse(value);
  if (
    !Number.isFinite(timestamp) ||
    new Date(timestamp).toISOString() !== value
  ) {
    throw new Error(`${label} must be canonical ISO-8601`);
  }
  return timestamp;
}

function assertSafeValue(value, pathLabel = "capsule") {
  if (typeof value === "string") {
    if (
      pathLabel.startsWith("capsule.context") &&
      (FORBIDDEN_CONTEXT_TEXT.test(value) ||
        /\b[0-9a-f]{64}\b/i.test(value) ||
        (value.match(/\p{L}+/gu) ?? []).length >= 12)
    ) {
      throw new Error(`${pathLabel} contains forbidden secret-like content`);
    }
    for (const pattern of FORBIDDEN_TEXT) {
      if (pattern.test(value))
        throw new Error(`${pathLabel} contains forbidden secret-like content`);
    }
    return;
  }
  if (Array.isArray(value)) {
    value.forEach((item, index) => {
      assertSafeValue(item, `${pathLabel}[${index}]`);
    });
    return;
  }
  if (plainObject(value)) {
    for (const [key, item] of Object.entries(value)) {
      if (FORBIDDEN_KEYS.test(key))
        throw new Error(`${pathLabel}.${key} is forbidden`);
      assertSafeValue(item, `${pathLabel}.${key}`);
    }
  }
}

function validateRelativePath(value, label) {
  text(value, label, 1024);
  if (
    path.isAbsolute(value) ||
    value.split(/[\\/]/).includes("..") ||
    value.includes("\0")
  ) {
    throw new Error(`${label} must be a repository-relative path`);
  }
  return value;
}

function validateRemoteUrl(value) {
  text(value, "git.remoteUrl", 2048);
  if (/^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+:[^\s]+$/.test(value)) return value;
  let remote;
  try {
    remote = new URL(value);
  } catch {
    throw new Error("git.remoteUrl is invalid");
  }
  if (
    remote.password ||
    remote.search ||
    remote.hash ||
    !["https:", "ssh:"].includes(remote.protocol) ||
    (remote.protocol === "https:" && remote.username)
  ) {
    throw new Error("git.remoteUrl is invalid or contains credentials");
  }
  return value;
}

function invalidLineageSize(value) {
  return !Array.isArray(value) || value.length === 0 || value.length > 32;
}

function invalidAdapterVersion(value) {
  return !Number.isSafeInteger(value) || value < 1;
}

function invalidOptionalDigest(value) {
  return value !== null && !HEX64.test(value);
}

function validateLineage(value) {
  if (invalidLineageSize(value)) {
    throw new Error("continuation.lineage must contain 1..32 entries");
  }
  return value.map((entry, index) => {
    const label = `continuation.lineage[${index}]`;
    exactKeys(
      entry,
      ["runtime", "sessionId", "checkpointId", "location"],
      label,
    );
    if (!RUNTIME_ID.test(entry.runtime))
      throw new Error(`${label}.runtime is invalid`);
    text(entry.sessionId, `${label}.sessionId`, 128);
    text(entry.checkpointId, `${label}.checkpointId`, 128);
    if (!LOCATIONS.has(entry.location))
      throw new Error(`${label}.location is invalid`);
    return entry;
  });
}

function adapterContractKey(adapter) {
  return `${adapter.runtime}@${adapter.schemaVersion}`;
}

function validateAdapter(adapter) {
  if (adapter === null) return null;
  exactKeys(
    adapter,
    ["runtime", "schemaVersion", "payload"],
    "continuation.adapter",
  );
  if (!RUNTIME_ID.test(adapter.runtime))
    throw new Error("continuation.adapter.runtime is invalid");
  if (invalidAdapterVersion(adapter.schemaVersion))
    throw new Error("continuation.adapter.schemaVersion is invalid");
  if (!SUPPORTED_ADAPTERS.has(adapterContractKey(adapter))) {
    throw new Error("continuation adapter is unsupported");
  }
  if (!plainObject(adapter.payload))
    throw new Error("continuation.adapter.payload must be an object");
  assertSafeValue(adapter.payload, "capsule.continuation.adapter.payload");
  return adapter;
}

function exactAdapterMissing(mode, adapter) {
  return mode === "exact" && adapter === null;
}

function exactAdapterUnsupported(mode, adapter) {
  return mode === "exact" && adapter !== null;
}

function sameCanonicalContract(left, right) {
  return canonicalJson(left) === canonicalJson(right);
}

function isContinuationLineageEntry(entry) {
  return (
    entry.type === "custom" &&
    typeof entry.customType === "string" &&
    entry.customType.startsWith("buzz.continuation.lineage.")
  );
}

function continuationLineageEntries(branch) {
  const entries = branch.filter(isContinuationLineageEntry);
  for (const entry of entries) {
    if (!CONTINUATION_LINEAGE_TYPES.has(entry.customType)) {
      throw new Error("persisted continuation lineage version is unsupported");
    }
  }
  return entries;
}

function validatedContinuation(capsule) {
  if (capsule.schemaVersion === 1) validateLegacyPi(capsule.pi);
  return normalizedContinuation(capsule);
}

function portableCapsule(capsule) {
  if (capsule.schemaVersion === CAPSULE_SCHEMA_VERSION) {
    return structuredClone(capsule);
  }
  const portable = structuredClone(capsule);
  delete portable.pi;
  portable.schemaVersion = CAPSULE_SCHEMA_VERSION;
  portable.continuation = validatedContinuation(capsule);
  return portable;
}

function normalizedExportIntent(capsule) {
  const portable = portableCapsule(capsule);
  portable.context.recentTail = [];
  return portable;
}

function continuationParentDigest(capsule) {
  return validatedContinuation(capsule).parentDigest;
}

function validatePersistedSourceBinding(lineage, binding) {
  const head = lineage.at(-1);
  const lineageBinding = {
    runtime: head.runtime,
    sessionId: head.sessionId,
    checkpointId: head.checkpointId,
  };
  if (!sameCanonicalContract(binding, lineageBinding)) {
    throw new Error("persisted continuation source binding is inconsistent");
  }
}

function validateImportedMarkerDigest(data, parentField) {
  if (!HEX64.test(data.capsuleDigest)) {
    throw new Error("persisted continuation lineage digest is invalid");
  }
  if (invalidOptionalDigest(data[parentField])) {
    throw new Error("persisted continuation parent digest is invalid");
  }
}

function validatedImportedLineage(entry) {
  if (!entry) return [];
  const data = entry.data;
  if (entry.customType === "buzz.continuation.lineage.v2") {
    exactKeys(
      data,
      [
        "capsuleDigest",
        "sourceRuntime",
        "sourceSessionId",
        "sourceCheckpointId",
        "parentDigest",
        "lineage",
      ],
      "persisted continuation lineage v2",
    );
    validateImportedMarkerDigest(data, "parentDigest");
    if (data.sourceRuntime !== "pi") {
      throw new Error("persisted continuation source runtime is invalid");
    }
    text(data.sourceSessionId, "persisted continuation sourceSessionId", 128);
    if (!PI_ENTRY.test(data.sourceCheckpointId)) {
      throw new Error("persisted continuation sourceCheckpointId is invalid");
    }
    validateLineage(data.lineage);
    validatePersistedSourceBinding(data.lineage, {
      runtime: data.sourceRuntime,
      sessionId: data.sourceSessionId,
      checkpointId: data.sourceCheckpointId,
    });
    return data.lineage;
  }
  exactKeys(
    data,
    [
      "capsuleDigest",
      "parentSessionId",
      "parentLeafId",
      "parentCapsuleDigest",
      "lineage",
    ],
    "persisted continuation lineage v1",
  );
  validateImportedMarkerDigest(data, "parentCapsuleDigest");
  text(data.parentSessionId, "persisted continuation parentSessionId", 128);
  if (!PI_ENTRY.test(data.parentLeafId)) {
    throw new Error("persisted continuation parentLeafId is invalid");
  }
  data.lineage.forEach(validateLegacyLineageEntry);
  const lineage = data.lineage.map((item) => ({
    runtime: "pi",
    sessionId: item.sessionId,
    checkpointId: item.leafId,
    location: item.location,
  }));
  validatePersistedSourceBinding(lineage, {
    runtime: "pi",
    sessionId: data.parentSessionId,
    checkpointId: data.parentLeafId,
  });
  return lineage;
}

function validatedPiBinding(capsule, label = "capsule") {
  const continuation = validatedContinuation(capsule);
  const adapter = continuation.adapter;
  if (adapter?.runtime !== "pi") {
    throw new Error(`${label} has no compatible Pi adapter payload`);
  }
  if (adapter.schemaVersion !== 1) {
    throw new Error(`${label} has no compatible Pi adapter payload`);
  }
  exactKeys(
    adapter.payload,
    ["sourceSessionId", "sourceLeafId"],
    `${label}.continuation.adapter.payload`,
  );
  const binding = {
    runtime: "pi",
    sessionId: text(
      adapter.payload.sourceSessionId,
      `${label}.continuation.adapter.payload.sourceSessionId`,
      128,
    ),
    checkpointId: adapter.payload.sourceLeafId,
  };
  if (!PI_ENTRY.test(binding.checkpointId)) {
    throw new Error(
      `${label}.continuation.adapter.payload.sourceLeafId is invalid`,
    );
  }
  const head = continuation.lineage.at(-1);
  const lineageBinding = {
    runtime: head.runtime,
    sessionId: head.sessionId,
    checkpointId: head.checkpointId,
  };
  if (!sameCanonicalContract(binding, lineageBinding)) {
    throw new Error("Pi adapter does not match the continuation lineage head");
  }
  return binding;
}

function validateLegacyLineageEntry(entry, index) {
  const label = `pi.lineage[${index}]`;
  exactKeys(entry, ["sessionId", "leafId", "location"], label);
  text(entry.sessionId, `${label}.sessionId`, 128);
  if (!PI_ENTRY.test(entry.leafId)) {
    throw new Error(`${label}.leafId is invalid`);
  }
  if (!LOCATIONS.has(entry.location)) {
    throw new Error(`${label}.location is invalid`);
  }
}

function validateLegacyPi(pi) {
  exactKeys(
    pi,
    ["sourceSessionId", "sourceLeafId", "lineage", "parentCapsuleDigest"],
    "pi",
  );
  text(pi.sourceSessionId, "pi.sourceSessionId", 128);
  if (!PI_ENTRY.test(pi.sourceLeafId)) {
    throw new Error("pi.sourceLeafId is invalid");
  }
  if (!Array.isArray(pi.lineage)) {
    throw new Error("pi.lineage must be an array");
  }
  pi.lineage.forEach(validateLegacyLineageEntry);
}

function validateContext(context) {
  exactKeys(
    context,
    [
      "goal",
      "constraints",
      "decisions",
      "completed",
      "pending",
      "files",
      "checks",
      "blockers",
      "unresolvedEffects",
      "recentTail",
    ],
    "context",
  );
  text(context.goal, "context.goal", MAX_TEXT);
  for (const key of [
    "constraints",
    "decisions",
    "completed",
    "pending",
    "checks",
    "blockers",
  ]) {
    stringList(context[key], `context.${key}`);
  }
  if (!Array.isArray(context.files) || context.files.length > MAX_ITEMS) {
    throw new Error("context.files must be a bounded array");
  }
  for (const [index, file] of context.files.entries()) {
    exactKeys(file, ["path", "symbols"], `context.files[${index}]`);
    validateRelativePath(file.path, `context.files[${index}].path`);
    stringList(file.symbols, `context.files[${index}].symbols`, 32);
  }
  if (!Array.isArray(context.unresolvedEffects)) {
    throw new Error("context.unresolvedEffects must be an array");
  }
  if (context.unresolvedEffects.length !== 0) {
    throw new Error("capsule export is blocked by unresolved effects");
  }
  if (!Array.isArray(context.recentTail) || context.recentTail.length !== 0) {
    throw new Error(
      "context.recentTail must be empty; raw transcripts are not portable",
    );
  }
}

export function validateCapsule(
  capsule,
  { now = Date.now(), allowExpired = false } = {},
) {
  if (!SUPPORTED_CAPSULE_SCHEMAS.has(capsule?.schemaVersion)) {
    throw new Error("unsupported capsule schemaVersion");
  }
  const legacyPi = capsule.schemaVersion === 1;
  exactKeys(
    capsule,
    [
      "schemaVersion",
      "capsuleId",
      "createdAt",
      "expiresAt",
      "task",
      "git",
      "ownership",
      legacyPi ? "pi" : "continuation",
      "context",
    ],
    "capsule",
  );
  if (!UUID.test(capsule.capsuleId)) throw new Error("capsuleId is invalid");
  const created = iso(capsule.createdAt, "createdAt");
  const expires = iso(capsule.expiresAt, "expiresAt");
  if (created > now + 5 * 60 * 1000)
    throw new Error("capsule creation time is in the future");
  if (expires <= created || expires - created > 24 * 60 * 60 * 1000)
    throw new Error("capsule expiry window is invalid");
  if (!allowExpired && now > expires) throw new Error("capsule is expired");

  exactKeys(
    capsule.task,
    ["relayUrl", "agentPubkey", "channelId", "threadRoot"],
    "task",
  );
  if (canonicalRelayUrl(capsule.task.relayUrl) !== capsule.task.relayUrl)
    throw new Error("task.relayUrl must be canonical");
  if (!HEX64.test(capsule.task.agentPubkey))
    throw new Error("task.agentPubkey is invalid");
  if (!UUID.test(capsule.task.channelId))
    throw new Error("task.channelId is invalid");
  if (!HEX64.test(capsule.task.threadRoot))
    throw new Error("task.threadRoot is invalid");

  exactKeys(
    capsule.git,
    ["repository", "remoteName", "remoteUrl", "branch", "commit", "tree"],
    "git",
  );
  if (!path.isAbsolute(capsule.git.repository))
    throw new Error("git.repository must be absolute");
  text(capsule.git.remoteName, "git.remoteName", 128);
  if (!/^[A-Za-z0-9._-]+$/.test(capsule.git.remoteName)) {
    throw new Error("git.remoteName is invalid");
  }
  validateRemoteUrl(capsule.git.remoteUrl);
  text(capsule.git.branch, "git.branch", 512);
  if (
    !HEX40_OR_64.test(capsule.git.commit) ||
    !HEX40_OR_64.test(capsule.git.tree)
  ) {
    throw new Error("git commit/tree binding is invalid");
  }

  exactKeys(
    capsule.ownership,
    ["generation", "sourceLocation", "targetLocation"],
    "ownership",
  );
  if (!UUID.test(capsule.ownership.generation))
    throw new Error("ownership.generation is invalid");
  if (
    !LOCATIONS.has(capsule.ownership.sourceLocation) ||
    !LOCATIONS.has(capsule.ownership.targetLocation)
  ) {
    throw new Error("ownership location is invalid");
  }
  if (capsule.ownership.sourceLocation === capsule.ownership.targetLocation) {
    throw new Error("ownership source and target must differ");
  }

  const continuation = validatedContinuation(capsule);
  if (!legacyPi) {
    exactKeys(
      continuation,
      ["mode", "lineage", "parentDigest", "adapter"],
      "continuation",
    );
  }
  if (!CONTINUATION_MODES.has(continuation.mode))
    throw new Error("continuation.mode is invalid");
  validateLineage(continuation.lineage);
  if (invalidOptionalDigest(continuation.parentDigest)) {
    throw new Error("continuation.parentDigest is invalid");
  }
  const adapter = validateAdapter(continuation.adapter);
  if (exactAdapterMissing(continuation.mode, adapter)) {
    throw new Error("exact continuation requires an adapter payload");
  }
  if (exactAdapterUnsupported(continuation.mode, adapter)) {
    throw new Error("continuation adapter does not support exact restore");
  }
  const last = continuation.lineage.at(-1);
  if (last.location !== capsule.ownership.sourceLocation) {
    throw new Error(
      "continuation lineage does not terminate at the source location",
    );
  }
  if (adapter !== null) {
    if (adapter.runtime !== last.runtime) {
      throw new Error(
        "continuation adapter runtime does not match lineage head",
      );
    }
    if (adapter.runtime === "pi") validatedPiBinding(capsule);
  }

  validateContext(capsule.context);
  assertSafeValue(capsule);
  const size = Buffer.byteLength(canonicalJson(capsule), "utf8");
  if (size > MAX_CAPSULE_BYTES)
    throw new Error(`capsule exceeds ${MAX_CAPSULE_BYTES} bytes`);
  return capsule;
}

export function assertIdleSession(sessionManager) {
  if (!sessionManager?.isPersisted?.())
    throw new Error("source Pi session is not persistent");
  const branch = sessionManager.getBranch();
  const outstanding = new Set();
  for (const entry of branch) {
    if (entry.type !== "message") continue;
    const message = entry.message;
    if (message?.role === "assistant") {
      if (message.stopReason === "pending")
        throw new Error("source Pi session is still streaming");
      for (const block of message.content ?? []) {
        if (block?.type === "toolCall" && typeof block.id === "string")
          outstanding.add(block.id);
      }
    } else if (message?.role === "toolResult") {
      if (!toolResultLeavesAmbiguousEffect(message)) {
        outstanding.delete(message.toolCallId);
      }
    }
  }
  if (outstanding.size > 0)
    throw new Error("source Pi session has unresolved tool effects");
  const leaf = sessionManager.getLeafEntry();
  const settledAssistant =
    leaf?.type === "message" &&
    leaf.message?.role === "assistant" &&
    leaf.message.stopReason !== "toolUse";
  const settledBuzzPublication =
    leaf?.type === "message" &&
    leaf.message?.role === "toolResult" &&
    leaf.message.toolName === "buzz_reply" &&
    leaf.message.isError === false;
  const settledDeliveryMetadata =
    leaf?.type === "custom" && leaf.customType === "buzz.delivery.v1";
  if (
    !settledAssistant &&
    !settledBuzzPublication &&
    !settledDeliveryMetadata
  ) {
    throw new Error("source Pi session is not at a settled effect boundary");
  }
  return true;
}

function assertBoundTaskSessionDirectory(sessionDir, task) {
  if (!path.isAbsolute(sessionDir))
    throw new Error("task sessionDir must be absolute");
  const identity = taskSessionIdentity(
    {
      relayUrl: task.relayUrl,
      agentPubkey: task.agentPubkey,
      channelId: task.channelId,
      taskThreadRoot: task.threadRoot,
    },
    task.relayUrl,
  );
  const normalized = path.resolve(sessionDir);
  if (
    path.basename(normalized) !== identity.digest ||
    path.basename(path.dirname(normalized)) !== identity.digest.slice(0, 2)
  ) {
    throw new Error(
      "Pi session directory does not match the capsule task identity",
    );
  }
  return identity;
}

export function createCapsule(draft, sessionManager, options = {}) {
  assertIdleSession(sessionManager);
  const leafId = sessionManager.getLeafId();
  if (!leafId) throw new Error("source Pi session has no active leaf");
  const sessionId = sessionManager.getSessionId();
  const draftBinding = validatedPiBinding(draft, "draft");
  const activeBinding = {
    runtime: "pi",
    sessionId,
    checkpointId: leafId,
  };
  if (!sameCanonicalContract(draftBinding, activeBinding)) {
    throw new Error("draft lineage is stale for the active Pi leaf");
  }
  const branch = sessionManager.getBranch();
  const imported = continuationLineageEntries(branch).at(-1);
  const inheritedLineage = validatedImportedLineage(imported);
  const derivedLineage = [
    ...inheritedLineage,
    {
      runtime: "pi",
      sessionId,
      checkpointId: leafId,
      location: draft.ownership.sourceLocation,
    },
  ];
  const recordedHead = readCapsuleHead(sessionManager.getSessionDir());
  const importedDigest = imported?.data?.capsuleDigest ?? null;
  if ((recordedHead?.digest ?? null) !== importedDigest) {
    throw new Error(
      "selected Pi session is not anchored at the current task lineage head",
    );
  }
  const derivedParent = importedDigest;
  const draftLineageContract = {
    lineage: validatedContinuation(draft).lineage,
    parentDigest: continuationParentDigest(draft),
  };
  const persistedLineageContract = {
    lineage: derivedLineage,
    parentDigest: derivedParent,
  };
  if (!sameCanonicalContract(draftLineageContract, persistedLineageContract)) {
    throw new Error(
      "draft lineage does not match the persisted Pi lineage head",
    );
  }
  const capsule = portableCapsule(draft);
  // Raw transcript text is never portable: deterministic secret detection
  // cannot prove arbitrary conversation text credential-free. Continuation
  // uses only the explicit, bounded summary fields validated below.
  capsule.context.recentTail = [];
  capsule.continuation.lineage = derivedLineage;
  capsule.continuation.parentDigest = derivedParent;
  capsule.continuation.adapter.payload = {
    sourceSessionId: sessionId,
    sourceLeafId: leafId,
  };
  validateCapsule(capsule, options);
  return { capsule, digest: capsuleDigest(capsule) };
}

export function validateEnvelope(envelope, options = {}) {
  exactKeys(envelope, ["capsule", "digest"], "envelope");
  if (!HEX64.test(envelope.digest))
    throw new Error("envelope digest is invalid");
  validateCapsule(envelope.capsule, options);
  if (capsuleDigest(envelope.capsule) !== envelope.digest)
    throw new Error("capsule digest mismatch");
  return envelope;
}

function validateImportResult(result, sessionDir, digest) {
  exactKeys(
    result,
    ["schemaVersion", "capsuleDigest", "sessionId", "leafId", "sessionFile"],
    "import result",
  );
  if (
    result.schemaVersion !== 1 ||
    result.capsuleDigest !== digest ||
    typeof result.sessionId !== "string" ||
    !PI_ENTRY.test(result.leafId) ||
    typeof result.sessionFile !== "string"
  ) {
    throw new Error("stored capsule import result is corrupt");
  }
  const file = fs.realpathSync(result.sessionFile);
  const root = `${fs.realpathSync(sessionDir)}${path.sep}`;
  if (!file.startsWith(root))
    throw new Error("stored capsule session escapes its task directory");
  const manager = SessionManager.open(file, sessionDir);
  const recordedLeafIsAncestor = manager
    .getBranch()
    .some((entry) => entry.id === result.leafId);
  if (manager.getSessionId() !== result.sessionId || !recordedLeafIsAncestor) {
    throw new Error("stored capsule import lineage is corrupt");
  }
  return result;
}

function atomicJson(file, value, sessionDir) {
  const encoded = `${canonicalJson(value)}\n`;
  const bytes = Buffer.byteLength(encoded);
  assertTaskSessionByteCapacity(sessionDir, bytes);
  const temporary = `${file}.${process.pid}.${crypto.randomUUID()}.tmp`;
  fs.writeFileSync(temporary, encoded, { mode: 0o600, flag: "wx" });
  fs.renameSync(temporary, file);
  fs.chmodSync(file, 0o600);
  assertTaskSessionByteBudget(sessionDir);
}

function readCapsuleHead(sessionDir) {
  const file = path.join(sessionDir, ".capsule-lineage-head.json");
  if (!fs.existsSync(file)) return null;
  const head = JSON.parse(fs.readFileSync(file, "utf8"));
  exactKeys(
    head,
    ["schemaVersion", "generation", "digest"],
    "capsule lineage head",
  );
  if (
    head.schemaVersion !== 1 ||
    !UUID.test(head.generation) ||
    !HEX64.test(head.digest)
  ) {
    throw new Error("capsule lineage head is corrupt");
  }
  return head;
}

function assertCapsuleParent(sessionDir, capsule, digest) {
  const head = readCapsuleHead(sessionDir);
  if (head?.digest === digest) {
    throw new Error(
      "capsule is already the lineage head without an import receipt",
    );
  }
  if ((head?.digest ?? null) !== continuationParentDigest(capsule)) {
    throw new Error("capsule parent is not the current task lineage head");
  }
  return head;
}

function acquireLineageLock(sessionDir, digest) {
  const lock = path.join(sessionDir, ".capsule-lineage.lock");
  const temporary = path.join(
    sessionDir,
    `.capsule-lineage.${process.pid}.${crypto.randomUUID()}.tmp`,
  );
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeFileSync(descriptor, `${digest} ${process.pid}\n`);
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  try {
    fs.linkSync(temporary, lock);
    return lock;
  } finally {
    fs.unlinkSync(temporary);
  }
}

export function recordCapsuleHead(
  sessionDir,
  capsule,
  digest,
  { lockHeld = false } = {},
) {
  if (!path.isAbsolute(sessionDir))
    throw new Error("sessionDir must be absolute");
  fs.mkdirSync(sessionDir, { recursive: true, mode: 0o700 });
  fs.chmodSync(sessionDir, 0o700);
  const lock = path.join(sessionDir, ".capsule-lineage.lock");
  let acquired = false;
  if (!lockHeld) {
    try {
      acquireLineageLock(sessionDir, digest);
      acquired = true;
    } catch (error) {
      if (
        error.code !== "EEXIST" ||
        !recoverCompletedLineageLock(sessionDir, digest)
      ) {
        throw new Error(
          "capsule lineage operation is concurrent or interrupted",
        );
      }
      try {
        acquireLineageLock(sessionDir, digest);
        acquired = true;
      } catch {
        throw new Error(
          "capsule lineage operation is concurrent or interrupted",
        );
      }
    }
  }
  try {
    const head = readCapsuleHead(sessionDir);
    if (head?.digest === digest) return head;
    if ((head?.digest ?? null) !== continuationParentDigest(capsule)) {
      throw new Error("capsule parent is not the current task lineage head");
    }
    const next = {
      schemaVersion: 1,
      generation: capsule.ownership.generation,
      digest,
    };
    atomicJson(
      path.join(sessionDir, ".capsule-lineage-head.json"),
      next,
      sessionDir,
    );
    return next;
  } finally {
    if (acquired && fs.existsSync(lock)) fs.unlinkSync(lock);
  }
}

function replaceCapsuleHead(sessionDir, replacedDigest, capsule, digest) {
  const lock = path.join(sessionDir, ".capsule-lineage.lock");
  try {
    acquireLineageLock(sessionDir, digest);
  } catch (error) {
    if (
      error.code !== "EEXIST" ||
      !recoverCompletedLineageLock(sessionDir, digest)
    ) {
      throw new Error("capsule lineage operation is concurrent or interrupted");
    }
    try {
      acquireLineageLock(sessionDir, digest);
    } catch {
      throw new Error("capsule lineage operation is concurrent or interrupted");
    }
  }
  try {
    const head = readCapsuleHead(sessionDir);
    if (head?.digest === digest) return head;
    if (head?.digest !== replacedDigest) {
      throw new Error("expired capsule is not the current task lineage head");
    }
    const next = {
      schemaVersion: 1,
      generation: capsule.ownership.generation,
      digest,
    };
    atomicJson(
      path.join(sessionDir, ".capsule-lineage-head.json"),
      next,
      sessionDir,
    );
    return next;
  } finally {
    if (fs.existsSync(lock)) fs.unlinkSync(lock);
  }
}

function processIsAlive(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error.code === "ESRCH") return false;
    return true;
  }
}

function recoverCompletedLineageLock(sessionDir, digest) {
  const lock = path.join(sessionDir, ".capsule-lineage.lock");
  if (!fs.existsSync(lock)) return true;
  const content = fs.readFileSync(lock, "utf8").trim();
  const match = /^([0-9a-f]{64}) ([0-9]+)$/.exec(content);
  if (!match) return false;
  if (match[1] !== digest) return false;
  if (processIsAlive(Number.parseInt(match[2], 10))) return false;
  fs.unlinkSync(lock);
  return true;
}

function exportCapsuleUnlocked(draft, sessionManager, options = {}) {
  const sessionDir = sessionManager.getSessionDir();
  assertBoundTaskSessionDirectory(sessionDir, draft?.task);
  if (
    fs.realpathSync(sessionManager.getCwd()) !==
    fs.realpathSync(draft?.git?.repository)
  ) {
    throw new Error(
      "source Pi session repository does not match the Git binding",
    );
  }
  const exportsDir = path.join(sessionDir, ".capsule-exports");
  fs.mkdirSync(exportsDir, { recursive: true, mode: 0o700 });
  fs.chmodSync(exportsDir, 0o700);
  if (!UUID.test(draft?.capsuleId ?? ""))
    throw new Error("capsuleId is invalid");
  const receipt = path.join(exportsDir, `${draft.capsuleId}.json`);
  if (fs.existsSync(receipt)) {
    const receiptNow = options.now ?? Date.now();
    const envelope = validateEnvelope(
      JSON.parse(fs.readFileSync(receipt, "utf8")),
      { now: receiptNow, allowExpired: true },
    );
    if (receiptNow > Date.parse(envelope.capsule.expiresAt)) {
      throw new Error("capsule export receipt expired; use reissue");
    }
    const storedBinding = validatedPiBinding(
      envelope.capsule,
      "stored capsule",
    );
    const draftBinding = validatedPiBinding(draft, "draft");
    const activeBinding = {
      runtime: "pi",
      sessionId: sessionManager.getSessionId(),
      checkpointId: sessionManager.getLeafId(),
    };
    const storedLeafIsActive = sameCanonicalContract(
      storedBinding,
      activeBinding,
    );
    const draftSelectsStoredLeaf = sameCanonicalContract(
      draftBinding,
      storedBinding,
    );
    const exportIntentMatches = sameCanonicalContract(
      normalizedExportIntent(draft),
      normalizedExportIntent(envelope.capsule),
    );
    if (!exportIntentMatches) {
      throw new Error(
        "capsule export receipt does not match the selected Pi leaf",
      );
    }
    if (!storedLeafIsActive) {
      throw new Error(
        "capsule export receipt does not match the selected Pi leaf",
      );
    }
    if (!draftSelectsStoredLeaf) {
      throw new Error(
        "capsule export receipt does not match the selected Pi leaf",
      );
    }
    recordCapsuleHead(sessionDir, envelope.capsule, envelope.digest);
    return envelope;
  }
  const envelope = createCapsule(draft, sessionManager, options);
  atomicJson(receipt, envelope, sessionDir);
  recordCapsuleHead(sessionDir, envelope.capsule, envelope.digest);
  return envelope;
}

export function exportCapsule(draft, sessionManager, options = {}) {
  const release = acquireTaskLease(
    sessionManager.getSessionDir(),
    "capsule-export",
  );
  try {
    return exportCapsuleUnlocked(draft, sessionManager, options);
  } finally {
    release();
  }
}

export function reissueCapsule(
  expiredEnvelope,
  replacement,
  sessionManager,
  { now = Date.now() } = {},
) {
  const release = acquireTaskLease(
    sessionManager.getSessionDir(),
    "capsule-reissue",
  );
  try {
    validateEnvelope(expiredEnvelope, { now, allowExpired: true });
    exactKeys(
      replacement,
      ["capsuleId", "createdAt", "expiresAt"],
      "capsule reissue",
    );
    const previous = expiredEnvelope.capsule;
    if (now <= Date.parse(previous.expiresAt)) {
      throw new Error("capsule reissue requires an expired source capsule");
    }
    const sessionDir = sessionManager.getSessionDir();
    assertBoundTaskSessionDirectory(sessionDir, previous.task);
    const previousBinding = validatedPiBinding(previous, "expired capsule");
    const activeBinding = {
      runtime: "pi",
      sessionId: sessionManager.getSessionId(),
      checkpointId: sessionManager.getLeafId(),
    };
    if (!sameCanonicalContract(activeBinding, previousBinding)) {
      throw new Error("capsule reissue source Pi leaf has advanced");
    }
    if (readCapsuleHead(sessionDir)?.digest !== expiredEnvelope.digest) {
      throw new Error("expired capsule is not the current task lineage head");
    }
    const capsule = portableCapsule(previous);
    capsule.capsuleId = replacement.capsuleId;
    capsule.createdAt = replacement.createdAt;
    capsule.expiresAt = replacement.expiresAt;
    validateCapsule(capsule, { now });
    const envelope = { capsule, digest: capsuleDigest(capsule) };
    const exportsDir = path.join(sessionDir, ".capsule-exports");
    fs.mkdirSync(exportsDir, { recursive: true, mode: 0o700 });
    fs.chmodSync(exportsDir, 0o700);
    const receipt = path.join(exportsDir, `${capsule.capsuleId}.json`);
    if (fs.existsSync(receipt)) {
      const stored = validateEnvelope(
        JSON.parse(fs.readFileSync(receipt, "utf8")),
        {
          now,
        },
      );
      if (stored.digest !== envelope.digest) {
        throw new Error(
          "capsule reissue receipt conflicts with the replacement",
        );
      }
      replaceCapsuleHead(
        sessionDir,
        expiredEnvelope.digest,
        stored.capsule,
        stored.digest,
      );
      return stored;
    }
    atomicJson(receipt, envelope, sessionDir);
    replaceCapsuleHead(
      sessionDir,
      expiredEnvelope.digest,
      capsule,
      envelope.digest,
    );
    return envelope;
  } finally {
    release();
  }
}

function reconcileCompletedHead(sessionDir, capsule, digest) {
  const head = readCapsuleHead(sessionDir);
  if (head?.digest === digest) return;
  if ((head?.digest ?? null) === continuationParentDigest(capsule)) {
    recordCapsuleHead(sessionDir, capsule, digest);
  }
  // A later descendant may already be authoritative. Never roll it back while
  // reconciling an older completed import.
}

function validatedImportTask(expected, capsule, digest) {
  const fields = [
    "generation",
    "location",
    "capsuleDigest",
    "relayUrl",
    "agentPubkey",
    "channelId",
    "threadRoot",
  ];
  const delegated = Object.hasOwn(expected ?? {}, "targetAgentPubkey");
  exactKeys(
    expected,
    delegated ? [...fields, "targetAgentPubkey"] : fields,
    "expected ownership",
  );
  const targetAgentPubkey = delegated
    ? expected.targetAgentPubkey
    : expected.agentPubkey;
  if (!HEX64.test(targetAgentPubkey))
    throw new Error("expected targetAgentPubkey is invalid");
  const expectedBinding = {
    generation: expected.generation,
    location: expected.location,
    capsuleDigest: expected.capsuleDigest,
    relayUrl: canonicalRelayUrl(expected.relayUrl),
    agentPubkey: expected.agentPubkey,
    channelId: expected.channelId,
    threadRoot: expected.threadRoot,
  };
  const capsuleBinding = {
    generation: capsule.ownership.generation,
    location: capsule.ownership.targetLocation,
    capsuleDigest: digest,
    ...capsule.task,
  };
  if (!sameCanonicalContract(expectedBinding, capsuleBinding)) {
    throw new Error(
      "capsule ownership generation, location, or task binding is stale",
    );
  }
  return { ...capsule.task, agentPubkey: targetAgentPubkey };
}

function invalidImportPaths(cwd, sessionDir) {
  return !path.isAbsolute(cwd) || !path.isAbsolute(sessionDir);
}

function validatedImportPreflight(
  envelope,
  { cwd, sessionDir, expected, now = Date.now() },
) {
  validateEnvelope(envelope, { now, allowExpired: true });
  const capsule = envelope.capsule;
  const importTask = validatedImportTask(expected, capsule, envelope.digest);
  if (invalidImportPaths(cwd, sessionDir)) {
    throw new Error("import paths must be absolute");
  }
  assertBoundTaskSessionDirectory(sessionDir, importTask);
  const sourceBinding = validatedPiBinding(capsule, "import capsule");
  const complete = path.join(
    sessionDir,
    ".capsule-imports",
    `${envelope.digest}.json`,
  );
  if (!fs.existsSync(complete)) {
    validateEnvelope(envelope, { now });
    verifyGitBinding(capsule.git, cwd);
  }
  return { sourceBinding };
}

function importCapsuleUnlocked(envelope, { cwd, sessionDir, sourceBinding }) {
  const capsule = envelope.capsule;
  fs.mkdirSync(sessionDir, { recursive: true, mode: 0o700 });
  fs.chmodSync(sessionDir, 0o700);
  const imports = path.join(sessionDir, ".capsule-imports");
  fs.mkdirSync(imports, { recursive: true, mode: 0o700 });
  fs.chmodSync(imports, 0o700);
  const complete = path.join(imports, `${envelope.digest}.json`);
  if (fs.existsSync(complete)) {
    const result = validateImportResult(
      JSON.parse(fs.readFileSync(complete, "utf8")),
      sessionDir,
      envelope.digest,
    );
    const head = readCapsuleHead(sessionDir);
    if (head?.digest === envelope.digest) {
      recoverCompletedLineageLock(sessionDir, envelope.digest);
      return result;
    }
    if (head !== null && head.digest !== continuationParentDigest(capsule)) {
      // A later descendant is already authoritative; an old completed receipt
      // remains idempotently queryable without rolling the head back.
      return result;
    }
    if (!recoverCompletedLineageLock(sessionDir, envelope.digest)) {
      throw new Error(
        "completed capsule import is still committing its lineage head",
      );
    }
    reconcileCompletedHead(sessionDir, capsule, envelope.digest);
    if (readCapsuleHead(sessionDir)?.digest !== envelope.digest) {
      throw new Error("completed capsule import has no committed lineage head");
    }
    return result;
  }
  // The capsule is at most 64 KiB. Four capsule lengths conservatively cover
  // both imported JSONL entries, the completion receipt, lineage head, and
  // their temporary atomic-write copies before any child-session effect.
  assertTaskSessionByteCapacity(sessionDir, MAX_CAPSULE_BYTES * 4);
  const lock = path.join(sessionDir, ".capsule-lineage.lock");
  try {
    acquireLineageLock(sessionDir, envelope.digest);
  } catch (error) {
    if (error.code === "EEXIST")
      throw new Error("capsule import is concurrent or previously interrupted");
    throw error;
  }
  let effectStarted = false;
  try {
    assertCapsuleParent(sessionDir, capsule, envelope.digest);
    effectStarted = true;
    const childFile = path.join(
      sessionDir,
      `continuation-${crypto.randomUUID()}.jsonl`,
    );
    const childFd = fs.openSync(childFile, "wx", 0o600);
    fs.closeSync(childFd);
    const manager = SessionManager.open(childFile, sessionDir, cwd);
    const continuation = validatedContinuation(envelope.capsule);
    manager.appendCustomEntry("buzz.continuation.lineage.v2", {
      capsuleDigest: envelope.digest,
      sourceRuntime: sourceBinding.runtime,
      sourceSessionId: sourceBinding.sessionId,
      sourceCheckpointId: sourceBinding.checkpointId,
      parentDigest: continuation.parentDigest,
      lineage: continuation.lineage,
    });
    manager.appendCustomMessageEntry(
      "buzz.continuation.context.v2",
      renderContinuationContext(envelope.capsule, envelope.digest),
      false,
      { capsuleDigest: envelope.digest },
    );
    const result = {
      schemaVersion: 1,
      capsuleDigest: envelope.digest,
      sessionId: manager.getSessionId(),
      leafId: manager.getLeafId(),
      sessionFile: manager.getSessionFile(),
    };
    atomicJson(complete, result, sessionDir);
    recordCapsuleHead(sessionDir, capsule, envelope.digest, { lockHeld: true });
    fs.unlinkSync(lock);
    return validateImportResult(result, sessionDir, envelope.digest);
  } catch (error) {
    if (!effectStarted && fs.existsSync(lock)) fs.unlinkSync(lock);
    throw error;
  }
}

export function importCapsule(envelope, options) {
  const preflight = validatedImportPreflight(envelope, options);
  fs.mkdirSync(options.sessionDir, { recursive: true, mode: 0o700 });
  fs.chmodSync(options.sessionDir, 0o700);
  const release = acquireTaskLease(options.sessionDir, "capsule-import");
  try {
    return importCapsuleUnlocked(envelope, { ...options, ...preflight });
  } finally {
    release();
  }
}
