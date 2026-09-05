import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import {
  canonicalJson,
  capsuleDigest,
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

export const CAPSULE_SCHEMA_VERSION = 1;
export const MAX_CAPSULE_BYTES = 64 * 1024;
const MAX_TEXT = 8 * 1024;
const MAX_ITEM_TEXT = 2 * 1024;
const MAX_ITEMS = 64;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const HEX40_OR_64 = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const HEX64 = /^[0-9a-f]{64}$/;
const PI_ENTRY = /^[0-9a-f]{8}$/;
const LOCATIONS = new Set(["local", "cloud"]);
const FORBIDDEN_KEYS =
  /(?:secret|password|credential|api[_-]?key|private[_-]?key|authorization|cookie|environment|rawToolOutput|thinking|reasoning)/i;
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

function validateLineage(value) {
  if (!Array.isArray(value) || value.length === 0 || value.length > 32) {
    throw new Error("pi.lineage must contain 1..32 entries");
  }
  return value.map((entry, index) => {
    exactKeys(
      entry,
      ["sessionId", "leafId", "location"],
      `pi.lineage[${index}]`,
    );
    text(entry.sessionId, `pi.lineage[${index}].sessionId`, 128);
    if (!PI_ENTRY.test(entry.leafId))
      throw new Error(`pi.lineage[${index}].leafId is invalid`);
    if (!LOCATIONS.has(entry.location))
      throw new Error(`pi.lineage[${index}].location is invalid`);
    return entry;
  });
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
      "pi",
      "context",
    ],
    "capsule",
  );
  if (capsule.schemaVersion !== CAPSULE_SCHEMA_VERSION)
    throw new Error("unsupported capsule schemaVersion");
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

  exactKeys(
    capsule.pi,
    ["sourceSessionId", "sourceLeafId", "lineage", "parentCapsuleDigest"],
    "pi",
  );
  text(capsule.pi.sourceSessionId, "pi.sourceSessionId", 128);
  if (!PI_ENTRY.test(capsule.pi.sourceLeafId))
    throw new Error("pi.sourceLeafId is invalid");
  validateLineage(capsule.pi.lineage);
  if (
    capsule.pi.parentCapsuleDigest !== null &&
    !HEX64.test(capsule.pi.parentCapsuleDigest)
  ) {
    throw new Error("pi.parentCapsuleDigest is invalid");
  }
  const last = capsule.pi.lineage.at(-1);
  if (
    last.sessionId !== capsule.pi.sourceSessionId ||
    last.leafId !== capsule.pi.sourceLeafId ||
    last.location !== capsule.ownership.sourceLocation
  ) {
    throw new Error("pi lineage does not terminate at the source session leaf");
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
  if (
    draft?.pi?.sourceSessionId !== sessionId ||
    draft?.pi?.sourceLeafId !== leafId
  ) {
    throw new Error("draft lineage is stale for the active Pi leaf");
  }
  const branch = sessionManager.getBranch();
  const imported = branch
    .filter(
      (entry) =>
        entry.type === "custom" &&
        entry.customType === "buzz.continuation.lineage.v1",
    )
    .at(-1);
  const inheritedLineage = imported?.data?.lineage ?? [];
  if (inheritedLineage.length > 0) validateLineage(inheritedLineage);
  const derivedLineage = [
    ...inheritedLineage,
    {
      sessionId,
      leafId,
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
  if (
    canonicalJson(draft.pi.lineage) !== canonicalJson(derivedLineage) ||
    draft.pi.parentCapsuleDigest !== derivedParent
  ) {
    throw new Error(
      "draft lineage does not match the persisted Pi lineage head",
    );
  }
  const capsule = structuredClone(draft);
  // Raw transcript text is never portable: deterministic secret detection
  // cannot prove arbitrary conversation text credential-free. Continuation
  // uses only the explicit, bounded summary fields validated below.
  capsule.context.recentTail = [];
  capsule.pi.sourceSessionId = sessionId;
  capsule.pi.sourceLeafId = leafId;
  capsule.pi.lineage = derivedLineage;
  capsule.pi.parentCapsuleDigest = derivedParent;
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
  if ((head?.digest ?? null) !== capsule.pi.parentCapsuleDigest) {
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
    if ((head?.digest ?? null) !== capsule.pi.parentCapsuleDigest) {
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
    const exportIntent = (capsule) => ({
      ...capsule,
      context: { ...capsule.context, recentTail: [] },
    });
    if (
      canonicalJson(exportIntent(draft)) !==
        canonicalJson(exportIntent(envelope.capsule)) ||
      envelope.capsule.pi.sourceSessionId !== sessionManager.getSessionId() ||
      envelope.capsule.pi.sourceLeafId !== sessionManager.getLeafId() ||
      draft.pi.sourceSessionId !== envelope.capsule.pi.sourceSessionId ||
      draft.pi.sourceLeafId !== envelope.capsule.pi.sourceLeafId
    ) {
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
    if (
      sessionManager.getSessionId() !== previous.pi.sourceSessionId ||
      sessionManager.getLeafId() !== previous.pi.sourceLeafId
    ) {
      throw new Error("capsule reissue source Pi leaf has advanced");
    }
    if (readCapsuleHead(sessionDir)?.digest !== expiredEnvelope.digest) {
      throw new Error("expired capsule is not the current task lineage head");
    }
    const capsule = structuredClone(previous);
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
  if ((head?.digest ?? null) === capsule.pi.parentCapsuleDigest) {
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
  if (canonicalJson(expectedBinding) !== canonicalJson(capsuleBinding)) {
    throw new Error(
      "capsule ownership generation, location, or task binding is stale",
    );
  }
  return { ...capsule.task, agentPubkey: targetAgentPubkey };
}

function importCapsuleUnlocked(
  envelope,
  { cwd, sessionDir, expected, now = Date.now() },
) {
  validateEnvelope(envelope, { now, allowExpired: true });
  const capsule = envelope.capsule;
  const importTask = validatedImportTask(expected, capsule, envelope.digest);
  if (!path.isAbsolute(cwd) || !path.isAbsolute(sessionDir))
    throw new Error("import paths must be absolute");
  assertBoundTaskSessionDirectory(sessionDir, importTask);
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
    if (head !== null && head.digest !== capsule.pi.parentCapsuleDigest) {
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
  validateEnvelope(envelope, { now });
  verifyGitBinding(capsule.git, cwd);
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
    manager.appendCustomEntry("buzz.continuation.lineage.v1", {
      capsuleDigest: envelope.digest,
      parentSessionId: envelope.capsule.pi.sourceSessionId,
      parentLeafId: envelope.capsule.pi.sourceLeafId,
      parentCapsuleDigest: envelope.capsule.pi.parentCapsuleDigest,
      lineage: envelope.capsule.pi.lineage,
    });
    manager.appendCustomMessageEntry(
      "buzz.continuation.context.v1",
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
  fs.mkdirSync(options.sessionDir, { recursive: true, mode: 0o700 });
  fs.chmodSync(options.sessionDir, 0o700);
  const release = acquireTaskLease(options.sessionDir, "capsule-import");
  try {
    return importCapsuleUnlocked(envelope, options);
  } finally {
    release();
  }
}
