import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { getAgentDir, SessionManager } from "@earendil-works/pi-coding-agent";

const HEX_EVENT = /^[0-9a-f]{64}$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const SIDE_EFFECTING_TOOLS = new Set(["buzz_reply", "bash", "edit", "write"]);
const READ_ONLY_TOOLS = new Set(["read", "grep", "find", "ls", "kanban_tasks"]);

export function toolResultLeavesAmbiguousEffect(message) {
  return (
    message?.role === "toolResult" &&
    message.isError === true &&
    SIDE_EFFECTING_TOOLS.has(message.toolName)
  );
}

function requiredLower(value, pattern, label) {
  if (typeof value !== "string" || !pattern.test(value.toLowerCase())) {
    throw new Error(`${label} is invalid`);
  }
  return value.toLowerCase();
}

export function canonicalRelayUrl(value) {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error("relayUrl is required");
  }
  const relay = new URL(value);
  if (
    !["wss:", "https:", "ws:", "http:"].includes(relay.protocol) ||
    relay.username ||
    relay.password
  ) {
    throw new Error("relayUrl is invalid");
  }
  if (relay.protocol === "https:") relay.protocol = "wss:";
  if (relay.protocol === "http:") relay.protocol = "ws:";
  relay.pathname = "/";
  relay.search = "";
  relay.hash = "";
  return relay.toString().replace(/\/$/, "");
}

export function taskSessionIdentity(buzzContext, relayUrl) {
  if (!buzzContext || typeof buzzContext !== "object") {
    throw new Error("authenticated Buzz task context is required");
  }
  const agentPubkey = requiredLower(
    buzzContext.agentPubkey,
    HEX_EVENT,
    "agentPubkey",
  );
  const channelId = requiredLower(buzzContext.channelId, UUID, "channelId");
  const threadRoot = requiredLower(
    buzzContext.taskThreadRoot,
    HEX_EVENT,
    "threadRoot",
  );
  const identity = {
    relayUrl: canonicalRelayUrl(relayUrl),
    agentPubkey,
    channelId,
    threadRoot,
  };
  const digest = crypto
    .createHash("sha256")
    .update(JSON.stringify(identity))
    .digest("hex");
  return { ...identity, digest };
}

export function acquireTaskLease(sessionDir, purpose) {
  const lease = path.join(sessionDir, ".task-execution.lock");
  const token = `${process.pid} ${crypto.randomUUID()} ${purpose}`;
  const tryCreate = () => {
    const temporary = `${lease}.${crypto.randomUUID()}.tmp`;
    fs.writeFileSync(temporary, `${token}\n`, { mode: 0o600, flag: "wx" });
    try {
      fs.linkSync(temporary, lease);
    } finally {
      fs.unlinkSync(temporary);
    }
  };
  try {
    tryCreate();
  } catch (error) {
    if (error.code !== "EEXIST") throw error;
    const staleToken = fs.readFileSync(lease, "utf8");
    const owner = Number.parseInt(staleToken, 10);
    let alive = true;
    try {
      process.kill(owner, 0);
    } catch (killError) {
      alive = killError.code !== "ESRCH";
    }
    if (alive) throw new Error("task execution or capsule export is active");
    const recoveryLock = `${lease}.recovery`;
    const recoveryToken = `${process.pid} ${crypto.randomUUID()}\n`;
    const createRecoveryLock = () =>
      fs.writeFileSync(recoveryLock, recoveryToken, {
        mode: 0o600,
        flag: "wx",
      });
    try {
      createRecoveryLock();
    } catch (recoveryError) {
      if (recoveryError.code !== "EEXIST") throw recoveryError;
      const abandonedToken = fs.readFileSync(recoveryLock, "utf8");
      const recoveryOwner = Number.parseInt(abandonedToken, 10);
      let recoveryAlive = true;
      try {
        process.kill(recoveryOwner, 0);
      } catch (killError) {
        recoveryAlive = killError.code !== "ESRCH";
      }
      if (recoveryAlive) {
        throw new Error("task lease recovery is already active");
      }
      if (fs.readFileSync(recoveryLock, "utf8") !== abandonedToken) {
        throw new Error("task lease recovery lock changed during recovery");
      }
      fs.unlinkSync(recoveryLock);
      try {
        createRecoveryLock();
      } catch {
        throw new Error("task lease recovery is already active");
      }
    }
    try {
      if (fs.readFileSync(lease, "utf8") !== staleToken) {
        throw new Error("task lease changed during stale recovery");
      }
      fs.unlinkSync(lease);
      tryCreate();
    } finally {
      if (
        fs.existsSync(recoveryLock) &&
        fs.readFileSync(recoveryLock, "utf8") === recoveryToken
      ) {
        fs.unlinkSync(recoveryLock);
      }
    }
  }
  try {
    const digest = path.basename(sessionDir);
    const prefix = path.basename(path.dirname(sessionDir));
    if (/^[0-9a-f]{64}$/.test(digest) && prefix === digest.slice(0, 2)) {
      pruneTaskSessionDirectories(
        path.dirname(path.dirname(sessionDir)),
        sessionDir,
      );
    }
    assertTaskSessionByteBudget(sessionDir);
  } catch (error) {
    if (
      fs.existsSync(lease) &&
      fs.readFileSync(lease, "utf8").trim() === token
    ) {
      fs.unlinkSync(lease);
    }
    throw error;
  }
  return () => {
    if (!fs.existsSync(lease)) return;
    if (fs.readFileSync(lease, "utf8").trim() !== token) {
      throw new Error("task lease ownership changed unexpectedly");
    }
    fs.unlinkSync(lease);
  };
}

export const MAX_TASK_SESSION_BYTES = 64 * 1024 * 1024;

function directoryBytes(directory) {
  let total = 0;
  for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
    const entryPath = path.join(directory, entry.name);
    if (entry.isSymbolicLink()) {
      throw new Error("task session directory must not contain symbolic links");
    }
    if (entry.isDirectory()) total += directoryBytes(entryPath);
    else if (entry.isFile()) total += fs.statSync(entryPath).size;
  }
  return total;
}

export function assertTaskSessionByteBudget(
  directory,
  maxBytes = MAX_TASK_SESSION_BYTES,
) {
  const bytes = directoryBytes(directory);
  if (bytes > maxBytes) {
    throw new Error(`task session exceeds ${maxBytes} byte budget`);
  }
  return bytes;
}

export function assertTaskSessionByteCapacity(
  directory,
  additionalBytes,
  maxBytes = MAX_TASK_SESSION_BYTES,
) {
  if (!Number.isSafeInteger(additionalBytes) || additionalBytes < 0) {
    throw new Error("task session byte reservation is invalid");
  }
  const used = assertTaskSessionByteBudget(directory, maxBytes);
  if (used + additionalBytes > maxBytes) {
    throw new Error(`task session exceeds ${maxBytes} byte budget`);
  }
  return used;
}

export function pruneTaskSessionDirectories(
  root,
  current,
  maxDirectories = 512,
) {
  const directories = [];
  for (const prefix of fs.readdirSync(root, { withFileTypes: true })) {
    if (!prefix.isDirectory() || !/^[0-9a-f]{2}$/.test(prefix.name)) continue;
    const prefixPath = path.join(root, prefix.name);
    for (const task of fs.readdirSync(prefixPath, { withFileTypes: true })) {
      if (!task.isDirectory() || !/^[0-9a-f]{64}$/.test(task.name)) continue;
      const directory = path.join(prefixPath, task.name);
      directories.push({
        directory,
        modified: fs.statSync(directory).mtimeMs,
        protected:
          directory === current ||
          fs.existsSync(path.join(directory, ".task-execution.lock")) ||
          fs
            .readdirSync(directory)
            .some((name) => name.startsWith(".capsule-")),
      });
    }
  }
  directories.sort((left, right) => left.modified - right.modified);
  let excess = Math.max(0, directories.length - maxDirectories);
  for (const candidate of directories) {
    if (excess === 0) break;
    if (candidate.protected) continue;
    const deletionLease = path.join(
      candidate.directory,
      ".task-execution.lock",
    );
    try {
      fs.writeFileSync(
        deletionLease,
        `${process.pid} ${crypto.randomUUID()} task-prune\n`,
        {
          mode: 0o600,
          flag: "wx",
        },
      );
    } catch (error) {
      if (error.code === "EEXIST" || error.code === "ENOENT") continue;
      throw error;
    }
    if (
      fs
        .readdirSync(candidate.directory)
        .some((name) => name.startsWith(".capsule-"))
    ) {
      fs.unlinkSync(deletionLease);
      continue;
    }
    fs.rmSync(candidate.directory, { recursive: true, force: true });
    excess -= 1;
  }
  if (excess > 0) {
    throw new Error(
      "task session quota is exhausted by active or capsule-bound tasks",
    );
  }
}

export function taskSessionDirectory(root, identity) {
  if (typeof root !== "string" || !path.isAbsolute(root)) {
    throw new Error("task session root must be an absolute path");
  }
  if (!identity || !/^[0-9a-f]{64}$/.test(identity.digest ?? "")) {
    throw new Error("task session identity digest is invalid");
  }
  const directory = path.join(
    root,
    identity.digest.slice(0, 2),
    identity.digest,
  );
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  fs.chmodSync(directory, 0o700);
  return directory;
}

function assertCommittedTaskLineage(manager, sessionDir) {
  if (fs.existsSync(path.join(sessionDir, ".capsule-lineage.lock"))) {
    throw new Error("task capsule lineage is concurrent or interrupted");
  }
  const lineage = manager
    .getBranch()
    .filter(
      (entry) =>
        entry.type === "custom" &&
        entry.customType === "buzz.continuation.lineage.v1",
    )
    .at(-1);
  const headFile = path.join(sessionDir, ".capsule-lineage-head.json");
  const head = fs.existsSync(headFile)
    ? JSON.parse(fs.readFileSync(headFile, "utf8"))
    : null;
  if ((lineage?.data?.capsuleDigest ?? null) !== (head?.digest ?? null)) {
    throw new Error(
      "task Pi session does not match the committed capsule lineage head",
    );
  }
}

function reopenSafeTaskSession(manager, cwd, sessionDir) {
  assertCommittedTaskLineage(manager, sessionDir);
  const branch = manager.getBranch();
  if (branch.length === 0) return manager;
  const outstanding = new Map();
  let settledEntry = null;
  let toolActivityAfterSettled = false;
  for (const entry of branch) {
    if (entry.type === "custom_message") {
      if (entry.customType === "buzz.continuation.context.v1") {
        settledEntry = entry;
        toolActivityAfterSettled = false;
      }
      continue;
    }
    if (
      entry.type === "custom" &&
      ["buzz.continuation.lineage.v1", "buzz.delivery.v1"].includes(
        entry.customType,
      )
    ) {
      settledEntry = entry;
      toolActivityAfterSettled = false;
      continue;
    }
    if (entry.type !== "message") continue;
    const message = entry.message;
    if (message?.role === "assistant") {
      for (const block of message.content ?? []) {
        if (block?.type === "toolCall" && typeof block.id === "string") {
          outstanding.set(block.id, block.name);
          if (SIDE_EFFECTING_TOOLS.has(block.name)) {
            toolActivityAfterSettled = true;
          }
        }
      }
      if (
        outstanding.size === 0 &&
        !["pending", "toolUse", "aborted"].includes(message.stopReason)
      ) {
        settledEntry = entry;
        toolActivityAfterSettled = false;
      }
    } else if (message?.role === "toolResult") {
      if (!toolResultLeavesAmbiguousEffect(message)) {
        outstanding.delete(message.toolCallId);
      }
      if (
        outstanding.size === 0 &&
        message.toolName === "buzz_reply" &&
        message.isError === false
      ) {
        settledEntry = entry;
        toolActivityAfterSettled = false;
      }
    }
  }
  const recoverableOutstanding =
    outstanding.size > 0 &&
    [...outstanding.values()].every(
      (toolName) => toolName === "buzz_reply" || READ_ONLY_TOOLS.has(toolName),
    );
  if (
    (outstanding.size > 0 && !recoverableOutstanding) ||
    (toolActivityAfterSettled && !recoverableOutstanding)
  ) {
    throw new Error("persistent task session has unresolved tool effects");
  }
  if (manager.getLeafId() === settledEntry?.id) return manager;
  if (!settledEntry) return SessionManager.create(cwd, sessionDir);
  const recoveredFile = manager.createBranchedSession(settledEntry.id);
  if (!recoveredFile)
    throw new Error("failed to recover persistent task session");
  return SessionManager.open(recoveredFile, sessionDir, cwd);
}

export function resetTaskSession(cwd, root, identity) {
  if (typeof cwd !== "string" || !path.isAbsolute(cwd)) {
    throw new Error("task session cwd must be an absolute path");
  }
  const sessionDir = taskSessionDirectory(root, identity);
  const current = reopenSafeTaskSession(
    SessionManager.continueRecent(cwd, sessionDir),
    cwd,
    sessionDir,
  );
  const branch = current.getBranch();
  const lineage = branch
    .filter(
      (entry) =>
        entry.type === "custom" &&
        entry.customType === "buzz.continuation.lineage.v1",
    )
    .at(-1);
  const headFile = path.join(sessionDir, ".capsule-lineage-head.json");
  if (fs.existsSync(headFile)) {
    const head = JSON.parse(fs.readFileSync(headFile, "utf8"));
    if (!lineage || lineage.data?.capsuleDigest !== head.digest) {
      throw new Error(
        "cannot reset a task outside its imported capsule lineage head",
      );
    }
  }
  const sessionFile = path.join(
    sessionDir,
    `rotation-${crypto.randomUUID()}.jsonl`,
  );
  const descriptor = fs.openSync(sessionFile, "wx", 0o600);
  fs.closeSync(descriptor);
  const reset = SessionManager.open(sessionFile, sessionDir, cwd);
  if (lineage) {
    reset.appendCustomEntry(lineage.customType, structuredClone(lineage.data));
  }
  const processedTriggerIds = [
    ...new Set(
      branch
        .filter(
          (entry) =>
            entry.type === "custom" && entry.customType === "buzz.delivery.v1",
        )
        .flatMap((entry) => entry.data?.triggerEventIds ?? [])
        .filter((eventId) => /^[0-9a-f]{64}$/.test(eventId)),
    ),
  ].slice(-256);
  if (processedTriggerIds.length > 0) {
    reset.appendCustomEntry("buzz.delivery.v1", {
      eventIds: [],
      triggerEventIds: processedTriggerIds,
    });
  }
  return reset;
}

function executeTaskControlUnlocked(type, cwd, buzzContext, env = process.env) {
  const identity = taskSessionIdentity(buzzContext, buzzContext?.relayUrl);
  const root =
    env.PI_ACP_TASK_SESSION_ROOT ||
    path.join(getAgentDir(), "buzz-task-sessions");
  if (type === "reset") {
    const reset = resetTaskSession(cwd, root, identity);
    return {
      outcome: "reset",
      taskDigest: identity.digest,
      sessionId: reset.getSessionId(),
    };
  }
  if (type !== "state") throw new Error("unknown task control operation");
  const manager = openTaskSession(cwd, root, identity);
  const deliveries = manager
    .getBranch()
    .filter(
      (entry) =>
        entry.type === "custom" && entry.customType === "buzz.delivery.v1",
    );
  const boundedIds = (field) =>
    [
      ...new Set(
        deliveries
          .flatMap((entry) => entry.data?.[field] ?? [])
          .filter((eventId) => /^[0-9a-f]{64}$/.test(eventId)),
      ),
    ].slice(-256);
  return {
    deliveredEventIds: boundedIds("eventIds"),
    processedTriggerEventIds: boundedIds("triggerEventIds"),
  };
}

export function executeTaskControl(type, cwd, buzzContext, env = process.env) {
  const identity = taskSessionIdentity(buzzContext, buzzContext?.relayUrl);
  const root =
    env.PI_ACP_TASK_SESSION_ROOT ||
    path.join(getAgentDir(), "buzz-task-sessions");
  const release = acquireTaskLease(
    taskSessionDirectory(root, identity),
    `task-${type}`,
  );
  try {
    return executeTaskControlUnlocked(type, cwd, buzzContext, env);
  } finally {
    release();
  }
}

export function openTaskSession(cwd, root, identity) {
  if (typeof cwd !== "string" || !path.isAbsolute(cwd)) {
    throw new Error("task session cwd must be an absolute path");
  }
  const sessionDir = taskSessionDirectory(root, identity);
  return reopenSafeTaskSession(
    SessionManager.continueRecent(cwd, sessionDir),
    cwd,
    sessionDir,
  );
}
