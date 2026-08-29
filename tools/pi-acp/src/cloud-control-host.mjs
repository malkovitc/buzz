#!/usr/bin/env node

import crypto from "node:crypto";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const HEX64 = /^[0-9a-f]{64}$/;
const COMMANDS = new Set(["-status", "-cloud", "-local"]);
const LOCATIONS = new Set(["local", "cloud"]);
const REQUEST_KEYS = {
  prepare: [
    "channelId",
    "command",
    "phase",
    "replyTo",
    "schemaVersion",
    "triggeringEventIds",
  ],
  commit: [
    "authorization",
    "channelId",
    "command",
    "operationId",
    "phase",
    "receiptContentSha256",
    "receiptEventId",
    "replyTo",
    "schemaVersion",
    "triggeringEventIds",
  ],
};
const CONFIG_KEYS = [
  "actionCloudCommand",
  "actionLocalCommand",
  "agentName",
  "agentPubkey",
  "approvedChannelId",
  "authorizationKeyCommand",
  "buzzReadCommand",
  "location",
  "manifestCommand",
  "ownerPubkey",
  "relayUrl",
  "schemaVersion",
  "spoolDirectory",
];

function fail(message) {
  throw new Error(message);
}

function exactKeys(value, expected) {
  return (
    value &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    JSON.stringify(Object.keys(value).sort()) === JSON.stringify(expected)
  );
}

function readJsonFile(file, maxBytes = 64 * 1024) {
  const stat = fs.lstatSync(file);
  if (
    !stat.isFile() ||
    stat.isSymbolicLink() ||
    stat.size > maxBytes ||
    (stat.mode & 0o077) !== 0 ||
    (typeof process.getuid === "function" && stat.uid !== process.getuid())
  ) {
    fail(`unsafe JSON file: ${file}`);
  }
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

function assertCommandVector(value, name, optional = false) {
  if (optional && value === null) return;
  if (
    !Array.isArray(value) ||
    value.length === 0 ||
    value.length > 16 ||
    !path.isAbsolute(value[0]) ||
    !value.every(
      (part) =>
        typeof part === "string" &&
        part.length > 0 &&
        part.length <= 1024 &&
        !part.includes("\0"),
    )
  ) {
    fail(`${name} must be a bounded absolute command vector`);
  }
}

function canonicalRelayUrl(value) {
  const relay = new URL(value);
  relay.hash = "";
  relay.search = "";
  relay.pathname = "/";
  return relay.toString().replace(/\/$/, "");
}

function loadConfig(env = process.env) {
  const file =
    env.PI_CLOUD_CONTROL_CONFIG ||
    path.join(os.homedir(), ".buzz", "cloud-control", "config.json");
  if (!path.isAbsolute(file)) fail("control config path must be absolute");
  const config = readJsonFile(file);
  if (!exactKeys(config, CONFIG_KEYS))
    fail("control config fields are invalid");
  if (
    config.schemaVersion !== 1 ||
    !LOCATIONS.has(config.location) ||
    !UUID.test(config.approvedChannelId || "") ||
    !HEX64.test(config.ownerPubkey || "") ||
    !HEX64.test(config.agentPubkey || "") ||
    typeof config.relayUrl !== "string" ||
    !/^wss:\/\/[A-Za-z0-9.-]+(?::[0-9]+)?\/?$/.test(config.relayUrl) ||
    typeof config.agentName !== "string" ||
    config.agentName.length < 1 ||
    config.agentName.length > 128 ||
    !path.isAbsolute(config.spoolDirectory)
  ) {
    fail("control config values are invalid");
  }
  config.relayUrl = canonicalRelayUrl(config.relayUrl);
  assertCommandVector(config.manifestCommand, "manifestCommand");
  assertCommandVector(config.buzzReadCommand, "buzzReadCommand");
  assertCommandVector(
    config.authorizationKeyCommand,
    "authorizationKeyCommand",
  );
  assertCommandVector(config.actionCloudCommand, "actionCloudCommand", true);
  assertCommandVector(config.actionLocalCommand, "actionLocalCommand", true);
  return config;
}

async function runVector(
  vector,
  { input, env = {}, allowFailure = false, timeoutMs = 300_000 } = {},
) {
  return await new Promise((resolve, reject) => {
    const child = spawn(vector[0], vector.slice(1), {
      detached: process.platform !== "win32",
      env: {
        HOME: os.homedir(),
        PATH: "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
        LANG: "C",
        ...env,
      },
      stdio: [input === undefined ? "ignore" : "pipe", "pipe", "pipe"],
    });
    let settled = false;
    let timer;
    let stdout = Buffer.alloc(0);
    let stderr = Buffer.alloc(0);
    const terminate = () => {
      try {
        if (process.platform === "win32") child.kill("SIGKILL");
        else if (child.pid) process.kill(-child.pid, "SIGKILL");
      } catch {
        // The fixed command process group already exited.
      }
    };
    const finish = (error, result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      process.removeListener("SIGINT", abort);
      process.removeListener("SIGTERM", abort);
      terminate();
      if (error) reject(error);
      else resolve(result);
    };
    const append = (current, chunk) => {
      const next = Buffer.concat([current, chunk]);
      if (next.length > 1024 * 1024)
        finish(new Error("fixed control command output is too large"));
      return next;
    };
    child.stdout.on("data", (chunk) => (stdout = append(stdout, chunk)));
    child.stderr.on("data", (chunk) => (stderr = append(stderr, chunk)));
    const abort = () => finish(new Error("fixed control command interrupted"));
    process.once("SIGINT", abort);
    process.once("SIGTERM", abort);
    child.once("error", () =>
      finish(new Error("fixed control command could not start")),
    );
    child.once("close", (code) => {
      const result = {
        status: code,
        stdout: stdout.toString("utf8"),
        stderr: stderr.toString("utf8"),
      };
      if (!allowFailure && code !== 0)
        finish(new Error("fixed control command failed"));
      else finish(undefined, result);
    });
    timer = setTimeout(
      () => finish(new Error("fixed control command timed out")),
      timeoutMs,
    );
    if (input !== undefined) child.stdin.end(input);
  });
}

async function readManifest(config) {
  const result = await runVector(config.manifestCommand);
  const manifest = JSON.parse(result.stdout);
  if (
    !manifest ||
    !["prepared", "cloud-owned", "local-owned", "blocked"].includes(
      manifest.state,
    ) ||
    !["cloud", "local", "none"].includes(manifest.owner) ||
    !UUID.test(manifest.generation || "") ||
    typeof manifest.branch !== "string" ||
    !/^[0-9a-f]{40}$/.test(manifest.commit || "")
  ) {
    fail("authoritative handoff manifest is invalid");
  }
  return manifest;
}

function validateRequest(request, config) {
  if (!request || !["prepare", "commit"].includes(request.phase))
    fail("unsupported control phase");
  if (!exactKeys(request, REQUEST_KEYS[request.phase]))
    fail("control request fields are invalid");
  if (
    request.schemaVersion !== 1 ||
    !COMMANDS.has(request.command) ||
    request.channelId !== config.approvedChannelId ||
    !HEX64.test(request.replyTo || "") ||
    !Array.isArray(request.triggeringEventIds) ||
    request.triggeringEventIds.length !== 1 ||
    !HEX64.test(request.triggeringEventIds[0] || "")
  ) {
    fail("control request binding is invalid");
  }
  if (
    request.phase === "commit" &&
    (!UUID.test(request.operationId || "") ||
      !HEX64.test(request.receiptContentSha256 || "") ||
      !HEX64.test(request.receiptEventId || "") ||
      !HEX64.test(request.authorization || ""))
  ) {
    fail("control commit binding is invalid");
  }
  return request;
}

function atomicJson(file, value) {
  const directory = path.dirname(file);
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  fs.chmodSync(directory, 0o700);
  const temporary = path.join(
    directory,
    `.${path.basename(file)}.${process.pid}.${crypto.randomUUID()}.tmp`,
  );
  const descriptor = fs.openSync(temporary, "wx", 0o600);
  try {
    fs.writeFileSync(descriptor, `${JSON.stringify(value)}\n`);
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  fs.renameSync(temporary, file);
  const dir = fs.openSync(directory, "r");
  try {
    fs.fsyncSync(dir);
  } finally {
    fs.closeSync(dir);
  }
}

function response(status, content, operationId) {
  return { status, content, ...(operationId ? { operationId } : {}) };
}

function preparedContent(request, manifest, operationId, relayUrl) {
  return [
    "[PI CLOUD CONTROL]",
    "schema=1",
    `status=${request.command === "-status" ? "STATUS" : "PREPARED"}`,
    `command=${request.command}`,
    `operation=${operationId}`,
    `command_event=${request.triggeringEventIds[0]}`,
    `relay=${relayUrl}`,
    `owner=${manifest.owner}`,
    `state=${manifest.state}`,
    `generation=${manifest.generation}`,
    `branch=${manifest.branch}`,
    `commit=${manifest.commit}`,
  ].join("\n");
}

function operationIdFor(request, generation) {
  const bytes = crypto
    .createHash("sha256")
    .update(
      `${request.channelId}\0${request.triggeringEventIds[0]}\0${request.command}\0${generation}`,
    )
    .digest()
    .subarray(0, 16);
  bytes[6] = (bytes[6] & 0x0f) | 0x50;
  bytes[8] = (bytes[8] & 0x3f) | 0x80;
  const hex = bytes.toString("hex");
  return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

async function prepare(request, config) {
  const manifest = await readManifest(config);
  const operationId = operationIdFor(request, manifest.generation);
  const content = preparedContent(
    request,
    manifest,
    operationId,
    config.relayUrl,
  );
  if (request.command === "-status") return response("noop", content);
  if (request.command === "-cloud" && manifest.state === "cloud-owned") {
    return response(
      "noop",
      content.replace("status=PREPARED", "status=CLOUD_ACTIVE"),
    );
  }
  if (request.command === "-local" && manifest.state === "local-owned") {
    return response(
      "noop",
      content.replace("status=PREPARED", "status=LOCAL_ACTIVE"),
    );
  }
  const expected = request.command === "-cloud" ? "local-owned" : "cloud-owned";
  if (manifest.state !== expected) {
    return response(
      "blocked",
      content.replace(
        "status=PREPARED",
        `status=BLOCKED_${manifest.state.toUpperCase()}`,
      ),
    );
  }
  const pending = {
    schemaVersion: 1,
    operationId,
    command: request.command,
    channelId: request.channelId,
    replyTo: request.replyTo,
    triggeringEventId: request.triggeringEventIds[0],
    relayUrl: config.relayUrl,
    generation: manifest.generation,
    baseContentSha256: crypto
      .createHash("sha256")
      .update(content)
      .digest("hex"),
    createdAt: new Date().toISOString(),
  };
  atomicJson(
    path.join(config.spoolDirectory, "pending", `${operationId}.json`),
    pending,
  );
  return response("ok", content, operationId);
}

async function commit(request, config) {
  const pendingFile = path.join(
    config.spoolDirectory,
    "pending",
    `${request.operationId}.json`,
  );
  let pending;
  try {
    pending = readJsonFile(pendingFile);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    const processedFile = path.join(
      config.spoolDirectory,
      "processed",
      `${request.operationId}.json`,
    );
    const processed = readJsonFile(processedFile);
    if (
      processed.operationId !== request.operationId ||
      processed.command !== request.command ||
      processed.channelId !== request.channelId ||
      processed.replyTo !== request.replyTo ||
      processed.triggeringEventId !== request.triggeringEventIds[0] ||
      processed.receiptEventId !== request.receiptEventId ||
      processed.authorization !== request.authorization ||
      processed.expectedContentSha256 !== request.receiptContentSha256
    ) {
      fail("processed control operation binding is invalid");
    }
    return response("noop", "COMMIT_ALREADY_PROCESSED");
  }
  if (
    pending.schemaVersion !== 1 ||
    pending.operationId !== request.operationId ||
    pending.command !== request.command ||
    pending.channelId !== request.channelId ||
    pending.replyTo !== request.replyTo ||
    pending.triggeringEventId !== request.triggeringEventIds[0] ||
    pending.relayUrl !== config.relayUrl ||
    !UUID.test(pending.generation || "") ||
    !HEX64.test(pending.baseContentSha256 || "")
  ) {
    fail("pending control operation binding is invalid");
  }
  const ready = {
    ...pending,
    receiptEventId: request.receiptEventId,
    authorization: request.authorization,
    expectedContentSha256: request.receiptContentSha256,
    committedAt: new Date().toISOString(),
  };
  atomicJson(
    path.join(config.spoolDirectory, "ready", `${request.operationId}.json`),
    ready,
  );
  try {
    fs.unlinkSync(pendingFile);
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
  }
  return response("noop", "COMMIT_QUEUED");
}

function tagValue(event, name, value) {
  return event.tags?.some(
    (tag) => Array.isArray(tag) && tag[0] === name && tag[1] === value,
  );
}

function validateAuthorization(request, key) {
  if (typeof key !== "string" || key.length === 0 || key.length > 256) {
    fail("control authorization key is unavailable");
  }
  const expected = crypto
    .createHmac("sha256", key)
    .update(
      JSON.stringify({
        schemaVersion: 1,
        relayUrl: request.relayUrl,
        command: request.command,
        channelId: request.channelId,
        replyTo: request.replyTo,
        triggeringEventIds: [request.triggeringEventId],
        operationId: request.operationId,
        baseContentSha256: request.baseContentSha256,
      }),
    )
    .digest("hex");
  const actual = Buffer.from(request.authorization || "", "hex");
  const wanted = Buffer.from(expected, "hex");
  if (
    actual.length !== wanted.length ||
    !crypto.timingSafeEqual(actual, wanted)
  ) {
    fail("control authorization capability is invalid");
  }
}

function validateEvents(events, request, config) {
  // Buzz's normalized read API intentionally omits Nostr signatures. The
  // authorization HMAC is the action authority: pi-acp creates it only after
  // buzz-acp admitted the signed owner event and durable publication returned.
  // These rows provide bounded relay visibility and routing correlation; a
  // forged reader cannot create a new valid capability.
  if (!Array.isArray(events)) fail("Buzz event reader returned invalid data");
  const owner = events.find((event) => event.id === request.triggeringEventId);
  const receipt = events.find((event) => event.id === request.receiptEventId);
  if (!owner || !receipt) fail("control events are not yet visible");
  if (
    owner?.kind !== 9 ||
    owner.pubkey !== config.ownerPubkey ||
    !tagValue(owner, "h", config.approvedChannelId) ||
    !tagValue(owner, "p", config.agentPubkey)
  ) {
    fail("signed owner command event validation failed");
  }
  if (
    receipt?.kind !== 9 ||
    receipt.pubkey !== config.agentPubkey ||
    !tagValue(receipt, "h", config.approvedChannelId) ||
    !tagValue(receipt, "e", request.replyTo) ||
    crypto
      .createHash("sha256")
      .update(receipt.content || "")
      .digest("hex") !== request.expectedContentSha256
  ) {
    fail("signed durable control receipt validation failed");
  }
}

async function readRecentEvents(config, env) {
  const since = String(Math.floor(Date.now() / 1000) - 900);
  const vector = config.buzzReadCommand.map((part) =>
    part.replaceAll("{since}", since),
  );
  const result = await runVector(vector, { env });
  return JSON.parse(result.stdout);
}

function parsePreparedReceipt(event, config) {
  if (
    event.kind !== 9 ||
    event.pubkey !== config.agentPubkey ||
    !tagValue(event, "h", config.approvedChannelId)
  ) {
    return null;
  }
  const lines = String(event.content || "").split("\n");
  if (lines[0] !== "[PI CLOUD CONTROL]") return null;
  const fields = Object.fromEntries(
    lines.slice(1).map((line) => {
      const offset = line.indexOf("=");
      return offset > 0
        ? [line.slice(0, offset), line.slice(offset + 1)]
        : ["", ""];
    }),
  );
  if (
    lines.length !== 13 ||
    fields.schema !== "1" ||
    fields.status !== "PREPARED" ||
    !["-cloud", "-local"].includes(fields.command) ||
    !UUID.test(fields.operation || "") ||
    !HEX64.test(fields.command_event || "") ||
    fields.relay !== config.relayUrl ||
    !UUID.test(fields.generation || "") ||
    !/^[0-9a-f]{40}$/.test(fields.commit || "") ||
    !HEX64.test(fields.authorization || "")
  ) {
    return null;
  }
  const replyTag = event.tags.find(
    (tag) => Array.isArray(tag) && tag[0] === "e" && HEX64.test(tag[1] || ""),
  );
  if (!replyTag) return null;
  return {
    schemaVersion: 1,
    operationId: fields.operation,
    command: fields.command,
    channelId: config.approvedChannelId,
    replyTo: replyTag[1],
    triggeringEventId: fields.command_event,
    relayUrl: fields.relay,
    generation: fields.generation,
    baseContentSha256: crypto
      .createHash("sha256")
      .update(lines.slice(0, -1).join("\n"))
      .digest("hex"),
    expectedContentSha256: crypto
      .createHash("sha256")
      .update(event.content)
      .digest("hex"),
    authorization: fields.authorization,
    receiptEventId: event.id,
    committedAt: new Date(Number(event.created_at) * 1000).toISOString(),
  };
}

function quarantineReady(config, file, reason) {
  const directory = path.join(config.spoolDirectory, "rejected");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stem = `${path.basename(file, ".json")}.${Date.now()}`;
  try {
    fs.renameSync(file, path.join(directory, `${stem}.json`));
  } catch (error) {
    if (error.code === "ENOENT") return;
    throw error;
  }
  atomicJson(path.join(directory, `${stem}.reason.json`), {
    reason,
    rejectedAt: new Date().toISOString(),
  });
}

function markProcessed(config, request) {
  const processed = path.join(
    config.spoolDirectory,
    "processed",
    `${request.operationId}.json`,
  );
  atomicJson(processed, { ...request, processedAt: new Date().toISOString() });
  for (const stage of ["pending", "ready", "claims"]) {
    try {
      fs.unlinkSync(
        path.join(config.spoolDirectory, stage, `${request.operationId}.json`),
      );
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
    }
  }
}

function isProcessed(config, operationId) {
  return fs.existsSync(
    path.join(config.spoolDirectory, "processed", `${operationId}.json`),
  );
}

function claimOperation(config, request) {
  const directory = path.join(config.spoolDirectory, "claims");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  const file = path.join(directory, `${request.operationId}.json`);
  try {
    const descriptor = fs.openSync(file, "wx", 0o600);
    try {
      fs.writeFileSync(
        descriptor,
        `${JSON.stringify({
          operationId: request.operationId,
          command: request.command,
          generation: request.generation,
          claimedAt: new Date().toISOString(),
        })}\n`,
      );
      fs.fsyncSync(descriptor);
    } finally {
      fs.closeSync(descriptor);
    }
    return true;
  } catch (error) {
    if (error.code !== "EEXIST") throw error;
    const claim = readJsonFile(file);
    const age = Date.now() - Date.parse(claim.claimedAt);
    if (!Number.isFinite(age) || age < 6 * 60 * 1000) return false;
    const stale = path.join(
      directory,
      `.${request.operationId}.${process.pid}.${crypto.randomUUID()}.stale`,
    );
    try {
      fs.renameSync(file, stale);
    } catch (takeoverError) {
      if (takeoverError.code === "ENOENT") return false;
      throw takeoverError;
    }
    try {
      return claimOperation(config, request);
    } finally {
      fs.rmSync(stale, { force: true });
    }
  }
}

async function executeClaimed(config, request, action, env) {
  if (!claimOperation(config, request)) return false;
  try {
    await runVector(action, { input: `${JSON.stringify(request)}\n`, env });
    markProcessed(config, request);
    return true;
  } catch (error) {
    try {
      fs.unlinkSync(
        path.join(
          config.spoolDirectory,
          "claims",
          `${request.operationId}.json`,
        ),
      );
    } catch {
      // A later idempotent reconciliation will inspect the durable owner state.
    }
    throw error;
  }
}

async function processSignedReceipts(config, events, authorizationKey, env) {
  if (config.location !== "local") return [];
  const outcomes = [];
  const chronological = [...events].sort(
    (left, right) =>
      Number(left.created_at) - Number(right.created_at) ||
      String(left.id).localeCompare(String(right.id)),
  );
  for (const event of chronological) {
    const request = parsePreparedReceipt(event, config);
    if (!request || isProcessed(config, request.operationId)) continue;
    if (Date.now() - Date.parse(request.committedAt) > 15 * 60 * 1000) continue;
    try {
      validateAuthorization(request, authorizationKey);
      validateEvents(events, request, config);
    } catch {
      continue;
    }
    const manifest = await readManifest(config);
    if (manifest.generation !== request.generation) continue;
    const expectedState =
      request.command === "-cloud" ? "local-owned" : "cloud-owned";
    const completedState =
      request.command === "-cloud" ? "cloud-owned" : "local-owned";
    if (manifest.state === completedState) {
      markProcessed(config, request);
      outcomes.push({
        operationId: request.operationId,
        command: request.command,
      });
      continue;
    }
    if (manifest.state !== expectedState) continue;
    const action =
      request.command === "-cloud"
        ? config.actionCloudCommand
        : config.actionLocalCommand;
    if (action === null) continue;
    if (await executeClaimed(config, request, action, env)) {
      outcomes.push({
        operationId: request.operationId,
        command: request.command,
      });
    }
  }
  return outcomes;
}

function expirePending(config) {
  const directory = path.join(config.spoolDirectory, "pending");
  fs.mkdirSync(directory, { recursive: true, mode: 0o700 });
  for (const name of fs
    .readdirSync(directory)
    .filter((entry) => UUID.test(entry.replace(/\.json$/, "")))) {
    const file = path.join(directory, name);
    try {
      const pending = readJsonFile(file);
      const age = Date.now() - Date.parse(pending.createdAt);
      if (!Number.isFinite(age) || age > 15 * 60 * 1000) {
        quarantineReady(
          config,
          file,
          "uncommitted control preparation expired",
        );
      }
    } catch (error) {
      if (error.code !== "ENOENT") quarantineReady(config, file, error.message);
    }
  }
}

async function processReady(config, env = {}) {
  expirePending(config);
  const keyResult = await runVector(config.authorizationKeyCommand, { env });
  const authorizationKey = keyResult.stdout.trim();
  const events = await readRecentEvents(config, env);
  const outcomes = await processSignedReceipts(
    config,
    events,
    authorizationKey,
    env,
  );
  const readyDirectory = path.join(config.spoolDirectory, "ready");
  fs.mkdirSync(readyDirectory, { recursive: true, mode: 0o700 });
  const files = fs
    .readdirSync(readyDirectory)
    .filter((name) => UUID.test(name.replace(/\.json$/, "")))
    .sort((left, right) => {
      const committedAt = (name) => {
        try {
          return Date.parse(
            readJsonFile(path.join(readyDirectory, name)).committedAt,
          );
        } catch {
          return Number.POSITIVE_INFINITY;
        }
      };
      return (
        committedAt(left) - committedAt(right) || left.localeCompare(right)
      );
    });
  for (const name of files) {
    const file = path.join(readyDirectory, name);
    let request;
    try {
      request = readJsonFile(file);
    } catch (error) {
      if (error.code !== "ENOENT") quarantineReady(config, file, error.message);
      continue;
    }
    if (isProcessed(config, request.operationId)) {
      try {
        fs.unlinkSync(file);
      } catch (error) {
        if (error.code !== "ENOENT") throw error;
      }
      continue;
    }
    const age = Date.now() - Date.parse(request.committedAt);
    if (!Number.isFinite(age) || age > 15 * 60 * 1000) {
      quarantineReady(config, file, "control request expired");
      continue;
    }
    try {
      validateAuthorization(request, authorizationKey);
      validateEvents(events, request, config);
    } catch (error) {
      if (error.message === "control events are not yet visible") continue;
      quarantineReady(config, file, error.message);
      continue;
    }
    const manifest = await readManifest(config);
    if (manifest.generation !== request.generation) {
      quarantineReady(config, file, "control request generation is stale");
      continue;
    }
    const expectedState =
      request.command === "-cloud" ? "local-owned" : "cloud-owned";
    const completedState =
      request.command === "-cloud" ? "cloud-owned" : "local-owned";
    if (manifest.state === completedState) {
      markProcessed(config, request);
      outcomes.push({
        operationId: request.operationId,
        command: request.command,
      });
      continue;
    }
    if (manifest.state !== expectedState) {
      quarantineReady(config, file, `control state is ${manifest.state}`);
      continue;
    }
    const action =
      request.command === "-cloud"
        ? config.actionCloudCommand
        : config.actionLocalCommand;
    if (action === null) fail("control action is unavailable at this location");
    if (await executeClaimed(config, request, action, env)) {
      outcomes.push({
        operationId: request.operationId,
        command: request.command,
      });
    }
  }
  return outcomes;
}

function readStdin(maxBytes = 16 * 1024) {
  const input = fs.readFileSync(0);
  if (input.length > maxBytes) fail("control request is too large");
  return JSON.parse(input.toString("utf8"));
}

export const testOnly = {
  canonicalRelayUrl,
  loadConfig,
  parsePreparedReceipt,
  prepare,
  commit,
  processReady,
  runVector,
  validateAuthorization,
  validateEvents,
};

if (
  process.argv[1] &&
  fs.realpathSync(fileURLToPath(import.meta.url)) ===
    fs.realpathSync(process.argv[1])
) {
  try {
    const config = loadConfig();
    const mode = process.argv[2] || "controller";
    if (mode === "controller") {
      const request = validateRequest(readStdin(), config);
      const result =
        request.phase === "prepare"
          ? await prepare(request, config)
          : await commit(request, config);
      process.stdout.write(`${JSON.stringify(result)}\n`);
    } else if (mode === "supervise-once") {
      process.stdout.write(
        `${JSON.stringify({ processed: await processReady(config) })}\n`,
      );
    } else {
      fail("unsupported cloud control host mode");
    }
  } catch (error) {
    process.stderr.write(`cloud control blocked: ${error.message}\n`);
    process.exitCode = 75;
  }
}
