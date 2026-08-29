import assert from "node:assert/strict";
import crypto from "node:crypto";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";

import { testOnly } from "../src/cloud-control-host.mjs";

const channelId = "61b56145-8e1a-41da-9038-043d24f621ec";
const ownerPubkey = "a".repeat(64);
const agentPubkey = "b".repeat(64);
const commandEvent = "c".repeat(64);
const replyTo = "d".repeat(64);
const receiptEvent = "e".repeat(64);
const authorizationKey = "test-control-authorization-key";
const relayUrl = "wss://relay.example";

function writeExecutable(file, body) {
  fs.writeFileSync(file, `#!/bin/sh\nset -eu\n${body}\n`, { mode: 0o700 });
}

function fixture(t, state = "local-owned") {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-cloud-control-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const manifest = path.join(root, "manifest.json");
  const events = path.join(root, "events.json");
  const cloudAction = path.join(root, "cloud-action");
  const localAction = path.join(root, "local-action");
  const keyCommand = path.join(root, "authorization-key");
  const marker = path.join(root, "marker");
  const generation = "11111111-1111-4111-8111-111111111111";
  fs.writeFileSync(
    manifest,
    JSON.stringify({
      state,
      owner: state === "cloud-owned" ? "cloud" : "local",
      generation,
      branch: "cloud/handoff-test",
      commit: "f".repeat(40),
    }),
  );
  fs.writeFileSync(events, "[]\n");
  writeExecutable(
    cloudAction,
    `cat >/dev/null; printf cloud >${JSON.stringify(marker)}`,
  );
  writeExecutable(
    localAction,
    `cat >/dev/null; printf local >${JSON.stringify(marker)}`,
  );
  writeExecutable(keyCommand, `printf %s ${JSON.stringify(authorizationKey)}`);
  const config = {
    actionCloudCommand: [cloudAction],
    actionLocalCommand: [localAction],
    agentName: "Caliper — AI Quality Engineer",
    agentPubkey,
    approvedChannelId: channelId,
    authorizationKeyCommand: [keyCommand],
    buzzReadCommand: ["/bin/cat", events],
    location: "local",
    manifestCommand: ["/bin/cat", manifest],
    ownerPubkey,
    relayUrl,
    schemaVersion: 1,
    spoolDirectory: path.join(root, "spool"),
  };
  return { cloudAction, config, events, generation, manifest, marker, root };
}

function request(command, phase = "prepare", extra = {}) {
  return {
    schemaVersion: 1,
    phase,
    command,
    channelId,
    replyTo,
    triggeringEventIds: [commandEvent],
    ...extra,
  };
}

function authorize(content, operationId, command = "-cloud") {
  const baseContentSha256 = crypto
    .createHash("sha256")
    .update(content)
    .digest("hex");
  const authorization = crypto
    .createHmac("sha256", authorizationKey)
    .update(
      JSON.stringify({
        schemaVersion: 1,
        relayUrl,
        command,
        channelId,
        replyTo,
        triggeringEventIds: [commandEvent],
        operationId,
        baseContentSha256,
      }),
    )
    .digest("hex");
  const finalContent = `${content}\nauthorization=${authorization}`;
  return {
    authorization,
    baseContentSha256,
    content: finalContent,
    receiptContentSha256: crypto
      .createHash("sha256")
      .update(finalContent)
      .digest("hex"),
  };
}

function eventsFor(content, operationId, generation, command = "-cloud") {
  const authorized = authorize(content, operationId, command);
  return [
    {
      id: commandEvent,
      pubkey: ownerPubkey,
      kind: 9,
      content: `@Caliper — AI Quality Engineer ${command}`,
      tags: [
        ["h", channelId],
        ["p", agentPubkey],
      ],
      created_at: Math.floor(Date.now() / 1000),
    },
    {
      id: receiptEvent,
      pubkey: agentPubkey,
      kind: 9,
      content: authorized.content,
      tags: [
        ["h", channelId],
        ["e", replyTo, "", "reply"],
      ],
      created_at: Math.floor(Date.now() / 1000),
      operationId,
      generation,
    },
  ];
}

test("canonicalizes equivalent relay community URLs", () => {
  assert.equal(
    testOnly.canonicalRelayUrl("wss://relay.example/"),
    "wss://relay.example",
  );
});

test("prepares status without an effect and binds commit after publication", async (t) => {
  const f = fixture(t);
  const status = await testOnly.prepare(request("-status"), f.config);
  assert.equal(status.status, "noop");
  assert.match(status.content, /status=STATUS/);

  const prepared = await testOnly.prepare(request("-cloud"), f.config);
  assert.equal(prepared.status, "ok");
  assert.match(prepared.operationId, /^[0-9a-f-]{36}$/);
  assert.equal(
    (await testOnly.prepare(request("-cloud"), f.config)).operationId,
    prepared.operationId,
  );
  const binding = authorize(prepared.content, prepared.operationId);
  const committed = await testOnly.commit(
    request("-cloud", "commit", {
      operationId: prepared.operationId,
      receiptEventId: receiptEvent,
      authorization: binding.authorization,
      receiptContentSha256: binding.receiptContentSha256,
    }),
    f.config,
  );
  assert.equal(committed.content, "COMMIT_QUEUED");
  assert.equal(
    fs.existsSync(
      path.join(
        f.config.spoolDirectory,
        "ready",
        `${prepared.operationId}.json`,
      ),
    ),
    true,
  );
});

test("supervisor validates signed owner and receipt bindings before cloud action", async (t) => {
  const f = fixture(t);
  const prepared = await testOnly.prepare(request("-cloud"), f.config);
  fs.writeFileSync(
    f.events,
    JSON.stringify(
      eventsFor(prepared.content, prepared.operationId, f.generation),
    ),
  );
  const outcomes = await testOnly.processReady(f.config, {});
  assert.deepEqual(outcomes, [
    { operationId: prepared.operationId, command: "-cloud" },
  ]);
  assert.equal(fs.readFileSync(f.marker, "utf8"), "cloud");
  const binding = authorize(prepared.content, prepared.operationId);
  const replayedCommit = await testOnly.commit(
    request("-cloud", "commit", {
      operationId: prepared.operationId,
      receiptEventId: receiptEvent,
      authorization: binding.authorization,
      receiptContentSha256: binding.receiptContentSha256,
    }),
    f.config,
  );
  assert.equal(replayedCommit.content, "COMMIT_ALREADY_PROCESSED");
  assert.equal((await testOnly.processReady(f.config, {})).length, 0);
});

test("overlapping supervisors claim one operation before its effect", async (t) => {
  const f = fixture(t);
  f.config.location = "cloud";
  writeExecutable(
    f.cloudAction,
    `cat >/dev/null; sleep 0.3; printf x >>${JSON.stringify(f.marker)}`,
  );
  const prepared = await testOnly.prepare(request("-cloud"), f.config);
  const binding = authorize(prepared.content, prepared.operationId);
  await testOnly.commit(
    request("-cloud", "commit", {
      operationId: prepared.operationId,
      receiptEventId: receiptEvent,
      authorization: binding.authorization,
      receiptContentSha256: binding.receiptContentSha256,
    }),
    f.config,
  );
  fs.writeFileSync(
    f.events,
    JSON.stringify(
      eventsFor(prepared.content, prepared.operationId, f.generation),
    ),
  );
  await Promise.all([
    testOnly.processReady(f.config, {}),
    testOnly.processReady(f.config, {}),
  ]);
  assert.equal(fs.readFileSync(f.marker, "utf8"), "x");
});

test("local supervisor discovers a cloud receipt and safely requests return", async (t) => {
  const f = fixture(t, "cloud-owned");
  const prepared = await testOnly.prepare(request("-local"), f.config);
  fs.writeFileSync(
    f.events,
    JSON.stringify(
      eventsFor(prepared.content, prepared.operationId, f.generation, "-local"),
    ),
  );
  const outcomes = await testOnly.processReady(f.config, {});
  assert.deepEqual(outcomes, [
    { operationId: prepared.operationId, command: "-local" },
  ]);
  assert.equal(fs.readFileSync(f.marker, "utf8"), "local");
});

test("stale requests are quarantined without wedging the queue", async (t) => {
  const f = fixture(t);
  const prepared = await testOnly.prepare(request("-cloud"), f.config);
  const binding = authorize(prepared.content, prepared.operationId);
  await testOnly.commit(
    request("-cloud", "commit", {
      operationId: prepared.operationId,
      receiptEventId: receiptEvent,
      authorization: binding.authorization,
      receiptContentSha256: binding.receiptContentSha256,
    }),
    f.config,
  );
  const ready = path.join(
    f.config.spoolDirectory,
    "ready",
    `${prepared.operationId}.json`,
  );
  const value = JSON.parse(fs.readFileSync(ready, "utf8"));
  value.committedAt = "2000-01-01T00:00:00.000Z";
  fs.writeFileSync(ready, JSON.stringify(value), { mode: 0o600 });
  assert.deepEqual(await testOnly.processReady(f.config, {}), []);
  assert.equal(fs.existsSync(ready), false);
  assert.equal(
    fs.readdirSync(path.join(f.config.spoolDirectory, "rejected")).length,
    2,
  );
});

test("unpublished preparations expire without accumulating durable state", async (t) => {
  const f = fixture(t);
  const prepared = await testOnly.prepare(request("-cloud"), f.config);
  const pending = path.join(
    f.config.spoolDirectory,
    "pending",
    `${prepared.operationId}.json`,
  );
  const value = JSON.parse(fs.readFileSync(pending, "utf8"));
  value.createdAt = "2000-01-01T00:00:00.000Z";
  fs.writeFileSync(pending, JSON.stringify(value), { mode: 0o600 });
  assert.deepEqual(await testOnly.processReady(f.config, {}), []);
  assert.equal(fs.existsSync(pending), false);
});

test("timed out fixed commands cannot leave descendants alive", async (t) => {
  const f = fixture(t);
  const late = path.join(f.root, "late-effect");
  const command = path.join(f.root, "slow-tree");
  writeExecutable(
    command,
    `(sleep 0.4; printf late >${JSON.stringify(late)}) & sleep 5`,
  );
  await assert.rejects(
    testOnly.runVector([command], { timeoutMs: 100 }),
    /timed out/,
  );
  await new Promise((resolve) => setTimeout(resolve, 600));
  assert.equal(fs.existsSync(late), false);
});

test("forged or stale event bindings fail closed", async (t) => {
  const f = fixture(t);
  const prepared = await testOnly.prepare(request("-cloud"), f.config);
  const forged = eventsFor(
    prepared.content,
    prepared.operationId,
    f.generation,
  );
  const capability = testOnly.parsePreparedReceipt(forged[1], f.config);
  assert.doesNotThrow(() =>
    testOnly.validateAuthorization(capability, authorizationKey),
  );
  assert.throws(
    () =>
      testOnly.validateAuthorization(
        { ...capability, authorization: "0".repeat(64) },
        authorizationKey,
      ),
    /authorization capability is invalid/,
  );
  forged[0].pubkey = "0".repeat(64);
  assert.throws(
    () =>
      testOnly.validateEvents(
        forged,
        {
          command: "-cloud",
          triggeringEventId: commandEvent,
          receiptEventId: receiptEvent,
          replyTo,
          expectedContentSha256: crypto
            .createHash("sha256")
            .update(prepared.content)
            .digest("hex"),
        },
        f.config,
      ),
    /owner command event validation failed/,
  );
});
