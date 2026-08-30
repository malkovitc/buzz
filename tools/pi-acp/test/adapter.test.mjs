import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { attachJsonlReader, writeJsonl } from "../src/jsonl.mjs";

const here = path.dirname(fileURLToPath(import.meta.url));
const adapterPath = path.join(here, "../src/pi-acp-rpc.mjs");
const fakePiPath = path.join(here, "fake-pi.mjs");
const fakeBuzzPath = path.join(here, "fake-buzz.mjs");
const fakeCloudControlPath = path.join(here, "fake-cloud-control.sh");
const fakeSlowCloudControlPath = path.join(here, "fake-cloud-control-slow.sh");
const fakeSlowCommitPath = path.join(here, "fake-cloud-control-slow-commit.sh");
const buzzMeta = {
  buzz: {
    relayUrl: "wss://relay.example",
    agentPubkey: "f".repeat(64),
    channelId: "4dcab690-a2ca-4a56-9e5d-d901d12f83c3",
    triggeringEventIds: ["a".repeat(64)],
    allowedReplyEventIds: ["a".repeat(64)],
    replyTo: "a".repeat(64),
    taskThreadRoot: "a".repeat(64),
  },
};

test("version output does not load the adapter dependency graph", async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-acp-version-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const sourceDir = path.join(root, "src");
  fs.mkdirSync(sourceDir);
  const isolatedEntrypoint = path.join(sourceDir, "pi-acp-rpc.mjs");
  fs.copyFileSync(adapterPath, isolatedEntrypoint);
  fs.writeFileSync(
    path.join(root, "package.json"),
    JSON.stringify({ type: "module", version: "0.2.5" }),
  );
  const child = spawn(process.execPath, [isolatedEntrypoint, "--version"], {
    stdio: ["ignore", "pipe", "pipe"],
  });
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => (stdout += chunk));
  child.stderr.on("data", (chunk) => (stderr += chunk));
  const [code] = await once(child, "close");
  assert.equal(code, 0, stderr);
  assert.equal(stdout, "pi-acp 0.2.5\n");
});

function startHarness(mode = "complete") {
  const receiptDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-acp-test-"));
  const piStartedFile = path.join(receiptDir, "pi-started");
  const child = spawn(process.execPath, [adapterPath], {
    cwd: here,
    env: {
      ...process.env,
      PI_ACP_PI_COMMAND: process.execPath,
      PI_ACP_PI_ARGS_JSON: JSON.stringify([fakePiPath]),
      FAKE_PI_MODE: mode,
      PI_ACP_BUZZ_COMMAND: fakeBuzzPath,
      PI_ACP_KANBAN_COMMAND: fakeBuzzPath,
      PI_ACP_CLOUD_CONTROL_COMMAND:
        mode === "control-cancel"
          ? fakeSlowCloudControlPath
          : mode === "control-commit-cancel"
            ? fakeSlowCommitPath
            : fakeCloudControlPath,
      BUZZ_ACP_CLOUD_CONTROL_CHANNEL_ID: buzzMeta.buzz.channelId,
      PI_ACP_RECEIPT_DIR: receiptDir,
      PI_ACP_TASK_SESSION_ROOT: path.join(receiptDir, "task-sessions"),
      FAKE_PI_STARTED_FILE: piStartedFile,
      BUZZ_PRIVATE_KEY: "1".repeat(64),
      BUZZ_RELAY_URL: "wss://relay.example",
      FAKE_BUZZ_DELAY_MS: mode === "broker-cancel" ? "2000" : "0",
      FAKE_KANBAN_DELAY_MS: mode === "terminal-publication" ? "2000" : "0",
    },
    stdio: ["pipe", "pipe", "pipe"],
  });
  const messages = [];
  const waiters = [];
  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk.toString();
  });
  attachJsonlReader(
    child.stdout,
    (message) => {
      messages.push(message);
      for (const waiter of [...waiters]) {
        if (waiter.predicate(message)) {
          waiters.splice(waiters.indexOf(waiter), 1);
          clearTimeout(waiter.timer);
          waiter.resolve(message);
        }
      }
    },
    (error) => {
      throw error;
    },
  );

  return {
    child,
    messages,
    piStartedFile,
    receiptDir,
    send(message) {
      writeJsonl(child.stdin, message);
    },
    waitFor(predicate, timeoutMs = 30_000) {
      const existing = messages.find(predicate);
      if (existing) return Promise.resolve(existing);
      return new Promise((resolve, reject) => {
        const waiter = { predicate, resolve };
        waiter.timer = setTimeout(() => {
          waiters.splice(waiters.indexOf(waiter), 1);
          reject(
            new Error(
              `timed out waiting for adapter message; stderr=${stderr}; messages=${JSON.stringify(messages)}`,
            ),
          );
        }, timeoutMs);
        waiters.push(waiter);
      });
    },
    async close() {
      child.stdin.end();
      await Promise.race([
        once(child, "exit"),
        new Promise((resolve) => setTimeout(resolve, 1_000)),
      ]);
      if (child.exitCode === null) child.kill("SIGKILL");
      fs.rmSync(receiptDir, { recursive: true, force: true });
    },
  };
}

async function handshake(harness) {
  harness.send({
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: { protocolVersion: 2, clientCapabilities: {} },
  });
  const initialized = await harness.waitFor((message) => message.id === 1);
  assert.equal(initialized.result.protocolVersion, 2);
  assert.equal(initialized.result._meta.steering.supported, true);
  assert.equal(initialized.result._meta.pilot.liveCanaryValidated, true);
  assert.equal(initialized.result._meta.pilot.fleetApproved, false);
  assert.equal(initialized.result.agentInfo.version, "0.2.8");

  harness.send({
    jsonrpc: "2.0",
    id: 2,
    method: "session/new",
    params: { cwd: here, mcpServers: [], systemPrompt: "Answer concisely." },
  });
  const created = await harness.waitFor((message) => message.id === 2);
  assert.match(created.result.sessionId, /^pi_/);
  return created.result.sessionId;
}

test("maps ACP prompt to Pi text, tool, usage, and completion events", async (t) => {
  const harness = startHarness();
  t.after(() => harness.close());
  const sessionId = await handshake(harness);

  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "status?" }],
      _meta: buzzMeta,
    },
  });
  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "end_turn");

  const text = harness.messages.find(
    (message) =>
      message.params?.update?.sessionUpdate === "agent_message_chunk",
  );
  assert.equal(text.params.update.content.text, "hello\u2028world");
  assert.ok(
    harness.messages.some(
      (message) => message.params?.update?.sessionUpdate === "tool_call",
    ),
  );
  assert.ok(
    harness.messages.some(
      (message) =>
        message.params?.update?.sessionUpdate === "tool_call_update" &&
        message.params.update.status === "completed",
    ),
  );
  const usage = harness.messages.find(
    (message) =>
      message.method === "_goose/unstable/session/update" &&
      message.params?.update?.sessionUpdate === "usage_update",
  );
  assert.equal(usage.params.update.accumulatedInputTokens, 185);
  assert.equal(usage.params.update.accumulatedOutputTokens, 20);
  assert.equal(usage.params.update.accumulatedCachedInputTokens, 80);
  assert.equal(usage.params.update.accumulatedCacheWriteTokens, 5);
  assert.equal(usage.params.update.accumulatedTotalTokens, 205);
  assert.equal(usage.params.update.model, "fake/test-model");
});

test("handles authenticated cloud control without spawning Pi or emitting model usage", async (t) => {
  const harness = startHarness();
  t.after(() => harness.close());
  const sessionId = await handshake(harness);

  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [
        { type: "text", text: "wrapped Buzz prompt must not reach a model" },
      ],
      _meta: {
        buzz: { ...buzzMeta.buzz, controlCommand: "-status" },
      },
    },
  });
  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "end_turn");
  assert.deepEqual(completed.result._meta.cloudControl, {
    command: "-status",
    deterministic: true,
  });
  assert.equal(fs.existsSync(harness.piStartedFile), false);
  assert.equal(
    harness.messages.some(
      (message) => message.params?.update?.sessionUpdate === "usage_update",
    ),
    false,
  );
  assert.equal(
    harness.messages.some(
      (message) => message.params?.update?.sessionUpdate === "tool_call",
    ),
    false,
  );
  assert.ok(
    harness.messages.some(
      (message) =>
        message.params?.update?.content?.text ===
        `STATUS_LOCAL branch=cloud/handoff-test head=${"a".repeat(40)}`,
    ),
  );
});

test("cancels deterministic cloud control with a normal cancelled prompt result", async (t) => {
  const harness = startHarness("control-cancel");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "-cloud" }],
      _meta: { buzz: { ...buzzMeta.buzz, controlCommand: "-cloud" } },
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 30));
  harness.send({
    jsonrpc: "2.0",
    method: "session/cancel",
    params: { sessionId },
  });
  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "cancelled");
  assert.equal(completed.error, undefined);
  assert.equal(fs.existsSync(harness.piStartedFile), false);
});

test("ignores late cancellation after the durable control reply boundary", async (t) => {
  const harness = startHarness("control-commit-cancel");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "-cloud" }],
      _meta: { buzz: { ...buzzMeta.buzz, controlCommand: "-cloud" } },
    },
  });
  const deadline = Date.now() + 10_000;
  while (
    !fs
      .readdirSync(harness.receiptDir, { recursive: true })
      .some((entry) => entry.endsWith("receipt.json"))
  ) {
    if (Date.now() >= deadline)
      assert.fail("durable control receipt was not written");
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
  harness.send({
    jsonrpc: "2.0",
    method: "session/cancel",
    params: { sessionId },
  });
  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "end_turn");
  assert.equal(fs.existsSync(harness.piStartedFile), false);
});

test("rejects unknown authenticated control metadata without an LLM fallback", async (t) => {
  const harness = startHarness();
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "-destroy" }],
      _meta: { buzz: { ...buzzMeta.buzz, controlCommand: "-destroy" } },
    },
  });
  const rejected = await harness.waitFor((message) => message.id === 3);
  assert.equal(rejected.error.code, -32602);
  assert.equal(fs.existsSync(harness.piStartedFile), false);
});

test("drops malformed and wrong-channel reserved commands before controller or model", async (t) => {
  const harness = startHarness();
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  for (const [id, controlCommand, channelId] of [
    [3, "__buzz_rejected_cloud_control__", buzzMeta.buzz.channelId],
    [4, "-status", "11111111-1111-4111-8111-111111111111"],
  ]) {
    harness.send({
      jsonrpc: "2.0",
      id,
      method: "session/prompt",
      params: {
        sessionId,
        prompt: [{ type: "text", text: "reserved command attempt" }],
        _meta: { buzz: { ...buzzMeta.buzz, channelId, controlCommand } },
      },
    });
    const completed = await harness.waitFor((message) => message.id === id);
    assert.equal(completed.result.stopReason, "end_turn");
    assert.deepEqual(completed.result._meta.cloudControl, {
      rejected: true,
      deterministic: true,
    });
  }
  assert.equal(fs.existsSync(harness.piStartedFile), false);
  assert.equal(
    harness.messages.some((message) =>
      ["agent_message_chunk", "usage_update", "tool_call"].includes(
        message.params?.update?.sessionUpdate,
      ),
    ),
    false,
  );
});

test("steers an active Pi prompt without starting a second ACP prompt", async (t) => {
  const harness = startHarness("steer");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);

  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "start" }],
      _meta: buzzMeta,
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 30));
  harness.send({
    jsonrpc: "2.0",
    id: 5,
    method: "_session/steering",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "wrong thread" }],
      _meta: {
        buzz: {
          taskThreadRoot: "b".repeat(64),
          deliveredEventIds: ["e".repeat(64)],
        },
      },
    },
  });
  const rejected = await harness.waitFor((message) => message.id === 5);
  assert.equal(rejected.error.code, -32602);
  harness.send({
    jsonrpc: "2.0",
    id: 4,
    method: "_session/steering",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "focus" }],
      _meta: {
        buzz: {
          taskThreadRoot: "a".repeat(64),
          deliveredEventIds: ["c".repeat(64)],
        },
      },
    },
  });

  const steered = await harness.waitFor((message) => message.id === 4);
  assert.deepEqual(steered.result, { outcome: "injected" });
  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "end_turn");
  assert.ok(
    harness.messages.some(
      (message) => message.params?.update?.content?.text === "steered:focus",
    ),
  );
});

test("queues startup steering until the SDK prompt is active", async (t) => {
  const harness = startHarness("startup-steer");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);

  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "start" }],
      _meta: buzzMeta,
    },
  });
  harness.send({
    jsonrpc: "2.0",
    id: 4,
    method: "_session/steering",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "early focus" }],
      _meta: {
        buzz: {
          taskThreadRoot: "a".repeat(64),
          deliveredEventIds: ["d".repeat(64)],
        },
      },
    },
  });

  const steered = await harness.waitFor((message) => message.id === 4);
  assert.deepEqual(steered.result, { outcome: "injected" });
  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "end_turn");
  assert.ok(
    harness.messages.some(
      (message) =>
        message.params?.update?.content?.text === "steered:early focus",
    ),
  );
});

test("maps ACP cancellation to Pi abort and drains a cancelled prompt", async (t) => {
  const harness = startHarness("cancel");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);

  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "wait" }],
      _meta: buzzMeta,
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 30));
  harness.send({
    jsonrpc: "2.0",
    method: "session/cancel",
    params: { sessionId },
  });

  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "cancelled");
});

test("rejects blank prompts and accepts multiple ACP sessions", async (t) => {
  const harness = startHarness();
  t.after(() => harness.close());
  const sessionId = await handshake(harness);

  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "   " }],
      _meta: buzzMeta,
    },
  });
  const blank = await harness.waitFor((message) => message.id === 3);
  assert.equal(blank.error.code, -32602);

  harness.send({
    jsonrpc: "2.0",
    id: 4,
    method: "session/new",
    params: { cwd: here, mcpServers: [] },
  });
  const second = await harness.waitFor((message) => message.id === 4);
  assert.match(second.result.sessionId, /^pi_/);
  assert.notEqual(second.result.sessionId, sessionId);
});

test("creates a fresh Pi process for every task in one ACP session", async (t) => {
  const harness = startHarness("pid");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);

  for (const id of [3, 4]) {
    harness.send({
      jsonrpc: "2.0",
      id,
      method: "session/prompt",
      params: {
        sessionId,
        prompt: [{ type: "text", text: `task-${id}` }],
        _meta: buzzMeta,
      },
    });
    const completed = await harness.waitFor((message) => message.id === id);
    assert.equal(completed.result.stopReason, "end_turn");
  }
  const pids = harness.messages
    .filter(
      (message) =>
        message.params?.update?.sessionUpdate === "agent_message_chunk" &&
        message.params.update.content.text.startsWith("pid:"),
    )
    .map((message) => message.params.update.content.text);
  assert.equal(pids.length, 2);
  assert.notEqual(pids[0], pids[1]);
});

test("derives a stable isolated Pi session identity from relay, channel, and thread", async (t) => {
  const harness = startHarness("task-id");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  const contexts = [
    buzzMeta,
    buzzMeta,
    {
      buzz: {
        ...buzzMeta.buzz,
        triggeringEventIds: ["b".repeat(64)],
        allowedReplyEventIds: ["b".repeat(64)],
        replyTo: "b".repeat(64),
        taskThreadRoot: "b".repeat(64),
      },
    },
    {
      buzz: {
        ...buzzMeta.buzz,
        triggeringEventIds: ["c".repeat(64), "d".repeat(64)],
        taskThreadRoot: undefined,
      },
    },
  ];
  for (const [index, metadata] of contexts.entries()) {
    const id = index + 3;
    harness.send({
      jsonrpc: "2.0",
      id,
      method: "session/prompt",
      params: {
        sessionId,
        prompt: [{ type: "text", text: `task-${id}` }],
        _meta: metadata,
      },
    });
    await harness.waitFor((message) => message.id === id);
  }
  const taskIds = harness.messages
    .filter((message) =>
      message.params?.update?.content?.text?.startsWith("task:"),
    )
    .map((message) => message.params.update.content.text);
  assert.equal(taskIds.length, 4);
  assert.equal(taskIds[0], taskIds[1]);
  assert.notEqual(taskIds[1], taskIds[2]);
  assert.match(taskIds[0], /^task:[0-9a-f]{64}$/);
  assert.equal(taskIds[3], "task:missing");
});

test("propagates an explicit ACP rotation reset to the durable Pi session", async (t) => {
  const harness = startHarness("task-reset");
  t.after(() => harness.close());
  harness.send({
    jsonrpc: "2.0",
    id: 1,
    method: "initialize",
    params: { protocolVersion: 2, clientCapabilities: {} },
  });
  await harness.waitFor((message) => message.id === 1);
  harness.send({
    jsonrpc: "2.0",
    id: 2,
    method: "session/new",
    params: {
      cwd: here,
      mcpServers: [],
    },
  });
  const created = await harness.waitFor((message) => message.id === 2);
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId: created.result.sessionId,
      prompt: [{ type: "text", text: "after rotation" }],
      _meta: {
        buzz: { ...buzzMeta.buzz, resetTaskSession: true },
      },
    },
  });
  await harness.waitFor((message) => message.id === 3);
  assert.ok(
    harness.messages.some(
      (message) => message.params?.update?.content?.text === "reset:1",
    ),
  );
});

test("reads empty durable delivery state without spawning Pi", async (t) => {
  const harness = startHarness();
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "_session/task_state",
    params: { sessionId, _meta: buzzMeta },
  });
  const state = await harness.waitFor((message) => message.id === 3);
  assert.deepEqual(state.result.deliveredEventIds, []);
  assert.deepEqual(state.result.processedTriggerEventIds, []);
  assert.equal(fs.existsSync(harness.piStartedFile), false);
});

test("keeps the Buzz signing key out of the Pi subprocess", async (t) => {
  const harness = startHarness("key-check");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "check key isolation" }],
      _meta: buzzMeta,
    },
  });
  await harness.waitFor((message) => message.id === 3);
  assert.ok(
    harness.messages.some(
      (message) => message.params?.update?.content?.text === "key:missing",
    ),
  );
});

test("accepts non-event prompts without authenticated reply routing", async (t) => {
  const harness = startHarness();
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "heartbeat" }],
    },
  });
  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "end_turn");
});

test("brokers Buzz publication without exposing the signing key", async (t) => {
  const harness = startHarness("broker");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "publish" }],
      _meta: buzzMeta,
    },
  });
  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "end_turn");
  assert.ok(
    harness.messages.some(
      (message) => message.params?.update?.content?.text === "broker:ok",
    ),
  );
});

test("terminal publication stays successful and aborts sibling broker work", async (t) => {
  const harness = startHarness("terminal-publication");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  const startedAt = Date.now();
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "publish and stop" }],
      _meta: buzzMeta,
    },
  });

  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "end_turn");
  assert.ok(Date.now() - startedAt < 1_500);
  assert.ok(
    harness.messages.some(
      (message) => message.params?.update?.content?.text === "sibling:aborted",
    ),
  );
});

test("aborts an in-flight publication when the ACP prompt is cancelled", async (t) => {
  const harness = startHarness("broker-cancel");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "publish slowly" }],
      _meta: buzzMeta,
    },
  });
  await new Promise((resolve) => setTimeout(resolve, 100));
  harness.send({
    jsonrpc: "2.0",
    method: "session/cancel",
    params: { sessionId },
  });
  const completed = await harness.waitFor((message) => message.id === 3);
  assert.equal(completed.result.stopReason, "cancelled");
  await new Promise((resolve) => setTimeout(resolve, 250));
  assert.equal(
    harness.messages.some(
      (message) => message.params?.update?.content?.text === "broker:ok",
    ),
    false,
  );
});

test("propagates Pi prompt failures as ACP errors", async (t) => {
  const harness = startHarness("prompt-fail");
  t.after(() => harness.close());
  const sessionId = await handshake(harness);
  harness.send({
    jsonrpc: "2.0",
    id: 3,
    method: "session/prompt",
    params: {
      sessionId,
      prompt: [{ type: "text", text: "fail" }],
      _meta: buzzMeta,
    },
  });
  const failed = await harness.waitFor((message) => message.id === 3);
  assert.equal(failed.error.code, -32603);
  assert.match(failed.error.message, /provider unavailable/);
});
