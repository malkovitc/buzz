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
const buzzMeta = {
  buzz: {
    channelId: "4dcab690-a2ca-4a56-9e5d-d901d12f83c3",
    triggeringEventIds: ["a".repeat(64)],
    allowedReplyEventIds: ["a".repeat(64)],
    replyTo: "a".repeat(64),
  },
};

function startHarness(mode = "complete") {
  const receiptDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-acp-test-"));
  const child = spawn(process.execPath, [adapterPath], {
    cwd: here,
    env: {
      ...process.env,
      PI_ACP_PI_COMMAND: process.execPath,
      PI_ACP_PI_ARGS_JSON: JSON.stringify([fakePiPath]),
      FAKE_PI_MODE: mode,
      PI_ACP_BUZZ_COMMAND: fakeBuzzPath,
      PI_ACP_KANBAN_COMMAND: fakeBuzzPath,
      PI_ACP_RECEIPT_DIR: receiptDir,
      BUZZ_PRIVATE_KEY: "1".repeat(64),
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
  assert.equal(initialized.result.agentInfo.version, "0.2.2");

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
    id: 4,
    method: "_session/steering",
    params: { sessionId, prompt: [{ type: "text", text: "focus" }] },
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
    params: { sessionId, prompt: [{ type: "text", text: "early focus" }] },
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
