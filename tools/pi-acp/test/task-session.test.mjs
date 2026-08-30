import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { InMemoryCredentialStore } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxProvider,
} from "@earendil-works/pi-ai/providers/faux";
import {
  createAgentSession,
  DefaultResourceLoader,
  ModelRuntime,
  SettingsManager,
} from "@earendil-works/pi-coding-agent";
import {
  acquireTaskLease,
  assertTaskSessionByteBudget,
  openTaskSession,
  pruneTaskSessionDirectories,
  resetTaskSession,
  taskSessionDirectory,
  taskSessionIdentity,
} from "../src/task-session.mjs";

const context = {
  agentPubkey: "f".repeat(64),
  channelId: "61b56145-8e1a-41da-9038-043d24f621ec",
  taskThreadRoot: "a".repeat(64),
};

test("task identity is canonical and scoped by relay, channel, and thread", () => {
  const first = taskSessionIdentity(context, "https://EXAMPLE.com/path?q=1");
  const same = taskSessionIdentity(context, "wss://example.com");
  const other = taskSessionIdentity(
    { ...context, taskThreadRoot: "b".repeat(64) },
    "wss://example.com",
  );
  const otherAgent = taskSessionIdentity(
    { ...context, agentPubkey: "e".repeat(64) },
    "wss://example.com",
  );
  assert.deepEqual(first, same);
  assert.notEqual(first.digest, other.digest);
  assert.notEqual(first.digest, otherAgent.digest);
  assert.deepEqual(
    taskSessionIdentity(context, "http://localhost:3000"),
    taskSessionIdentity(context, "ws://localhost:3000"),
  );
});

test("task session directory is private and cannot escape its root", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-task-session-"));
  const identity = taskSessionIdentity(context, "wss://example.com");
  const directory = taskSessionDirectory(root, identity);
  assert.equal(path.relative(root, directory).startsWith(".."), false);
  assert.equal(fs.statSync(directory).mode & 0o777, 0o700);
});

test("task session pruning bounds unprotected history and preserves capsule state", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-task-prune-"));
  const directories = ["1", "2", "3"].map((suffix) =>
    taskSessionDirectory(
      root,
      taskSessionIdentity(
        { ...context, taskThreadRoot: suffix.repeat(64) },
        "wss://example.com",
      ),
    ),
  );
  fs.writeFileSync(
    path.join(directories[0], ".capsule-lineage-head.json"),
    "{}\n",
  );
  const activeLease = path.join(directories[1], ".task-execution.lock");
  fs.writeFileSync(activeLease, "active\n", { flag: "wx" });
  assert.throws(
    () => pruneTaskSessionDirectories(root, directories[2], 2),
    /quota is exhausted/,
  );
  assert.equal(fs.existsSync(directories[1]), true);
  fs.unlinkSync(activeLease);
  pruneTaskSessionDirectories(root, directories[2], 2);
  assert.equal(fs.existsSync(directories[0]), true);
  assert.equal(fs.existsSync(directories[1]), false);
  assert.equal(fs.existsSync(directories[2]), true);
  assert.throws(
    () => pruneTaskSessionDirectories(root, directories[2], 1),
    /quota is exhausted/,
  );
});

test("task session byte budget includes nested files and rejects links", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-task-bytes-"));
  const nested = path.join(root, "nested");
  fs.mkdirSync(nested);
  fs.writeFileSync(path.join(root, "session.jsonl"), "1234");
  fs.writeFileSync(path.join(nested, "receipt.json"), "56");
  assert.equal(assertTaskSessionByteBudget(root, 6), 6);
  assert.throws(
    () => assertTaskSessionByteBudget(root, 5),
    /exceeds 5 byte budget/,
  );
  fs.symlinkSync(path.join(root, "session.jsonl"), path.join(root, "link"));
  assert.throws(
    () => assertTaskSessionByteBudget(root, 100),
    /must not contain symbolic links/,
  );
});

test("task lease recovery accepts an interrupted prune token", () => {
  const sessionDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-task-lease-"));
  const lease = path.join(sessionDir, ".task-execution.lock");
  fs.writeFileSync(lease, "2147483647 old-prune task-prune\n");
  fs.writeFileSync(`${lease}.recovery`, "2147483647 old-recovery\n");
  const release = acquireTaskLease(sessionDir, "next-turn");
  assert.match(fs.readFileSync(lease, "utf8"), /^\d+ [0-9a-f-]+ next-turn\n$/);
  release();
  assert.equal(fs.existsSync(lease), false);
});

test("task session reopens the same leaf and isolates another thread", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-task-reopen-"));
  const identity = taskSessionIdentity(context, "wss://example.com");
  const created = openTaskSession(process.cwd(), root, identity);
  created.appendMessage({
    role: "user",
    content: "checkpoint",
    timestamp: Date.now(),
  });
  created.appendMessage({
    role: "assistant",
    content: [{ type: "text", text: "ready" }],
    api: "test",
    provider: "test",
    model: "test",
    usage: {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "stop",
    timestamp: Date.now(),
  });
  const leaf = created.appendCustomEntry("buzz.delivery.v1", {
    eventIds: ["a".repeat(64)],
  });
  const reopened = openTaskSession(process.cwd(), root, identity);
  assert.equal(reopened.getSessionId(), created.getSessionId());
  assert.equal(reopened.getLeafId(), leaf);
  const isolated = openTaskSession(
    process.cwd(),
    root,
    taskSessionIdentity(
      { ...context, taskThreadRoot: "b".repeat(64) },
      "wss://example.com",
    ),
  );
  assert.notEqual(isolated.getSessionId(), created.getSessionId());
  assert.equal(isolated.getLeafId(), null);
});

test("reopen fails closed for unresolved tools and trims an interrupted user tail", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-task-recovery-"));
  const toolIdentity = taskSessionIdentity(
    { ...context, taskThreadRoot: "c".repeat(64) },
    "wss://example.com",
  );
  const toolSession = openTaskSession(process.cwd(), root, toolIdentity);
  toolSession.appendMessage({
    role: "user",
    content: "run tool",
    timestamp: Date.now(),
  });
  toolSession.appendMessage({
    role: "assistant",
    content: [
      { type: "toolCall", id: "write-1", name: "write", arguments: {} },
    ],
    api: "test",
    provider: "test",
    model: "test",
    usage: {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "toolUse",
    timestamp: Date.now(),
  });
  assert.throws(
    () => openTaskSession(process.cwd(), root, toolIdentity),
    /unresolved tool effects/,
  );

  const replyIdentity = taskSessionIdentity(
    { ...context, taskThreadRoot: "e".repeat(64) },
    "wss://example.com",
  );
  const replySession = openTaskSession(process.cwd(), root, replyIdentity);
  replySession.appendMessage({
    role: "user",
    content: "first",
    timestamp: Date.now(),
  });
  const replySettledLeaf = replySession.appendMessage({
    role: "assistant",
    content: [{ type: "text", text: "ready" }],
    api: "test",
    provider: "test",
    model: "test",
    usage: {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "stop",
    timestamp: Date.now(),
  });
  replySession.appendMessage({
    role: "user",
    content: "reply",
    timestamp: Date.now(),
  });
  replySession.appendMessage({
    role: "assistant",
    content: [
      {
        type: "toolCall",
        id: "reply-1",
        name: "buzz_reply",
        arguments: { content: "done" },
      },
    ],
    api: "test",
    provider: "test",
    model: "test",
    usage: {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "toolUse",
    timestamp: Date.now(),
  });
  const replyRecovered = openTaskSession(process.cwd(), root, replyIdentity);
  assert.notEqual(replyRecovered.getSessionId(), replySession.getSessionId());
  assert.equal(replyRecovered.getLeafId(), replySettledLeaf);

  const userIdentity = taskSessionIdentity(
    { ...context, taskThreadRoot: "d".repeat(64) },
    "wss://example.com",
  );
  const interrupted = openTaskSession(process.cwd(), root, userIdentity);
  interrupted.appendMessage({
    role: "user",
    content: "first",
    timestamp: Date.now(),
  });
  const settledLeaf = interrupted.appendMessage({
    role: "assistant",
    content: [{ type: "text", text: "settled" }],
    api: "test",
    provider: "test",
    model: "test",
    usage: {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "stop",
    timestamp: Date.now(),
  });
  interrupted.appendMessage({
    role: "user",
    content: "interrupted",
    timestamp: Date.now(),
  });
  const recovered = openTaskSession(process.cwd(), root, userIdentity);
  assert.notEqual(recovered.getSessionId(), interrupted.getSessionId());
  assert.equal(recovered.getLeafId(), settledLeaf);
  assert.doesNotMatch(JSON.stringify(recovered.getBranch()), /interrupted/);
});

test("task rotation preserves imported lineage metadata without model context", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-task-reset-"));
  const identity = taskSessionIdentity(context, "wss://example.com");
  const current = openTaskSession(process.cwd(), root, identity);
  current.appendMessage({
    role: "user",
    content: "work",
    timestamp: Date.now(),
  });
  current.appendMessage({
    role: "assistant",
    content: [{ type: "text", text: "done" }],
    api: "test",
    provider: "test",
    model: "test",
    usage: {
      input: 1,
      output: 1,
      cacheRead: 0,
      cacheWrite: 0,
      totalTokens: 2,
      cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
    },
    stopReason: "stop",
    timestamp: Date.now(),
  });
  const digest = "a".repeat(64);
  current.appendCustomEntry("buzz.continuation.lineage.v1", {
    capsuleDigest: digest,
    lineage: [{ sessionId: "source", leafId: "deadbeef", location: "local" }],
  });
  fs.writeFileSync(
    path.join(current.getSessionDir(), ".capsule-lineage-head.json"),
    JSON.stringify({
      schemaVersion: 1,
      generation: "22222222-2222-4222-8222-222222222222",
      digest,
    }),
  );
  const reset = resetTaskSession(process.cwd(), root, identity);
  assert.notEqual(reset.getSessionId(), current.getSessionId());
  assert.equal(reset.buildSessionContext().messages.length, 0);
  const marker = reset.getEntries().find((entry) => entry.type === "custom");
  assert.equal(marker.data.capsuleDigest, digest);
});

test("a real AgentSession reopens task history after process-style replacement", async (t) => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-task-sdk-"));
  t.after(() => fs.rmSync(root, { recursive: true, force: true }));
  const identity = taskSessionIdentity(context, "wss://example.com");
  const faux = fauxProvider();
  let restoredContext = "";
  faux.setResponses([
    fauxAssistantMessage("first result"),
    async (messages) => {
      restoredContext = JSON.stringify(messages);
      return fauxAssistantMessage("second result");
    },
  ]);
  const modelRuntime = await ModelRuntime.create({
    credentials: new InMemoryCredentialStore(),
    refreshOnCreate: false,
  });
  modelRuntime.registerNativeProvider(faux.provider);
  await modelRuntime.setRuntimeApiKey(faux.provider.id, "test-only-key");
  const resourceLoader = new DefaultResourceLoader({
    cwd: process.cwd(),
    agentDir: path.join(root, "agent"),
    noExtensions: true,
    noSkills: true,
    noPromptTemplates: true,
    noThemes: true,
    noContextFiles: true,
  });
  await resourceLoader.reload();
  const options = {
    cwd: process.cwd(),
    model: faux.getModel(),
    modelRuntime,
    resourceLoader,
    settingsManager: SettingsManager.inMemory({
      compaction: { enabled: false },
      retry: { enabled: false },
    }),
  };
  const firstManager = openTaskSession(process.cwd(), root, identity);
  const first = await createAgentSession({
    ...options,
    sessionManager: firstManager,
  });
  await first.session.prompt("first request");
  const sourceSessionId = first.session.sessionId;
  first.session.dispose();

  const reopenedManager = openTaskSession(process.cwd(), root, identity);
  const reopened = await createAgentSession({
    ...options,
    sessionManager: reopenedManager,
  });
  assert.equal(reopened.session.sessionId, sourceSessionId);
  await reopened.session.prompt("second request");
  assert.match(restoredContext, /first request/);
  assert.match(restoredContext, /first result/);
  reopened.session.dispose();
});

test("task identity rejects malformed trust-boundary fields", () => {
  assert.throws(
    () =>
      taskSessionIdentity(
        { ...context, taskThreadRoot: "../escape" },
        "wss://example.com",
      ),
    /threadRoot is invalid/,
  );
  assert.doesNotThrow(() =>
    taskSessionIdentity(context, "ws://buzz-relay:3000"),
  );
  assert.throws(
    () => taskSessionIdentity(context, "ftp://example.com"),
    /relayUrl is invalid/,
  );
  assert.throws(
    () => taskSessionDirectory("relative", { digest: "a".repeat(64) }),
    /absolute path/,
  );
});
