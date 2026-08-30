import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { execFileSync } from "node:child_process";
import test from "node:test";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import {
  capsuleDigest,
  createCapsule,
  exportCapsule,
  importCapsule,
  reissueCapsule,
  renderContinuationContext,
  validateEnvelope,
} from "../src/continuation-capsule.mjs";
import {
  acquireTaskLease,
  assertTaskSessionByteBudget,
  MAX_TASK_SESSION_BYTES,
  taskSessionDirectory,
  taskSessionIdentity,
} from "../src/task-session.mjs";

function git(repo, ...args) {
  return execFileSync("git", ["-C", repo, ...args], {
    encoding: "utf8",
  }).trim();
}

function nowOrZero() {
  return Date.now();
}

function expectedFor(draft, capsuleDigest) {
  return {
    generation: draft.ownership.generation,
    capsuleDigest,
    location: draft.ownership.targetLocation,
    relayUrl: draft.task.relayUrl,
    agentPubkey: draft.task.agentPubkey,
    channelId: draft.task.channelId,
    threadRoot: draft.task.threadRoot,
  };
}

function sessionDirectory(base, task, location) {
  return taskSessionDirectory(
    path.join(base, `${location}-root`),
    taskSessionIdentity(
      {
        relayUrl: task.relayUrl,
        agentPubkey: task.agentPubkey,
        channelId: task.channelId,
        taskThreadRoot: task.threadRoot,
      },
      task.relayUrl,
    ),
  );
}

function fixture() {
  const base = fs.mkdtempSync(path.join(os.tmpdir(), "pi-capsule-"));
  const repoPath = path.join(base, "repo");
  fs.mkdirSync(repoPath);
  const repo = fs.realpathSync(repoPath);
  git(repo, "init", "-b", "capsule-test");
  git(repo, "config", "user.email", "test@example.com");
  git(repo, "config", "user.name", "Capsule Test");
  fs.writeFileSync(path.join(repo, "work.txt"), "checkpoint\n");
  git(repo, "add", "work.txt");
  git(repo, "commit", "-m", "checkpoint");
  git(repo, "remote", "add", "fork", "https://github.com/malkovitc/buzz.git");

  const task = {
    relayUrl: "wss://relay.example",
    agentPubkey: "f".repeat(64),
    channelId: "61b56145-8e1a-41da-9038-043d24f621ec",
    threadRoot: "a".repeat(64),
  };
  const source = SessionManager.create(
    repo,
    sessionDirectory(base, task, "source"),
  );
  source.appendMessage({
    role: "user",
    content: "safe source context",
    timestamp: nowOrZero(),
  });
  source.appendMessage({
    role: "assistant",
    content: [
      { type: "thinking", thinking: "private internal reasoning" },
      { type: "text", text: "checkpoint ready" },
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
    stopReason: "stop",
    timestamp: nowOrZero(),
  });
  const now = Date.now();
  const draft = {
    schemaVersion: 1,
    capsuleId: "11111111-1111-4111-8111-111111111111",
    createdAt: new Date(now).toISOString(),
    expiresAt: new Date(now + 60 * 60 * 1000).toISOString(),
    task,
    git: {
      repository: repo,
      remoteName: "fork",
      remoteUrl: "https://github.com/malkovitc/buzz.git",
      branch: "capsule-test",
      commit: git(repo, "rev-parse", "HEAD"),
      tree: git(repo, "rev-parse", "HEAD^{tree}"),
    },
    ownership: {
      generation: "22222222-2222-4222-8222-222222222222",
      sourceLocation: "local",
      targetLocation: "cloud",
    },
    pi: {
      sourceSessionId: source.getSessionId(),
      sourceLeafId: source.getLeafId(),
      lineage: [
        {
          sessionId: source.getSessionId(),
          leafId: source.getLeafId(),
          location: "local",
        },
      ],
      parentCapsuleDigest: null,
    },
    context: {
      goal: "Continue the reviewed implementation.",
      constraints: ["Git is authoritative."],
      decisions: ["Use a sanitized capsule."],
      completed: ["Checkpoint committed."],
      pending: ["Run the next focused test."],
      files: [{ path: "work.txt", symbols: [] }],
      checks: ["unit tests passed"],
      blockers: [],
      unresolvedEffects: [],
      recentTail: [
        { role: "user", content: "Continue." },
        { role: "assistant", content: "Checkpoint is ready." },
      ],
    },
  };
  return { base, repo, source, draft, now };
}

test("capsule writers reserve space within the task byte budget", () => {
  const { source, draft, now } = fixture();
  const sessionDir = source.getSessionDir();
  const used = assertTaskSessionByteBudget(sessionDir);
  const filler = path.join(sessionDir, "budget-filler");
  fs.closeSync(fs.openSync(filler, "wx", 0o600));
  fs.truncateSync(filler, MAX_TASK_SESSION_BYTES - used - 1024);
  assert.throws(() => exportCapsule(draft, source, { now }), /byte budget/);
  assert.equal(
    fs.existsSync(
      path.join(sessionDir, ".capsule-exports", `${draft.capsuleId}.json`),
    ),
    false,
  );
});

test("creates a canonical, bounded capsule bound to the active Pi leaf", () => {
  const { source, draft, now } = fixture();
  const envelope = createCapsule(draft, source, { now });
  assert.match(envelope.digest, /^[0-9a-f]{64}$/);
  assert.equal(envelope.digest, capsuleDigest(envelope.capsule));
  assert.equal(validateEnvelope(envelope, { now }), envelope);
  const rendered = renderContinuationContext(envelope.capsule, envelope.digest);
  assert.match(rendered, /^\[BUZZ CONTINUATION CAPSULE v1\]/);
  assert.match(rendered, /Continue the reviewed implementation/);
  assert.doesNotMatch(rendered, /safe source context/);
  assert.doesNotMatch(rendered, /private internal reasoning/);

  const deliveryLeaf = source.appendCustomEntry("buzz.delivery.v1", {
    eventIds: ["a".repeat(64)],
  });
  const deliveredDraft = structuredClone(draft);
  deliveredDraft.pi.sourceLeafId = deliveryLeaf;
  deliveredDraft.pi.lineage[0].leafId = deliveryLeaf;
  assert.doesNotThrow(() => createCapsule(deliveredDraft, source, { now }));
});

test("export receipt recovers the same capsule after output loss", () => {
  const { base, repo, source, draft, now } = fixture();
  const releaseLease = acquireTaskLease(source.getSessionDir(), "active-turn");
  assert.throws(
    () => exportCapsule(draft, source, { now }),
    /task execution or capsule export is active/,
  );
  releaseLease();
  const first = exportCapsule(draft, source, { now });
  fs.unlinkSync(
    path.join(source.getSessionDir(), ".capsule-lineage-head.json"),
  );
  fs.writeFileSync(
    path.join(source.getSessionDir(), ".capsule-lineage.lock"),
    `${first.digest} 2147483647\n`,
    { mode: 0o600 },
  );
  const second = exportCapsule(draft, source, { now });
  assert.deepEqual(second, first);
  assert.equal(
    fs.existsSync(path.join(source.getSessionDir(), ".capsule-lineage.lock")),
    false,
  );
  const receipt = path.join(
    source.getSessionDir(),
    ".capsule-exports",
    `${draft.capsuleId}.json`,
  );
  assert.equal(fs.statSync(receipt).mode & 0o777, 0o600);
  assert.equal(
    JSON.parse(
      fs.readFileSync(
        path.join(source.getSessionDir(), ".capsule-lineage-head.json"),
        "utf8",
      ),
    ).digest,
    first.digest,
  );
  assert.throws(
    () =>
      reissueCapsule(
        first,
        {
          capsuleId: "99999999-9999-4999-8999-999999999999",
          createdAt: new Date(now).toISOString(),
          expiresAt: new Date(now + 60 * 60 * 1000).toISOString(),
        },
        source,
        { now },
      ),
    /requires an expired source capsule/,
  );
  const reissueNow = now + 2 * 60 * 60 * 1000;
  assert.throws(
    () => exportCapsule(draft, source, { now: reissueNow }),
    /receipt expired; use reissue/,
  );
  const reissued = reissueCapsule(
    first,
    {
      capsuleId: "88888888-8888-4888-8888-888888888888",
      createdAt: new Date(reissueNow).toISOString(),
      expiresAt: new Date(reissueNow + 60 * 60 * 1000).toISOString(),
    },
    source,
    { now: reissueNow },
  );
  assert.equal(
    reissued.capsule.pi.parentCapsuleDigest,
    first.capsule.pi.parentCapsuleDigest,
  );
  assert.equal(
    JSON.parse(
      fs.readFileSync(
        path.join(source.getSessionDir(), ".capsule-lineage-head.json"),
        "utf8",
      ),
    ).digest,
    reissued.digest,
  );

  const reissueTargetPath = path.join(base, "reissue-target-repo");
  fs.cpSync(repo, reissueTargetPath, { recursive: true });
  const reissueTarget = fs.realpathSync(reissueTargetPath);
  assert.doesNotThrow(() =>
    importCapsule(reissued, {
      cwd: reissueTarget,
      sessionDir: sessionDirectory(base, draft.task, "reissue-target"),
      expected: expectedFor(draft, reissued.digest),
      now: reissueNow,
    }),
  );

  const staleExport = structuredClone(draft);
  staleExport.capsuleId = "55555555-5555-4555-8555-555555555555";
  assert.throws(
    () => createCapsule(staleExport, source, { now }),
    /not anchored at the current task lineage head/,
  );

  const crossTask = structuredClone(draft);
  crossTask.capsuleId = "66666666-6666-4666-8666-666666666666";
  crossTask.task.threadRoot = "b".repeat(64);
  assert.throws(
    () => exportCapsule(crossTask, source, { now }),
    /does not match the capsule task identity/,
  );

  const otherPath = path.join(base, "other-repo");
  fs.cpSync(repo, otherPath, { recursive: true });
  const otherRepo = fs.realpathSync(otherPath);
  const crossRepository = structuredClone(draft);
  crossRepository.capsuleId = "77777777-7777-4777-8777-777777777777";
  crossRepository.git.repository = otherRepo;
  assert.throws(
    () => exportCapsule(crossRepository, source, { now }),
    /repository does not match the Git binding/,
  );
});

test("raw transcript text is omitted from the portable capsule", () => {
  const { base, repo, draft, now } = fixture();
  const verbose = SessionManager.create(
    repo,
    path.join(base, "verbose-sessions"),
  );
  for (let index = 0; index < 12; index += 1) {
    verbose.appendMessage({
      role: "user",
      content: `user-${index}-${"u".repeat(8 * 1024 - 16)}`,
      timestamp: now + index,
    });
    verbose.appendMessage({
      role: "assistant",
      content: [
        {
          type: "text",
          text: `assistant-${index}-${"a".repeat(8 * 1024 - 24)}`,
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
      stopReason: "stop",
      timestamp: now + index,
    });
  }
  const verboseDraft = structuredClone(draft);
  verboseDraft.pi.sourceSessionId = verbose.getSessionId();
  verboseDraft.pi.sourceLeafId = verbose.getLeafId();
  verboseDraft.pi.lineage = [
    {
      sessionId: verbose.getSessionId(),
      leafId: verbose.getLeafId(),
      location: "local",
    },
  ];
  const envelope = createCapsule(verboseDraft, verbose, { now });
  assert.ok(Buffer.byteLength(JSON.stringify(envelope.capsule)) < 64 * 1024);
  assert.deepEqual(envelope.capsule.context.recentTail, []);
  assert.equal(JSON.stringify(envelope).includes("assistant-11"), false);
});

test("successful terminal Buzz publication is an exportable idle boundary", () => {
  const { base, repo, draft, now } = fixture();
  const published = SessionManager.create(
    repo,
    path.join(base, "published-sessions"),
  );
  published.appendMessage({ role: "user", content: "publish", timestamp: now });
  published.appendMessage({
    role: "assistant",
    content: [
      {
        type: "toolCall",
        id: "reply-call",
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
    timestamp: now,
  });
  published.appendMessage({
    role: "toolResult",
    toolCallId: "reply-call",
    toolName: "buzz_reply",
    content: [{ type: "text", text: "published" }],
    isError: false,
    timestamp: now,
  });
  const publicationDraft = structuredClone(draft);
  publicationDraft.pi.sourceSessionId = published.getSessionId();
  publicationDraft.pi.sourceLeafId = published.getLeafId();
  publicationDraft.pi.lineage = [
    {
      sessionId: published.getSessionId(),
      leafId: published.getLeafId(),
      location: "local",
    },
  ];
  assert.doesNotThrow(() =>
    createCapsule(publicationDraft, published, { now }),
  );

  const ambiguous = SessionManager.create(
    repo,
    path.join(base, "ambiguous-sessions"),
  );
  ambiguous.appendMessage({ role: "user", content: "publish", timestamp: now });
  ambiguous.appendMessage({
    role: "assistant",
    content: [
      {
        type: "toolCall",
        id: "ambiguous-reply",
        name: "buzz_reply",
        arguments: { content: "maybe" },
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
    timestamp: now,
  });
  ambiguous.appendMessage({
    role: "toolResult",
    toolCallId: "ambiguous-reply",
    toolName: "buzz_reply",
    content: [{ type: "text", text: "network result unknown" }],
    isError: true,
    timestamp: now,
  });
  ambiguous.appendMessage({
    role: "assistant",
    content: [{ type: "text", text: "publication failed" }],
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
    timestamp: now,
  });
  const ambiguousDraft = structuredClone(draft);
  ambiguousDraft.pi.sourceSessionId = ambiguous.getSessionId();
  ambiguousDraft.pi.sourceLeafId = ambiguous.getLeafId();
  ambiguousDraft.pi.lineage = [
    {
      sessionId: ambiguous.getSessionId(),
      leafId: ambiguous.getLeafId(),
      location: "local",
    },
  ];
  assert.throws(
    () => createCapsule(ambiguousDraft, ambiguous, { now }),
    /unresolved tool effects/,
  );
});

test("import reserves child-session bytes before starting its effect", () => {
  const { base, repo, source, draft, now } = fixture();
  const envelope = createCapsule(draft, source, { now });
  const targetPath = path.join(base, "budget-target-repo");
  fs.cpSync(repo, targetPath, { recursive: true });
  const target = fs.realpathSync(targetPath);
  const sessionDir = sessionDirectory(base, draft.task, "budget-target");
  const used = assertTaskSessionByteBudget(sessionDir);
  const filler = path.join(sessionDir, "budget-filler");
  fs.closeSync(fs.openSync(filler, "wx", 0o600));
  fs.truncateSync(filler, MAX_TASK_SESSION_BYTES - used - 128 * 1024);
  assert.throws(
    () =>
      importCapsule(envelope, {
        cwd: target,
        sessionDir,
        expected: expectedFor(draft, envelope.digest),
        now,
      }),
    /byte budget/,
  );
  assert.equal(
    fs.existsSync(path.join(sessionDir, ".capsule-lineage.lock")),
    false,
  );
  assert.equal(
    fs.readdirSync(sessionDir).some((name) => name.startsWith("continuation-")),
    false,
  );
});

test("imports one fresh child session and is idempotent by capsule digest", () => {
  const { base, repo, source, draft, now } = fixture();
  const envelope = createCapsule(draft, source, { now });
  const targetPath = path.join(base, "target-repo");
  fs.cpSync(repo, targetPath, { recursive: true });
  const target = fs.realpathSync(targetPath);
  const sessionDir = sessionDirectory(base, draft.task, "target");
  const expected = expectedFor(draft, envelope.digest);
  const first = importCapsule(envelope, {
    cwd: target,
    sessionDir,
    expected,
    now,
  });
  const progressed = SessionManager.open(first.sessionFile);
  progressed.appendMessage({
    role: "user",
    content: "next step",
    timestamp: Date.now(),
  });
  progressed.appendMessage({
    role: "assistant",
    content: [{ type: "text", text: "continued" }],
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
  fs.unlinkSync(path.join(sessionDir, ".capsule-lineage-head.json"));
  fs.writeFileSync(
    path.join(sessionDir, ".capsule-lineage.lock"),
    `${envelope.digest} 2147483647\n`,
    { mode: 0o600 },
  );
  fs.writeFileSync(path.join(target, "post-import-dirty.txt"), "later state\n");
  const second = importCapsule(envelope, {
    cwd: target,
    sessionDir,
    expected,
    now: now + 2 * 60 * 60 * 1000,
  });
  fs.unlinkSync(path.join(target, "post-import-dirty.txt"));
  assert.deepEqual(second, first);
  assert.notEqual(first.sessionId, source.getSessionId());
  const child = SessionManager.open(first.sessionFile);
  const lineage = child
    .getEntries()
    .find(
      (entry) =>
        entry.type === "custom" &&
        entry.customType === "buzz.continuation.lineage.v1",
    );
  assert.equal(lineage.data.parentSessionId, source.getSessionId());
  assert.ok(child.getEntry(first.leafId));
  assert.notEqual(child.getLeafId(), first.leafId);
  assert.equal(
    fs.existsSync(path.join(sessionDir, ".capsule-lineage.lock")),
    false,
  );
  assert.equal(
    JSON.parse(
      fs.readFileSync(
        path.join(sessionDir, ".capsule-lineage-head.json"),
        "utf8",
      ),
    ).digest,
    envelope.digest,
  );
  const liveLock = path.join(sessionDir, ".capsule-lineage.lock");
  fs.unlinkSync(path.join(sessionDir, ".capsule-lineage-head.json"));
  fs.writeFileSync(liveLock, `${envelope.digest} ${process.pid}\n`, {
    mode: 0o600,
  });
  assert.throws(
    () =>
      importCapsule(envelope, {
        cwd: target,
        sessionDir,
        expected,
        now,
      }),
    /still committing its lineage head/,
  );
  assert.equal(fs.existsSync(liveLock), true);
  fs.unlinkSync(liveLock);
  assert.deepEqual(
    importCapsule(envelope, {
      cwd: target,
      sessionDir,
      expected,
      now,
    }),
    first,
  );
  assert.equal(fs.statSync(first.sessionFile).mode & 0o777, 0o600);
  assert.equal(fs.statSync(sessionDir).mode & 0o777, 0o700);

  const competingDraft = structuredClone(draft);
  competingDraft.capsuleId = "44444444-4444-4444-8444-444444444444";
  const competing = createCapsule(competingDraft, source, { now });
  assert.throws(
    () =>
      importCapsule(competing, {
        cwd: target,
        sessionDir,
        expected: expectedFor(draft, competing.digest),
        now,
      }),
    /parent is not the current task lineage head/,
  );
});

test("never exports credential-like text from the visible transcript", () => {
  const { source, draft, now } = fixture();
  source.appendMessage({
    role: "user",
    content: "the password is hunter2",
    timestamp: now,
  });
  source.appendMessage({
    role: "assistant",
    content: [{ type: "text", text: "credential received" }],
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
    timestamp: now,
  });
  draft.pi.sourceLeafId = source.getLeafId();
  draft.pi.lineage[0].leafId = source.getLeafId();
  const envelope = createCapsule(draft, source, { now });
  assert.deepEqual(envelope.capsule.context.recentTail, []);
  assert.equal(JSON.stringify(envelope).includes("hunter2"), false);
});

test("rejects tampering, secrets, stale generation/leaf, effects, and expiry", () => {
  const { base, repo, source, draft, now } = fixture();
  const envelope = createCapsule(draft, source, { now });

  const tampered = structuredClone(envelope);
  tampered.capsule.context.goal = "different";
  assert.throws(() => validateEnvelope(tampered, { now }), /digest mismatch/);

  const legacyTranscript = structuredClone(envelope);
  legacyTranscript.capsule.context.recentTail = [
    {
      role: "user",
      content:
        "AWS secret access key is wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    },
  ];
  legacyTranscript.digest = capsuleDigest(legacyTranscript.capsule);
  assert.throws(
    () => validateEnvelope(legacyTranscript, { now }),
    /recentTail must be empty/,
  );

  const credentialRemote = structuredClone(draft);
  credentialRemote.git.remoteUrl =
    "https://token@github.com/malkovitc/buzz.git";
  assert.throws(
    () => createCapsule(credentialRemote, source, { now }),
    /contains credentials/,
  );
  credentialRemote.git.remoteUrl =
    "https://github.com/malkovitc/buzz.git?access_token=secret";
  assert.throws(
    () => createCapsule(credentialRemote, source, { now }),
    /contains credentials/,
  );

  for (const secretText of [
    `token nsec1${"q".repeat(40)}`,
    "OPENAI_API_KEY=sk-legacy-example-value",
    "ANTHROPIC_API_KEY=example-secret-value",
    "PASSWORD=hunter2",
    "the password is hunter2",
    "The password for production is hunter2",
    "Wallet recovery phrase is abandon abandon abandon about",
    "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about",
    "あいこくしん　あいこくしん　あいこくしん　あいこくしん　あいこくしん　あいこくしん　あいこくしん　あいこくしん　あいこくしん　あいこくしん　あいこくしん　あおぞら",
    "的 一 是 在 不 了 有 和 人 这 中 大",
    "AWS secret access key is wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "AWS secret access key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    "AKIAIOSFODNN7EXAMPLE",
    "xoxb-123456789012-123456789012-abcdefghijklmnopqrstuvwx",
    "f".repeat(64),
    "TOKEN=secret-value",
    "API_KEY=secret-value",
    "DATABASE_URL=postgres://alice:hunter2@db.example/app",
    "sk-svcacct-1234567890abcdefghijklmnop",
    "sk-1234567890abcdefghijklmnop",
  ]) {
    const secret = structuredClone(draft);
    secret.context.pending = [secretText];
    assert.throws(
      () => createCapsule(secret, source, { now }),
      /forbidden secret-like/,
    );
  }

  const oversized = structuredClone(draft);
  oversized.context.goal = "x".repeat(9 * 1024);
  assert.throws(
    () => createCapsule(oversized, source, { now }),
    /exceeds 8192 bytes/,
  );

  const unresolved = structuredClone(draft);
  unresolved.context.unresolvedEffects = ["publication pending"];
  assert.throws(
    () => createCapsule(unresolved, source, { now }),
    /unresolved effects/,
  );

  const invented = structuredClone(draft);
  invented.pi.lineage.unshift({
    sessionId: "invented-session",
    leafId: "deadbeef",
    location: "cloud",
  });
  assert.throws(
    () => createCapsule(invented, source, { now }),
    /persisted Pi lineage head/,
  );

  const stale = structuredClone(draft);
  stale.pi.sourceLeafId = "deadbeef";
  stale.pi.lineage[0].leafId = "deadbeef";
  assert.throws(
    () => createCapsule(stale, source, { now }),
    /lineage is stale/,
  );

  assert.throws(
    () =>
      importCapsule(envelope, {
        cwd: repo,
        sessionDir: sessionDirectory(base, draft.task, "stale-target"),
        expected: {
          ...expectedFor(draft, envelope.digest),
          generation: "33333333-3333-4333-8333-333333333333",
        },
        now,
      }),
    /binding is stale/,
  );

  assert.throws(
    () => validateEnvelope(envelope, { now: now + 2 * 60 * 60 * 1000 }),
    /expired/,
  );
});

test("import fails closed for dirty Git and an ambiguous prior import", () => {
  const { base, repo, source, draft, now } = fixture();
  const envelope = createCapsule(draft, source, { now });
  fs.writeFileSync(path.join(repo, "dirty.txt"), "dirty\n");
  assert.throws(
    () =>
      importCapsule(envelope, {
        cwd: repo,
        sessionDir: sessionDirectory(base, draft.task, "dirty-target"),
        expected: expectedFor(draft, envelope.digest),
        now,
      }),
    /worktree is dirty/,
  );
  fs.unlinkSync(path.join(repo, "dirty.txt"));

  const sessionDir = sessionDirectory(base, draft.task, "locked-target");
  const imports = path.join(sessionDir, ".capsule-imports");
  fs.mkdirSync(imports, { recursive: true });
  fs.writeFileSync(
    path.join(sessionDir, ".capsule-lineage.lock"),
    "interrupted\n",
  );
  assert.throws(
    () =>
      importCapsule(envelope, {
        cwd: repo,
        sessionDir,
        expected: expectedFor(draft, envelope.digest),
        now,
      }),
    /concurrent or previously interrupted/,
  );
});
