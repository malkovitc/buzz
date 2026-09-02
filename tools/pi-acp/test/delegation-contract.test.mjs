import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import {
  canonicalJson,
  capsuleDigest,
} from "../src/continuation-canonical.mjs";
import {
  delegationOfferEnvelope,
  renderDelegationDecision,
  renderDelegationOffer,
  validateDelegationDecisionEvent,
} from "../src/delegation-contract.mjs";
import {
  createFenceProof,
  GRANT_MARKER,
  protocolDigest,
  renderDelegationGrant,
  renderDelegationReady,
  validateDelegationGrantEvent,
  validateDelegationReadyEvent,
} from "../src/delegation-grant-contract.mjs";
import {
  activateTarget,
  commitGrant,
  prepareGrant,
  testOnly,
} from "../src/delegation-host.mjs";

const SOURCE = "11".repeat(32);
const TARGET = "22".repeat(32);
const AGENT = "33".repeat(32);
const ROOT = "44".repeat(32);
const OFFER_EVENT = "55".repeat(32);
const DECISION_EVENT = "66".repeat(32);
const READY_EVENT = "77".repeat(32);
const GRANT_EVENT = "88".repeat(32);
const GENERATION = "11111111-1111-4111-8111-111111111111";
const PROOF_KEY = "dd".repeat(32);
const STALE_LOCK =
  '{"childPid":null,"childProcessIdentity":null,"pid":99999999,"processIdentity":"x","schemaVersion":1}\n';
const NOW = Date.now();
const CREATED = new Date(NOW - 10_000).toISOString();
const DECIDED = new Date(NOW - 8_000).toISOString();
const READY_AT = new Date(NOW - 5_000).toISOString();
const OBSERVED_AT = new Date(NOW - 4_000).toISOString();
const GRANTED_AT = new Date(NOW - 3_000).toISOString();
const EXPIRES = new Date(NOW + 60_000).toISOString();

function capsuleEnvelope() {
  const capsule = {
    schemaVersion: 1,
    capsuleId: "22222222-2222-4222-8222-222222222222",
    createdAt: CREATED,
    expiresAt: EXPIRES,
    task: {
      relayUrl: "wss://workspace.example",
      agentPubkey: AGENT,
      channelId: "33333333-3333-4333-8333-333333333333",
      threadRoot: ROOT,
    },
    git: {
      repository: "/work/repo",
      remoteName: "fork",
      remoteUrl: "https://github.com/example/repo.git",
      branch: "feat/delegation",
      commit: "99".repeat(20),
      tree: "aa".repeat(20),
    },
    ownership: {
      generation: GENERATION,
      sourceLocation: "local",
      targetLocation: "cloud",
    },
    pi: {
      sourceSessionId: "session-source",
      sourceLeafId: "a1b2c3d4",
      lineage: [
        {
          sessionId: "session-source",
          leafId: "a1b2c3d4",
          location: "local",
        },
      ],
      parentCapsuleDigest: null,
    },
    context: {
      goal: "Continue one bounded task",
      constraints: ["Use one writer"],
      decisions: [],
      completed: ["Checkpoint exported"],
      pending: ["Target readiness"],
      files: [],
      checks: [],
      blockers: [],
      unresolvedEffects: [],
      recentTail: [],
    },
  };
  return { capsule, digest: capsuleDigest(capsule) };
}

function ownershipEvidence() {
  return {
    relayUrl: "wss://workspace.example",
    agentPubkey: AGENT,
    ownerPubkey: SOURCE,
    generation: GENERATION,
    location: "local",
  };
}

function offerDraft(capsule) {
  return {
    schemaVersion: 1,
    offerId: "44444444-4444-4444-8444-444444444444",
    createdAt: CREATED,
    expiresAt: EXPIRES,
    capsuleDigest: capsule.digest,
    source: {
      ownerPubkey: SOURCE,
      generation: GENERATION,
      location: "local",
    },
    target: { ownerPubkey: TARGET, location: "cloud" },
    task: capsule.capsule.task,
    git: {
      remoteUrl: capsule.capsule.git.remoteUrl,
      branch: capsule.capsule.git.branch,
      commit: capsule.capsule.git.commit,
      tree: capsule.capsule.git.tree,
    },
    capabilities: {
      repository: capsule.capsule.git.remoteUrl,
      branch: capsule.capsule.git.branch,
      tools: ["bash", "read"],
      effects: ["git:write"],
    },
  };
}

function offerEnvelope(capsule = capsuleEnvelope()) {
  return delegationOfferEnvelope(offerDraft(capsule), {
    capsuleEnvelope: capsule,
    ownershipEvidence: ownershipEvidence(),
    now: NOW,
  });
}

function offerEvent(offer) {
  return {
    id: OFFER_EVENT,
    pubkey: SOURCE,
    kind: 9,
    content: renderDelegationOffer(offer),
    tags: [
      ["h", offer.offer.task.channelId],
      ["e", ROOT, "", "reply"],
      ["p", TARGET],
      ["p", AGENT],
    ],
    signatureVerified: true,
  };
}

function decisionValue(offer, decision = "accept") {
  return {
    schemaVersion: 1,
    offerId: offer.offer.offerId,
    offerEventId: OFFER_EVENT,
    offerDigest: offer.digest,
    sourceGeneration: GENERATION,
    activationGeneration: offer.activationGeneration,
    decision,
    decidedAt: DECIDED,
  };
}

function decisionEvent(offer, decision = "accept") {
  return {
    id: DECISION_EVENT,
    pubkey: decision === "cancel" ? SOURCE : TARGET,
    kind: 9,
    content: renderDelegationDecision(decisionValue(offer, decision), offer, {
      now: NOW,
    }),
    tags: [
      ["h", offer.offer.task.channelId],
      ["e", ROOT, "", "root"],
      ["e", OFFER_EVENT, "", "reply"],
      ["p", SOURCE],
      ["p", TARGET],
      ["p", AGENT],
    ],
    signatureVerified: true,
  };
}

function admittedDecision(offer = offerEnvelope()) {
  return validateDelegationDecisionEvent(
    decisionEvent(offer),
    offerEvent(offer),
    { ownershipEvidence: ownershipEvidence(), now: NOW },
  );
}

function readyValue(admitted) {
  const offer = admitted.offer.envelope;
  return {
    schemaVersion: 1,
    offerEventId: admitted.offer.event.id,
    decisionEventId: admitted.event.id,
    offerDigest: offer.digest,
    capsuleDigest: offer.offer.capsuleDigest,
    sourceGeneration: offer.offer.source.generation,
    activationGeneration: offer.activationGeneration,
    importReceiptDigest: "bb".repeat(32),
    readyAt: READY_AT,
  };
}

function readyEvent(admitted) {
  const offer = admitted.offer.envelope.offer;
  return {
    id: READY_EVENT,
    pubkey: TARGET,
    kind: 9,
    content: renderDelegationReady(readyValue(admitted), admitted, {
      now: NOW,
    }),
    tags: [
      ["h", offer.task.channelId],
      ["e", ROOT, "", "root"],
      ["e", DECISION_EVENT, "", "reply"],
      ["p", SOURCE],
      ["p", TARGET],
      ["p", AGENT],
    ],
    signatureVerified: true,
  };
}

function admittedReady(offer = offerEnvelope()) {
  const decision = admittedDecision(offer);
  return validateDelegationReadyEvent(
    readyEvent(decision),
    decisionEvent(offer),
    offerEvent(offer),
    { ownershipEvidence: ownershipEvidence(), now: NOW },
  );
}

function fenceEvidence(ready) {
  return {
    sourceGeneration: ready.ready.sourceGeneration,
    activationGeneration: ready.ready.activationGeneration,
    stateDigest: "cc".repeat(32),
    readyObservedAt: OBSERVED_AT,
  };
}

function grantValue(ready) {
  const evidence = fenceEvidence(ready);
  const grant = {
    schemaVersion: 1,
    offerEventId: ready.decision.offer.event.id,
    decisionEventId: ready.decision.event.id,
    readyEventId: ready.event.id,
    offerDigest: ready.ready.offerDigest,
    readyDigest: protocolDigest(ready.ready),
    capsuleDigest: ready.ready.capsuleDigest,
    sourceGeneration: ready.ready.sourceGeneration,
    activationGeneration: ready.ready.activationGeneration,
    fencedStateDigest: evidence.stateDigest,
    ownershipDigest: protocolDigest(ownershipEvidence()),
    readyObservedAt: evidence.readyObservedAt,
    grantedAt: GRANTED_AT,
  };
  return { ...grant, fenceProof: createFenceProof(grant, PROOF_KEY) };
}

function grantEvent(ready) {
  const offer = ready.decision.offer.envelope.offer;
  const evidence = fenceEvidence(ready);
  return {
    id: GRANT_EVENT,
    pubkey: AGENT,
    kind: 9,
    content: renderDelegationGrant(
      grantValue(ready),
      ready,
      evidence,
      PROOF_KEY,
      ownershipEvidence(),
    ),
    tags: [
      ["h", offer.task.channelId],
      ["e", ROOT, "", "root"],
      ["e", READY_EVENT, "", "reply"],
      ["p", SOURCE],
      ["p", TARGET],
      ["p", AGENT],
    ],
    signatureVerified: true,
  };
}

test("offer, accept, READY, and fenced grant form one exact lineage", () => {
  const offer = offerEnvelope();
  const ready = admittedReady(offer);
  const grant = validateDelegationGrantEvent(
    grantEvent(ready),
    readyEvent(ready.decision),
    decisionEvent(offer),
    offerEvent(offer),
    PROOF_KEY,
    { ownershipEvidence: ownershipEvidence() },
  );
  assert.equal(grant.grant.activationGeneration, offer.activationGeneration);
  assert.equal(grant.grant.readyDigest, protocolDigest(ready.ready));
});

test("wrong principals, community, routing, lineage, and expiry fail closed", () => {
  const offer = offerEnvelope();
  const decision = admittedDecision(offer);
  const admitReady = (event, options = {}) =>
    validateDelegationReadyEvent(
      event,
      decisionEvent(offer),
      offerEvent(offer),
      { ownershipEvidence: ownershipEvidence(), now: NOW, ...options },
    );
  const sameLocation = offerDraft(capsuleEnvelope());
  sameLocation.source.location = "cloud";
  assert.throws(
    () =>
      delegationOfferEnvelope(sameLocation, {
        ownershipEvidence: { ...ownershipEvidence(), location: "cloud" },
        now: NOW,
      }),
    /source and target locations must differ/,
  );
  assert.throws(
    () => admitReady({ ...readyEvent(decision), pubkey: SOURCE }),
    /signer is not the target owner/,
  );
  assert.throws(
    () =>
      admitReady({
        ...readyEvent(decision),
        tags: [...readyEvent(decision).tags, "bad"],
      }),
    /tags are invalid/,
  );
  assert.throws(
    () =>
      admitReady({
        ...readyEvent(decision),
        tags: [
          ["h", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"],
          ...readyEvent(decision).tags,
        ],
      }),
    /routing is invalid/,
  );
  assert.throws(
    () =>
      admitReady(readyEvent(decision), {
        ownershipEvidence: {
          ...ownershipEvidence(),
          relayUrl: "wss://other.example",
        },
      }),
    /authoritative ownership/,
  );
  assert.throws(
    () => admitReady(readyEvent(decision), { now: Date.parse(EXPIRES) + 1 }),
    /expired/,
  );
  const ready = admittedReady(offer);
  assert.throws(
    () =>
      validateDelegationGrantEvent(
        { ...grantEvent(ready), pubkey: TARGET },
        readyEvent(decision),
        decisionEvent(offer),
        offerEvent(offer),
        PROOF_KEY,
        { ownershipEvidence: ownershipEvidence() },
      ),
    /signer is not the fenced source agent/,
  );
  const forgedGrant = { ...grantValue(ready), fenceProof: "ee".repeat(32) };
  assert.throws(
    () =>
      validateDelegationGrantEvent(
        {
          ...grantEvent(ready),
          content: `${GRANT_MARKER}\n${canonicalJson(forgedGrant)}`,
        },
        readyEvent(decision),
        decisionEvent(offer),
        offerEvent(offer),
        PROOF_KEY,
        { ownershipEvidence: ownershipEvidence() },
      ),
    /fence proof is invalid/,
  );
});

function hostConfig(role, stateDirectory) {
  return {
    schemaVersion: 1,
    role,
    stateDirectory,
    fenceProofKeyCommand: ["/fake/key"],
    decisionReadCommand: role === "source" ? ["/fake/decisions"] : null,
    sourceFenceCommand: role === "source" ? ["/fake/fence"] : null,
    targetActivateCommand: role === "target" ? ["/fake/activate"] : null,
  };
}

function protocolCommandResult(command, offer) {
  if (command[0] === "/fake/key") return { key: PROOF_KEY };
  if (command[0] === "/fake/decisions") {
    return { events: [decisionEvent(offer)] };
  }
  return null;
}

function hostRequest(offer, decision) {
  return {
    offerEvent: offerEvent(offer),
    readyEvent: readyEvent(decision),
    ownershipEvidence: ownershipEvidence(),
  };
}

test("source fence is durable before grant publication and retries idempotently", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-source-"));
  try {
    const offer = offerEnvelope();
    const decision = admittedDecision(offer);
    const request = hostRequest(offer, decision);
    let fenceCalls = 0;
    let decisionsRead = false;
    const fence = async (command, input) => {
      if (command[0] === "/fake/decisions") decisionsRead = true;
      const key = protocolCommandResult(command, offer);
      if (key) return key;
      fenceCalls += 1;
      return {
        status: "fenced",
        sourceGeneration: input.sourceGeneration,
        activationGeneration: input.activationGeneration,
        stateDigest: "cc".repeat(32),
      };
    };
    const prepared = await prepareGrant(request, hostConfig("source", root), {
      now: () => {
        assert.equal(decisionsRead, true);
        return NOW;
      },
      commandRunner: fence,
    });
    assert.equal(prepared.status, "publish");
    assert.equal(fenceCalls, 1);
    const duplicate = await prepareGrant(request, hostConfig("source", root), {
      now: NOW,
      commandRunner: fence,
    });
    assert.equal(duplicate.content, prepared.content);
    assert.equal(fenceCalls, 1);
    const recovered = await prepareGrant(request, hostConfig("source", root), {
      now: Date.parse(EXPIRES) + 1,
      commandRunner: fence,
    });
    assert.equal(recovered.content, prepared.content);
    assert.equal(fenceCalls, 1);

    const ready = admittedReady(offer);
    const event = { ...grantEvent(ready), content: prepared.content };
    const grantRequest = {
      ...request,
      decisionEvent: decisionEvent(offer),
      ownershipEvidence: {
        ...request.ownershipEvidence,
        ownerPubkey: TARGET,
        generation: ready.ready.activationGeneration,
        location: "cloud",
      },
    };
    const committed = await commitGrant(
      {
        ...grantRequest,
        operationId: prepared.operationId,
        grantEvent: event,
      },
      hostConfig("source", root),
      { commandRunner: fence },
    );
    assert.equal(committed.status, "published");
    assert.equal(
      (
        await commitGrant(
          {
            ...grantRequest,
            operationId: prepared.operationId,
            grantEvent: event,
          },
          hostConfig("source", root),
          { commandRunner: fence },
        )
      ).status,
      "noop",
    );
    const publishedRetry = await prepareGrant(
      request,
      hostConfig("source", root),
      {
        now: Date.parse(EXPIRES) + 1,
        commandRunner: fence,
      },
    );
    assert.equal(publishedRetry.status, "noop");
    assert.equal(publishedRetry.grantEventId, event.id);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("authoritative cancellation blocks fencing", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-cancel-"));
  try {
    const offer = offerEnvelope();
    const request = hostRequest(offer, admittedDecision(offer));
    let fenceCalls = 0;
    const runner = async (command, input) => {
      if (command[0] === "/fake/key") return { key: PROOF_KEY };
      if (command[0] === "/fake/decisions") {
        return {
          events: [
            decisionEvent(offer),
            {
              ...decisionEvent(offer, "cancel"),
              id: "ab".repeat(32),
            },
          ],
        };
      }
      fenceCalls += 1;
      return {
        status: "fenced",
        sourceGeneration: input.sourceGeneration,
        activationGeneration: input.activationGeneration,
        stateDigest: "cc".repeat(32),
      };
    };
    await assert.rejects(
      prepareGrant(request, hostConfig("source", root), {
        now: NOW,
        commandRunner: runner,
      }),
      /offer is canceled/,
    );
    assert.equal(fenceCalls, 0);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("concurrent READY processing invokes one fence command", async () => {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), "pi-delegation-concurrent-"),
  );
  try {
    const offer = offerEnvelope();
    const request = hostRequest(offer, admittedDecision(offer));
    let releaseFence;
    let reportStarted;
    const started = new Promise((resolve) => {
      reportStarted = resolve;
    });
    const waitForRelease = new Promise((resolve) => {
      releaseFence = resolve;
    });
    let fenceCalls = 0;
    const fence = async (command, input) => {
      const key = protocolCommandResult(command, offer);
      if (key) return key;
      fenceCalls += 1;
      reportStarted();
      await waitForRelease;
      return {
        status: "fenced",
        sourceGeneration: input.sourceGeneration,
        activationGeneration: input.activationGeneration,
        stateDigest: "cc".repeat(32),
      };
    };
    const first = prepareGrant(request, hostConfig("source", root), {
      now: NOW,
      commandRunner: fence,
    });
    await started;
    await assert.rejects(
      prepareGrant(request, hostConfig("source", root), {
        now: NOW,
        commandRunner: fence,
      }),
      /operation is concurrent/,
    );
    releaseFence();
    assert.equal((await first).status, "publish");
    assert.equal(fenceCalls, 1);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("stale lock recovery admits one retry", async () => {
  const seedRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "pi-delegation-seed-"),
  );
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-stale-"));
  try {
    const offer = offerEnvelope();
    const request = hostRequest(offer, admittedDecision(offer));
    const baseRunner = async (command, input) => {
      const protocol = protocolCommandResult(command, offer);
      if (protocol) return protocol;
      return {
        status: "fenced",
        sourceGeneration: input.sourceGeneration,
        activationGeneration: input.activationGeneration,
        stateDigest: "cc".repeat(32),
      };
    };
    const operationId = (
      await prepareGrant(request, hostConfig("source", seedRoot), {
        now: NOW,
        commandRunner: baseRunner,
      })
    ).operationId;
    fs.writeFileSync(path.join(root, `.${operationId}.lock`), STALE_LOCK, {
      mode: 0o600,
    });
    let fenceCalls = 0;
    const runner = async (command, input) => {
      const protocol = protocolCommandResult(command, offer);
      if (protocol) return protocol;
      fenceCalls += 1;
      await new Promise((resolve) => setTimeout(resolve, 30));
      return {
        status: "fenced",
        sourceGeneration: input.sourceGeneration,
        activationGeneration: input.activationGeneration,
        stateDigest: "cc".repeat(32),
      };
    };
    const results = await Promise.allSettled([
      prepareGrant(request, hostConfig("source", root), {
        now: NOW,
        commandRunner: runner,
      }),
      prepareGrant(request, hostConfig("source", root), {
        now: NOW,
        commandRunner: runner,
      }),
    ]);
    assert.equal(
      results.filter((result) => result.status === "fulfilled").length,
      1,
    );
    assert.equal(
      results.filter((result) => result.status === "rejected").length,
      1,
    );
    assert.equal(fenceCalls, 1);
    fs.writeFileSync(path.join(root, `.${operationId}.lock`), STALE_LOCK, {
      mode: 0o600,
    });
    const recovery = path.join(root, `.${operationId}.recovery`);
    fs.mkdirSync(recovery, { mode: 0o700 });
    await assert.rejects(
      prepareGrant(request, hostConfig("source", root), {
        now: NOW,
        commandRunner: runner,
      }),
      /lock recovery is concurrent or interrupted/,
    );
    fs.rmdirSync(recovery);
    assert.equal(
      (
        await prepareGrant(request, hostConfig("source", root), {
          now: NOW,
          commandRunner: runner,
        })
      ).status,
      "publish",
    );
    assert.equal(fs.existsSync(path.join(root, `.${operationId}.lock`)), false);
    assert.equal(fenceCalls, 1);
  } finally {
    fs.rmSync(seedRoot, { recursive: true, force: true });
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("interrupted fence is retried without releasing grant content", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-fence-"));
  try {
    const offer = offerEnvelope();
    const decision = admittedDecision(offer);
    const request = hostRequest(offer, decision);
    let fenceCalls = 0;
    const fence = async (command, input) => {
      const key = protocolCommandResult(command, offer);
      if (key) return key;
      fenceCalls += 1;
      if (fenceCalls === 1) {
        const stateFile = fs
          .readdirSync(root)
          .find((entry) => entry.endsWith(".json"));
        assert.equal(
          JSON.parse(fs.readFileSync(path.join(root, stateFile))).phase,
          "fencing",
        );
        throw new Error("interrupted after fence effect");
      }
      return {
        status: "fenced",
        sourceGeneration: input.sourceGeneration,
        activationGeneration: input.activationGeneration,
        stateDigest: "cc".repeat(32),
      };
    };
    await assert.rejects(
      prepareGrant(request, hostConfig("source", root), {
        now: NOW,
        commandRunner: fence,
      }),
      /interrupted after fence effect/,
    );
    assert.equal(
      fs.readdirSync(root).filter((entry) => entry.endsWith(".json")).length,
      1,
    );
    assert.equal(
      (
        await prepareGrant(request, hostConfig("source", root), {
          now: Date.parse(EXPIRES) + 1,
          commandRunner: fence,
        })
      ).status,
      "publish",
    );
    assert.equal(fenceCalls, 2);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("state capacity fails before a fence effect", async () => {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), "pi-delegation-capacity-"),
  );
  try {
    for (let index = 0; index < 512; index += 1) {
      fs.writeFileSync(path.join(root, `${index}.json`), "{}", { mode: 0o600 });
    }
    const offer = offerEnvelope();
    const request = hostRequest(offer, admittedDecision(offer));
    let fenceCalls = 0;
    const runner = async (command, input) => {
      const protocol = protocolCommandResult(command, offer);
      if (protocol) return protocol;
      fenceCalls += 1;
      return {
        status: "fenced",
        sourceGeneration: input.sourceGeneration,
        activationGeneration: input.activationGeneration,
        stateDigest: "cc".repeat(32),
      };
    };
    await assert.rejects(
      prepareGrant(request, hostConfig("source", root), {
        now: NOW,
        commandRunner: runner,
      }),
      /state capacity is exhausted/,
    );
    assert.equal(fenceCalls, 0);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("capacity reservations serialize distinct operations", () => {
  const root = fs.mkdtempSync(
    path.join(os.tmpdir(), "pi-delegation-reservation-"),
  );
  try {
    const release = testOnly.reserveStateCapacity(root);
    assert.throws(
      () => testOnly.reserveStateCapacity(root),
      /operation is concurrent/,
    );
    release.release();
    testOnly.reserveStateCapacity(root).release();
    const state = path.join(root, "durable.json");
    fs.writeFileSync(state, '{"status":"fenced"}\n', { mode: 0o600 });
    const originalFsync = fs.fsyncSync;
    let barriers = 0;
    fs.fsyncSync = (descriptor) => {
      barriers += 1;
      if (barriers === 2) throw new Error("simulated directory fsync failure");
      return originalFsync(descriptor);
    };
    try {
      assert.throws(() => testOnly.readDurableJson(state), /fsync failure/);
    } finally {
      fs.fsyncSync = originalFsync;
    }
    assert.deepEqual(testOnly.readDurableJson(state), { status: "fenced" });
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("fixed command malformed JSON and oversized descendants are contained", async () => {
  await assert.rejects(
    testOnly.runVector(
      [process.execPath, "-e", "process.stdin.destroy();process.exit(0)"],
      { payload: "x".repeat(4 * 1024 * 1024) },
    ),
    /command (rejected its input|failed)/,
  );
  await assert.rejects(
    testOnly.runVector(
      [process.execPath, "-e", "process.stdout.write('not-json')"],
      {},
    ),
    /returned invalid JSON/,
  );
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-child-"));
  const marker = path.join(root, "survived");
  try {
    const script = [
      "const fs=require('node:fs')",
      "process.stdout.write('x'.repeat(70000))",
      "setTimeout(()=>fs.writeFileSync(process.argv[1],'alive'),100)",
      "setInterval(()=>{},1000)",
    ].join(";");
    await assert.rejects(
      testOnly.runVector([process.execPath, "-e", script, marker], {}),
      /output is too large/,
    );
    await new Promise((resolve) => setTimeout(resolve, 200));
    assert.equal(fs.existsSync(marker), false);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("target command is unreachable before an admitted grant and runs once", async () => {
  const sourceRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "pi-delegation-source-"),
  );
  const targetRoot = fs.mkdtempSync(
    path.join(os.tmpdir(), "pi-delegation-target-"),
  );
  try {
    const offer = offerEnvelope();
    const decision = admittedDecision(offer);
    const request = hostRequest(offer, decision);
    const prepared = await prepareGrant(
      request,
      hostConfig("source", sourceRoot),
      {
        now: NOW,
        commandRunner: async (command, input) => {
          const key = protocolCommandResult(command, offer);
          if (key) return key;
          return {
            status: "fenced",
            sourceGeneration: input.sourceGeneration,
            activationGeneration: input.activationGeneration,
            stateDigest: "cc".repeat(32),
          };
        },
      },
    );
    const ready = admittedReady(offer);
    const event = { ...grantEvent(ready), content: prepared.content };
    const grantRequest = { ...request, decisionEvent: decisionEvent(offer) };
    let activationCalls = 0;
    let crashAfterFirstEffect = true;
    const activate = async (command, input) => {
      const key = protocolCommandResult(command, offer);
      if (key) return key;
      activationCalls += 1;
      if (crashAfterFirstEffect) {
        crashAfterFirstEffect = false;
        throw new Error("simulated crash after target effect");
      }
      return {
        status: "active",
        activationGeneration: input.activationGeneration,
      };
    };
    await assert.rejects(
      activateTarget(
        {
          ...grantRequest,
          grantEvent: { ...event, signatureVerified: false },
        },
        hostConfig("target", targetRoot),
        { commandRunner: activate },
      ),
      /signature is not verified/,
    );
    assert.equal(activationCalls, 0);
    await assert.rejects(
      activateTarget(
        { ...grantRequest, grantEvent: event },
        hostConfig("target", targetRoot),
        { commandRunner: activate },
      ),
      /simulated crash after target effect/,
    );
    assert.equal(activationCalls, 1);
    const transferredOwnership = {
      ...ownershipEvidence(),
      ownerPubkey: TARGET,
      generation: ready.ready.activationGeneration,
      location: "cloud",
    };
    assert.equal(
      (
        await activateTarget(
          {
            ...grantRequest,
            grantEvent: event,
            ownershipEvidence: transferredOwnership,
          },
          hostConfig("target", targetRoot),
          { commandRunner: activate },
        )
      ).status,
      "active",
    );
    assert.equal(activationCalls, 2);
    const equivalentEvent = { ...event, id: "89".repeat(32) };
    assert.equal(
      (
        await activateTarget(
          {
            ...grantRequest,
            grantEvent: equivalentEvent,
            ownershipEvidence: transferredOwnership,
          },
          hostConfig("target", targetRoot),
          { commandRunner: activate },
        )
      ).status,
      "noop",
    );
    assert.equal(activationCalls, 2);
    const stateFile = fs
      .readdirSync(targetRoot)
      .find((entry) => entry.endsWith(".json"));
    assert.ok(stateFile);
    const statePath = path.join(targetRoot, stateFile);
    const corruptState = JSON.parse(fs.readFileSync(statePath, "utf8"));
    fs.writeFileSync(
      statePath,
      `${JSON.stringify({ ...corruptState, phase: "broken" })}\n`,
      { mode: 0o600 },
    );
    await assert.rejects(
      activateTarget(
        {
          ...grantRequest,
          grantEvent: event,
          ownershipEvidence: transferredOwnership,
        },
        hostConfig("target", targetRoot),
        { commandRunner: activate },
      ),
      /target delegation state is invalid/,
    );
    assert.equal(activationCalls, 2);
  } finally {
    fs.rmSync(sourceRoot, { recursive: true, force: true });
    fs.rmSync(targetRoot, { recursive: true, force: true });
  }
});
