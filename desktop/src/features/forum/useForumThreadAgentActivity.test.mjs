import assert from "node:assert/strict";
import { beforeEach, describe, it } from "node:test";

import {
  clearActiveTurnsForAgent,
  getActiveTurnsByChannel,
  resetActiveAgentTurnsStore,
  syncAgentTurnsFromEvents,
} from "@/features/agents/activeAgentTurnsStore";
import {
  mergeForumThreadTypingPubkeys,
  resolveForumThreadAgentActivity,
} from "./useForumThreadAgentActivity.ts";

const AGENT = {
  name: "Conductor",
  pubkey: "abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234abcd1234",
};

function event(seq, kind, overrides = {}) {
  return {
    seq,
    timestamp: "2026-08-25T16:00:00Z",
    kind,
    agentIndex: 0,
    channelId: "forum-channel",
    sessionId: "session",
    turnId: "turn-a",
    payload: null,
    ...overrides,
  };
}

const THREAD_A = new Set(["post-a", "mention-a"]);
const resolve = (threadEventIds = THREAD_A, channelId = "forum-channel") =>
  resolveForumThreadAgentActivity(
    [AGENT],
    getActiveTurnsByChannel(),
    channelId,
    threadEventIds,
  ).workingAgentPubkeys;
const start = (overrides = {}) =>
  event(1, "turn_started", {
    payload: {
      triggeringEventIds: ["mention-a"],
      associationEventIds: ["mention-a", "post-a"],
    },
    ...overrides,
  });

describe("Forum thread agent activity", () => {
  beforeEach(resetActiveAgentTurnsStore);

  it("matches an active turn only to its triggering Forum thread", () => {
    syncAgentTurnsFromEvents(AGENT.pubkey, [start()]);
    assert.deepEqual(resolve(), [AGENT.pubkey]);
    assert.deepEqual(resolve(new Set(["post-b"])), []);
    assert.deepEqual(resolve(new Set(["post-a"])), [AGENT.pubkey]);
    assert.deepEqual(resolve(THREAD_A, "other"), []);
  });

  it("extends association when a native steer joins another Forum thread", () => {
    syncAgentTurnsFromEvents(AGENT.pubkey, [
      start(),
      event(2, "turn_association_update", {
        payload: { associationEventIds: ["mention-b", "post-b"] },
      }),
    ]);
    const activity = resolveForumThreadAgentActivity(
      [AGENT],
      getActiveTurnsByChannel(),
      "forum-channel",
      new Set(["post-b"]),
    );
    assert.deepEqual(activity.workingAgentPubkeys, [AGENT.pubkey]);
    assert.deepEqual(activity.workingTurnIds, ["turn-a"]);
  });

  it("recovers a dropped steer update from later liveness", () => {
    syncAgentTurnsFromEvents(AGENT.pubkey, [
      start(),
      event(2, "turn_liveness", {
        payload: { associationEventIds: ["mention-a", "post-a", "post-b"] },
      }),
    ]);
    assert.deepEqual(resolve(new Set(["post-b"])), [AGENT.pubkey]);
  });

  it("keeps thread-scoped typing as the observer fallback", () => {
    assert.deepEqual(
      mergeForumThreadTypingPubkeys([AGENT.pubkey], ["remote-a"]),
      [AGENT.pubkey, "remote-a"],
    );
  });

  it("retains association across journal-sized activity and clears on stop", () => {
    syncAgentTurnsFromEvents(AGENT.pubkey, [start()]);
    syncAgentTurnsFromEvents(
      AGENT.pubkey,
      Array.from({ length: 3_100 }, (_, index) => event(index + 2, "acp_read")),
    );
    assert.deepEqual(resolve(), [AGENT.pubkey]);
    clearActiveTurnsForAgent(AGENT.pubkey);
    assert.deepEqual(resolve(), []);
  });

  it("scopes null-ID terminals to their channel", () => {
    syncAgentTurnsFromEvents(AGENT.pubkey, [
      start(),
      start({ seq: 2, channelId: "other", turnId: "turn-b" }),
      event(3, "turn_error", { channelId: "other", turnId: null }),
    ]);
    assert.deepEqual(resolve(), [AGENT.pubkey]);
  });

  it("clears completed turns and community resets", () => {
    syncAgentTurnsFromEvents(AGENT.pubkey, [
      start(),
      event(2, "turn_completed"),
    ]);
    assert.deepEqual(resolve(), []);
    syncAgentTurnsFromEvents(AGENT.pubkey, [start({ seq: 3 })]);
    resetActiveAgentTurnsStore();
    assert.deepEqual(resolve(), []);
  });
});
