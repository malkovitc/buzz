import assert from "node:assert/strict";
import test from "node:test";

import {
  CALIPER_CLOUD_CONTROL_PUBKEY,
  cloudControlWireCommand,
} from "@/features/messages/lib/cloudControlCommands";
import { submitComposerCloudControl } from "./submitComposerCloudControl.ts";

const threadContext = {
  parentEventId: "a".repeat(64),
  threadHeadId: "a".repeat(64),
};

test("a completed control clears only the submitted thread draft", async () => {
  const draftRef = { current: "thread:source" };
  const clearedDrafts = [];
  let clearedComposer = false;
  await submitComposerCloudControl({
    agentPubkey: CALIPER_CLOUD_CONTROL_PUBKEY,
    channelId: "channel",
    clearCurrentComposer: () => {
      clearedComposer = true;
    },
    getCurrentContent: () => "/cloud",
    markDraftSent: (...args) => clearedDrafts.push(args),
    command: cloudControlWireCommand("/cloud"),
    currentDraftKeyRef: draftRef,
    onError: () => assert.fail("unexpected error"),
    send: async () => {
      draftRef.current = "thread:destination";
    },
    sentDraftKey: "thread:source",
    submitLockedRef: { current: false },
    submittedContent: "/cloud",
    submittedDraftKey: "thread:source",
    threadContext,
  });
  assert.deepEqual(clearedDrafts, [
    ["thread:source", "/cloud", "channel", [], []],
  ]);
  assert.equal(clearedComposer, false);
});

test("text entered in the same thread during publication is preserved", async () => {
  let content = "/cloud";
  let cleared = false;
  await submitComposerCloudControl({
    agentPubkey: CALIPER_CLOUD_CONTROL_PUBKEY,
    channelId: "channel",
    clearCurrentComposer: () => {
      cleared = true;
    },
    getCurrentContent: () => content,
    markDraftSent: () => {},
    command: cloudControlWireCommand("/cloud"),
    currentDraftKeyRef: { current: "thread:source" },
    onError: () => assert.fail("unexpected error"),
    send: async () => {
      content = "next message";
    },
    sentDraftKey: null,
    submitLockedRef: { current: false },
    submittedContent: "/cloud",
    submittedDraftKey: "thread:source",
    threadContext,
  });
  assert.equal(cleared, false);
});

test("a failed control preserves the slash draft and releases its lock", async () => {
  const lock = { current: false };
  let cleared = false;
  const pending = [];
  let reportedError = null;
  await submitComposerCloudControl({
    agentPubkey: CALIPER_CLOUD_CONTROL_PUBKEY,
    channelId: "channel",
    clearCurrentComposer: () => {
      cleared = true;
    },
    getCurrentContent: () => "/local",
    markDraftSent: () => {
      cleared = true;
    },
    command: cloudControlWireCommand("/local"),
    currentDraftKeyRef: { current: "thread:source" },
    onError: (error) => {
      reportedError = error;
    },
    onPreparingChange: (value) => pending.push(value),
    send: async () => {
      throw new Error("offline");
    },
    sentDraftKey: "thread:source",
    submitLockedRef: lock,
    submittedContent: "/local",
    submittedDraftKey: "thread:source",
    threadContext,
  });
  assert.match(reportedError?.message ?? "", /offline/);
  assert.equal(cleared, false);
  assert.equal(lock.current, false);
  assert.deepEqual(pending, [true, false]);
});
