import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { JSDOM } from "jsdom";

import {
  CALIPER_CLOUD_CONTROL_CHANNEL_ID,
  CALIPER_CLOUD_CONTROL_OWNER_PUBKEY,
  CALIPER_CLOUD_CONTROL_PUBKEY,
} from "@/features/messages/lib/cloudControlCommands";
import { useCloudControlState } from "./useCloudControlState.ts";

const dom = new JSDOM("<!doctype html><html><body></body></html>");
before(() => {
  Object.assign(globalThis, {
    document: dom.window.document,
    HTMLElement: dom.window.HTMLElement,
    IS_REACT_ACT_ENVIRONMENT: true,
    window: dom.window,
  });
});
after(() => dom.window.close());

const message = (id, body) => ({
  id,
  createdAt: 1,
  pubkey: CALIPER_CLOUD_CONTROL_PUBKEY,
  signerPubkey: CALIPER_CLOUD_CONTROL_PUBKEY,
  author: "Caliper",
  time: "now",
  body,
  depth: 1,
});
const head = (id) => message(id, "Task thread");
const status = (id, state) =>
  message(id, `[PI CLOUD CONTROL]\nschema=1\nstatus=STATUS\nstate=${state}`);

const props = (threadId, historyPending, threadMessages) => ({
  channelId: CALIPER_CLOUD_CONTROL_CHANNEL_ID,
  currentPubkey: CALIPER_CLOUD_CONTROL_OWNER_PUBKEY,
  historyPending,
  threadHead: head(threadId),
  threadMessages,
});

test("history becomes a baseline and only a later reply updates location", async () => {
  const { renderHook, act, cleanup } = await import("@testing-library/react");
  const threadA = "a".repeat(64);
  const view = renderHook((input) => useCloudControlState(input), {
    initialProps: props(threadA, true, []),
  });
  await act(async () => {
    view.rerender(
      props(threadA, false, [status("1".repeat(64), "cloud-owned")]),
    );
  });
  assert.equal(view.result.current?.state, "unknown");
  await act(async () => {
    view.rerender(
      props(threadA, false, [
        status("1".repeat(64), "cloud-owned"),
        status("2".repeat(64), "local-owned"),
      ]),
    );
  });
  assert.equal(view.result.current?.state, "local");

  const threadB = "b".repeat(64);
  await act(async () => {
    view.rerender(
      props(threadB, false, [status("3".repeat(64), "cloud-owned")]),
    );
  });
  assert.equal(view.result.current?.state, "unknown");
  cleanup();
});
