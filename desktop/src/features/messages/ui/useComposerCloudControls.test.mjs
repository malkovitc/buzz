import assert from "node:assert/strict";
import { after, before, test } from "node:test";
import { JSDOM } from "jsdom";

import {
  CALIPER_CLOUD_CONTROL_PUBKEY,
  cloudControlWireCommand,
} from "@/features/messages/lib/cloudControlCommands";
import { useComposerCloudControls } from "./useComposerCloudControls.tsx";

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

test("a pending handoff permits status but blocks transition aliases", async () => {
  const { act, cleanup, renderHook } = await import("@testing-library/react");
  const view = renderHook(() =>
    useComposerCloudControls({
      config: {
        agentPubkey: CALIPER_CLOUD_CONTROL_PUBKEY,
        state: "switching-cloud",
      },
      contentEmpty: true,
      disabled: false,
      editing: false,
      mediaBlocked: false,
      richText: {
        focusEnd: () => {},
        getPlainTextAndCursor: () => ({ text: "", cursor: 0 }),
        setContent: () => {},
      },
    }),
  );
  await act(async () => view.result.current.onEditorUpdate("/local", 6));
  assert.equal(view.result.current.isSendBlocked, true);
  assert.equal(
    view.result.current.isCommandBlocked(cloudControlWireCommand("/status")),
    false,
  );
  await act(async () => view.result.current.onEditorUpdate("/", 1));
  assert.deepEqual(
    view.result.current.autocomplete.props.suggestions.map(({ id }) => id),
    ["status"],
  );
  cleanup();
});
