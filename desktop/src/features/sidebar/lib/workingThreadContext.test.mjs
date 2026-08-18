import assert from "node:assert/strict";
import test from "node:test";

import { resolveWorkingThreadContext } from "./workingThreadContext.ts";

const ROOT = "a".repeat(64);
const REPLY = "b".repeat(64);

function event({ id, kind, content, tags = [] }) {
  return {
    id,
    kind,
    content,
    tags,
    pubkey: "c".repeat(64),
    created_at: 1,
  };
}

test("resolves a triggering forum comment to the root title", async () => {
  const events = new Map([
    [
      REPLY,
      event({
        id: REPLY,
        kind: 45003,
        content: "@Agent please review",
        tags: [
          ["e", ROOT, "", "root"],
          ["e", ROOT, "", "reply"],
        ],
      }),
    ],
    [
      ROOT,
      event({
        id: ROOT,
        kind: 45001,
        content: "## Contract decision: session idempotency\nDetails",
      }),
    ],
  ]);

  const result = await resolveWorkingThreadContext(
    REPLY,
    "channel-1",
    async (id) => events.get(id),
  );
  assert.deepEqual(result, {
    rootId: ROOT,
    label: "Contract decision: session idempotency",
  });
});

test("returns null when a trigger is not in a thread", async () => {
  const result = await resolveWorkingThreadContext(
    REPLY,
    "channel-1",
    async () => event({ id: REPLY, kind: 9, content: "standalone" }),
  );
  assert.equal(result, null);
});
