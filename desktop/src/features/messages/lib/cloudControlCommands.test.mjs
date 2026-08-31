import assert from "node:assert/strict";
import test from "node:test";

import {
  CALIPER_CLOUD_CONTROL_CHANNEL_ID,
  CALIPER_CLOUD_CONTROL_OWNER_PUBKEY,
  CALIPER_CLOUD_CONTROL_PUBKEY,
  cloudControlSnapshot,
  cloudControlSuggestions,
  cloudControlUiState,
  cloudControlWireCommand,
  isCaliperCloudControlContext,
  publishCloudControlCommand,
  reconcileCloudControlState,
} from "./cloudControlCommands.ts";

const ownerCommand = (id, body, createdAt = 1) => ({
  id,
  createdAt,
  pubkey: CALIPER_CLOUD_CONTROL_OWNER_PUBKEY,
  signerPubkey: CALIPER_CLOUD_CONTROL_OWNER_PUBKEY,
  author: "Owner",
  time: "now",
  body,
  depth: 1,
});

const controlMessage = (body, createdAt = 1) => ({
  id: `${createdAt}`,
  createdAt,
  pubkey: CALIPER_CLOUD_CONTROL_PUBKEY,
  author: "Caliper",
  time: "now",
  body,
  depth: 1,
});

test("controls are exposed only in the approved channel and a real event thread", () => {
  assert.equal(
    isCaliperCloudControlContext(
      CALIPER_CLOUD_CONTROL_CHANNEL_ID,
      "a".repeat(64),
      CALIPER_CLOUD_CONTROL_OWNER_PUBKEY,
    ),
    true,
  );
  assert.equal(
    isCaliperCloudControlContext(
      "wrong-channel",
      "a".repeat(64),
      CALIPER_CLOUD_CONTROL_OWNER_PUBKEY,
    ),
    false,
  );
  assert.equal(
    isCaliperCloudControlContext(
      CALIPER_CLOUD_CONTROL_CHANNEL_ID,
      "draft",
      CALIPER_CLOUD_CONTROL_OWNER_PUBKEY,
    ),
    false,
  );
  assert.equal(
    isCaliperCloudControlContext(
      CALIPER_CLOUD_CONTROL_CHANNEL_ID,
      "a".repeat(64),
      "b".repeat(64),
    ),
    false,
  );
});

test("slash suggestions are bounded to an otherwise empty command input", () => {
  assert.deepEqual(
    cloudControlSuggestions("/cl", 3).map((command) => command.slash),
    ["/cloud"],
  );
  assert.deepEqual(cloudControlSuggestions("hello /cl", 9), []);
  assert.deepEqual(cloudControlSuggestions("/cloud extra", 6), []);
});

test("control publication uses only the Caliper p-tag and bypass-ready send callback", async () => {
  const calls = [];
  await publishCloudControlCommand({
    command: cloudControlWireCommand("/cloud"),
    agentPubkey: CALIPER_CLOUD_CONTROL_PUBKEY.toUpperCase(),
    channelId: CALIPER_CLOUD_CONTROL_CHANNEL_ID,
    threadContext: {
      parentEventId: "a".repeat(64),
      threadHeadId: "a".repeat(64),
    },
    send: async (...args) => calls.push(args),
  });
  assert.deepEqual(calls, [
    [
      "-cloud",
      [CALIPER_CLOUD_CONTROL_PUBKEY],
      undefined,
      CALIPER_CLOUD_CONTROL_CHANNEL_ID,
      {
        parentEventId: "a".repeat(64),
        threadHeadId: "a".repeat(64),
      },
      true,
    ],
  ]);
});

test("slash commands translate only exact aliases to reserved wire controls", () => {
  assert.equal(cloudControlWireCommand(" /cloud ")?.wire, "-cloud");
  assert.equal(cloudControlWireCommand("/local")?.wire, "-local");
  assert.equal(cloudControlWireCommand("/status")?.wire, "-status");
  assert.equal(cloudControlWireCommand("/cloud now"), null);
  assert.equal(cloudControlWireCommand("-cloud"), null);
});

test("authoritative Caliper replies drive location state without optimism", () => {
  assert.equal(
    cloudControlUiState([
      controlMessage(
        "[PI CLOUD CONTROL]\nschema=1\nstatus=STATUS\nstate=local-owned",
      ),
    ]),
    "local",
  );
  assert.equal(
    cloudControlUiState([
      controlMessage(
        "[PI CLOUD CONTROL]\nschema=1\nstatus=PREPARED\ncommand=-cloud\nstate=local-owned",
        2,
      ),
    ]),
    "switching-cloud",
  );
  assert.equal(
    cloudControlUiState([
      controlMessage(
        "[PI CLOUD CONTROL]\nschema=1\nstatus=BLOCKED_LOCAL-OWNED\nstate=local-owned",
        3,
      ),
    ]),
    "blocked",
  );
  assert.equal(
    cloudControlUiState([
      controlMessage(
        "[PI CLOUD CONTROL]\nschema=1\nstatus=STATUS\nstate=blocked",
        3,
      ),
    ]),
    "blocked",
  );
  assert.equal(
    cloudControlUiState([
      {
        ...controlMessage(
          "[PI CLOUD CONTROL]\nschema=1\nstatus=PREPARED\ncommand=-cloud\nstate=local-owned",
          4,
        ),
        id: "f".repeat(64),
      },
      {
        ...controlMessage(
          "[PI CLOUD CONTROL]\nschema=1\nstatus=CLOUD_ACTIVE\ncommand=-cloud\nstate=cloud-owned",
          4,
        ),
        id: "0".repeat(64),
      },
    ]),
    "cloud",
  );

  const oldCommandId = "1".repeat(64);
  const newCommandId = "2".repeat(64);
  assert.equal(
    cloudControlUiState([
      ownerCommand(oldCommandId, "-status", 5),
      {
        ...controlMessage(
          `[PI CLOUD CONTROL]\nschema=1\nstatus=STATUS\ncommand=-status\ncommand_event=${oldCommandId}\nstate=local-owned`,
          5,
        ),
        id: "f".repeat(64),
      },
      ownerCommand(newCommandId, "-cloud", 5),
      {
        ...controlMessage(
          `[PI CLOUD CONTROL]\nschema=1\nstatus=PREPARED\ncommand=-cloud\ncommand_event=${newCommandId}\nstate=local-owned`,
          5,
        ),
        id: "0".repeat(64),
      },
    ]),
    "unknown",
  );
  assert.equal(
    cloudControlUiState([
      ownerCommand(oldCommandId, "-status", 5),
      {
        ...controlMessage(
          `[PI CLOUD CONTROL]\nschema=1\nstatus=STATUS\ncommand=-status\ncommand_event=${oldCommandId}\nstate=local-owned`,
          5,
        ),
        id: "f".repeat(64),
      },
      ownerCommand(newCommandId, "-cloud", 6),
      {
        ...controlMessage(
          `[PI CLOUD CONTROL]\nschema=1\nstatus=PREPARED\ncommand=-cloud\ncommand_event=${newCommandId}\nstate=local-owned`,
          6,
        ),
        id: "0".repeat(64),
      },
    ]),
    "switching-cloud",
  );
});

test("prepared status preserves an accepted pending direction", () => {
  const snapshot = cloudControlSnapshot([
    controlMessage(
      "[PI CLOUD CONTROL]\nschema=1\nstatus=STATUS\nstate=prepared",
    ),
  ]);
  assert.equal(snapshot?.state, "unknown");
  assert.equal(snapshot?.preservePending, true);
});

test("a source-owner status cannot roll back an accepted pending transition", () => {
  assert.equal(
    reconcileCloudControlState("switching-cloud", "local"),
    "switching-cloud",
  );
  assert.equal(reconcileCloudControlState("switching-cloud", "cloud"), "cloud");
  assert.equal(
    reconcileCloudControlState("switching-local", "blocked"),
    "blocked",
  );
  assert.equal(reconcileCloudControlState("local", "unknown"), "unknown");
  assert.equal(
    reconcileCloudControlState("switching-cloud", "unknown", true),
    "switching-cloud",
  );
});

test("lookalike replies from another pubkey cannot change the UI owner", () => {
  assert.equal(
    cloudControlUiState([
      {
        ...controlMessage(
          "[PI CLOUD CONTROL]\nschema=1\nstatus=STATUS\nstate=cloud-owned",
        ),
        pubkey: CALIPER_CLOUD_CONTROL_PUBKEY,
        signerPubkey: "a".repeat(64),
      },
    ]),
    "unknown",
  );
});
