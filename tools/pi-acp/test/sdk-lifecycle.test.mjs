import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { InMemoryCredentialStore } from "@earendil-works/pi-ai";
import {
  fauxAssistantMessage,
  fauxProvider,
  fauxToolCall,
} from "@earendil-works/pi-ai/providers/faux";
import {
  createAgentSession,
  DefaultResourceLoader,
  defineTool,
  ModelRuntime,
  SessionManager,
  SettingsManager,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { BrokerRequestRegistry } from "../src/sdk-broker.mjs";
import { SteeringDeliveryGate } from "../src/sdk-steering.mjs";
import {
  isSuccessfulBuzzReplyCompletion,
  TerminalPublicationLifecycle,
} from "../src/sdk-lifecycle.mjs";

const successfulReplyEvent = {
  type: "tool_execution_end",
  toolName: "buzz_reply",
  isError: false,
};

test("recognizes only successful buzz_reply completion as terminal", async () => {
  assert.equal(isSuccessfulBuzzReplyCompletion(successfulReplyEvent), true);
  assert.equal(
    isSuccessfulBuzzReplyCompletion({
      ...successfulReplyEvent,
      isError: true,
    }),
    false,
  );
  assert.equal(
    isSuccessfulBuzzReplyCompletion({
      ...successfulReplyEvent,
      toolName: "bash",
    }),
    false,
  );

  const calls = [];
  const lifecycle = new TerminalPublicationLifecycle();
  assert.equal(lifecycle.acceptsSteering(), false);
  lifecycle.beginPrompt();
  const settlement = lifecycle.settle(
    successfulReplyEvent,
    {
      clearQueue() {
        calls.push("clear");
      },
      async abort() {
        calls.push("abort");
      },
    },
    () => calls.push("terminal"),
  );
  await settlement;
  assert.equal(lifecycle.settle(successfulReplyEvent, {}), null);
  assert.deepEqual(calls, ["terminal", "clear", "abort"]);
  assert.equal(lifecycle.acceptsSteering(), false);
  lifecycle.beginPrompt();
  assert.equal(lifecycle.acceptsSteering(), true);
  lifecycle.endPrompt();
  assert.equal(lifecycle.acceptsSteering(), false);
});

test("a real AgentSession applies steering and settles after terminal publication", async (t) => {
  const agentDir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-acp-sdk-"));
  t.after(() => fs.rmSync(agentDir, { recursive: true, force: true }));
  const faux = fauxProvider();
  let sawSteering = false;
  let releaseReplyResponse;
  let markReplyRequestStarted;
  const replyRequestStarted = new Promise((resolve) => {
    markReplyRequestStarted = resolve;
  });
  faux.setResponses([
    fauxAssistantMessage(fauxToolCall("gate", {})),
    async (context) => {
      sawSteering = JSON.stringify(context).includes(
        "focus on the steered result",
      );
      markReplyRequestStarted();
      await new Promise((resolve) => {
        releaseReplyResponse = resolve;
      });
      return fauxAssistantMessage([
        fauxToolCall("kanban_tasks", {}),
        fauxToolCall("buzz_reply", { content: "steered result" }),
      ]);
    },
    fauxAssistantMessage("unexpected provider continuation"),
  ]);

  const modelRuntime = await ModelRuntime.create({
    credentials: new InMemoryCredentialStore(),
    refreshOnCreate: false,
  });
  modelRuntime.registerNativeProvider(faux.provider);
  await modelRuntime.setRuntimeApiKey(faux.provider.id, "test-only-key");

  const settingsManager = SettingsManager.inMemory({
    compaction: { enabled: false },
    retry: { enabled: false },
  });
  const resourceLoader = new DefaultResourceLoader({
    cwd: process.cwd(),
    agentDir,
    noExtensions: true,
    noSkills: true,
    noPromptTemplates: true,
    noThemes: true,
    noContextFiles: true,
    systemPrompt: "Follow steering and call buzz_reply once.",
  });
  await resourceLoader.reload();

  let releaseGate;
  let markGateStarted;
  const gateStarted = new Promise((resolve) => {
    markGateStarted = resolve;
  });
  const gate = defineTool({
    name: "gate",
    label: "Wait for steering",
    description: "Wait until the test supplies steering.",
    parameters: Type.Object({}),
    execute: async () =>
      await new Promise((resolve) => {
        releaseGate = () =>
          resolve({
            content: [{ type: "text", text: "continue" }],
            details: {},
          });
        markGateStarted();
      }),
  });
  const brokerRequests = new BrokerRequestRegistry();
  let siblingStarted = false;
  const kanbanTasks = defineTool({
    name: "kanban_tasks",
    label: "Read Kanban",
    description: "Remain pending until terminal publication aborts the call.",
    parameters: Type.Object({}),
    execute: async (_toolCallId, params, signal) =>
      await brokerRequests.request("kanban_tasks", params, signal, () => {
        siblingStarted = true;
      }),
  });
  const buzzReply = defineTool({
    name: "buzz_reply",
    label: "Reply in Buzz",
    description: "Publish the final result.",
    parameters: Type.Object({ content: Type.String() }),
    execute: async (_toolCallId, params, signal) =>
      await brokerRequests.request("buzz_reply", params, signal, (request) => {
        brokerRequests.respond({
          type: "broker_tool_response",
          id: request.id,
          success: true,
          result: {
            content: [{ type: "text", text: "published" }],
            details: { accepted: true },
          },
        });
      }),
  });
  const created = await createAgentSession({
    cwd: process.cwd(),
    model: faux.getModel(),
    modelRuntime,
    tools: ["gate", "kanban_tasks", "buzz_reply"],
    customTools: [gate, kanbanTasks, buzzReply],
    resourceLoader,
    sessionManager: SessionManager.inMemory(process.cwd()),
    settingsManager,
  });
  const { session } = created;
  const events = [];
  const lifecycle = new TerminalPublicationLifecycle();
  const steeringDelivery = new SteeringDeliveryGate();
  lifecycle.beginPrompt();
  let terminalSettlement = null;
  let terminalNotifications = 0;
  const acceptedSteering = [];
  const rejectedSteering = [];
  session.subscribe((event) => {
    events.push(event);
    if (event.type === "queue_update") {
      steeringDelivery.observeQueue(
        event.steering,
        (command) => acceptedSteering.push(command.id),
        (command) => rejectedSteering.push(command.id),
      );
    }
    const settlement = lifecycle.settle(event, session, () => {
      steeringDelivery.rejectAll((command) =>
        rejectedSteering.push(command.id),
      );
      terminalNotifications += 1;
    });
    if (settlement) terminalSettlement = settlement;
  });

  try {
    const prompt = session.prompt("start");
    await gateStarted;
    steeringDelivery.enqueue({ id: "consumed" }, "focus on the steered result");
    await session.steer("focus on the steered result");
    releaseGate();
    await replyRequestStarted;
    steeringDelivery.enqueue(
      { id: "unconsumed" },
      "late steering must not restart the provider",
    );
    await session.steer("late steering must not restart the provider");
    releaseReplyResponse();
    await prompt;
    await terminalSettlement;

    assert.ok(events.some((event) => event.type === "agent_settled"));
    assert.ok(
      events.some(
        (event) =>
          event.type === "queue_update" &&
          event.steering.includes("focus on the steered result"),
      ),
    );
    assert.equal(sawSteering, true);
    assert.equal(lifecycle.acceptsSteering(), false);
    assert.equal(session.pendingMessageCount, 0);
    assert.equal(terminalNotifications, 1);
    assert.deepEqual(acceptedSteering, ["consumed"]);
    assert.deepEqual(rejectedSteering, ["unconsumed"]);
    assert.equal(siblingStarted, true);
    assert.equal(brokerRequests.size, 0);
    assert.ok(
      events.some(
        (event) =>
          event.type === "tool_execution_end" &&
          event.toolName === "kanban_tasks" &&
          event.isError,
      ),
    );
    assert.equal(faux.state.callCount, 2);
    assert.equal(faux.getPendingResponseCount(), 1);
  } finally {
    session.dispose();
    faux.provider.dispose?.();
  }
});
