#!/usr/bin/env node

import path from "node:path";
import {
  createAgentSession,
  DefaultResourceLoader,
  defineTool,
  getAgentDir,
  SessionManager,
} from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";
import { EventBudget } from "./event-budget.mjs";
import { attachJsonlReader, writeJsonl } from "./jsonl.mjs";
import { BrokerRequestRegistry } from "./sdk-broker.mjs";
import { TerminalPublicationLifecycle } from "./sdk-lifecycle.mjs";
import { SteeringDeliveryGate } from "./sdk-steering.mjs";
import {
  acquireTaskLease,
  openTaskSession,
  resetTaskSession,
  taskSessionDirectory,
  taskSessionIdentity,
} from "./task-session.mjs";

function option(name) {
  const index = process.argv.indexOf(name);
  return index >= 0 ? process.argv[index + 1] : undefined;
}

function positiveInteger(name, fallback) {
  const value = Number.parseInt(process.env[name] || "", 10);
  return Number.isSafeInteger(value) && value > 0 ? value : fallback;
}

const limits = {
  turns: positiveInteger("PI_ACP_MAX_TURNS", 3),
  tools: positiveInteger("PI_ACP_MAX_TOOLS", 3),
  tokens: positiveInteger("PI_ACP_MAX_PROCESSED_TOKENS", 75_000),
};
const budget = new EventBudget(limits);
let session;
let streaming = false;
let disposed = false;
const brokerRequests = new BrokerRequestRegistry();
const terminalPublication = new TerminalPublicationLifecycle();
const steeringDelivery = new SteeringDeliveryGate();
let sessionManager;
let taskIdentity;
let pendingDeliveryIds = [];
let pendingTriggerIds = [];
let releaseTaskLease;

function configuredSessionManager() {
  const raw = process.env.PI_ACP_TASK_IDENTITY_JSON;
  if (!raw) return SessionManager.inMemory(process.cwd());
  let supplied;
  try {
    supplied = JSON.parse(raw);
  } catch (error) {
    throw new Error(`PI_ACP_TASK_IDENTITY_JSON is invalid: ${error.message}`);
  }
  const normalized = taskSessionIdentity(supplied, supplied?.relayUrl);
  if (normalized.digest !== supplied?.digest) {
    throw new Error("PI_ACP_TASK_IDENTITY_JSON digest mismatch");
  }
  const root =
    process.env.PI_ACP_TASK_SESSION_ROOT ||
    path.join(getAgentDir(), "buzz-task-sessions");
  taskIdentity = normalized;
  releaseTaskLease = acquireTaskLease(
    taskSessionDirectory(root, normalized),
    "pi-session",
  );
  if (process.env.PI_ACP_RESET_TASK_SESSION === "1") {
    return resetTaskSession(process.cwd(), root, normalized);
  }
  return openTaskSession(process.cwd(), root, normalized);
}

function callBroker(toolName, args, signal) {
  return brokerRequests.request(toolName, args, signal, (request) =>
    writeJsonl(process.stdout, request),
  );
}

const budgetExtension = {
  name: "buzz-event-budget",
  factory(pi) {
    pi.on("tool_call", async () => budget.onToolCall());
  },
};

const builtInTools = (option("--tools") || process.env.PI_ACP_TOOLS || "read")
  .split(",")
  .map((value) => value.trim())
  .filter(Boolean);
const customTools = [
  defineTool({
    name: "buzz_reply",
    label: "Reply in Buzz",
    description:
      "Publish exactly one non-empty reply through the trusted Buzz broker. Routing is fixed by the harness.",
    parameters: Type.Object({
      content: Type.String({ minLength: 1, maxLength: 64 * 1024 }),
    }),
    execute: async (_toolCallId, params, signal) =>
      await callBroker("buzz_reply", params, signal),
  }),
  defineTool({
    name: "kanban_tasks",
    label: "Read compact Kanban tasks",
    description: "Read one bounded, filtered compact Kanban AI task list.",
    parameters: Type.Object({
      sprint: Type.Optional(Type.Integer({ minimum: 1 })),
      status: Type.Optional(
        Type.Union([
          Type.Literal("todo"),
          Type.Literal("in-progress"),
          Type.Literal("done"),
        ]),
      ),
      channel: Type.Optional(Type.String({ minLength: 1, maxLength: 128 })),
      search: Type.Optional(Type.String({ minLength: 1, maxLength: 256 })),
      limit: Type.Optional(Type.Integer({ minimum: 1, maximum: 20 })),
    }),
    execute: async (_toolCallId, params, signal) =>
      await callBroker("kanban_tasks", params, signal),
  }),
];
const tools = [...builtInTools, ...customTools.map((tool) => tool.name)];
const resourceLoader = new DefaultResourceLoader({
  cwd: process.cwd(),
  agentDir: getAgentDir(),
  noExtensions: true,
  noSkills: true,
  noPromptTemplates: true,
  noThemes: true,
  noContextFiles: true,
  systemPrompt: option("--system-prompt"),
  extensionFactories: [budgetExtension],
});

const ready = (async () => {
  await resourceLoader.reload();
  sessionManager = configuredSessionManager();
  const created = await createAgentSession({
    cwd: process.cwd(),
    tools,
    customTools,
    resourceLoader,
    sessionManager,
  });
  session = created.session;
  session.subscribe((event) => {
    if (event.type === "agent_start") streaming = true;
    if (event.type === "agent_settled") {
      streaming = false;
      if (pendingDeliveryIds.length > 0) {
        sessionManager.appendCustomEntry("buzz.delivery.v1", {
          eventIds: pendingDeliveryIds,
          triggerEventIds: pendingTriggerIds,
        });
        pendingDeliveryIds = [];
        pendingTriggerIds = [];
      }
      releaseTaskLease?.();
      releaseTaskLease = undefined;
      terminalPublication.endPrompt();
      steeringDelivery.rejectAll((command) =>
        response(
          command,
          false,
          undefined,
          "agent settled before steering was consumed",
        ),
      );
    }
    if (event.type === "queue_update") {
      steeringDelivery.observeQueue(
        event.steering,
        (command) => {
          const consumedIds = Array.isArray(command.deliveredEventIds)
            ? command.deliveredEventIds
            : [];
          pendingDeliveryIds = [
            ...new Set([...pendingDeliveryIds, ...consumedIds]),
          ];
          pendingTriggerIds = [
            ...new Set([...pendingTriggerIds, ...consumedIds]),
          ];
          response(command, true);
        },
        (command) =>
          response(
            command,
            false,
            undefined,
            "Pi steering queue diverged before the message was consumed",
          ),
      );
    }
    if (event.type === "turn_start" && budget.onTurnStart() === "abort") {
      void session.abort();
    }
    writeJsonl(process.stdout, event);
    if (event.type === "turn_end") {
      const outcome = budget.onTurnEnd(event);
      if (outcome.checkpoint && terminalPublication.acceptsSteering()) {
        const entry = steeringDelivery.enqueueInternal(outcome.message);
        void session
          .steer(outcome.message)
          .catch(() => steeringDelivery.remove(entry));
      }
    }
    const terminalSettlement = terminalPublication.settle(
      event,
      session,
      () => {
        steeringDelivery.rejectAll((command) =>
          response(
            command,
            false,
            undefined,
            "terminal Buzz publication completed before steering was consumed",
          ),
        );
        writeJsonl(process.stdout, { type: "terminal_publication" });
      },
    );
    if (terminalSettlement) {
      void terminalSettlement.catch(() => {
        process.stderr.write(
          "[pi-acp-sdk] terminal publication did not settle the session\n",
        );
      });
    }
  });
})();

function response(command, success, data, error) {
  writeJsonl(process.stdout, {
    id: command.id,
    type: "response",
    command: command.type,
    success,
    ...(data === undefined ? {} : { data }),
    ...(error === undefined ? {} : { error }),
  });
}

async function handle(command) {
  try {
    await ready;
    switch (command.type) {
      case "get_state":
        response(command, true, {
          model: session.model,
          thinkingLevel: session.thinkingLevel,
          isStreaming: streaming,
          sessionId: session.sessionId,
          sessionFile: session.sessionFile,
          leafId: sessionManager.getLeafId(),
          taskDigest: taskIdentity?.digest,
          autoCompactionEnabled: session.autoCompactionEnabled,
          budget: budget.snapshot(),
        });
        break;
      case "prompt":
        if (streaming) {
          response(command, false, undefined, "agent is already streaming");
          break;
        }
        budget.reset();
        pendingDeliveryIds = Array.isArray(command.deliveredEventIds)
          ? [...new Set(command.deliveredEventIds)]
          : [];
        pendingTriggerIds = Array.isArray(command.processedTriggerEventIds)
          ? [...new Set(command.processedTriggerEventIds)]
          : [];
        terminalPublication.beginPrompt();
        response(command, true);
        void session.prompt(command.message).catch((error) => {
          process.stderr.write(
            `[pi-acp-sdk] prompt failed: ${error.message}\n`,
          );
          streaming = false;
          pendingDeliveryIds = [];
          pendingTriggerIds = [];
          terminalPublication.endPrompt();
          steeringDelivery.rejectAll((pending) =>
            response(
              pending,
              false,
              undefined,
              "prompt failed before steering was consumed",
            ),
          );
          writeJsonl(process.stdout, {
            type: "prompt_failed",
            error: error.message,
          });
        });
        break;
      case "steer": {
        if (!terminalPublication.acceptsSteering()) {
          response(
            command,
            false,
            undefined,
            "no active Pi prompt accepts steering",
          );
          break;
        }
        const entry = steeringDelivery.enqueue(command, command.message);
        try {
          await session.steer(command.message);
        } catch (error) {
          if (steeringDelivery.remove(entry)) throw error;
        }
        break;
      }
      case "abort":
        pendingDeliveryIds = [];
        pendingTriggerIds = [];
        steeringDelivery.rejectAll((pending) =>
          response(
            pending,
            false,
            undefined,
            "agent aborted before steering was consumed",
          ),
        );
        await session.abort();
        response(command, true);
        break;
      default:
        response(
          command,
          false,
          undefined,
          `unsupported SDK bridge command: ${command.type}`,
        );
    }
  } catch (error) {
    response(command, false, undefined, error.message);
  }
}

attachJsonlReader(
  process.stdin,
  (command) => {
    if (command?.type === "broker_tool_response") {
      brokerRequests.respond(command);
      return;
    }
    void handle(command);
  },
  (error) => process.stderr.write(`[pi-acp-sdk] ${error.message}\n`),
);

async function shutdown() {
  if (disposed) return;
  disposed = true;
  brokerRequests.rejectAll();
  steeringDelivery.rejectAll((command) =>
    response(
      command,
      false,
      undefined,
      "bridge closed before steering was consumed",
    ),
  );
  try {
    await ready;
    await session.abort();
    session.dispose();
  } catch {
    // Startup or shutdown already failed.
  } finally {
    releaseTaskLease?.();
    releaseTaskLease = undefined;
  }
}

process.stdin.on("end", () => void shutdown());
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, async () => {
    await shutdown();
    process.exit(0);
  });
}
