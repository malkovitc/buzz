#!/usr/bin/env node

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
let brokerRequestId = 0;
const brokerResponses = new Map();

function callBroker(toolName, args) {
  const id = `broker-${++brokerRequestId}`;
  return new Promise((resolve, reject) => {
    brokerResponses.set(id, { resolve, reject });
    writeJsonl(process.stdout, {
      type: "broker_tool_request",
      id,
      toolName,
      args,
    });
  });
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
    execute: async (_toolCallId, params) =>
      await callBroker("buzz_reply", params),
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
    execute: async (_toolCallId, params) =>
      await callBroker("kanban_tasks", params),
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
  const created = await createAgentSession({
    cwd: process.cwd(),
    tools,
    customTools,
    resourceLoader,
    sessionManager: SessionManager.inMemory(process.cwd()),
  });
  session = created.session;
  session.subscribe((event) => {
    writeJsonl(process.stdout, event);
    if (event.type === "agent_start") streaming = true;
    if (event.type === "agent_settled") streaming = false;
    if (event.type === "turn_start" && budget.onTurnStart() === "abort") {
      void session.abort();
    }
    if (event.type === "turn_end") {
      const outcome = budget.onTurnEnd(event);
      if (outcome.checkpoint) void session.steer(outcome.message);
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
        response(command, true);
        void session.prompt(command.message).catch((error) => {
          process.stderr.write(
            `[pi-acp-sdk] prompt failed: ${error.message}\n`,
          );
          streaming = false;
          writeJsonl(process.stdout, {
            type: "prompt_failed",
            error: error.message,
          });
        });
        break;
      case "steer":
        await session.steer(command.message);
        response(command, true);
        break;
      case "abort":
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
      const pending = brokerResponses.get(command.id);
      if (!pending) return;
      brokerResponses.delete(command.id);
      if (command.success) pending.resolve(command.result);
      else pending.reject(new Error(command.error || "brokered tool failed"));
      return;
    }
    void handle(command);
  },
  (error) => process.stderr.write(`[pi-acp-sdk] ${error.message}\n`),
);

async function shutdown() {
  if (disposed) return;
  disposed = true;
  try {
    await ready;
    await session.abort();
    session.dispose();
  } catch {
    // Startup or shutdown already failed.
  }
}

process.stdin.on("end", () => void shutdown());
for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, async () => {
    await shutdown();
    process.exit(0);
  });
}
