#!/usr/bin/env node

import fs from "node:fs";
import { attachJsonlReader, writeJsonl } from "../src/jsonl.mjs";

if (process.env.FAKE_PI_STARTED_FILE)
  fs.writeFileSync(process.env.FAKE_PI_STARTED_FILE, `${process.pid}\n`);

let active = false;
let terminalReplyCompleted = false;
let timer;
const mode = process.env.FAKE_PI_MODE || "complete";

function respond(command, data) {
  writeJsonl(process.stdout, {
    id: command.id,
    type: "response",
    command: command.type,
    success: true,
    ...(data === undefined ? {} : { data }),
  });
}

function complete(text = "hello\u2028world") {
  if (!active) return;
  active = false;
  writeJsonl(process.stdout, {
    type: "message_update",
    usage: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, totalTokens: 0 },
    assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: text },
  });
  writeJsonl(process.stdout, {
    type: "tool_execution_start",
    toolCallId: "tool-1",
    toolName: "read",
    args: { path: "README.md" },
  });
  writeJsonl(process.stdout, {
    type: "tool_execution_end",
    toolCallId: "tool-1",
    toolName: "read",
    result: { content: [{ type: "text", text: "ok" }] },
    isError: false,
  });
  writeJsonl(process.stdout, {
    type: "turn_end",
    message: {
      role: "assistant",
      provider: "fake",
      model: "test-model",
      stopReason: "stop",
      usage: {
        input: 100,
        output: 20,
        cacheRead: 80,
        cacheWrite: 5,
        totalTokens: 205,
        cost: { total: 0.01 },
      },
    },
    toolResults: [],
  });
  writeJsonl(process.stdout, { type: "agent_settled" });
}

attachJsonlReader(
  process.stdin,
  (command) => {
    switch (command.type) {
      case "get_state": {
        const state = {
          model: { provider: "fake", id: "test-model" },
          isStreaming: false,
          sessionId: "fake-session",
        };
        if (mode === "startup-steer") {
          setTimeout(() => respond(command, state), 100);
        } else {
          respond(command, state);
        }
        break;
      }
      case "prompt":
        active = true;
        respond(command);
        if (mode === "complete") timer = setTimeout(() => complete(), 10);
        else if (mode === "steer" || mode === "startup-steer")
          timer = setTimeout(() => complete("unsteered"), 500);
        else if (mode === "pid")
          timer = setTimeout(() => complete(`pid:${process.pid}`), 10);
        else if (mode === "key-check")
          timer = setTimeout(
            () =>
              complete(
                process.env.BUZZ_PRIVATE_KEY === undefined
                  ? "key:missing"
                  : "key:exposed",
              ),
            10,
          );
        else if (mode === "prompt-fail") {
          active = false;
          setTimeout(
            () =>
              writeJsonl(process.stdout, {
                type: "prompt_failed",
                error: "provider unavailable",
              }),
            5,
          );
        } else if (
          mode === "broker" ||
          mode === "broker-cancel" ||
          mode === "terminal-publication"
        ) {
          writeJsonl(process.stdout, {
            type: "broker_tool_request",
            id: "broker-test",
            toolName: "buzz_reply",
            args: { content: "brokered reply" },
          });
          if (mode === "terminal-publication") {
            writeJsonl(process.stdout, {
              type: "broker_tool_request",
              id: "broker-sibling",
              toolName: "kanban_tasks",
              args: { limit: 1 },
            });
          }
        }
        break;
      case "broker_tool_response":
        if (mode === "broker" || mode === "broker-cancel") {
          setTimeout(
            () => complete(`broker:${command.success ? "ok" : command.error}`),
            5,
          );
        } else if (
          mode === "terminal-publication" &&
          command.id === "broker-test"
        ) {
          terminalReplyCompleted = true;
          writeJsonl(process.stdout, {
            type: "tool_execution_end",
            toolCallId: "reply-tool",
            toolName: "buzz_reply",
            result: command.result,
            isError: !command.success,
          });
          writeJsonl(process.stdout, { type: "terminal_publication" });
        } else if (
          mode === "terminal-publication" &&
          command.id === "broker-sibling" &&
          terminalReplyCompleted
        ) {
          active = false;
          writeJsonl(process.stdout, {
            type: "message_update",
            assistantMessageEvent: {
              type: "text_delta",
              contentIndex: 0,
              delta: `sibling:${command.success ? "ok" : "aborted"}`,
            },
          });
          writeJsonl(process.stdout, {
            type: "turn_end",
            message: {
              role: "assistant",
              provider: "fake",
              model: "test-model",
              stopReason: "aborted",
              usage: {
                input: 10,
                output: 2,
                cacheRead: 0,
                cacheWrite: 0,
                totalTokens: 12,
                cost: { total: 0 },
              },
            },
            toolResults: [],
          });
          writeJsonl(process.stdout, { type: "agent_settled" });
        }
        break;
      case "steer":
        clearTimeout(timer);
        respond(command);
        setTimeout(() => complete(`steered:${command.message}`), 5);
        break;
      case "abort":
        clearTimeout(timer);
        respond(command);
        if (active) {
          active = false;
          writeJsonl(process.stdout, {
            type: "turn_end",
            message: {
              role: "assistant",
              provider: "fake",
              model: "test-model",
              stopReason: "aborted",
              usage: {
                input: 10,
                output: 0,
                cacheRead: 0,
                cacheWrite: 0,
                totalTokens: 10,
                cost: { total: 0 },
              },
            },
            toolResults: [],
          });
          writeJsonl(process.stdout, { type: "agent_settled" });
        }
        break;
      default:
        writeJsonl(process.stdout, {
          id: command.id,
          type: "response",
          command: command.type,
          success: false,
          error: "unsupported fake command",
        });
    }
  },
  (error) => {
    process.stderr.write(`${error.message}\n`);
    process.exitCode = 1;
  },
);
