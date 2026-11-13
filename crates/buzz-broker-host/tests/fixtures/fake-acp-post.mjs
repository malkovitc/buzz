#!/usr/bin/env node
// Minimal ACP peer for the explicit keyless host smoke test.
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { createInterface } from "node:readline";

const exec = promisify(execFile);
const write = (message) => process.stdout.write(`${JSON.stringify(message)}\n`);
const input = createInterface({ input: process.stdin, crlfDelay: Infinity });

for await (const line of input) {
  if (!line.trim()) continue;
  const request = JSON.parse(line);
  if (request.id === undefined || typeof request.method !== "string") continue;

  switch (request.method) {
    case "initialize":
      write({
        jsonrpc: "2.0",
        id: request.id,
        result: { protocolVersion: 2, agentCapabilities: {} },
      });
      break;
    case "session/new":
      write({
        jsonrpc: "2.0",
        id: request.id,
        result: { sessionId: "keyless-smoke" },
      });
      break;
    case "session/prompt": {
      const prompt = (request.params?.prompt ?? [])
        .filter((part) => part?.type === "text")
        .map((part) => part.text ?? "")
        .join("\n");
      const channel = prompt.match(/^Channel: ([0-9a-f-]{36})$/m)?.[1];
      const parent = prompt.match(/^Event ID: ([0-9a-f]{64})$/m)?.[1];
      const marker = prompt.match(/\bAE-ID:([A-Za-z0-9._:-]+)\b/)?.[1];
      if (!channel || !parent || !marker || !process.env.BUZZ_E2E_CLI_BIN) {
        throw new Error("keyless prompt omitted channel, parent, marker, or CLI path");
      }
      const answer = `AE-ACK:${marker}`;
      await exec(
        process.env.BUZZ_E2E_CLI_BIN,
        ["messages", "send", "--channel", channel, "--content", answer, "--reply-to", parent],
        { env: process.env },
      );
      write({
        jsonrpc: "2.0",
        method: "session/update",
        params: {
          sessionId: request.params?.sessionId,
          update: {
            sessionUpdate: "agent_message_chunk",
            content: { type: "text", text: answer },
          },
        },
      });
      write({
        jsonrpc: "2.0",
        id: request.id,
        result: { stopReason: "end_turn" },
      });
      break;
    }
    case "session/cancel":
      write({ jsonrpc: "2.0", id: request.id, result: {} });
      break;
    default:
      write({
        jsonrpc: "2.0",
        id: request.id,
        error: { code: -32601, message: "Unsupported fixture method" },
      });
  }
}
