import crypto from "node:crypto";
import path from "node:path";
import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";
import packageMetadata from "../package.json" with { type: "json" };
import { createBuzzTools } from "./buzz-tools.mjs";
import { attachJsonlReader, writeJsonl } from "./jsonl.mjs";

const SDK_BRIDGE_PATH = fileURLToPath(
  new URL("./sdk-bridge.mjs", import.meta.url),
);

const INVALID_REQUEST = -32600;
const METHOD_NOT_FOUND = -32601;
const INVALID_PARAMS = -32602;
const INTERNAL_ERROR = -32603;
const MAX_SYSTEM_PROMPT_BYTES = 64 * 1024;
const MAX_TOOL_OUTPUT_BYTES = 16 * 1024;

function rpcResult(id, result) {
  return { jsonrpc: "2.0", id, result };
}

function rpcError(id, code, message) {
  return { jsonrpc: "2.0", id: id ?? null, error: { code, message } };
}

function promptText(blocks) {
  if (!Array.isArray(blocks)) return null;
  const texts = [];
  for (const block of blocks) {
    if (block?.type !== "text" || typeof block.text !== "string") return null;
    texts.push(block.text);
  }
  const joined = texts.join("\n\n");
  return joined.trim().length > 0 ? joined : null;
}

function boundedText(value) {
  const text =
    typeof value === "string" ? value : JSON.stringify(value ?? null);
  return Buffer.byteLength(text, "utf8") <= MAX_TOOL_OUTPUT_BYTES
    ? text
    : `${Buffer.from(text).subarray(0, MAX_TOOL_OUTPUT_BYTES).toString("utf8")}\n…[truncated]`;
}

function emptyUsage() {
  return {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    totalReliable: true,
    cost: 0,
    model: null,
  };
}

function addUsage(target, usage) {
  if (!usage || typeof usage !== "object") return;
  for (const key of ["input", "output", "cacheRead", "cacheWrite"]) {
    const value = usage[key];
    if (Number.isFinite(value) && value >= 0) target[key] += value;
  }
  if (Number.isFinite(usage.totalTokens) && usage.totalTokens >= 0) {
    target.totalTokens += usage.totalTokens;
  } else {
    target.totalReliable = false;
  }
  const cost = usage.cost?.total;
  if (Number.isFinite(cost) && cost >= 0) target.cost += cost;
}

export class PiAcpAdapter {
  constructor({ input, output, errorOutput, env = process.env }) {
    this.input = input;
    this.output = output;
    this.errorOutput = errorOutput;
    this.env = env;
    this.sessions = new Map();
    this.pi = null;
    this.piRequestId = 0;
    this.piResponses = new Map();
    this.currentPrompt = null;
    this.shuttingDown = false;
    this.brokerTools = new Map(
      createBuzzTools({
        getContext: () => this.currentPrompt?.buzzContext,
        env: this.env,
      }).map((tool) => [tool.name, tool]),
    );
  }

  start() {
    attachJsonlReader(
      this.input,
      (message) => void this.#handleAcp(message),
      (error) => this.#send(rpcError(null, -32700, error.message)),
    );
    this.input.on("end", () => void this.shutdown());
  }

  async shutdown() {
    if (this.shuttingDown) return;
    this.shuttingDown = true;
    this.currentPrompt?.brokerAbortController.abort();
    this.#stopPi();
  }

  #stopPi() {
    if (!this.pi) return;
    const child = this.pi;
    this.pi = null;
    try {
      if (process.platform !== "win32" && child.pid)
        process.kill(-child.pid, "SIGTERM");
      else child.kill("SIGTERM");
    } catch {
      // The child has already exited.
    }
  }

  #send(value) {
    writeJsonl(this.output, value);
  }

  #log(message) {
    this.errorOutput.write(`[pi-acp-rpc-spike] ${message}\n`);
  }

  async #handleAcp(message) {
    if (message?.jsonrpc !== "2.0" || typeof message.method !== "string") {
      if (message?.id !== undefined)
        this.#send(
          rpcError(message.id, INVALID_REQUEST, "invalid JSON-RPC request"),
        );
      return;
    }
    try {
      switch (message.method) {
        case "initialize":
          this.#initialize(message);
          break;
        case "session/new":
          await this.#sessionNew(message);
          break;
        case "session/prompt":
          await this.#sessionPrompt(message);
          break;
        case "session/cancel":
          await this.#sessionCancel(message.params);
          break;
        case "_session/steering":
          await this.#steer(message);
          break;
        default:
          if (message.id !== undefined)
            this.#send(
              rpcError(
                message.id,
                METHOD_NOT_FOUND,
                `method not found: ${message.method}`,
              ),
            );
      }
    } catch (error) {
      this.#log(error.stack ?? error.message);
      if (message.id !== undefined)
        this.#send(rpcError(message.id, INTERNAL_ERROR, error.message));
    }
  }

  #initialize(message) {
    const requested = message.params?.protocolVersion;
    if (!Number.isInteger(requested) || requested < 1) {
      this.#send(
        rpcError(
          message.id,
          INVALID_PARAMS,
          "initialize: protocolVersion must be a positive integer",
        ),
      );
      return;
    }
    this.#send(
      rpcResult(message.id, {
        protocolVersion: Math.min(requested, 2),
        agentCapabilities: {
          loadSession: false,
          promptCapabilities: {
            image: false,
            audio: false,
            embeddedContext: false,
          },
          mcpCapabilities: { http: false, sse: false },
        },
        agentInfo: { name: "pi-acp", version: packageMetadata.version },
        _meta: {
          steering: { supported: true },
          pilot: { liveCanaryValidated: true, fleetApproved: false },
        },
      }),
    );
  }

  async #sessionNew(message) {
    const cwd = message.params?.cwd;
    const systemPrompt = message.params?.systemPrompt;
    const mcpServers = message.params?.mcpServers;
    if (typeof cwd !== "string" || !path.isAbsolute(cwd)) {
      this.#send(
        rpcError(
          message.id,
          INVALID_PARAMS,
          "session/new: cwd must be an absolute path",
        ),
      );
      return;
    }
    if (systemPrompt !== undefined && typeof systemPrompt !== "string") {
      this.#send(
        rpcError(
          message.id,
          INVALID_PARAMS,
          "session/new: systemPrompt must be a string",
        ),
      );
      return;
    }
    if (
      Buffer.byteLength(systemPrompt ?? "", "utf8") > MAX_SYSTEM_PROMPT_BYTES
    ) {
      this.#send(
        rpcError(
          message.id,
          INVALID_PARAMS,
          `session/new: systemPrompt exceeds ${MAX_SYSTEM_PROMPT_BYTES} bytes`,
        ),
      );
      return;
    }
    if (!Array.isArray(mcpServers)) {
      this.#send(
        rpcError(
          message.id,
          INVALID_PARAMS,
          "session/new: mcpServers must be an array",
        ),
      );
      return;
    }
    if (mcpServers.length > 0) {
      this.#send(
        rpcError(
          message.id,
          INVALID_PARAMS,
          "session/new: MCP servers are unsupported by the RPC spike",
        ),
      );
      return;
    }

    const sessionId = `pi_${crypto.randomUUID()}`;
    this.sessions.set(sessionId, {
      id: sessionId,
      cwd,
      systemPrompt,
      cumulative: emptyUsage(),
    });
    this.#send(rpcResult(message.id, { sessionId }));
  }

  async #spawnPi(cwd, systemPrompt) {
    const command = this.env.PI_ACP_PI_COMMAND || process.execPath;
    let configuredArgs;
    try {
      configuredArgs = this.env.PI_ACP_PI_ARGS_JSON
        ? JSON.parse(this.env.PI_ACP_PI_ARGS_JSON)
        : [SDK_BRIDGE_PATH];
    } catch (error) {
      throw new Error(`PI_ACP_PI_ARGS_JSON is invalid: ${error.message}`);
    }
    if (
      !Array.isArray(configuredArgs) ||
      configuredArgs.some((arg) => typeof arg !== "string")
    ) {
      throw new Error("PI_ACP_PI_ARGS_JSON must be a JSON array of strings");
    }
    const args = [...configuredArgs];
    if (this.env.PI_ACP_ALLOW_DISCOVERY !== "1")
      args.push("--no-extensions", "--no-skills", "--no-prompt-templates");
    args.push("--tools", this.env.PI_ACP_TOOLS || "read");
    if (systemPrompt?.trim()) args.push("--system-prompt", systemPrompt);

    // The Pi SDK and its dependencies never receive the Buzz signing key.
    // Publication is brokered by this trusted adapter process instead.
    const childEnv = { ...this.env };
    delete childEnv.BUZZ_PRIVATE_KEY;
    delete childEnv.BUZZ_AUTH_TAG;
    const child = spawn(command, args, {
      cwd,
      env: childEnv,
      stdio: ["pipe", "pipe", "pipe"],
      detached: process.platform !== "win32",
    });
    this.pi = child;
    child.stderr.on("data", (chunk) => this.errorOutput.write(chunk));
    attachJsonlReader(
      child.stdout,
      (event) => this.#handlePi(event),
      (error) =>
        this.#failPi(new Error(`Pi RPC protocol error: ${error.message}`)),
    );
    child.on("error", (error) => this.#failPi(error));
    child.on("exit", (code, signal) => {
      if (!this.shuttingDown && this.pi === child) {
        this.#failPi(
          new Error(
            `Pi RPC exited unexpectedly (code=${code}, signal=${signal})`,
          ),
        );
      }
    });
    try {
      await this.#sendPiCommand("get_state");
    } catch (error) {
      await this.shutdown();
      this.shuttingDown = false;
      throw new Error(`Pi RPC readiness failed: ${error.message}`);
    }
  }

  #sessionFor(params, id, stage) {
    const session = this.sessions.get(params?.sessionId);
    if (!session) {
      this.#send(rpcError(id, INVALID_PARAMS, `${stage}: unknown session`));
      return null;
    }
    return session;
  }

  async #sessionPrompt(message) {
    const session = this.#sessionFor(
      message.params,
      message.id,
      "session/prompt",
    );
    if (!session) return;
    if (this.currentPrompt) {
      this.#send(
        rpcError(message.id, INVALID_PARAMS, "session/prompt: adapter is busy"),
      );
      return;
    }
    const text = promptText(message.params?.prompt);
    if (!text) {
      this.#send(
        rpcError(
          message.id,
          INVALID_PARAMS,
          "session/prompt: prompt must contain non-empty text blocks",
        ),
      );
      return;
    }
    const buzzContext = message.params?._meta?.buzz ?? null;
    if (
      buzzContext !== null &&
      (typeof buzzContext.channelId !== "string" ||
        !Array.isArray(buzzContext.triggeringEventIds) ||
        typeof buzzContext.replyTo !== "string")
    ) {
      this.#send(
        rpcError(
          message.id,
          INVALID_PARAMS,
          "session/prompt: malformed authenticated _meta.buzz routing context",
        ),
      );
      return;
    }
    let resolveSdkPromptStart;
    const sdkPromptStart = new Promise((resolve) => {
      resolveSdkPromptStart = resolve;
    });
    this.currentPrompt = {
      acpId: message.id,
      sessionId: session.id,
      buzzContext,
      cancelled: false,
      terminalPublished: false,
      sdkPromptStart,
      resolveSdkPromptStart,
      brokerAbortController: new AbortController(),
      usage: emptyUsage(),
      finalStopReason: "end_turn",
    };
    try {
      // A fresh SDK process/session for every task prevents history and cached
      // context from crossing inbound event boundaries.
      await this.#spawnPi(session.cwd, session.systemPrompt);
      if (this.currentPrompt?.cancelled) {
        this.#settlePrompt();
        return;
      }
      await this.#sendPiCommand("prompt", { message: text });
      this.currentPrompt?.resolveSdkPromptStart(true);
    } catch (error) {
      const prompt = this.currentPrompt;
      prompt?.resolveSdkPromptStart(false);
      this.currentPrompt = null;
      this.#stopPi();
      if (prompt)
        this.#send(
          rpcError(
            prompt.acpId,
            INTERNAL_ERROR,
            `Pi rejected prompt: ${error.message}`,
          ),
        );
    }
  }

  async #sessionCancel(params) {
    if (
      !this.sessions.has(params?.sessionId) ||
      this.currentPrompt?.sessionId !== params?.sessionId
    )
      return;
    this.currentPrompt.cancelled = true;
    this.currentPrompt.brokerAbortController.abort();
    try {
      await this.#sendPiCommand("abort");
    } catch (error) {
      this.#failPi(error);
    }
  }

  async #steer(message) {
    const session = this.#sessionFor(message.params, message.id, "steering");
    if (!session) return;
    const text = promptText(message.params?.prompt);
    if (!text) {
      this.#send(
        rpcError(
          message.id,
          INVALID_PARAMS,
          "steering: prompt must contain non-empty text blocks",
        ),
      );
      return;
    }
    if (!this.currentPrompt || this.currentPrompt.sessionId !== session.id) {
      this.#send(
        rpcError(message.id, INVALID_PARAMS, "steering: no active prompt"),
      );
      return;
    }
    const prompt = this.currentPrompt;
    const started = await prompt.sdkPromptStart;
    if (!started || this.currentPrompt !== prompt) {
      this.#send(
        rpcError(message.id, INVALID_PARAMS, "steering: prompt did not start"),
      );
      return;
    }
    await this.#sendPiCommand("steer", { message: text });
    this.#send(rpcResult(message.id, { outcome: "injected" }));
  }

  #sendPiCommand(type, fields = {}) {
    if (!this.pi?.stdin.writable)
      return Promise.reject(new Error("Pi RPC is not running"));
    const id = `pi-${++this.piRequestId}`;
    const command = { id, type, ...fields };
    return new Promise((resolve, reject) => {
      this.piResponses.set(id, { resolve, reject, command: type });
      writeJsonl(this.pi.stdin, command);
    });
  }

  #handlePi(event) {
    if (event?.type === "response" && event.id !== undefined) {
      const pending = this.piResponses.get(event.id);
      if (!pending) return;
      this.piResponses.delete(event.id);
      if (event.success) pending.resolve(event.data);
      else
        pending.reject(new Error(event.error || `${pending.command} failed`));
      return;
    }
    if (event?.type === "broker_tool_request") {
      void this.#brokerTool(event);
      return;
    }
    const prompt = this.currentPrompt;
    if (!prompt) return;
    switch (event?.type) {
      case "message_update":
        this.#messageUpdate(event.assistantMessageEvent);
        break;
      case "tool_execution_start":
        this.#toolStart(event);
        break;
      case "tool_execution_update":
        this.#toolUpdate(event);
        break;
      case "tool_execution_end":
        this.#toolEnd(event);
        break;
      case "turn_end":
        this.#turnEnd(event);
        break;
      case "terminal_publication":
        this.#terminalPublication();
        break;
      case "agent_settled":
        this.#settlePrompt();
        break;
      case "prompt_failed":
        this.#failPrompt(new Error(event.error || "Pi prompt failed"));
        break;
      case "extension_ui_request":
        if (["select", "confirm", "input", "editor"].includes(event.method)) {
          writeJsonl(this.pi.stdin, {
            type: "extension_ui_response",
            id: event.id,
            cancelled: true,
          });
        }
        break;
      default:
        break;
    }
  }

  async #brokerTool(event) {
    const tool = this.brokerTools.get(event.toolName);
    const prompt = this.currentPrompt;
    const brokerInput = this.pi?.stdin;
    if (!tool || !prompt || !brokerInput?.writable) return;
    try {
      const result = await tool.execute(
        String(event.id),
        event.args ?? {},
        prompt.brokerAbortController.signal,
      );
      if (brokerInput.writable)
        writeJsonl(brokerInput, {
          type: "broker_tool_response",
          id: event.id,
          success: true,
          result,
        });
    } catch (error) {
      if (brokerInput.writable)
        writeJsonl(brokerInput, {
          type: "broker_tool_response",
          id: event.id,
          success: false,
          error: error.message,
        });
    }
  }

  #update(update) {
    const sessionId = this.currentPrompt?.sessionId;
    if (!sessionId) return;
    this.#send({
      jsonrpc: "2.0",
      method: "session/update",
      params: { sessionId, update },
    });
  }

  #messageUpdate(delta) {
    if (delta?.type === "text_delta" && typeof delta.delta === "string") {
      this.#update({
        sessionUpdate: "agent_message_chunk",
        content: { type: "text", text: delta.delta },
      });
    } else if (
      delta?.type === "thinking_delta" &&
      typeof delta.delta === "string"
    ) {
      this.#update({
        sessionUpdate: "agent_thought_chunk",
        content: { type: "text", text: delta.delta },
      });
    }
  }

  #toolStart(event) {
    this.#update({
      sessionUpdate: "tool_call",
      toolCallId: String(event.toolCallId),
      title: String(event.toolName || "tool"),
      kind: event.toolName === "bash" ? "shell" : "other",
      status: "in_progress",
      rawInput: event.args ?? {},
    });
  }

  #toolUpdate(event) {
    this.#update({
      sessionUpdate: "tool_call_update",
      toolCallId: String(event.toolCallId),
      status: "in_progress",
      content: [
        {
          type: "content",
          content: { type: "text", text: boundedText(event.partialResult) },
        },
      ],
    });
  }

  #toolEnd(event) {
    this.#update({
      sessionUpdate: "tool_call_update",
      toolCallId: String(event.toolCallId),
      status: event.isError ? "failed" : "completed",
      content: [
        {
          type: "content",
          content: { type: "text", text: boundedText(event.result) },
        },
      ],
    });
  }

  #terminalPublication() {
    if (!this.currentPrompt || this.currentPrompt.terminalPublished) return;
    this.currentPrompt.terminalPublished = true;
    this.currentPrompt.finalStopReason = "end_turn";
    this.currentPrompt.brokerAbortController.abort();
  }

  #turnEnd(event) {
    const usage = event.message?.usage;
    addUsage(this.currentPrompt.usage, usage);
    if (event.message?.provider && event.message?.model) {
      this.currentPrompt.usage.model = `${event.message.provider}/${event.message.model}`;
    }
    for (const result of event.toolResults ?? [])
      addUsage(this.currentPrompt.usage, result?.usage);
    if (event.message?.stopReason === "length")
      this.currentPrompt.finalStopReason = "max_tokens";
    else if (
      event.message?.stopReason === "aborted" &&
      !this.currentPrompt.terminalPublished
    )
      this.currentPrompt.finalStopReason = "cancelled";
  }

  #settlePrompt() {
    const prompt = this.currentPrompt;
    if (!prompt) return;
    prompt.resolveSdkPromptStart(false);
    prompt.brokerAbortController.abort();
    this.currentPrompt = null;
    const usage = prompt.usage;
    const session = this.sessions.get(prompt.sessionId);
    if (!session) {
      this.#stopPi();
      return;
    }
    const cumulative = session.cumulative;
    for (const key of [
      "input",
      "output",
      "cacheRead",
      "cacheWrite",
      "totalTokens",
      "cost",
    ]) {
      cumulative[key] += usage[key];
    }
    cumulative.totalReliable &&= usage.totalReliable;
    cumulative.model = usage.model || cumulative.model;
    const update = {
      sessionUpdate: "usage_update",
      used: 0,
      contextLimit: 0,
      accumulatedInputTokens:
        cumulative.input + cumulative.cacheRead + cumulative.cacheWrite,
      accumulatedOutputTokens: cumulative.output,
      accumulatedCachedInputTokens: cumulative.cacheRead,
      accumulatedCacheWriteTokens: cumulative.cacheWrite,
      accumulatedCost: cumulative.cost,
      model: cumulative.model,
    };
    if (cumulative.totalReliable)
      update.accumulatedTotalTokens = cumulative.totalTokens;
    this.#send({
      jsonrpc: "2.0",
      method: "_goose/unstable/session/update",
      params: { sessionId: prompt.sessionId, update },
    });
    this.#send(
      rpcResult(prompt.acpId, {
        stopReason: prompt.cancelled ? "cancelled" : prompt.finalStopReason,
      }),
    );
    this.#stopPi();
  }

  #failPrompt(error) {
    const prompt = this.currentPrompt;
    if (!prompt) return;
    prompt.resolveSdkPromptStart(false);
    prompt.brokerAbortController.abort();
    this.currentPrompt = null;
    this.#send(rpcError(prompt.acpId, INTERNAL_ERROR, error.message));
    this.#stopPi();
  }

  #failPi(error) {
    this.#log(error.message);
    this.pi = null;
    for (const pending of this.piResponses.values()) pending.reject(error);
    this.piResponses.clear();
    if (this.currentPrompt) {
      this.currentPrompt.resolveSdkPromptStart(false);
      this.currentPrompt.brokerAbortController.abort();
      this.#send(
        rpcError(this.currentPrompt.acpId, INTERNAL_ERROR, error.message),
      );
      this.currentPrompt = null;
    }
  }
}
