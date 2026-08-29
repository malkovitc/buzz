import crypto from "node:crypto";
import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { defineTool } from "@earendil-works/pi-coding-agent";
import { Type } from "typebox";

const HEX_EVENT = /^[0-9a-f]{64}$/i;
const UUID =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const MAX_CONTENT_BYTES = 64 * 1024;
const MAX_OUTPUT_BYTES = 64 * 1024;
const MAX_CONTROL_CONTENT_BYTES = 8 * 1024;
const CLOUD_CONTROL_COMMANDS = new Set(["-status", "-cloud", "-local"]);

function validateContext(context) {
  if (!context || !UUID.test(context.channelId || "")) {
    throw new Error("authenticated Buzz channel context is unavailable");
  }
  if (!HEX_EVENT.test(context.replyTo || "")) {
    throw new Error("authenticated Buzz reply target is unavailable");
  }
  if (
    !Array.isArray(context.triggeringEventIds) ||
    context.triggeringEventIds.length === 0 ||
    !context.triggeringEventIds.every((eventId) => HEX_EVENT.test(eventId))
  ) {
    throw new Error("authenticated Buzz triggering event set is invalid");
  }
  if (
    !Array.isArray(context.allowedReplyEventIds) ||
    !context.allowedReplyEventIds.every((eventId) => HEX_EVENT.test(eventId)) ||
    !context.allowedReplyEventIds.includes(context.replyTo)
  ) {
    throw new Error("authenticated Buzz reply authorization set is invalid");
  }
  return context;
}

function toolText(text, details = {}) {
  return { content: [{ type: "text", text }], details };
}

function cloudControlEnvironment(env) {
  return {
    PATH: "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
    ...Object.fromEntries(
      ["HOME", "USER", "LOGNAME", "TMPDIR", "LANG", "LC_ALL"]
        .filter((name) => typeof env[name] === "string")
        .map((name) => [name, env[name]]),
    ),
  };
}

function cloudControlTimeout(env) {
  const value = Number.parseInt(env.PI_ACP_CLOUD_CONTROL_TIMEOUT_MS || "", 10);
  return Number.isSafeInteger(value) && value >= 1_000 && value <= 900_000
    ? value
    : 600_000;
}

function parseCloudControlResponse(stdout) {
  let response;
  try {
    response = JSON.parse(stdout);
  } catch {
    throw new Error("cloud control returned non-JSON output");
  }
  if (
    !response ||
    typeof response !== "object" ||
    Array.isArray(response) ||
    !["ok", "blocked"].includes(response.status) ||
    typeof response.content !== "string" ||
    response.content.trim().length === 0 ||
    Buffer.byteLength(response.content, "utf8") > MAX_CONTROL_CONTENT_BYTES ||
    Object.keys(response).some((key) => !["status", "content"].includes(key))
  ) {
    throw new Error("cloud control response violates the strict contract");
  }
  return response;
}

async function run(
  command,
  args,
  { input, signal, env = process.env, timeoutMs = 30_000 } = {},
) {
  return await new Promise((resolve, reject) => {
    let timer;
    let child;
    let settled = false;
    let spawned = false;
    let abortRequested = false;
    const abortError = (preSpawn = false) => {
      const error = new Error("tool execution aborted");
      if (preSpawn) error.code = "PI_ACP_SAFE_UNSTARTED";
      return error;
    };
    const terminate = (signalName) => {
      if (!child) return;
      try {
        if (process.platform === "win32" && child.pid) {
          const killed = spawnSync(
            "taskkill",
            ["/PID", String(child.pid), "/T", "/F"],
            { stdio: "ignore", windowsHide: true },
          );
          if (killed.status !== 0) child.kill(signalName);
        } else if (child.pid) {
          process.kill(-child.pid, signalName);
        } else {
          child.kill(signalName);
        }
      } catch {
        // The broker command group has already exited.
      }
    };
    const abort = () => {
      abortRequested = true;
      terminate("SIGKILL");
    };
    const finish = (error, result) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      signal?.removeEventListener("abort", abort);
      if (error) reject(error);
      else resolve(result);
    };
    if (signal?.aborted) {
      finish(abortError(true));
      return;
    }
    signal?.addEventListener("abort", abort, { once: true });
    if (signal?.aborted) abort();
    if (abortRequested) {
      finish(abortError(true));
      return;
    }
    child = spawn(command, args, {
      env: { ...env, PI_ACP_BROKER_PARENT_PID: String(process.pid) },
      stdio: [input === undefined ? "ignore" : "pipe", "pipe", "pipe"],
      detached: process.platform !== "win32",
    });
    if (abortRequested) {
      terminate("SIGKILL");
      finish(abortError());
      return;
    }
    let stdout = Buffer.alloc(0);
    let stderr = Buffer.alloc(0);
    const append = (current, chunk) => {
      const next = Buffer.concat([current, chunk]);
      if (next.length > MAX_OUTPUT_BYTES) {
        terminate("SIGKILL");
        finish(new Error(`tool output exceeds ${MAX_OUTPUT_BYTES} bytes`));
      }
      return next;
    };
    child.once("spawn", () => {
      spawned = true;
    });
    child.stdout.on("data", (chunk) => {
      stdout = append(stdout, chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr = append(stderr, chunk);
    });
    child.on("error", (error) => {
      if (!spawned) error.code = "PI_ACP_SAFE_UNSTARTED";
      finish(error);
    });
    child.on("close", (code, exitSignal) => {
      // Wait for stdout/stderr to close before accepting the receipt. A
      // wrapper may also background a publisher, so reap its process group.
      terminate("SIGKILL");
      const result = {
        code,
        signal: exitSignal,
        stdout: stdout.toString("utf8").trim(),
        stderr: stderr.toString("utf8").trim(),
      };
      if (code === 0) finish(undefined, result);
      else
        finish(
          new Error(result.stderr || `${command} exited with code ${code}`),
        );
    });
    timer = setTimeout(() => {
      terminate("SIGKILL");
      finish(new Error(`${command} timed out after ${timeoutMs}ms`));
    }, timeoutMs);
    if (input !== undefined) child.stdin.end(input);
  });
}

function reservationKey(context, env) {
  const secret = env.BUZZ_PRIVATE_KEY;
  if (!secret) throw new Error("BUZZ_PRIVATE_KEY is unavailable");
  const material = JSON.stringify({
    relay: env.BUZZ_RELAY_URL || "",
    channelId: context.channelId,
    triggeringEventIds: [...context.triggeringEventIds].sort(),
    replyTo: context.replyTo,
  });
  return crypto.createHmac("sha256", secret).update(material).digest("hex");
}

function assertPowerLossDurability(env, platform = process.platform) {
  if (
    env.PI_ACP_REQUIRE_POWER_LOSS_DURABILITY === "1" &&
    platform !== "linux"
  ) {
    throw new Error(
      "strict power-loss receipt durability is supported only on Linux",
    );
  }
}

async function syncDirectory(directory, fileSystem = fs) {
  const handle = await fileSystem.open(directory, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function syncRecordDurable(file, fileSystem = fs) {
  const handle = await fileSystem.open(file, "r");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
  await syncDirectory(path.dirname(file), fileSystem);
}

async function ensureDirectoryDurable(directory, fileSystem = fs) {
  let cursor = path.resolve(directory);
  const missing = [];
  while (true) {
    try {
      const stat = await fileSystem.lstat(cursor);
      if (!stat.isDirectory())
        throw new Error("receipt path is not a directory");
      cursor = await fileSystem.realpath(cursor);
      break;
    } catch (error) {
      if (error.code !== "ENOENT") throw error;
      const parent = path.dirname(cursor);
      if (parent === cursor) throw error;
      missing.unshift(path.basename(cursor));
      cursor = parent;
    }
  }
  const anchorParent = path.dirname(cursor);
  if (anchorParent !== cursor) await syncDirectory(anchorParent, fileSystem);
  for (const component of missing) {
    const child = path.join(cursor, component);
    try {
      await fileSystem.mkdir(child, { mode: 0o700 });
    } catch (error) {
      if (
        error.code !== "EEXIST" ||
        !(await fileSystem.lstat(child)).isDirectory()
      )
        throw error;
    }
    await syncDirectory(cursor, fileSystem);
    cursor = child;
  }
  return cursor;
}

async function writeJsonAtomicDurable(
  directory,
  filename,
  value,
  fileSystem = fs,
) {
  const temporary = path.join(
    directory,
    `.${filename}.${process.pid}.${crypto.randomUUID()}.tmp`,
  );
  let renamed = false;
  try {
    const handle = await fileSystem.open(temporary, "wx", 0o600);
    try {
      await handle.writeFile(`${JSON.stringify(value)}\n`);
      await handle.sync();
    } finally {
      await handle.close();
    }
    await fileSystem.rename(temporary, path.join(directory, filename));
    renamed = true;
    await syncDirectory(directory, fileSystem);
  } catch (error) {
    if (!renamed)
      await fileSystem.rm(temporary, { force: true }).catch(() => {});
    throw error;
  }
}

async function reservePublication(context, content, env, fileSystem, platform) {
  const contentSha256 = crypto
    .createHash("sha256")
    .update(content)
    .digest("hex");
  assertPowerLossDurability(env, platform);
  const configuredRoot =
    env.PI_ACP_RECEIPT_DIR ||
    path.join(os.homedir(), ".buzz", "pi-acp-receipts");
  const root = await ensureDirectoryDurable(configuredRoot, fileSystem);
  const directory = path.join(root, reservationKey(context, env));
  try {
    await fileSystem.mkdir(directory, { mode: 0o700 });
    await syncDirectory(root, fileSystem);
    await writeJsonAtomicDurable(
      directory,
      "request.json",
      { ...context, contentSha256 },
      fileSystem,
    );
    return { directory, existingReceipt: null };
  } catch (error) {
    if (error.code !== "EEXIST") throw error;
    try {
      const requestFile = path.join(directory, "request.json");
      const receiptFile = path.join(directory, "receipt.json");
      const request = JSON.parse(
        await fileSystem.readFile(requestFile, "utf8"),
      );
      if (request.contentSha256 !== contentSha256) {
        throw new Error(
          "a different publication is already reserved for this inbound event",
        );
      }
      const receipt = JSON.parse(
        await fileSystem.readFile(receiptFile, "utf8"),
      );
      if (env.PI_ACP_REQUIRE_POWER_LOSS_DURABILITY === "1") {
        await syncRecordDurable(requestFile, fileSystem);
        await syncRecordDurable(receiptFile, fileSystem);
      }
      return { directory, existingReceipt: receipt };
    } catch (readError) {
      if (readError.code === "ENOENT") {
        throw new Error(
          "publication is already reserved; refusing an ambiguous retry",
        );
      }
      throw readError;
    }
  }
}

async function storeReceipt(directory, receipt, fileSystem) {
  await writeJsonAtomicDurable(directory, "receipt.json", receipt, fileSystem);
}

export function createBuzzTools({
  getContext,
  env = process.env,
  runCommand = run,
  fileSystem = fs,
  platform = process.platform,
  includeCloudControl = false,
} = {}) {
  const buzzReply = defineTool({
    name: "buzz_reply",
    label: "Reply in Buzz",
    description:
      "Publish exactly one non-empty reply to the authenticated inbound Buzz event. Routing is fixed by the harness; do not supply channel or event IDs.",
    parameters: Type.Object({
      content: Type.String({ minLength: 1, maxLength: MAX_CONTENT_BYTES }),
    }),
    execute: async (_toolCallId, params, signal) => {
      const content = params.content;
      if (typeof content !== "string" || content.trim().length === 0) {
        throw new Error("buzz_reply content must not be blank");
      }
      if (Buffer.byteLength(content, "utf8") > MAX_CONTENT_BYTES) {
        throw new Error(
          `buzz_reply content exceeds ${MAX_CONTENT_BYTES} bytes`,
        );
      }
      const context = validateContext(getContext?.());
      const reservation = await reservePublication(
        context,
        content,
        env,
        fileSystem,
        platform,
      );
      if (reservation.existingReceipt) {
        return toolText(JSON.stringify(reservation.existingReceipt), {
          receipt: reservation.existingReceipt,
          replay: true,
        });
      }
      const command = env.PI_ACP_BUZZ_COMMAND || "buzz";
      let result;
      try {
        result = await runCommand(
          command,
          [
            "--format",
            "compact",
            "messages",
            "send",
            "--channel",
            context.channelId,
            "--reply-to",
            context.replyTo,
            "--content",
            "-",
          ],
          { input: content, signal, env },
        );
      } catch (error) {
        if (error.code === "PI_ACP_SAFE_UNSTARTED") {
          await fileSystem.rm(reservation.directory, {
            recursive: true,
            force: true,
          });
        }
        throw error;
      }
      let response;
      try {
        response = JSON.parse(result.stdout);
      } catch {
        throw new Error(
          "buzz_reply returned a non-JSON receipt; publication remains reserved",
        );
      }
      if (
        typeof response.event_id !== "string" ||
        !HEX_EVENT.test(response.event_id)
      ) {
        throw new Error(
          "buzz_reply receipt is missing a valid event_id; publication remains reserved",
        );
      }
      if (response.accepted === false) {
        throw new Error(
          "Buzz rejected the publication; reservation retained for operator review",
        );
      }
      const receipt = {
        event_id: response.event_id,
        accepted: true,
        channel_id: context.channelId,
        reply_to: context.replyTo,
        triggering_event_ids: context.triggeringEventIds,
      };
      await storeReceipt(reservation.directory, receipt, fileSystem);
      return toolText(JSON.stringify(receipt), { receipt, replay: false });
    },
  });

  const cloudControl = defineTool({
    name: "cloud_control",
    label: "Run deterministic cloud ownership command",
    description:
      "Execute one authenticated, exact cloud ownership command without an LLM.",
    parameters: Type.Object({
      command: Type.Union([
        Type.Literal("-status"),
        Type.Literal("-cloud"),
        Type.Literal("-local"),
      ]),
    }),
    execute: async (_toolCallId, params, signal) => {
      if (!CLOUD_CONTROL_COMMANDS.has(params.command)) {
        throw new Error("unsupported cloud control command");
      }
      const context = validateContext(getContext?.());
      const command = env.PI_ACP_CLOUD_CONTROL_COMMAND;
      if (typeof command !== "string" || !path.isAbsolute(command)) {
        throw new Error(
          "PI_ACP_CLOUD_CONTROL_COMMAND must be an absolute executable path",
        );
      }
      const input = `${JSON.stringify({
        schemaVersion: 1,
        command: params.command,
        channelId: context.channelId,
        replyTo: context.replyTo,
        triggeringEventIds: context.triggeringEventIds,
      })}\n`;
      let result;
      try {
        result = await runCommand(command, [], {
          input,
          signal,
          env: cloudControlEnvironment(env),
          timeoutMs: cloudControlTimeout(env),
        });
      } catch (error) {
        const failure = new Error("cloud control command failed");
        if (error.code === "PI_ACP_SAFE_UNSTARTED")
          failure.code = "PI_ACP_SAFE_UNSTARTED";
        throw failure;
      }
      if (result.code !== 0) throw new Error("cloud control command failed");
      const response = parseCloudControlResponse(result.stdout);
      return toolText(response.content, {
        command: params.command,
        status: response.status,
        deterministic: true,
      });
    },
  });

  const kanbanTasks = defineTool({
    name: "kanban_tasks",
    label: "Read compact Kanban tasks",
    description:
      "Read one bounded, filtered compact Kanban AI task list. Never downloads the full board.",
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
    execute: async (_toolCallId, params, signal) => {
      const command =
        env.PI_ACP_KANBAN_COMMAND ||
        path.join(os.homedir(), ".buzz", "bin", "kanban-ai");
      const args = ["tasks", "--limit", String(params.limit || 10), "--json"];
      if (params.sprint) args.push("--sprint", String(params.sprint));
      if (params.status) args.push("--status", params.status);
      if (params.channel) args.push("--channel", params.channel);
      if (params.search) args.push("--search", params.search);
      const result = await runCommand(command, args, { signal, env });
      JSON.parse(result.stdout);
      return toolText(result.stdout, {
        bounded: true,
        staleProvenanceIncluded: true,
      });
    },
  });

  return [
    buzzReply,
    ...(includeCloudControl ? [cloudControl] : []),
    kanbanTasks,
  ];
}

export const testOnly = {
  validateContext,
  reservationKey,
  run,
  assertPowerLossDurability,
  ensureDirectoryDurable,
  syncRecordDurable,
  writeJsonAtomicDurable,
  cloudControlEnvironment,
  cloudControlTimeout,
  parseCloudControlResponse,
};
