import assert from "node:assert/strict";
import fs from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import test from "node:test";
import { createBuzzTools, testOnly } from "../src/buzz-tools.mjs";

const context = {
  channelId: "4dcab690-a2ca-4a56-9e5d-d901d12f83c3",
  triggeringEventIds: ["a".repeat(64)],
  allowedReplyEventIds: ["a".repeat(64)],
  replyTo: "a".repeat(64),
};
const eventId = "b".repeat(64);

async function fixture(t, runCommand, options = {}) {
  const receiptDir = await fs.mkdtemp(path.join(os.tmpdir(), "pi-acp-tools-"));
  t.after(() => fs.rm(receiptDir, { recursive: true, force: true }));
  const env = {
    BUZZ_PRIVATE_KEY: "nsec_test_only_high_entropy_secret",
    BUZZ_RELAY_URL: "wss://relay.example",
    PI_ACP_RECEIPT_DIR: receiptDir,
    PI_ACP_BUZZ_COMMAND: "/test/buzz",
    PI_ACP_KANBAN_COMMAND: "/test/kanban-ai",
    ...(options.env ?? {}),
  };
  const tools = createBuzzTools({
    getContext: () => context,
    env,
    runCommand,
    ...(options.fileSystem ? { fileSystem: options.fileSystem } : {}),
    ...(options.platform ? { platform: options.platform } : {}),
  });
  return {
    env,
    receiptDir,
    reply: tools.find((tool) => tool.name === "buzz_reply"),
    kanban: tools.find((tool) => tool.name === "kanban_tasks"),
  };
}

test("buzz_reply binds routing, connects stdin, and replays one durable receipt", async (t) => {
  const calls = [];
  const f = await fixture(t, async (command, args, options) => {
    calls.push({ command, args, options });
    return {
      code: 0,
      stdout: JSON.stringify({ event_id: eventId, accepted: true }),
      stderr: "",
    };
  });

  const first = await f.reply.execute("call-1", { content: "Visible answer" });
  assert.equal(calls.length, 1);
  assert.equal(calls[0].command, "/test/buzz");
  assert.deepEqual(calls[0].args, [
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
  ]);
  assert.equal(calls[0].options.input, "Visible answer");
  assert.equal(first.details.replay, false);
  assert.equal(first.details.receipt.event_id, eventId);

  const second = await f.reply.execute("call-2", { content: "Visible answer" });
  assert.equal(calls.length, 1, "receipt replay must not publish twice");
  assert.equal(second.details.replay, true);
  assert.equal(second.details.receipt.event_id, eventId);
  await assert.rejects(
    f.reply.execute("call-3", { content: "Different answer" }),
    /different publication is already reserved/,
  );
  assert.equal(
    calls.length,
    1,
    "different content must not bypass the reservation",
  );
});

test("buzz_reply rejects blank content before reserving or running a command", async (t) => {
  let calls = 0;
  const f = await fixture(t, async () => {
    calls += 1;
  });
  await assert.rejects(
    f.reply.execute("call", { content: "  \n" }),
    /must not be blank/,
  );
  assert.equal(calls, 0);
  assert.deepEqual(await fs.readdir(f.receiptDir), []);
});

test("buzz_reply fails closed after an ambiguous reserved publication", async (t) => {
  let calls = 0;
  const f = await fixture(t, async () => {
    calls += 1;
    throw new Error("network result unknown");
  });
  await assert.rejects(
    f.reply.execute("call-1", { content: "answer" }),
    /network result unknown/,
  );
  await assert.rejects(
    f.reply.execute("call-2", { content: "answer" }),
    /already reserved/,
  );
  assert.equal(calls, 1);
});

test("strict replay re-barriers an uncertain receipt without republishing", async (t) => {
  let failReceiptDirectorySync = false;
  const rebarriered = [];
  const fileSystem = {
    lstat: (...args) => fs.lstat(...args),
    stat: (...args) => fs.stat(...args),
    realpath: (...args) => fs.realpath(...args),
    mkdir: (...args) => fs.mkdir(...args),
    readFile: (...args) => fs.readFile(...args),
    rename: async (source, destination) => {
      await fs.rename(source, destination);
      if (path.basename(destination) === "receipt.json")
        failReceiptDirectorySync = true;
    },
    rm: (...args) => fs.rm(...args),
    open: async (target, flags, mode) => {
      const handle = await fs.open(target, flags, mode);
      return {
        writeFile: (...args) => handle.writeFile(...args),
        sync: async () => {
          if (
            failReceiptDirectorySync &&
            flags === "r" &&
            path.basename(target) !== "request.json" &&
            path.basename(target) !== "receipt.json"
          ) {
            failReceiptDirectorySync = false;
            throw new Error("receipt directory flush failed");
          }
          await handle.sync();
          if (flags === "r" && path.basename(target).endsWith(".json"))
            rebarriered.push(path.basename(target));
        },
        close: () => handle.close(),
      };
    },
  };
  let publications = 0;
  const f = await fixture(
    t,
    async () => {
      publications += 1;
      return {
        code: 0,
        stdout: JSON.stringify({ event_id: eventId, accepted: true }),
        stderr: "",
      };
    },
    {
      fileSystem,
      platform: "linux",
      env: { PI_ACP_REQUIRE_POWER_LOSS_DURABILITY: "1" },
    },
  );
  await assert.rejects(
    f.reply.execute("call-1", { content: "durable answer" }),
    /receipt directory flush failed/,
  );
  const replay = await f.reply.execute("call-2", {
    content: "durable answer",
  });
  assert.equal(publications, 1, "reconciliation must not publish again");
  assert.equal(replay.details.replay, true);
  assert.deepEqual(rebarriered, ["request.json", "receipt.json"]);
});

test("buzz_reply rejects unbound reply targets", async () => {
  const invalid = {
    ...context,
    allowedReplyEventIds: ["c".repeat(64)],
  };
  const [reply] = createBuzzTools({
    getContext: () => invalid,
    env: { BUZZ_PRIVATE_KEY: "secret" },
    runCommand: async () => assert.fail("must not run"),
  });
  await assert.rejects(
    reply.execute("call", { content: "answer" }),
    /reply authorization set is invalid/,
  );
});

test("pre-spawn abort releases the safe publication reservation", async (t) => {
  const f = await fixture(t, testOnly.run);
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    f.reply.execute("call", { content: "safe to retry" }, controller.signal),
    /aborted/,
  );
  assert.deepEqual(await fs.readdir(f.receiptDir), []);
});

test("missing publisher executables release the unstarted reservation", async (t) => {
  const f = await fixture(t, testOnly.run);
  await assert.rejects(
    f.reply.execute("call", { content: "retry after install" }),
    (error) => error.code === "PI_ACP_SAFE_UNSTARTED",
  );
  assert.deepEqual(await fs.readdir(f.receiptDir), []);
});

test("broker commands receive the authoritative adapter parent pid", async () => {
  const result = await testOnly.run(process.execPath, [
    "-e",
    "process.stdout.write(process.env.PI_ACP_BROKER_PARENT_PID || '')",
  ]);
  assert.equal(result.stdout, String(process.pid));
});

test("pre-aborted command signals never spawn a publication process", async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "pi-acp-abort-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const marker = path.join(dir, "spawned");
  const controller = new AbortController();
  controller.abort();
  await assert.rejects(
    testOnly.run(
      process.execPath,
      ["-e", `require('node:fs').writeFileSync(${JSON.stringify(marker)}, '')`],
      { signal: controller.signal },
    ),
    (error) =>
      error.message.includes("aborted") &&
      error.code === "PI_ACP_SAFE_UNSTARTED",
  );
  await assert.rejects(fs.access(marker));
});

test("durable JSON records flush in write-sync-rename-directory-sync order", async (t) => {
  const dir = await fs.mkdtemp(
    path.join(os.tmpdir(), "pi-acp-durable-record-"),
  );
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const operations = [];
  const fileSystem = {
    open: async (...args) => {
      const kind = args[1] === "r" ? "directory" : "file";
      operations.push(`open:${kind}`);
      const handle = await fs.open(...args);
      return {
        writeFile: async (...writeArgs) => {
          operations.push("write:file");
          await handle.writeFile(...writeArgs);
        },
        sync: async () => {
          operations.push(`sync:${kind}`);
          await handle.sync();
        },
        close: async () => {
          operations.push(`close:${kind}`);
          await handle.close();
        },
      };
    },
    rename: async (...args) => {
      operations.push("rename");
      await fs.rename(...args);
    },
    rm: (...args) => fs.rm(...args),
  };
  await testOnly.writeJsonAtomicDurable(
    dir,
    "receipt.json",
    { event_id: eventId, accepted: true },
    fileSystem,
  );
  assert.deepEqual(operations, [
    "open:file",
    "write:file",
    "sync:file",
    "close:file",
    "rename",
    "open:directory",
    "sync:directory",
    "close:directory",
  ]);
  assert.deepEqual(
    JSON.parse(await fs.readFile(path.join(dir, "receipt.json"), "utf8")),
    { event_id: eventId, accepted: true },
  );
  assert.equal(
    (await fs.stat(path.join(dir, "receipt.json"))).mode & 0o777,
    0o600,
  );
  assert.deepEqual(await fs.readdir(dir), ["receipt.json"]);
});

test("first-run receipt roots sync every newly linked directory", async (t) => {
  const base = await fs.mkdtemp(path.join(os.tmpdir(), "pi-acp-durable-root-"));
  t.after(() => fs.rm(base, { recursive: true, force: true }));
  const operations = [];
  const fileSystem = {
    lstat: (...args) => fs.lstat(...args),
    realpath: (...args) => fs.realpath(...args),
    mkdir: async (directory, options) => {
      operations.push(`mkdir:${path.basename(directory)}`);
      await fs.mkdir(directory, options);
    },
    open: async (directory, flags) => {
      operations.push(`open:${path.basename(directory)}`);
      const handle = await fs.open(directory, flags);
      return {
        sync: async () => {
          operations.push(`sync:${path.basename(directory)}`);
          await handle.sync();
        },
        close: () => handle.close(),
      };
    },
  };
  const root = await testOnly.ensureDirectoryDurable(
    path.join(base, ".buzz", "pi-acp-receipts"),
    fileSystem,
  );
  const canonicalBase = await fs.realpath(base);
  assert.equal(root, path.join(canonicalBase, ".buzz", "pi-acp-receipts"));
  assert.deepEqual(operations, [
    `open:${path.basename(path.dirname(canonicalBase))}`,
    `sync:${path.basename(path.dirname(canonicalBase))}`,
    "mkdir:.buzz",
    `open:${path.basename(base)}`,
    `sync:${path.basename(base)}`,
    "mkdir:pi-acp-receipts",
    "open:.buzz",
    "sync:.buzz",
  ]);
});

test("an existing first-use root resyncs its parent before publication", async (t) => {
  const parent = await fs.mkdtemp(path.join(os.tmpdir(), "pi-acp-root-race-"));
  const root = path.join(parent, "receipts");
  await fs.mkdir(root);
  t.after(() => fs.rm(parent, { recursive: true, force: true }));
  const synced = [];
  const fileSystem = {
    lstat: (...args) => fs.lstat(...args),
    realpath: (...args) => fs.realpath(...args),
    open: async (directory, flags) => {
      const handle = await fs.open(directory, flags);
      return {
        sync: async () => {
          synced.push(await fs.realpath(directory));
          await handle.sync();
        },
        close: () => handle.close(),
      };
    },
  };
  const canonicalRoot = await testOnly.ensureDirectoryDurable(root, fileSystem);
  assert.equal(canonicalRoot, await fs.realpath(root));
  assert.deepEqual(synced, [await fs.realpath(parent)]);
});

test("strict power-loss mode fails closed off Linux", () => {
  assert.throws(
    () =>
      testOnly.assertPowerLossDurability(
        { PI_ACP_REQUIRE_POWER_LOSS_DURABILITY: "1" },
        "darwin",
      ),
    /supported only on Linux/,
  );
  assert.doesNotThrow(() =>
    testOnly.assertPowerLossDurability(
      { PI_ACP_REQUIRE_POWER_LOSS_DURABILITY: "1" },
      "linux",
    ),
  );
});

test("failed durability barriers retain a fail-closed record state", async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "pi-acp-durable-fault-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  let syncNumber = 0;
  const fileSystem = {
    open: async (...args) => {
      const handle = await fs.open(...args);
      return {
        writeFile: (...writeArgs) => handle.writeFile(...writeArgs),
        sync: async () => {
          syncNumber += 1;
          await handle.sync();
          if (syncNumber === 2) throw new Error("directory flush failed");
        },
        close: () => handle.close(),
      };
    },
    rename: (...args) => fs.rename(...args),
    rm: (...args) => fs.rm(...args),
  };
  await assert.rejects(
    testOnly.writeJsonAtomicDurable(
      dir,
      "request.json",
      { contentSha256: "a".repeat(64) },
      fileSystem,
    ),
    /directory flush failed/,
  );
  assert.deepEqual(await fs.readdir(dir), ["request.json"]);
});

test("visible records can be re-barriered before durable replay", async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "pi-acp-rebarrier-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const file = path.join(dir, "receipt.json");
  await fs.writeFile(file, JSON.stringify({ event_id: eventId }), {
    mode: 0o600,
  });
  const operations = [];
  const fileSystem = {
    open: async (target, flags) => {
      const kind = target === file ? "file" : "directory";
      operations.push(`open:${kind}`);
      const handle = await fs.open(target, flags);
      return {
        sync: async () => {
          operations.push(`sync:${kind}`);
          await handle.sync();
        },
        close: () => handle.close(),
      };
    },
  };
  await testOnly.syncRecordDurable(file, fileSystem);
  assert.deepEqual(operations, [
    "open:file",
    "sync:file",
    "open:directory",
    "sync:directory",
  ]);
});

test("pre-rename durability failure removes incomplete temporary records", async (t) => {
  const dir = await fs.mkdtemp(
    path.join(os.tmpdir(), "pi-acp-durable-pre-rename-"),
  );
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const fileSystem = {
    open: async (...args) => {
      const handle = await fs.open(...args);
      return {
        writeFile: (...writeArgs) => handle.writeFile(...writeArgs),
        sync: async () => {
          await handle.sync();
          throw new Error("file flush failed");
        },
        close: () => handle.close(),
      };
    },
    rename: (...args) => fs.rename(...args),
    rm: (...args) => fs.rm(...args),
  };
  await assert.rejects(
    testOnly.writeJsonAtomicDurable(
      dir,
      "request.json",
      { contentSha256: "a".repeat(64) },
      fileSystem,
    ),
    /file flush failed/,
  );
  assert.deepEqual(await fs.readdir(dir), []);
});

test("publisher output waits for inherited stdio to close", {
  skip: process.platform === "win32",
}, async () => {
  const result = await testOnly.run("/bin/sh", [
    "-c",
    "(sleep 0.1; printf delayed-receipt) &",
  ]);
  assert.equal(result.stdout, "delayed-receipt");
});

test("aborting a wrapper terminates its publication process group", {
  skip: process.platform === "win32",
}, async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "pi-acp-group-abort-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const marker = path.join(dir, "descendant-published");
  const childCode = `process.on('SIGTERM', () => {}); setTimeout(() => require('node:fs').writeFileSync(${JSON.stringify(marker)}, ''), 600)`;
  const controller = new AbortController();
  const running = testOnly.run(
    "/bin/sh",
    ["-c", `${process.execPath} -e ${JSON.stringify(childCode)} & wait`],
    { signal: controller.signal },
  );
  setTimeout(() => controller.abort(), 100);
  await assert.rejects(running);
  await new Promise((resolve) => setTimeout(resolve, 700));
  await assert.rejects(fs.access(marker));
});

test("an exited wrapper cannot leave a background publisher alive", {
  skip: process.platform === "win32",
}, async (t) => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "pi-acp-wrapper-exit-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const marker = path.join(dir, "background-published");
  const childCode = `setTimeout(() => require('node:fs').writeFileSync(${JSON.stringify(marker)}, ''), 500)`;
  await testOnly.run("/bin/sh", [
    "-c",
    `${process.execPath} -e ${JSON.stringify(childCode)} >/dev/null 2>&1 &`,
  ]);
  await new Promise((resolve) => setTimeout(resolve, 650));
  await assert.rejects(fs.access(marker));
});

test("kanban_tasks emits one bounded filtered compact query", async (t) => {
  const calls = [];
  const f = await fixture(t, async (command, args) => {
    calls.push({ command, args });
    return { code: 0, stdout: '[{"id":"task-1"}]', stderr: "" };
  });
  const result = await f.kanban.execute("call", {
    sprint: 1,
    status: "in-progress",
    channel: "linza-ui-ux",
    search: "report",
    limit: 5,
  });
  assert.equal(calls.length, 1);
  assert.deepEqual(calls[0], {
    command: "/test/kanban-ai",
    args: [
      "tasks",
      "--limit",
      "5",
      "--json",
      "--sprint",
      "1",
      "--status",
      "in-progress",
      "--channel",
      "linza-ui-ux",
      "--search",
      "report",
    ],
  });
  assert.equal(result.details.bounded, true);
});

test("reservation keys separate identities without exposing private keys", () => {
  const first = testOnly.reservationKey(context, {
    BUZZ_PRIVATE_KEY: "identity-one",
    BUZZ_RELAY_URL: "wss://relay.example",
  });
  const second = testOnly.reservationKey(context, {
    BUZZ_PRIVATE_KEY: "identity-two",
    BUZZ_RELAY_URL: "wss://relay.example",
  });
  assert.notEqual(first, second);
  assert.doesNotMatch(first, /identity/);
  assert.match(first, /^[0-9a-f]{64}$/);
});
