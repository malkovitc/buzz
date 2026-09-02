import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { testOnly } from "../src/delegation-host.mjs";

test("first state-directory admission syncs its parent", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-parent-"));
  const directory = path.join(root, "state");
  const originalFsync = fs.fsyncSync;
  let directoryBarriers = 0;
  fs.fsyncSync = (descriptor) => {
    if (fs.fstatSync(descriptor).isDirectory()) directoryBarriers += 1;
    return originalFsync(descriptor);
  };
  try {
    testOnly.ensureStateDirectory(directory);
  } finally {
    fs.fsyncSync = originalFsync;
  }
  assert.equal(directoryBarriers, 1);
  fs.rmSync(root, { recursive: true, force: true });
});

test("activation serialization rejects a reused live PID identity", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-lock-"));
  try {
    assert.notEqual(testOnly.processIdentityFor(process.pid), "reused");
    const release = testOnly.acquireOperationLock(root, "activation");
    assert.throws(
      () => testOnly.acquireOperationLock(root, "activation"),
      /operation is concurrent/,
    );
    release.release();
    const stale = path.join(root, ".activation.lock");
    fs.writeFileSync(
      stale,
      `${JSON.stringify({ schemaVersion: 1, pid: process.pid, processIdentity: "reused", childPid: null, childProcessIdentity: null })}\n`,
      { mode: 0o600 },
    );
    testOnly.acquireOperationLock(root, "activation").release();
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("an orphan helper keeps its operation lock live", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-orphan-"));
  const child = spawn(process.execPath, ["-e", "setInterval(()=>{},1000)"], {
    detached: true,
    stdio: "ignore",
  });
  try {
    await new Promise((resolve) => child.once("spawn", resolve));
    const childProcessIdentity = testOnly.processIdentityFor(child.pid);
    assert.ok(childProcessIdentity);
    fs.writeFileSync(
      path.join(root, ".orphan.lock"),
      `${JSON.stringify({ schemaVersion: 1, pid: 99999999, processIdentity: "gone", childPid: child.pid, childProcessIdentity })}\n`,
      { mode: 0o600 },
    );
    assert.throws(
      () => testOnly.acquireOperationLock(root, "orphan"),
      /operation is concurrent/,
    );
  } finally {
    if (child.pid) process.kill(-child.pid, "SIGKILL");
    await new Promise((resolve) => child.once("close", resolve));
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("unknown process identity fails closed and failed writes remove temp state", () => {
  assert.equal(
    testOnly.processOwnsIdentity(42, "expected", {
      exists: () => true,
      identityFor: () => null,
    }),
    true,
  );
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-write-"));
  const state = path.join(root, "state");
  const originalFsync = fs.fsyncSync;
  fs.fsyncSync = (descriptor) => {
    if (fs.fstatSync(descriptor).isFile()) throw new Error("flush failed");
    return originalFsync(descriptor);
  };
  try {
    assert.throws(
      () => testOnly.atomicJson(path.join(state, "record.json"), {}),
      /flush failed/,
    );
  } finally {
    fs.fsyncSync = originalFsync;
  }
  assert.deepEqual(fs.readdirSync(state), []);
  fs.rmSync(root, { recursive: true, force: true });
});

test("completed wrappers cannot leave a background helper alive", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-reap-"));
  const marker = path.join(root, "survived");
  try {
    const descendant = `setTimeout(()=>require('fs').writeFileSync(${JSON.stringify(marker)},'alive'),150)`;
    const script = [
      "process.stdin.resume()",
      "process.stdin.on('end',()=>{",
      `require('child_process').spawn(process.execPath,['-e',${JSON.stringify(descendant)}],{stdio:'ignore'}).unref()`,
      "process.stdout.write('bad-json')",
      "})",
    ].join(";");
    await assert.rejects(
      testOnly.runVector([process.execPath, "-e", script], {}),
      /returned invalid JSON/,
    );
    await new Promise((resolve) => setTimeout(resolve, 250));
    assert.equal(fs.existsSync(marker), false);
  } finally {
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test("delegation input is rejected before buffering beyond its limit", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-delegation-input-"));
  const input = path.join(root, "request.json");
  fs.writeFileSync(input, "x".repeat(256 * 1024 + 1), { mode: 0o600 });
  const descriptor = fs.openSync(input, "r");
  try {
    assert.throws(
      () => testOnly.readBoundedInput(descriptor),
      /request size is invalid/,
    );
  } finally {
    fs.closeSync(descriptor);
    fs.rmSync(root, { recursive: true, force: true });
  }
});
