import crypto from "node:crypto";
import { execFileSync, spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { canonicalJson } from "./continuation-canonical.mjs";

const MAX_INPUT_BYTES = 256 * 1024;
const MAX_STATE_FILES = 512;
const MAX_STATE_BYTES = 64 * 1024 * 1024;

function isPlainObject(value) {
  return value !== null && typeof value === "object" && !Array.isArray(value);
}

function exactObject(value, fields, label) {
  if (!isPlainObject(value)) throw new Error(`${label} must be an object`);
  const actual = Object.keys(value).sort();
  const expected = [...fields].sort();
  if (canonicalJson(actual) !== canonicalJson(expected)) {
    throw new Error(`${label} has unknown or missing fields`);
  }
  return value;
}

export function readJson(file, maxBytes = MAX_INPUT_BYTES) {
  const stat = fs.lstatSync(file);
  const ownedByCurrentUser =
    typeof process.getuid !== "function" || stat.uid === process.getuid();
  const safeFile =
    stat.isFile() &&
    !stat.isSymbolicLink() &&
    stat.size <= maxBytes &&
    (stat.mode & 0o077) === 0 &&
    ownedByCurrentUser;
  if (!safeFile) throw new Error("delegation state file is unsafe");
  return JSON.parse(fs.readFileSync(file, "utf8"));
}

export function ensureStateDirectory(directory) {
  const parent = path.dirname(directory);
  const parentStat = fs.lstatSync(parent);
  const safeParent = parentStat.isDirectory() && !parentStat.isSymbolicLink();
  if (!safeParent) {
    throw new Error("delegation state parent directory is unsafe");
  }
  try {
    fs.mkdirSync(directory, { mode: 0o700 });
  } catch (error) {
    if (error.code !== "EEXIST") throw error;
  }
  const stat = fs.lstatSync(directory);
  const ownedByCurrentUser =
    typeof process.getuid !== "function" || stat.uid === process.getuid();
  const safeDirectory =
    stat.isDirectory() &&
    !stat.isSymbolicLink() &&
    (stat.mode & 0o077) === 0 &&
    ownedByCurrentUser;
  if (!safeDirectory) throw new Error("delegation state directory is unsafe");
  const parentDescriptor = fs.openSync(parent, "r");
  try {
    fs.fsyncSync(parentDescriptor);
  } finally {
    fs.closeSync(parentDescriptor);
  }
}

export function readDurableJson(file) {
  const descriptor = fs.openSync(file, "r");
  try {
    fs.fsyncSync(descriptor);
  } finally {
    fs.closeSync(descriptor);
  }
  const directoryDescriptor = fs.openSync(path.dirname(file), "r");
  try {
    fs.fsyncSync(directoryDescriptor);
  } finally {
    fs.closeSync(directoryDescriptor);
  }
  return readJson(file);
}

export function assertStateCapacity(directory, ignoredEntry) {
  ensureStateDirectory(directory);
  const entries = fs.readdirSync(directory, { withFileTypes: true });
  const validEntries = entries.every((entry) => entry.isFile());
  if (!validEntries) {
    throw new Error("delegation state directory contains unsupported entries");
  }
  const quotaEntries = entries.filter((entry) => entry.name !== ignoredEntry);
  const bytes = quotaEntries.reduce((total, entry) => {
    const stat = fs.lstatSync(path.join(directory, entry.name));
    if (stat.isSymbolicLink()) {
      throw new Error("delegation state directory contains a link");
    }
    return total + stat.size;
  }, 0);
  const hasCapacity =
    quotaEntries.length < MAX_STATE_FILES &&
    bytes + MAX_INPUT_BYTES <= MAX_STATE_BYTES;
  if (!hasCapacity) {
    throw new Error("delegation state capacity is exhausted");
  }
}

export function processIdentityFor(pid) {
  const validPid = Number.isSafeInteger(pid) && pid > 0;
  if (!validPid) return null;
  try {
    if (process.platform === "linux") {
      const bootId = fs.readFileSync("/proc/sys/kernel/random/boot_id", "utf8");
      const stat = fs.readFileSync(`/proc/${pid}/stat`, "utf8");
      const startTime = stat.slice(stat.lastIndexOf(")") + 2).split(/\s+/)[19];
      return `linux:${bootId.trim()}:${startTime}`;
    }
    if (process.platform === "darwin") {
      const startTime = execFileSync(
        "/bin/ps",
        ["-o", "lstart=", "-p", String(pid)],
        {
          encoding: "utf8",
          stdio: ["ignore", "pipe", "ignore"],
          timeout: 1_000,
        },
      ).trim();
      return startTime.length > 0 ? `darwin:${startTime}` : null;
    }
  } catch {
    return null;
  }
  return null;
}

function lockRecord(content) {
  const record = JSON.parse(content);
  exactObject(
    record,
    [
      "schemaVersion",
      "pid",
      "processIdentity",
      "childPid",
      "childProcessIdentity",
    ],
    "lock",
  );
  const validParent =
    record.schemaVersion === 1 &&
    Number.isSafeInteger(record.pid) &&
    record.pid > 0 &&
    typeof record.processIdentity === "string" &&
    record.processIdentity.length > 0 &&
    record.processIdentity.length <= 256;
  const childIsAbsent =
    record.childPid === null && record.childProcessIdentity === null;
  const childIsValid =
    Number.isSafeInteger(record.childPid) &&
    record.childPid > 0 &&
    typeof record.childProcessIdentity === "string" &&
    record.childProcessIdentity.length > 0 &&
    record.childProcessIdentity.length <= 256;
  const validRecord = validParent && (childIsAbsent || childIsValid);
  if (!validRecord) throw new Error("delegation operation lock is corrupt");
  return record;
}

function processExists(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    return error.code === "ESRCH" ? false : null;
  }
}

export function processOwnsIdentity(
  pid,
  identity,
  { identityFor = processIdentityFor, exists = processExists } = {},
) {
  const existence = exists(pid);
  if (existence !== true) return existence === null;
  const observedIdentity = identityFor(pid);
  return observedIdentity === null || observedIdentity === identity;
}

function lockOwnerIsAlive(record) {
  const parentIsAlive = processOwnsIdentity(record.pid, record.processIdentity);
  const childIsAlive =
    record.childPid !== null &&
    processOwnsIdentity(record.childPid, record.childProcessIdentity);
  return parentIsAlive || childIsAlive;
}

export function atomicJson(file, value) {
  const directory = path.dirname(file);
  ensureStateDirectory(directory);
  const temporary = path.join(
    directory,
    `.${path.basename(file)}.${process.pid}.${crypto.randomUUID()}.tmp`,
  );
  let renamed = false;
  try {
    const descriptor = fs.openSync(temporary, "wx", 0o600);
    try {
      fs.writeFileSync(descriptor, `${canonicalJson(value)}\n`);
      fs.fsyncSync(descriptor);
    } finally {
      fs.closeSync(descriptor);
    }
    fs.renameSync(temporary, file);
    renamed = true;
    const directoryDescriptor = fs.openSync(directory, "r");
    try {
      fs.fsyncSync(directoryDescriptor);
    } finally {
      fs.closeSync(directoryDescriptor);
    }
  } catch (error) {
    if (!renamed) fs.rmSync(temporary, { force: true });
    throw error;
  }
}

export function acquireOperationLock(directory, operationId) {
  ensureStateDirectory(directory);
  const lock = path.join(directory, `.${operationId}.lock`);
  const processIdentity = processIdentityFor(process.pid);
  if (processIdentity === null)
    throw new Error("delegation process identity is unavailable");
  let token = {
    schemaVersion: 1,
    pid: process.pid,
    processIdentity,
    childPid: null,
    childProcessIdentity: null,
  };
  const encodedToken = () => `${canonicalJson(token)}\n`;
  const create = () => {
    const descriptor = fs.openSync(lock, "wx", 0o600);
    try {
      fs.writeFileSync(descriptor, encodedToken());
      fs.fsyncSync(descriptor);
    } finally {
      fs.closeSync(descriptor);
    }
  };
  try {
    create();
  } catch (error) {
    if (error.code !== "EEXIST") throw error;
    let staleToken;
    try {
      staleToken = fs.readFileSync(lock, "utf8");
    } catch (readError) {
      if (readError.code !== "ENOENT") throw readError;
      throw new Error("delegation operation is concurrent");
    }
    const owner = lockRecord(staleToken);
    if (lockOwnerIsAlive(owner)) {
      throw new Error("delegation operation is concurrent");
    }
    const recovery = path.join(directory, `.${operationId}.recovery`);
    try {
      fs.mkdirSync(recovery, { mode: 0o700 });
    } catch (recoveryError) {
      if (recoveryError.code !== "EEXIST") throw recoveryError;
      throw new Error("delegation lock recovery is concurrent or interrupted");
    }
    try {
      if (fs.readFileSync(lock, "utf8") !== staleToken) {
        throw new Error("delegation operation lock changed during recovery");
      }
      const stale = path.join(
        directory,
        `.${operationId}.${crypto.randomUUID()}.stale`,
      );
      fs.renameSync(lock, stale);
      fs.unlinkSync(stale);
      create();
    } finally {
      fs.rmdirSync(recovery);
    }
  }
  const trackChild = (pid) => {
    const childProcessIdentity = processIdentityFor(pid);
    if (childProcessIdentity === null) {
      throw new Error("delegation child process identity is unavailable");
    }
    if (fs.readFileSync(lock, "utf8") !== encodedToken()) {
      throw new Error("delegation operation lock changed before child start");
    }
    token = { ...token, childPid: pid, childProcessIdentity };
    atomicJson(lock, token);
  };
  const release = () => {
    if (!fs.existsSync(lock)) return;
    if (fs.readFileSync(lock, "utf8") !== encodedToken()) {
      throw new Error("delegation operation lock changed before release");
    }
    fs.unlinkSync(lock);
  };
  return { release, trackChild };
}

export function reserveStateCapacity(directory) {
  const operationId = "capacity";
  const lease = acquireOperationLock(directory, operationId);
  try {
    assertStateCapacity(directory, `.${operationId}.lock`);
    return lease;
  } catch (error) {
    lease.release();
    throw error;
  }
}

export async function runVector(vector, input, { lease } = {}) {
  return await new Promise((resolve, reject) => {
    const child = spawn(vector[0], vector.slice(1), {
      detached: process.platform !== "win32",
      env: {
        HOME: os.homedir(),
        PATH: "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
        LANG: "C",
      },
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = Buffer.alloc(0);
    let stderr = Buffer.alloc(0);
    let requested = null;
    let settled = false;
    let timer;
    const terminate = () => {
      try {
        if (process.platform === "win32") child.kill("SIGKILL");
        else if (child.pid) process.kill(-child.pid, "SIGKILL");
      } catch {}
    };
    const settle = (error, value) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      terminate();
      process.removeListener("SIGINT", abort);
      process.removeListener("SIGTERM", abort);
      if (error) reject(error);
      else resolve(value);
    };
    const requestStop = (error) => {
      if (requested === null) requested = { error };
      terminate();
    };
    const abort = () =>
      requestStop(new Error("delegation command was interrupted"));
    const append = (current, chunk) => {
      if (requested !== null) return current;
      const next = Buffer.concat([current, chunk]);
      if (next.length > 64 * 1024) {
        requestStop(new Error("delegation command output is too large"));
        return current;
      }
      return next;
    };
    child.stdout.on("data", (chunk) => {
      stdout = append(stdout, chunk);
    });
    child.stderr.on("data", (chunk) => {
      stderr = append(stderr, chunk);
    });
    process.once("SIGINT", abort);
    process.once("SIGTERM", abort);
    child.once("error", () =>
      settle(new Error("delegation command could not start")),
    );
    child.stdin.once("error", () =>
      requestStop(new Error("delegation command rejected its input")),
    );
    child.once("close", (code) => {
      if (requested?.error) {
        settle(requested.error);
        return;
      }
      if (code !== 0) {
        settle(new Error("delegation command failed"));
        return;
      }
      try {
        settle(undefined, JSON.parse(stdout.toString("utf8")));
      } catch {
        settle(new Error("delegation command returned invalid JSON"));
      }
    });
    timer = setTimeout(
      () => requestStop(new Error("delegation command timed out")),
      30_000,
    );
    try {
      const childCanBeTracked = lease !== undefined && child.pid !== undefined;
      if (childCanBeTracked) lease.trackChild(child.pid);
      child.stdin.end(`${canonicalJson(input)}\n`);
    } catch (error) {
      requestStop(error);
    }
  });
}

export function readBoundedInput(descriptor = 0, maxBytes = MAX_INPUT_BYTES) {
  const chunks = [];
  const buffer = Buffer.allocUnsafe(16 * 1024);
  let total = 0;
  while (true) {
    const count = fs.readSync(descriptor, buffer, 0, buffer.length, null);
    if (count === 0) break;
    total += count;
    if (total > maxBytes) {
      throw new Error("delegation request size is invalid");
    }
    chunks.push(Buffer.from(buffer.subarray(0, count)));
  }
  if (total === 0) throw new Error("delegation request size is invalid");
  return Buffer.concat(chunks, total);
}
