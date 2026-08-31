#!/usr/bin/env node

import fs from "node:fs";
import path from "node:path";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import {
  canonicalJson,
  exportCapsule,
  importCapsule,
  reissueCapsule,
  validateEnvelope,
  verifyGitBinding,
} from "./continuation-capsule.mjs";

const MAX_INPUT_BYTES = 128 * 1024;

function readRequest() {
  const chunks = [];
  const buffer = Buffer.allocUnsafe(16 * 1024);
  let total = 0;
  while (true) {
    const count = fs.readSync(0, buffer, 0, buffer.length, null);
    if (count === 0) break;
    total += count;
    if (total > MAX_INPUT_BYTES) {
      throw new Error(`stdin must contain 1..${MAX_INPUT_BYTES} bytes`);
    }
    chunks.push(Buffer.from(buffer.subarray(0, count)));
  }
  if (total === 0) {
    throw new Error(`stdin must contain 1..${MAX_INPUT_BYTES} bytes`);
  }
  return JSON.parse(Buffer.concat(chunks, total).toString("utf8"));
}

function exactRequest(value, keys, label) {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error(`${label} request is invalid`);
  }
  if (
    JSON.stringify(Object.keys(value).sort()) !==
    JSON.stringify([...keys].sort())
  ) {
    throw new Error(`${label} request has unknown or missing fields`);
  }
}

function requiredString(value, label) {
  if (typeof value !== "string" || value.length === 0)
    throw new Error(`${label} is required`);
  return value;
}

function requiredUuid(value, label) {
  requiredString(value, label);
  if (
    !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(
      value,
    )
  ) {
    throw new Error(`${label} is invalid`);
  }
  return value;
}

function main() {
  const command = process.argv[2];
  const request = readRequest();
  let result;
  if (command === "validate") {
    result = validateEnvelope(request);
  } else if (command === "export") {
    exactRequest(request, ["draft", "sourceSessionFile"], "export");
    requiredString(request.sourceSessionFile, "sourceSessionFile");
    verifyGitBinding(request.draft?.git);
    const source = SessionManager.open(request.sourceSessionFile);
    result = exportCapsule(request.draft, source);
  } else if (command === "reissue") {
    exactRequest(
      request,
      ["sourceSessionFile", "expiredCapsuleId", "replacement"],
      "reissue",
    );
    requiredString(request.sourceSessionFile, "sourceSessionFile");
    requiredUuid(request.expiredCapsuleId, "expiredCapsuleId");
    const source = SessionManager.open(request.sourceSessionFile);
    const receipt = path.join(
      source.getSessionDir(),
      ".capsule-exports",
      `${request.expiredCapsuleId}.json`,
    );
    const expired = JSON.parse(fs.readFileSync(receipt, "utf8"));
    verifyGitBinding(expired?.capsule?.git);
    result = reissueCapsule(expired, request.replacement, source);
  } else if (command === "import") {
    exactRequest(
      request,
      ["envelope", "cwd", "sessionDir", "expected"],
      "import",
    );
    requiredString(request.cwd, "cwd");
    requiredString(request.sessionDir, "sessionDir");
    result = importCapsule(request.envelope, {
      cwd: request.cwd,
      sessionDir: request.sessionDir,
      expected: request.expected,
    });
  } else {
    throw new Error(
      "usage: pi-continuation-capsule <validate|export|reissue|import>",
    );
  }
  process.stdout.write(`${canonicalJson(result)}\n`);
}

try {
  main();
} catch (error) {
  process.stderr.write(`pi-continuation-capsule: ${error.message}\n`);
  process.exitCode = 1;
}
