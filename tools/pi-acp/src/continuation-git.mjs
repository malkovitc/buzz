import fs from "node:fs";
import path from "node:path";
import { execFileSync } from "node:child_process";

const HEX40_OR_64 = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

function git(repo, args) {
  return execFileSync("git", ["-C", repo, ...args], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "pipe"],
    env: {
      PATH: process.env.PATH,
      HOME: process.env.HOME,
      GIT_CONFIG_NOSYSTEM: "1",
    },
    timeout: 30_000,
    killSignal: "SIGKILL",
    maxBuffer: 1024 * 1024,
  }).trim();
}

function validateRemoteUrl(value) {
  if (typeof value !== "string" || value.length === 0 || value.length > 2048) {
    throw new Error("git.remoteUrl is invalid");
  }
  if (/^[A-Za-z0-9._-]+@[A-Za-z0-9.-]+:[^\s]+$/.test(value)) return;
  let remote;
  try {
    remote = new URL(value);
  } catch {
    throw new Error("git.remoteUrl is invalid");
  }
  if (
    remote.password ||
    remote.search ||
    remote.hash ||
    !["https:", "ssh:"].includes(remote.protocol) ||
    (remote.protocol === "https:" && remote.username)
  ) {
    throw new Error("git.remoteUrl is invalid or contains credentials");
  }
}

function validateBinding(binding) {
  const expected = [
    "repository",
    "remoteName",
    "remoteUrl",
    "branch",
    "commit",
    "tree",
  ].sort();
  if (
    !binding ||
    typeof binding !== "object" ||
    Array.isArray(binding) ||
    JSON.stringify(Object.keys(binding).sort()) !== JSON.stringify(expected)
  ) {
    throw new Error("git has unknown or missing fields");
  }
  if (!path.isAbsolute(binding.repository)) {
    throw new Error("git.repository must be absolute");
  }
  if (!/^[A-Za-z0-9._-]+$/.test(binding.remoteName)) {
    throw new Error("git.remoteName is invalid");
  }
  validateRemoteUrl(binding.remoteUrl);
  if (
    typeof binding.branch !== "string" ||
    binding.branch.length === 0 ||
    Buffer.byteLength(binding.branch, "utf8") > 512
  ) {
    throw new Error("git.branch is invalid");
  }
  if (!HEX40_OR_64.test(binding.commit) || !HEX40_OR_64.test(binding.tree)) {
    throw new Error("git commit/tree binding is invalid");
  }
}

export function verifyGitBinding(binding, worktree = binding?.repository) {
  validateBinding(binding);
  const repository = fs.realpathSync(worktree);
  if (repository !== worktree) {
    throw new Error("git repository path is non-canonical");
  }
  if (
    git(repository, [
      "status",
      "--porcelain=v1",
      "--untracked-files=all",
      "--ignore-submodules=none",
    ]) !== ""
  ) {
    throw new Error("git worktree is dirty");
  }
  if (
    git(repository, ["for-each-ref", "--format=%(refname)", "refs/replace"]) !==
    ""
  ) {
    throw new Error("git replace refs are unsupported");
  }
  git(repository, ["check-ref-format", "--branch", binding.branch]);
  if (
    git(repository, ["symbolic-ref", "--quiet", "--short", "HEAD"]) !==
    binding.branch
  ) {
    throw new Error("git branch binding mismatch");
  }
  if (git(repository, ["rev-parse", "HEAD"]) !== binding.commit) {
    throw new Error("git commit binding mismatch");
  }
  if (git(repository, ["rev-parse", "HEAD^{tree}"]) !== binding.tree) {
    throw new Error("git tree binding mismatch");
  }
  if (
    git(repository, ["remote", "get-url", binding.remoteName]) !==
    binding.remoteUrl
  ) {
    throw new Error("git remote binding mismatch");
  }
  return true;
}
