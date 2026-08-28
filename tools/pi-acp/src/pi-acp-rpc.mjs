#!/usr/bin/env node

import packageMetadata from "../package.json" with { type: "json" };
import { PiAcpAdapter } from "./adapter.mjs";

if (process.argv.includes("--version") || process.argv.includes("-V")) {
  process.stdout.write(`pi-acp ${packageMetadata.version}\n`);
  process.exit(0);
}
if (process.platform === "win32") {
  process.stderr.write(
    "pi-acp: this internal pilot supports macOS and Linux only\n",
  );
  process.exit(1);
}

const adapter = new PiAcpAdapter({
  input: process.stdin,
  output: process.stdout,
  errorOutput: process.stderr,
});
adapter.start();

for (const signal of ["SIGINT", "SIGTERM"]) {
  process.on(signal, async () => {
    await adapter.shutdown();
    process.exit(0);
  });
}
