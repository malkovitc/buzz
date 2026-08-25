#!/usr/bin/env node

process.stdin.resume();
process.stdin.on("end", () => {
  process.stdout.write(
    `${JSON.stringify({ event_id: "f".repeat(64), accepted: true })}\n`,
  );
});
