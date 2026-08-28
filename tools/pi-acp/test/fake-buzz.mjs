#!/usr/bin/env node

process.stdin.resume();
process.stdin.on("end", () => {
  const delayVariable = process.argv.includes("tasks")
    ? "FAKE_KANBAN_DELAY_MS"
    : "FAKE_BUZZ_DELAY_MS";
  const delay = Number.parseInt(process.env[delayVariable] || "0", 10);
  setTimeout(
    () => {
      process.stdout.write(
        `${JSON.stringify({ event_id: "f".repeat(64), accepted: true })}\n`,
      );
    },
    Number.isSafeInteger(delay) && delay > 0 ? delay : 0,
  );
});
