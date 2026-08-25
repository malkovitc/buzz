#!/usr/bin/env node

process.stdin.resume();
process.stdin.on("end", () => {
  const delay = Number.parseInt(process.env.FAKE_BUZZ_DELAY_MS || "0", 10);
  setTimeout(
    () => {
      process.stdout.write(
        `${JSON.stringify({ event_id: "f".repeat(64), accepted: true })}\n`,
      );
    },
    Number.isSafeInteger(delay) && delay > 0 ? delay : 0,
  );
});
