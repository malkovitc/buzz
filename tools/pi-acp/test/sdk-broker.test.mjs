import assert from "node:assert/strict";
import test from "node:test";
import { BrokerRequestRegistry } from "../src/sdk-broker.mjs";

test("aborts a pending broker request and ignores its late response", async () => {
  const requests = new BrokerRequestRegistry();
  const controller = new AbortController();
  let outbound;
  const pending = requests.request(
    "kanban_tasks",
    { limit: 1 },
    controller.signal,
    (request) => {
      outbound = request;
    },
  );

  assert.equal(requests.size, 1);
  controller.abort();
  await assert.rejects(pending, { name: "AbortError" });
  assert.equal(requests.size, 0);
  assert.equal(
    requests.respond({ id: outbound.id, success: true, result: {} }),
    false,
  );
});

test("cleans up a broker request when transport send fails", async () => {
  const requests = new BrokerRequestRegistry();
  const pending = requests.request("buzz_reply", {}, undefined, () => {
    throw new Error("closed pipe");
  });

  await assert.rejects(pending, /closed pipe/);
  assert.equal(requests.size, 0);
});
