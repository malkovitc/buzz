import assert from "node:assert/strict";
import test from "node:test";
import { SteeringDeliveryGate } from "../src/sdk-steering.mjs";

test("acknowledges only the exact steering message Pi dequeues", () => {
  const gate = new SteeringDeliveryGate();
  const accepted = [];
  gate.enqueueInternal("budget checkpoint");
  gate.enqueue({ id: "steer-1" }, "external update");

  gate.observeQueue(
    ["budget checkpoint", "external update"],
    (pending) => accepted.push(pending.id),
    assert.fail,
  );
  gate.observeQueue(
    ["external update"],
    (pending) => accepted.push(pending.id),
    assert.fail,
  );
  assert.deepEqual(accepted, []);
  assert.equal(gate.size, 1);

  gate.observeQueue([], (pending) => accepted.push(pending.id), assert.fail);
  assert.deepEqual(accepted, ["steer-1"]);
  assert.equal(gate.size, 0);
});

test("rejects unconsumed steering before terminal queue cleanup", () => {
  const gate = new SteeringDeliveryGate();
  const rejected = [];
  const entry = gate.enqueue({ id: "steer-2" }, "late update");
  gate.observeQueue(["late update"], assert.fail, assert.fail);

  gate.rejectAll((pending) => rejected.push(pending.id));
  gate.observeQueue([], assert.fail, assert.fail);
  assert.deepEqual(rejected, ["steer-2"]);
  assert.equal(gate.size, 0);
  assert.equal(gate.remove(entry), false);
});

test("fails closed when Pi queue provenance diverges", () => {
  const gate = new SteeringDeliveryGate();
  const rejected = [];
  gate.enqueue({ id: "steer-3" }, "expected");

  gate.observeQueue(["different"], assert.fail, (pending) =>
    rejected.push(pending.id),
  );
  assert.deepEqual(rejected, ["steer-3"]);
  assert.equal(gate.size, 0);
});
