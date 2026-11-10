import { driver } from "../src/drivers.mjs";
import { randomUUID } from "node:crypto";
import assert from "node:assert/strict";
const name = `t3-sandbox-smoke-${randomUUID()}`;
const d = driver({ driver: "boat", image: process.env.T3_BOAT_SNAPSHOT ?? "fleet-base" });
let ref;
try {
  const resource = await d.create({ name, ttl: 600 });
  ref = resource.ref;
  assert.ok(ref);
  assert.equal((await d.get(ref)).state, "running");
  await d.setDeadline(ref, 600);
  assert.equal(await d.exec(ref, "printf sandbox-probe"), "sandbox-probe");
  await d.stop(ref);
  const stopped = await d.get(ref);
  assert.ok(["stopped", "stopping"].includes(stopped.state));
  console.log("Boat: create, inspect, exec, provider deadline, and stop acceptance verified.");
  if (stopped.state === "stopped") {
    await d.start(ref);
    assert.equal((await d.get(ref)).state, "running");
    console.log("Boat: resume verified.");
  } else console.log("Boat snapshot is still stopping; resume was not tested.");
} finally {
  ref ??= (await d.list()).find((r) => r.name === name)?.ref;
  if (ref) {
    await d.stop(ref);
    await d.remove(ref);
    console.log("Task-owned Boat sandbox deleted.");
  }
}
