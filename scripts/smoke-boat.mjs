import { driver } from "../src/drivers.mjs";
import { command } from "../src/io.mjs";
import { randomUUID } from "node:crypto";
import assert from "node:assert/strict";
const name = `t3-sandbox-smoke-${randomUUID()}`;
const d = driver({ driver: "boat", image: "fleet-base" }, async (executable, args, options) => {
  const output = await command(executable, args, options);
  if (args.includes("--json")) {
    const frames = output
      .split("\n")
      .filter((x) => x.trim())
      .map((x) => JSON.parse(x));
    console.log("Boat operation", args[2], frames.map((x) => Object.keys(x).join(",")).join(";"));
    await (
      await import("node:fs/promises")
    ).writeFile("/tmp/sandbox-boat-last-" + args[2] + ".json", output, { mode: 0o600 });
  }
  return output;
});
let ref;
try {
  const resource = await d.create({ name, ttl: 600 });
  ref = resource.ref;
  assert.ok(ref);
  console.log("Task-owned Boat sandbox", ref);
  const info = await d.get(ref);
  assert.equal(info.state, "running");
  await d.setDeadline(ref, 600);
  assert.equal(await d.exec(ref, "printf sandbox-probe"), "sandbox-probe");
  await d.stop(ref);
  assert.equal((await d.get(ref)).state, "stopped");
  await d.start(ref);
  assert.equal((await d.get(ref)).state, "running");
  console.log("Boat create, inspect, exec, provider deadline, stop and resume verified.");
} finally {
  ref ??= (await d.list()).find((r) => r.name === name)?.ref;
  if (ref) {
    await d.stop(ref);
    await d.remove(ref);
    console.log("Task-owned Boat sandbox deleted.");
  }
}
