import { driver } from "../src/drivers.mjs";
import { randomUUID } from "node:crypto";
import assert from "node:assert/strict";
const d = driver({
  driver: "docker",
  image: process.env.T3_DOCKER_IMAGE ?? "docker.io/library/node:24",
});
let ref;
try {
  const resource = await d.create({ name: `t3-sandbox-smoke-${randomUUID()}`, ttl: 600 });
  ref = resource.ref;
  assert.equal((await d.get(ref)).state, "running");
  await d.exec(ref, "mkdir -p /home/user/t3-sandbox/plugin");
  await d.write(ref, "/home/user/t3-sandbox/plugin/probe", "persistent");
  assert.equal(await d.exec(ref, "cat /home/user/t3-sandbox/plugin/probe"), "persistent");
  assert.match(await d.expose(ref), /^http:\/\/127\.0\.0\.1:\d+$/);
  await d.stop(ref);
  assert.equal((await d.get(ref)).state, "stopped");
  await d.start(ref);
  assert.equal(await d.exec(ref, "cat /home/user/t3-sandbox/plugin/probe"), "persistent");
  console.log(
    "Docker: create, inspect, upload, exec, expose, stop, resume with retained disk verified.",
  );
} finally {
  if (ref) {
    try {
      await d.stop(ref);
    } finally {
      await d.remove(ref);
    }
  }
}
