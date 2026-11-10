import { test } from "node:test";
import assert from "node:assert/strict";
import { driver } from "../src/drivers.mjs";
test("Boat accepts JSONL create progress, maps idle, enforces a finite lease and confirms removal", async () => {
  const calls = [];
  const d = driver({ driver: "boat", image: "fleet-base" }, async (_exe, args) => {
    calls.push(args);
    return args.includes("new")
      ? '\u007b"event":"created","id":"bx_test"}\n{"event":"state","id":"bx_test","state":"starting"}\n{"event":"ready","id":"bx_test","state":"idle"}'
      : args.includes("info")
        ? '\u007b"sandbox":{"id":"bx_test","state":"stopping"}}'
        : "{}";
  });
  const created = await d.create({ name: "sandbox", ttl: 600 });
  assert.equal(created.ref, "bx_test");
  assert.equal(created.state, "running");
  assert.ok(calls[0].includes("--ttl"));
  assert.ok(calls[0].includes("--no-env"));
  assert.equal((await d.get(created.ref)).state, "stopping");
  await d.remove(created.ref);
  assert.ok(calls.at(-1).includes("--yes"));
});
