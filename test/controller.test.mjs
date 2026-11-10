import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createController } from "../src/controller.mjs";
import { decide, defaults, deadline } from "../src/policy.mjs";
async function fixture(t) {
  const directory = await mkdtemp(join(tmpdir(), "sandboxes-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  let time = 10000000;
  const storage = new Map();
  const resources = [];
  let creates = 0,
    stops = 0,
    removes = 0;
  let failExport = false,
    failCreate = false;
  let activity = { active: true, waiting: false, closed: false, lastActivity: time };
  const d = {
    capabilities: { deadline: true, stopKeepsDisk: true },
    list: async () => resources,
    create: async (spec) => {
      creates++;
      const x = { ref: "test", name: spec.name, state: "running" };
      resources.push(x);
      if (failCreate) throw new Error("Lost response");
      return x;
    },
    get: async () => resources[0],
    start: async () => {
      resources[0].state = "running";
    },
    stop: async () => {
      stops++;
      resources[0].state = "stopped";
    },
    remove: async () => {
      removes++;
      resources[0].state = "deleted";
    },
    setDeadline: async () => {},
    write: async () => {},
    exec: async () => "token",
    expose: async () => "https://example.test",
  };
  const g = {
    threads: async () => [{ threadId: "thread" }],
    transcript: async () => {
      if (failExport) throw new Error("offline");
      return { items: ["complete history"] };
    },
    call: async (name) =>
      name === "t3_project_list"
        ? { projects: [] }
        : name === "t3_project_create"
          ? { id: "project" }
          : { threadId: "thread" },
    activity: async () => activity,
  };
  const options = {
    directory,
    secrets: {
      get: async (key) => storage.get(key) ?? null,
      set: async (key, value) => storage.set(key, value),
      remove: async (key) => storage.delete(key),
    },
    drivers: () => d,
    guest: () => g,
    source: "plugin",
    now: () => time,
  };
  let controller = await createController(options);
  t.after(() => controller.close());
  await controller.configure({ id: "test", driver: "boat", image: "test" });
  const spec = {
    profile: "test",
    repository: "https://example.test/repo",
    base: "a".repeat(40),
    requestId: "11111111-1111-1111-1111-111111111111",
  };
  return {
    directory,
    spec,
    get c() {
      return controller;
    },
    create: () => controller.create(spec),
    tick: () => controller.tick(),
    row: () => controller.snapshot().rows[0],
    counts: () => ({ creates, stops, removes }),
    reporterKey: () => storage.get(`reporter-${controller.snapshot().rows[0].id}`),
    setActivity: (x) => {
      activity = x;
    },
    advance: (ms) => {
      time += ms;
    },
    failExport: () => {
      failExport = true;
    },
    failCreate: () => {
      failCreate = true;
    },
    restart: async () => {
      await controller.close();
      controller = await createController(options);
    },
  };
}
test("idempotent provisioning survives an uncertain reply and controller restart", async (t) => {
  const f = await fixture(t);
  f.failCreate();
  const id = await f.create();
  await f.tick();
  assert.equal(f.counts().creates, 1);
  await f.restart();
  assert.equal(await f.create(), id);
  await f.tick();
  assert.equal(f.row().state, "running");
  assert.equal(f.counts().creates, 1);
});
test("idle stop exports transcript, resume keeps identity, explicit deletion requires consent", async (t) => {
  const f = await fixture(t);
  const id = await f.create();
  await f.tick();
  f.setActivity({ active: false, waiting: false, lastActivity: 0 });
  f.advance(16 * 60000);
  await f.tick();
  assert.equal(f.row().state, "stopped");
  assert.equal(f.counts().stops, 1);
  assert.ok(f.row().retainedAt);
  await assert.rejects(f.c.action(id, "deleted"));
  await f.c.action(id, "running");
  await f.tick();
  assert.equal(f.row().state, "running");
  assert.equal(f.counts().creates, 1);
  await f.c.action(id, "deleted", { confirm: true });
  await f.tick();
  assert.equal(f.row().state, "deleted");
  assert.equal(f.counts().removes, 1);
});
test("failed retention never calls provider stop or delete", async (t) => {
  const f = await fixture(t);
  const id = await f.create();
  await f.tick();
  f.failExport();
  await f.c.action(id, "stopped");
  await f.tick();
  assert.equal(f.counts().stops, 0);
  assert.equal(f.counts().removes, 0);
  assert.ok(f.row().error);
});
test("a running background task wins over another thread waiting for approval", () => {
  const row = { desired: "running", startedAt: 0, lastActivity: 0, waitingSince: 0 };
  assert.equal(decide(row, { active: true, waiting: true }, 61 * 60000, defaults), "running");
});
test("hard lifetime caps keep awake and unknown activity", () => {
  const row = { desired: "running", startedAt: 0, lastActivity: 0, keepAwakeUntil: 999999999 };
  assert.equal(decide(row, null, 721 * 60000, defaults), "stopped");
  assert.equal(deadline(row, { active: true }, 0, defaults), 720 * 60000);
});
test("invalid existing ledger fails closed", async (t) => {
  const f = await fixture(t);
  await f.c.close();
  await writeFile(join(f.directory, "ledger.json"), "{broken");
  await assert.rejects(f.restart());
});

test("pin prevents clean stopped sandbox expiry and unpin permits safe deletion", async (t) => {
  const f = await fixture(t);
  const id = await f.create();
  await f.tick();
  await f.c.action(id, "stopped");
  await f.tick();
  await f.c.action(id, "pin", { pinned: true });
  f.advance(8 * 86400000);
  await f.tick();
  assert.equal(f.row().state, "stopped");
  assert.equal(f.counts().removes, 0);
  await f.c.action(id, "pin", { pinned: false });
  await f.tick();
  assert.equal(f.row().state, "stopped");
  assert.equal(f.counts().removes, 0);
  assert.match(f.row().error, /deletion blocked/);
});
test("limits count persisted provisioning requests and prevent simultaneous over-allocation", async (t) => {
  const f = await fixture(t);
  await f.c.setPolicy({ maxRunning: 1 });
  await f.create();
  await assert.rejects(
    f.c.create({ ...f.spec, requestId: "22222222-2222-2222-2222-222222222222" }),
    /limit/,
  );
  assert.equal(f.c.snapshot().rows.length, 1);
});

test("a quick resume keeps the reporter and optional Rehome credential valid for the existing process", async (t) => {
  const f = await fixture(t);
  const id = await f.create();
  await f.tick();
  const key = f.reporterKey();
  await f.c.action(id, "stopped");
  await f.tick();
  await f.c.action(id, "running");
  await f.tick();
  assert.equal(f.reporterKey(), key);
});
