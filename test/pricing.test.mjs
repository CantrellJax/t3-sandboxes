import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createController } from "../src/controller.mjs";
import { driver } from "../src/drivers.mjs";
import { readQuote } from "../src/pricing.mjs";
const NOW = Date.parse("2026-11-10T12:00:00Z");
const tiers = (overrides = {}) => ({
  currency: "USD",
  unit: "minute",
  rounding: "up",
  choices: [
    { id: "small", label: "Small", rate: 0.001 },
    { id: "large", label: "Large", rate: 0.004 },
  ],
  ...overrides,
});
test("quotes accept valid zero rates and pick the requested or first choice", () => {
  const free = readQuote(tiers({ choices: [{ id: "free", rate: 0 }] }), undefined, NOW);
  assert.equal(free.rate, 0);
  assert.equal(free.choice, "free");
  assert.equal(free.validUntil, NOW + 15 * 60000);
  assert.equal(readQuote(tiers(), undefined, NOW).choice, "small");
  const granted = readQuote(tiers({ includedCredits: 100, grant: "basic" }), undefined, NOW);
  assert.ok(!("includedCredits" in granted) && !("grant" in granted), "grant fields are ignored");
  const large = readQuote(tiers({ validUntil: "2026-11-10T13:00:00Z" }), "large", NOW);
  assert.equal(large.rate, 0.004);
  assert.equal(large.validUntil, Date.parse("2026-11-10T13:00:00Z"));
  assert.deepEqual(
    large.choices.map((c) => c.id),
    ["small", "large"],
  );
});
test("quote numbers must be JSON numbers in range", () => {
  for (const rate of [null, "", "0.001", true, false, [1], {}, -1, 1e6, NaN, Infinity, undefined])
    assert.throws(
      () => readQuote(tiers({ choices: [{ id: "small", rate }] }), "small", NOW),
      /Quote rate is not usable/,
      `rate ${JSON.stringify(rate)}`,
    );
});
test("quotes with unsupported billing models, expiry or shape are rejected", () => {
  const cases = [
    [null, /Quote missing/],
    [[], /Quote missing/],
    [tiers({ currency: "EUR" }), /Unsupported quote currency/],
    [tiers({ currency: undefined }), /Unsupported quote currency/],
    [tiers({ unit: "month" }), /Unsupported quote billing unit/],
    [tiers({ unit: "toString" }), /Unsupported quote billing unit/],
    [tiers({ rounding: undefined }), /Unsupported quote rounding/],
    [tiers({ rounding: "ceil-5-minutes" }), /Unsupported quote rounding/],
    [tiers({ validUntil: "2026-11-10T11:59:59Z" }), /Quote expired/],
    [tiers({ validUntil: 1e15 }), /not an ISO 8601 UTC timestamp/],
    [tiers({ validUntil: "soon" }), /not an ISO 8601 UTC timestamp/],
    [tiers({ validUntil: "Tue, 10 Nov 2026 13:00:00 GMT" }), /not an ISO 8601 UTC timestamp/],
    [tiers({ validUntil: "2026-11-10T13:00:00+01:00" }), /not an ISO 8601 UTC timestamp/],
    [tiers({ validUntil: "2026-02-30T13:00:00Z" }), /not an ISO 8601 UTC timestamp/],
    [tiers({ validUntil: "2026-11-11T12:00:01Z" }), /more than 24 hours ahead/],
    [tiers({ validUntil: "9999-12-31T23:59:59Z" }), /more than 24 hours ahead/],
    [tiers({ choices: [] }), /no resource choices/],
    [
      tiers({
        choices: [
          { id: "a", rate: 1 },
          { id: "a", rate: 2 },
        ],
      }),
      /repeat/,
    ],
    [tiers({ choices: [{ id: "a b", rate: 1 }] }), /choice id/],
    [tiers({ choices: [{ id: 1, rate: 1 }] }), /choice is not usable/],
  ];
  for (const [raw, error] of cases) assert.throws(() => readQuote(raw, undefined, NOW), error);
  assert.throws(() => readQuote(tiers(), "huge", NOW), /no resource choice huge/);
});
// A mocked external provider and guest with an injected clock: the controller
// runs its real create -> running -> stop -> resume lifecycle against them.
async function fixture(t, respond) {
  const directory = await mkdtemp(join(tmpdir(), "sandboxes-quote-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const storage = new Map();
  const calls = [];
  const resources = new Map();
  let time = NOW,
    loseNextCreate = false,
    loseNextStart = false;
  const run = async (_exe, _args, opts) => {
    const body = JSON.parse(opts.input);
    calls.push(body);
    const answer = await respond(body);
    if (answer !== undefined) return JSON.stringify(answer);
    const [arg] = body.input;
    const reply = (value) => JSON.stringify(value ?? null);
    switch (body.operation) {
      case "list":
        return reply([...resources.values()]);
      case "create": {
        const made = { ref: `r${resources.size + 1}`, name: arg.name, state: "running" };
        resources.set(made.ref, made);
        if (loseNextCreate) {
          loseNextCreate = false;
          throw new Error("Lost response");
        }
        return reply(made);
      }
      case "get":
        return reply(resources.get(arg));
      case "start":
        resources.get(arg).state = "running";
        if (loseNextStart) {
          loseNextStart = false;
          throw new Error("Lost response");
        }
        return reply();
      case "stop":
        resources.get(arg).state = "stopped";
        return reply();
      case "exec":
        return reply("token");
      case "expose":
        return reply("https://guest.example.test");
      default:
        return reply();
    }
  };
  const guest = () => ({
    threads: async () => [{ threadId: "thread" }],
    transcript: async () => ({ items: [] }),
    call: async (name) =>
      name === "t3_project_list"
        ? { projects: [] }
        : name === "t3_thread_launch"
          ? { threadId: "thread" }
          : { id: "project" },
    activity: async () => ({ active: true, waiting: false, closed: false, lastActivity: time }),
  });
  const options = {
    directory,
    source: "plugin",
    secrets: {
      get: async (key) => storage.get(key) ?? null,
      set: async (key, value) => storage.set(key, value),
      remove: async (key) => storage.delete(key),
    },
    drivers: (profile) => driver(profile, run),
    guest,
    now: () => time,
  };
  let controller = await createController(options);
  t.after(() => controller.close());
  let n = 0;
  const create = (profile) =>
    controller.create({
      profile,
      repository: "https://example.test/repo",
      base: "a".repeat(40),
      requestId: `aaaaaaaa-aaaa-aaaa-aaaa-${String(++n).padStart(12, "0")}`,
    });
  return {
    get controller() {
      return controller;
    },
    calls,
    create,
    loseNextCreate: () => {
      loseNextCreate = true;
    },
    loseNextStart: () => {
      loseNextStart = true;
    },
    providerStops: (id) => {
      resources.get(controller.snapshot().rows.find((r) => r.id === id).ref).state = "stopped";
    },
    count: (operation) => calls.filter((c) => c.operation === operation).length,
    row: (id) => controller.snapshot().rows.find((r) => r.id === id),
    advance: (ms) => {
      time += ms;
    },
    restart: async () => {
      await controller.close();
      controller = await createController(options);
    },
  };
}
test("a multi-tier external driver gets the chosen tier from configuration through create", async (t) => {
  const f = await fixture(t, (body) => (body.operation === "quote" ? tiers() : undefined));
  await f.controller.configure({
    id: "big",
    driver: "external",
    executable: "driver",
    pricing: "provider",
    choice: "large",
  });
  await f.controller.configure({
    id: "default",
    driver: "external",
    executable: "driver",
    pricing: "provider",
  });
  const profiles = f.controller.snapshot().profiles;
  assert.equal(profiles.find((p) => p.id === "big").choice, "large");
  assert.equal(profiles.find((p) => p.id === "default").choice, "small");
  await assert.rejects(
    f.controller.configure({
      id: "bad",
      driver: "external",
      executable: "driver",
      pricing: "provider",
      choice: "huge",
    }),
    /Provider quote refused: Quote has no resource choice huge/,
  );
  await f.controller.setPolicy({ ...f.controller.snapshot().policy, monthlyBudget: 10 });
  const id = await f.create("big");
  const row = f.controller.snapshot().rows.find((r) => r.id === id);
  assert.equal(row.choice, "large");
  assert.equal(row.price.rate, 0.004);
  assert.equal(row.price.choice, "large");
  assert.equal(row.price.quotedAt, NOW);
  await f.controller.tick();
  const made = f.calls.find((c) => c.operation === "create");
  assert.equal(made.input[0].choice, "large");
  assert.equal(made.input[0].requestId, id);
});
test("a failed, expired or malformed quote refuses create instead of using the cached price", async (t) => {
  let mode = "ok";
  const f = await fixture(t, (body) => {
    if (body.operation !== "quote") return undefined;
    if (mode === "down") throw new Error("Driver operation failed (401)");
    if (mode === "expired") return tiers({ validUntil: "2026-11-10T12:00:00Z" });
    if (mode === "malformed") return tiers({ choices: [{ id: "small", rate: null }] });
    return tiers();
  });
  await f.controller.configure({
    id: "q",
    driver: "external",
    executable: "driver",
    pricing: "provider",
  });
  f.advance(60000);
  for (const budget of [1, 0])
    for (const [next, error] of [
      ["down", /Provider quote failed: Driver operation failed \(401\)/],
      ["expired", /Provider quote failed: Quote expired/],
      ["malformed", /Provider quote failed: Quote rate is not usable/],
    ]) {
      await f.controller.setPolicy({ ...f.controller.snapshot().policy, monthlyBudget: budget });
      mode = next;
      await assert.rejects(f.create("q"), error);
      const p = f.controller.snapshot().profiles[0];
      assert.equal(p.quote.quotedAt, NOW, "the cached quote is kept only for display");
      assert.equal(p.quoteError.at, NOW + 60000);
    }
  assert.equal(f.controller.snapshot().rows.length, 0);
  mode = "ok";
  await f.create("q");
  assert.equal(f.controller.snapshot().profiles[0].quoteError, undefined);
  assert.equal(f.controller.snapshot().rows[0].price.quotedAt, NOW + 60000);
});
test("an unbudgeted failed quote cannot leave an unpriced row that hides spend from a later budget", async (t) => {
  let fail = false;
  const f = await fixture(t, (body) => {
    if (body.operation !== "quote") return undefined;
    if (fail) throw new Error("401 bad key");
    return tiers({ choices: [{ id: "small", rate: 1 }] });
  });
  await f.controller.configure({
    id: "q",
    driver: "external",
    executable: "driver",
    pricing: "provider",
  });
  fail = true;
  await assert.rejects(f.create("q"), /Provider quote failed: 401 bad key/);
  assert.equal(f.controller.snapshot().rows.length, 0);
  fail = false;
  await f.create("q");
  f.advance(10 * 60000);
  await f.controller.setPolicy({ ...f.controller.snapshot().policy, monthlyBudget: 5 });
  assert.equal(f.controller.usage().monthCost, 10);
  await assert.rejects(f.create("q"), /Monthly estimated budget reached/);
});
test("a quote endpoint answering 401 refuses a budgeted create", async (t) => {
  const realFetch = globalThis.fetch;
  t.after(() => {
    globalThis.fetch = realFetch;
  });
  const statuses = [200, 401];
  const requests = [];
  globalThis.fetch = async (url, init) => {
    requests.push({ url, auth: init.headers.Authorization, body: JSON.parse(init.body) });
    const status = statuses.shift();
    return new Response(status === 200 ? JSON.stringify(tiers()) : "unauthorized", { status });
  };
  const f = await fixture(t, () => undefined);
  await f.controller.configure({
    id: "web",
    driver: "external",
    endpoint: "https://driver.example.test/v1",
    token: "secret-key",
    pricing: "provider",
  });
  await f.controller.setPolicy({ ...f.controller.snapshot().policy, monthlyBudget: 1 });
  await assert.rejects(f.create("web"), /Provider quote failed: Driver operation failed \(401\)/);
  assert.equal(f.controller.snapshot().rows.length, 0);
  assert.deepEqual(
    requests.map((r) => [r.body.operation, r.auth]),
    [
      ["quote", "Bearer secret-key"],
      ["quote", "Bearer secret-key"],
    ],
  );
});
test("external drivers without quote support keep the existing create contract", async (t) => {
  const f = await fixture(t, () => undefined);
  await f.controller.configure({
    id: "plain",
    driver: "external",
    executable: "driver",
    hourlyRate: 1,
  });
  await assert.rejects(
    f.controller.configure({ id: "docker", driver: "docker", image: "x", pricing: "provider" }),
    /Provider quotes need an external driver/,
  );
  await f.create("plain");
  await f.controller.tick();
  assert.ok(!f.calls.some((c) => c.operation === "quote"));
  const made = f.calls.find((c) => c.operation === "create");
  assert.deepEqual(Object.keys(made.input[0]).sort(), ["name", "requestId", "ttl"]);
  const row = f.controller.snapshot().rows[0];
  assert.equal(row.hourlyRate, 1);
  assert.equal(row.price, undefined);
});
// A quote that changes rate and can fail or expire on demand.
function provider() {
  const q = { rate: 1, unit: "minute", rounding: "up", fail: false, validFor: null, quotes: 0 };
  q.respond = (body) => {
    if (body.operation !== "quote") return undefined;
    q.quotes++;
    if (q.fail) throw new Error("offline");
    return tiers({
      unit: q.unit,
      rounding: q.rounding,
      choices: [{ id: "small", rate: q.rate }],
      ...(q.validFor === null
        ? {}
        : { validUntil: new Date(NOW + q.time() + q.validFor).toISOString() }),
    });
  };
  return q;
}
async function quotedFixture(t) {
  const q = provider();
  const f = await fixture(t, q.respond);
  let elapsed = 0;
  q.time = () => elapsed;
  const advance = f.advance;
  f.advance = (ms) => {
    elapsed += ms;
    advance(ms);
  };
  await f.controller.configure({
    id: "q",
    driver: "external",
    executable: "driver",
    pricing: "provider",
  });
  return { f, q };
}
async function runningThenStopped(f) {
  const id = await f.create("q");
  await f.controller.tick();
  assert.equal(f.row(id).state, "running");
  f.advance(60000);
  await f.controller.action(id, "stopped");
  await f.controller.tick();
  assert.equal(f.row(id).state, "stopped");
  return id;
}
test("resume needs a fresh quote and prices only the new session with it", async (t) => {
  const { f, q } = await quotedFixture(t);
  const id = await runningThenStopped(f);
  q.rate = 10;
  await f.controller.action(id, "running");
  assert.equal(f.row(id).nextPrice.rate, 10);
  q.fail = true;
  await assert.rejects(f.controller.action(id, "running"), /Provider quote failed: offline/);
  const pending = f.row(id);
  assert.equal(pending.nextPrice.rate, 10, "a refused duplicate keeps the accepted price");
  assert.equal(pending.desired, "running");
  assert.equal(pending.resumeRequested, true);
  await f.restart();
  assert.equal(f.row(id).nextPrice.rate, 10, "the accepted intent and price persist together");
  await f.controller.tick();
  const row = f.row(id);
  assert.equal(row.state, "running");
  assert.equal(row.price.rate, 1, "the create-time snapshot is not repriced");
  assert.equal(row.sessions.length, 2);
  assert.equal(row.sessions[1].price.rate, 10);
  assert.equal(row.nextPrice, undefined);
  f.advance(60000);
  await f.controller.action(id, "stopped");
  await f.controller.tick();
  assert.equal(f.controller.usage().items[0].cost, 11);
});
for (const step of ["create", "resume"]) {
  test(`a ${step} whose quote expires before the worker runs uses a refreshed quote`, async (t) => {
    const { f, q } = await quotedFixture(t);
    const id = step === "create" ? await f.create("q") : await runningThenStopped(f);
    if (step === "resume") {
      q.validFor = 1000;
      await f.controller.action(id, "running");
      q.validFor = null;
    }
    q.rate = 0.5;
    f.advance(step === "create" ? 15 * 60000 + 2000 : 2000);
    const before = q.quotes;
    await f.restart();
    await f.controller.tick();
    assert.equal(q.quotes, before + 1, "the worker asked for a fresh quote");
    const row = f.row(id);
    assert.equal(row.state, "running");
    const price = step === "create" ? row.price : row.sessions.at(-1).price;
    assert.equal(price.rate, 0.5, "a lower rate on identical terms is used");
    assert.equal(price.unit, "minute");
    assert.equal(price.rounding, "up");
    assert.ok(price.validUntil > NOW + q.time());
    assert.equal(f.count(step === "create" ? "create" : "start"), 1);
  });
  const refusals = {
    fails: [{ fail: true }, "provider quote failed: offline"],
    rises: [
      { rate: 900 },
      "the provider price rose from 1 to 900 USD per minute; request again to accept it",
    ],
    "moves to an equal hourly rate with a larger rounded unit": [
      { rate: 60, unit: "hour" },
      "the provider billing terms changed from USD per minute, rounding up to USD per hour, rounding up; request again to accept them",
    ],
    "moves to a lower hourly rate with a larger rounded unit": [
      { rate: 30, unit: "hour" },
      "the provider billing terms changed from USD per minute, rounding up to USD per hour, rounding up; request again to accept them",
    ],
    "changes rounding": [
      { rate: 0.5, rounding: "none" },
      "the provider billing terms changed from USD per minute, rounding up to USD per minute, rounding none; request again to accept them",
    ],
  };
  for (const [why, [change, reason]] of Object.entries(refusals))
    test(`a ${step} whose quote expires and whose refresh ${why} is cancelled before the provider call`, async (t) => {
      const { f, q } = await quotedFixture(t);
      const id = step === "create" ? await f.create("q") : await runningThenStopped(f);
      if (step === "resume") {
        q.validFor = 1000;
        await f.controller.action(id, "running");
        q.validFor = null;
      } else f.advance(15 * 60000);
      Object.assign(q, change);
      f.advance(2000);
      await f.restart();
      await f.controller.tick();
      const row = f.row(id);
      if (step === "create") {
        assert.equal(f.count("create"), 0);
        assert.equal(row.state, "deleted");
        assert.equal(row.error, `Create cancelled before provisioning: ${reason}`);
      } else {
        assert.equal(f.count("start"), 0);
        assert.equal(row.state, "stopped");
        assert.equal(row.desired, "stopped");
        assert.equal(row.resumeRequested, false);
        assert.equal(row.nextPrice, undefined);
        assert.equal(row.sessions.length, 1);
        assert.equal(row.error, `Resume cancelled: ${reason}`);
      }
      await f.restart();
      await f.controller.tick();
      f.advance(10 * 3600000);
      assert.equal(
        f.count(step === "create" ? "create" : "start"),
        0,
        "nothing is provisioned or started later",
      );
      assert.equal(f.controller.usage().monthCost, step === "create" ? 0 : 1, "nothing bills");
    });
}
test("a create whose request was sent is reconciled by name without re-quoting", async (t) => {
  const { f, q } = await quotedFixture(t);
  f.loseNextCreate();
  const id = await f.create("q");
  await f.controller.tick();
  assert.equal(f.count("create"), 1);
  assert.equal(f.row(id).ref, undefined);
  assert.equal(f.row(id).createAttempted, true);
  f.advance(16 * 60000);
  q.fail = true;
  const before = q.quotes;
  await f.restart();
  await f.controller.tick();
  assert.equal(q.quotes, before, "a created resource is never re-quoted");
  assert.equal(f.count("create"), 1);
  assert.equal(f.row(id).state, "running");
});
test("a start whose reply is lost bills from the start request at the accepted price", async (t) => {
  const { f, q } = await quotedFixture(t);
  const id = await runningThenStopped(f);
  q.rate = 5;
  await f.controller.action(id, "running");
  f.loseNextStart();
  await f.controller.tick();
  assert.equal(f.count("start"), 1);
  f.advance(120000);
  await f.controller.tick();
  const row = f.row(id);
  assert.equal(row.state, "running");
  assert.equal(row.resumeRequested, false);
  assert.equal(row.nextPrice, undefined);
  assert.equal(row.sessions.length, 2);
  assert.equal(row.sessions[1].price.rate, 5);
  assert.equal(f.count("start"), 1);
  f.advance(10 * 60000);
  assert.equal(f.controller.usage().items[0].cost, 1 + 12 * 5);
});
test("resume on a running sandbox is ignored, so a later provider stop is not restarted", async (t) => {
  const { f, q } = await quotedFixture(t);
  const id = await f.create("q");
  await f.controller.tick();
  q.rate = 7;
  await f.controller.action(id, "running");
  assert.equal(f.row(id).resumeRequested, undefined);
  assert.equal(f.row(id).nextPrice, undefined);
  await f.controller.tick();
  f.advance(60000);
  f.providerStops(id);
  f.advance(60000);
  await f.controller.tick();
  f.advance(10 * 60000);
  const row = f.row(id);
  assert.equal(row.state, "stopped");
  assert.equal(f.count("start"), 0);
  assert.equal(row.sessions.length, 1);
  assert.equal(f.controller.usage().items[0].cost, 2);
});
test("stopping a queued resume clears its pending price", async (t) => {
  const { f, q } = await quotedFixture(t);
  const id = await runningThenStopped(f);
  q.rate = 3;
  await f.controller.action(id, "running");
  await f.controller.action(id, "stopped");
  await f.controller.tick();
  const row = f.row(id);
  assert.equal(row.resumeRequested, false);
  assert.equal(row.nextPrice, undefined);
  assert.equal(f.count("start"), 0);
  f.advance(3600000);
  assert.equal(f.controller.usage().items[0].cost, 1);
});
test("a sandbox deleted before provisioning never bills", async (t) => {
  const f = await fixture(t, () => undefined);
  await f.controller.configure({
    id: "typed",
    driver: "external",
    executable: "driver",
    hourlyRate: 60,
  });
  const id = await f.create("typed");
  await f.controller.action(id, "deleted", { confirm: true });
  await f.controller.tick();
  f.advance(10 * 3600000);
  assert.equal(f.row(id).state, "deleted");
  assert.equal(f.count("create"), 0);
  assert.equal(f.controller.usage().monthCost, 0);
});
test("a provider stop followed by a user stop before the next tick closes the session", async (t) => {
  const { f } = await quotedFixture(t);
  const id = await f.create("q");
  await f.controller.tick();
  f.advance(60000);
  f.providerStops(id);
  await f.controller.action(id, "stopped");
  await f.controller.tick();
  f.advance(10 * 3600000);
  const row = f.row(id);
  assert.equal(row.state, "stopped");
  assert.equal(row.sessions.length, 1);
  assert.notEqual(row.sessions[0].end, null);
  assert.equal(f.controller.usage().items[0].cost, 1);
});
test("a lost start reply followed by a user stop bills the resumed session, not the gap", async (t) => {
  const { f, q } = await quotedFixture(t);
  const id = await runningThenStopped(f);
  f.advance(60 * 60000);
  q.rate = 2;
  await f.controller.action(id, "running");
  f.loseNextStart();
  await f.controller.tick();
  f.advance(5 * 60000);
  await f.controller.action(id, "stopped");
  await f.controller.tick();
  f.advance(10 * 3600000);
  const row = f.row(id);
  assert.equal(row.state, "stopped");
  assert.equal(f.count("start"), 1);
  assert.equal(row.sessions.length, 2);
  assert.equal(
    row.sessions[0].end - row.sessions[0].start,
    60000,
    "the earlier session keeps its end",
  );
  assert.equal(row.sessions[1].price.rate, 2);
  assert.equal(row.sessions[1].end - row.sessions[1].start, 5 * 60000);
  assert.equal(row.startRequestedAt, undefined);
  assert.equal(row.nextPrice, undefined);
  assert.equal(f.controller.usage().items[0].cost, 1 + 5 * 2);
});
