import { test } from "node:test";
import assert from "node:assert/strict";
import { usage } from "../src/billing.mjs";
test("monthly estimates split at UTC month boundaries and preserve historical hourly rates", () => {
  const start = Date.parse("2026-10-31T23:00:00Z"),
    end = Date.parse("2026-11-01T01:00:00Z");
  const result = usage(
    [{ id: "one", profile: "p", hourlyRate: 2, ref: "resource", sessions: [{ start, end }] }],
    [{ id: "p", hourlyRate: 99 }],
    end,
  );
  assert.equal(result.monthCost, 2);
  assert.equal(result.items[0].cost, 4);
  assert.equal(result.items[0].seconds, 7200);
});
test("only provider-enforced deadlines bound offline runtime estimates", () => {
  const row = {
    id: "one",
    profile: "p",
    ref: "resource",
    sessions: [{ start: 0, end: null }],
    deadline: 3600000,
  };
  assert.equal(
    usage([{ ...row, enforcedDeadline: true }], [{ id: "p", hourlyRate: 1 }], 7200000).items[0]
      .cost,
    1,
  );
  assert.equal(usage([row], [{ id: "p", hourlyRate: 1 }], 7200000).items[0].cost, 2);
});
const quoted = (rate, rounding = "up") => ({
  source: "provider",
  currency: "USD",
  unit: "minute",
  rounding,
  choice: "small",
  label: "Small",
  rate,
  quotedAt: 0,
  validUntil: 900000,
});
test("a quote snapshot prices its row: sessions round up per billing unit and split at UTC months", () => {
  const start = Date.parse("2026-10-31T23:59:00Z");
  const result = usage(
    [
      {
        id: "q",
        profile: "p",
        hourlyRate: 0,
        price: quoted(0.5),
        sessions: [
          { start, end: start + 90500 },
          { start: start + 600000, end: start + 630000, price: quoted(1) },
        ],
      },
    ],
    [{ id: "p", hourlyRate: 99 }],
    start + 700000,
  );
  // 60 s in October (1 unit) + 30.5 s in November (1 unit) at 0.5; 30 s in November at 1.
  assert.equal(result.items[0].cost, 0.5 * 2 + 1);
  assert.equal(result.monthCost, 0.5 + 1);
  assert.equal(result.items[0].rateKnown, true);
  // 30 s each side of midnight: rounded up separately, and the lifetime total agrees.
  const split = usage(
    [{ id: "s", price: quoted(1), sessions: [{ start: start + 30000, end: start + 90000 }] }],
    [],
    start + 90000,
  );
  assert.equal(split.monthCost, 1);
  assert.equal(split.items[0].cost, 2);
  const free = usage(
    [{ id: "f", price: quoted(0), sessions: [{ start, end: start + 60000 }] }],
    [],
    start + 60000,
  );
  assert.equal(free.items[0].cost, 0);
  assert.equal(free.items[0].rateKnown, true);
});
test("deleted rows keep their captured price after their profile is replaced", () => {
  const start = Date.parse("2026-11-02T00:00:00Z");
  const rows = [
    {
      id: "typed",
      profile: "p",
      state: "deleted",
      hourlyRate: 2,
      sessions: [{ start, end: start + 3600000 }],
    },
    {
      id: "quoted",
      profile: "q",
      state: "deleted",
      hourlyRate: 0,
      price: quoted(0.01, "none"),
      sessions: [{ start, end: start + 3600000 }],
    },
  ];
  const before = usage(
    rows,
    [
      { id: "p", hourlyRate: 2 },
      { id: "q", pricing: "provider", hourlyRate: 0, quote: quoted(0.01) },
    ],
    start + 7200000,
  );
  // Both profiles replaced: p becomes a free quoted profile, q gets a new price and grant-like fields.
  const after = usage(
    rows,
    [
      { id: "p", pricing: "provider", hourlyRate: 0, quote: quoted(0), price: quoted(0) },
      {
        id: "q",
        pricing: "provider",
        hourlyRate: 0,
        quote: { ...quoted(5), includedCredits: 100 },
        price: quoted(5),
      },
    ],
    start + 7200000,
  );
  assert.deepEqual(after, before);
  assert.equal(after.items[0].cost, 2);
  assert.equal(Math.round(after.items[1].cost * 1e9) / 1e9, 0.6);
});
test("budgets ignore grant or allowance fields a quote advertises", () => {
  const start = Date.parse("2026-11-02T00:00:00Z");
  const result = usage(
    ["a", "b"].map((id) => ({
      id,
      price: { ...quoted(1), grant: "basic", includedCredits: 100, creditsPerMinute: 1 },
      sessions: [{ start, end: start + 60000 }],
    })),
    [],
    start + 60000,
  );
  assert.equal(result.monthCost, 2);
});
