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
