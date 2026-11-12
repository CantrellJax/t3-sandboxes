import { chargeFor } from "./pricing.mjs";
// Each row is priced only from what it captured at create: a quote snapshot on the
// session or row, else its own hourly rate. Profiles are a fallback only for legacy
// rows that recorded neither, and never supply a quote.
export function usage(rows, profiles, now) {
  const monthStart = new Date(now);
  monthStart.setUTCDate(1);
  monthStart.setUTCHours(0, 0, 0, 0);
  const start = +monthStart;
  const items = rows.map((row) => {
    const rate = row.hourlyRate ?? profiles.find((p) => p.id === row.profile)?.hourlyRate ?? 0;
    const spans = row.sessions ?? [{ start: row.startedAt, end: row.stoppedAt ?? null }];
    const end = (span) =>
      span.end ?? (row.enforcedDeadline ? Math.min(now, row.deadline ?? now) : now);
    const charge = (span, seconds) => {
      const price = span.price ?? row.price;
      return price ? chargeFor(price, seconds) : (seconds * rate) / 3600;
    };
    let seconds = 0,
      cost = 0,
      monthCost = 0;
    // Lifetime cost is the sum of each session's UTC-month parts, so it always
    // agrees with the monthly totals when a quote rounds up.
    for (const span of spans) {
      for (let from = span.start, stop = end(span); from < stop;) {
        const date = new Date(from);
        const to = Math.min(stop, Date.UTC(date.getUTCFullYear(), date.getUTCMonth() + 1, 1));
        const part = charge(span, (to - from) / 1000);
        seconds += (to - from) / 1000;
        cost += part;
        if (from >= start) monthCost += part;
        from = to;
      }
    }
    return {
      id: row.id,
      title: row.title,
      seconds,
      cost,
      monthCost,
      rateKnown: Boolean(row.price || spans.some((span) => span.price)) || rate > 0,
    };
  });
  return { items, monthCost: items.reduce((total, item) => total + item.monthCost, 0) };
}
