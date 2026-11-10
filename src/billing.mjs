export function usage(rows, profiles, now) {
  const monthStart = new Date(now);
  monthStart.setUTCDate(1);
  monthStart.setUTCHours(0, 0, 0, 0);
  const start = +monthStart;
  const items = rows.map((row) => {
    const rate = row.hourlyRate ?? profiles.find((p) => p.id === row.profile)?.hourlyRate ?? 0;
    const spans = row.sessions ?? [{ start: row.startedAt, end: row.stoppedAt ?? null }];
    const seconds = spans.reduce(
      (total, span) =>
        total +
        Math.max(
          0,
          (span.end ?? (row.enforcedDeadline ? Math.min(now, row.deadline ?? now) : now)) -
            span.start,
        ) /
          1000,
      0,
    );
    const monthSeconds = spans.reduce(
      (total, span) =>
        total +
        Math.max(
          0,
          (span.end ?? (row.enforcedDeadline ? Math.min(now, row.deadline ?? now) : now)) -
            Math.max(start, span.start),
        ) /
          1000,
      0,
    );
    return {
      id: row.id,
      title: row.title,
      seconds,
      cost: (seconds * rate) / 3600,
      monthCost: (monthSeconds * rate) / 3600,
      rateKnown: rate > 0,
    };
  });
  return { items, monthCost: items.reduce((total, item) => total + item.monthCost, 0) };
}
