export const defaults = Object.freeze({
  idleMinutes: 15,
  waitingMinutes: 60,
  unattendedMinutes: 120,
  continuousMinutes: 720,
  deleteDays: 7,
  deleteOnThreadDelete: true,
  maxRunning: 3,
  monthlyBudget: 0,
});
export function policy(input = {}) {
  const result = { ...defaults, ...input };
  for (const [key, value] of Object.entries(result)) {
    if (key === "deleteOnThreadDelete") {
      if (typeof value !== "boolean") throw new Error(`Invalid policy: ${key}`);
    } else if (
      !Object.hasOwn(defaults, key) ||
      !Number.isFinite(value) ||
      value < 0 ||
      value > 525600
    )
      throw new Error(`Invalid policy: ${key}`);
  }
  for (const key of [
    "idleMinutes",
    "waitingMinutes",
    "unattendedMinutes",
    "continuousMinutes",
    "maxRunning",
  ])
    if (result[key] < 1) throw new Error(`Invalid policy: ${key}`);
  return result;
}
export function decide(row, activity, now, p) {
  if (row.desired !== "running") return row.desired;
  if (now >= row.startedAt + p.continuousMinutes * 60000) return "stopped";
  if (row.keepAwakeUntil > now) return "running";
  if (!activity) return "unknown";
  if (activity.closed) return "stopped";
  if (activity.active) return "running";
  if (activity.waiting)
    return now >= (row.waitingSince ?? now) + p.waitingMinutes * 60000 ? "stopped" : "running";
  return now >= Math.max(row.lastActivity, activity.lastActivity ?? 0) + p.idleMinutes * 60000
    ? "stopped"
    : "running";
}
export function deadline(row, activity, now, p) {
  const hard = row.startedAt + p.continuousMinutes * 60000;
  const next =
    row.keepAwakeUntil > now
      ? row.keepAwakeUntil
      : activity?.active
        ? now + p.unattendedMinutes * 60000
        : activity?.waiting
          ? (row.waitingSince ?? now) + p.waitingMinutes * 60000
          : row.lastActivity + p.idleMinutes * 60000;
  return Math.min(hard, next);
}
export function tone(state) {
  return state === "running"
    ? "success"
    : state === "stopped"
      ? "muted"
      : ["starting", "stopping", "deleting"].includes(state)
        ? "warning"
        : "danger";
}
