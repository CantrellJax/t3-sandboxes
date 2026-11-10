import { readFile, mkdir } from "node:fs/promises";
import { join } from "node:path";
import { randomUUID, randomBytes } from "node:crypto";
import { atomic, safeUrl } from "./io.mjs";
import { driver, bootstrap, pairingScript } from "./drivers.mjs";
import { Guest } from "./mcp.mjs";
import { policy, decide, deadline } from "./policy.mjs";
import { usage } from "./billing.mjs";
export async function createController({
  directory,
  secrets,
  source,
  changed = () => {},
  diagnostic = () => {},
  preserve = async () => false,
  drivers = driver,
  guest = (url, token) => new Guest(url, token),
  now = Date.now,
}) {
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const file = join(directory, "ledger.json");
  let state;
  try {
    state = JSON.parse(await readFile(file, "utf8"));
    if (state.version !== 1 || !Array.isArray(state.rows) || !Array.isArray(state.profiles))
      throw new Error("Invalid ledger");
  } catch (error) {
    if (error.code !== "ENOENT") throw error;
    state = { version: 1, owner: randomUUID(), policy: policy(), profiles: [], rows: [] };
    await atomic(file, state);
  }
  let tail = Promise.resolve();
  let saveTail = Promise.resolve();
  const workers = new Map();
  let closed = false;
  const serial = (fn) => {
    const result = tail.then(() => {
      if (closed) throw new Error("Controller closed");
      return fn();
    });
    tail = result.catch(() => {});
    return result;
  };
  const save = async () => {
    const snapshot = structuredClone(state);
    saveTail = saveTail.then(() => atomic(file, snapshot));
    await saveTail;
    changed();
  };
  const profile = (id) => {
    const p = state.profiles.find((x) => x.id === id);
    if (!p) throw new Error("Missing profile");
    return p;
  };
  const getDriver = async (row) => {
    const p = profile(row.profile);
    return drivers({ ...p, token: await secrets.get(`provider-${p.id}`) });
  };
  const getGuest = async (row) => guest(row.url, await secrets.get(`guest-${row.id}`));
  async function retain(row) {
    const g = await getGuest(row);
    let paused = false;
    try {
      await g.call("t3_plugin_run", {
        pluginId: "sandboxes",
        commandId: "prepare-stop",
        input: {},
        permission: "orchestration:operate",
      });
      paused = true;
    } catch {
      row.workSafe = false;
    }
    const threads = await g.threads();
    for (const thread of threads) {
      const transcript = await g.transcript(thread.threadId);
      await atomic(
        join(
          directory,
          "transcripts",
          row.id,
          `${Buffer.from(thread.threadId).toString("hex")}.json`,
        ),
        transcript,
      );
    }
    try {
      row.rehomed = await preserve(row, threads);
    } catch {
      row.rehomed = false;
    }
    try {
      row.workSafe =
        paused &&
        (await (
          await getDriver(row)
        ).exec(
          row.ref,
          `cd /home/user/t3-sandbox/repo && git fetch origin >/dev/null 2>&1 && if [ -z "$(git status --porcelain)" ] && [ "$(git rev-list --count HEAD --not --remotes)" = 0 ]; then printf safe; else printf unsafe; fi`,
        )) === "safe";
    } catch {
      row.workSafe = false;
    }
    row.retainedAt = now();
    await save();
  }
  async function reconcile(row) {
    const d = await getDriver(row);
    const p = profile(row.profile);
    row.enforcedDeadline = d.capabilities.deadline;
    try {
      if (!row.ref) {
        const matches = (await d.list()).filter((x) => x.name === row.name);
        if (matches.length > 1) throw new Error("Duplicate provider resources require recovery");
        if (matches.length) row.ref = matches[0].ref;
        else if (row.desired !== "running") {
          row.state = "deleted";
          await save();
          return;
        } else {
          if (row.createAttempted)
            throw new Error("Create outcome unknown; reconcile the provider before retrying");
          row.createAttempted = true;
          await save();
          const made = await d.create({
            name: row.name,
            requestId: row.id,
            ttl: state.policy.unattendedMinutes * 60,
          });
          if (!made.ref) throw new Error("Provider returned no resource id");
          row.ref = made.ref;
          await save();
        }
      }
      row.stage = "Inspecting resource";
      let observed;
      try {
        observed = await d.get(row.ref);
      } catch (cause) {
        const inventory = await d.list();
        if (inventory.some((item) => item.ref === row.ref)) throw cause;
        row.state = "deleted";
        row.desired = "deleted";
        await save();
        return;
      }
      if (row.orphan && row.desired === "stopped") {
        if (observed.state === "running") await d.stop(row.ref);
        row.state = "stopped";
        row.stoppedAt ??= now();
        await save();
        return;
      }
      if (observed.state === "deleted") {
        row.state = "deleted";
        row.desired = "deleted";
        await save();
        return;
      }
      if (observed.state === "stopped" && row.desired === "running" && !row.resumeRequested) {
        row.desired = "stopped";
        row.state = "stopped";
        row.stoppedAt ??= now();
        row.bootstrapped = false;
        row.error = "Provider stopped the sandbox; resume explicitly when ready.";
        if (row.sessions?.at(-1)) row.sessions.at(-1).end = Math.min(now(), row.deadline ?? now());
        await save();
        return;
      }
      if (observed.state === "stopped" && row.desired === "stopped") {
        row.state = "stopped";
        row.stoppedAt ??= now();
        await save();
        return;
      }
      if (["creating", "starting", "stopping"].includes(observed.state)) {
        row.state = observed.state === "creating" ? "starting" : observed.state;
        await save();
        return;
      }
      if (row.desired === "running") {
        row.stage = "Starting sandbox";
        if (observed.state === "stopped") {
          await d.start(row.ref);
          row.resumeRequested = false;
          row.startedAt = now();
          (row.sessions ??= []).push({ start: now(), end: null });
          row.lastActivity = now();
          row.bootstrapped = false;
          row.deadline = null;
        }
        if (!row.bootstrapped) {
          row.state = "starting";
          await save();
          const key = (await secrets.get(`reporter-${row.id}`)) ?? randomBytes(32).toString("hex");
          await secrets.set(`reporter-${row.id}`, key);
          await d.exec(row.ref, "mkdir -p /home/user/t3-sandbox/plugin");
          await d.write(row.ref, "/home/user/t3-sandbox/plugin/plugin.mjs", source);
          row.stage = "Bootstrapping T3";
          const token = await d.exec(row.ref, bootstrap({ ...row, reporterKey: key }, p));
          await secrets.set(`guest-${row.id}`, String(token).trim());
          row.url = p.url ? safeUrl(p.url.replaceAll("{id}", row.ref)) : await d.expose(row.ref);
          row.bootstrapped = true;
          await save();
        }
        if (row.desired !== "running") {
          await stop(row, d);
          return;
        }
        const g = await getGuest(row);
        row.stage = "Starting thread";
        if (!row.threadId) {
          const projects = await g.call("t3_project_list", { limit: 100 });
          const project =
            projects.projects.find((x) => x.workspaceRoot === "/home/user/t3-sandbox/repo") ??
            (await g.call("t3_project_create", {
              title: row.title,
              workspaceRoot: "/home/user/t3-sandbox/repo",
            }));
          const result = await g.call("t3_thread_launch", {
            projectId: project.id,
            title: row.title,
            ...(row.message ? { message: row.message } : {}),
            ...(row.modelSelection ? { modelSelection: row.modelSelection } : {}),
            clientRequestId: row.id,
            workspaceStrategy: { type: "root" },
          });
          row.threadId = result.threadId;
          row.message = null;
          await save();
        }
        row.state = "running";
        row.error = null;
        await d.exec(row.ref, `mkdir -p /home/user/t3-sandbox/home/plugins/sandboxes`);
        await d.write(
          row.ref,
          "/home/user/t3-sandbox/home/plugins/sandboxes/status.json",
          JSON.stringify({
            state: row.state,
            detail: row.deadline
              ? "Provider stop deadline: " + new Date(row.deadline).toISOString()
              : "Dedicated sandbox environment",
          }),
        );
        let activity;
        try {
          activity = await g.activity();
        } catch {
          activity = null;
        }
        if (activity) {
          if (activity.environmentId) row.environmentId = activity.environmentId;
          if (activity.active) row.lastActivity = now();
          else row.lastActivity = Math.max(row.lastActivity, activity.lastActivity);
          if (activity.waiting) row.waitingSince ??= now();
          else row.waitingSince = null;
          row.activity = activity;
        }
        const action = decide(row, activity, now(), state.policy);
        if (action === "stopped") {
          row.desired = "stopped";
          await save();
          await stop(row, d);
          return;
        }
        if (activity) {
          const until = deadline(row, activity, now(), state.policy);
          if (
            !row.deadline ||
            until < row.deadline - 30000 ||
            row.deadline - now() < (until - now()) / 2
          ) {
            await d.setDeadline(row.ref, Math.ceil((until - now()) / 1000));
            row.deadline = until;
          }
        }
      } else if (row.desired === "stopped") {
        await stop(row, d);
        return;
      } else {
        if (observed.state !== "stopped") {
          row.desired = "stopped";
          await stop(row, d);
          row.desired = "deleted";
          await save();
        }
        if (!row.confirmDelete && !row.autoDeleteSafe)
          throw new Error("Explicit data-loss confirmation required");
        row.state = "deleting";
        await save();
        await d.remove(row.ref);
        row.state = "deleted";
        await secrets.remove(`guest-${row.id}`);
        await secrets.remove(`reporter-${row.id}`);
      }
      await save();
    } catch (cause) {
      diagnostic(row.stage, cause);
      row.error = `Could not complete ${row.stage ?? "provider operation"}. Check the profile and connectivity; provider data was retained.`;
      if (row.state !== "running") row.state = "error";
      await save();
    }
  }
  async function stop(row, d) {
    if (row.state === "stopped") return;
    row.state = "stopping";
    await save();
    // A failed export keeps the machine until its provider lease expires.
    if (row.threadId) await retain(row);
    await d.stop(row.ref);
    row.state = (await d.get(row.ref)).state;
    if (row.state !== "stopped") {
      await save();
      return;
    }
    row.stoppedAt = now();
    if (row.sessions?.at(-1)) row.sessions.at(-1).end = now();
    row.bootstrapped = false;
    await save();
  }
  const api = {
    snapshot: () => structuredClone(state),
    usage: () => usage(state.rows, state.profiles, now()),
    configure: (input) =>
      serial(async () => {
        const id = String(input.id ?? "");
        if (!/^[a-z][a-z0-9-]{0,30}$/.test(id)) throw new Error("Invalid profile id");
        if (state.rows.some((r) => r.profile === id && r.state !== "deleted"))
          throw new Error("Stop and remove resources before replacing a profile");
        if (!["boat", "docker", "external"].includes(input.driver))
          throw new Error("Invalid driver");
        if (input.driver !== "external" && (!input.image || typeof input.image !== "string"))
          throw new Error("Image or snapshot required");
        const p = {
          id,
          driver: input.driver,
          image: input.image,
          environment: input.environment || undefined,
          url: input.url || undefined,
          controllerUrl: input.controllerUrl ? safeUrl(input.controllerUrl) : undefined,
          t3Command: input.t3Command || "t3",
          executable: input.executable || undefined,
          endpoint: input.endpoint ? safeUrl(input.endpoint) : undefined,
          rehomePath: input.rehomePath || undefined,
          hourlyRate: Number(input.hourlyRate ?? 0),
          capabilities: input.capabilities ?? { deadline: false, stopKeepsDisk: true },
        };
        if (!Number.isFinite(p.hourlyRate) || p.hourlyRate < 0)
          throw new Error("Invalid hourly rate");
        state.profiles = state.profiles.filter((x) => x.id !== id).concat(p);
        if (input.token) await secrets.set(`provider-${id}`, input.token);
        await save();
      }),
    setPolicy: (input) =>
      serial(async () => {
        state.policy = policy(input);
        await save();
      }),
    create: (input) =>
      serial(async () => {
        const id = input.requestId ?? randomUUID();
        if (!/^[a-f0-9-]{36}$/.test(id)) throw new Error("Invalid request id");
        const old = state.rows.find((r) => r.id === id);
        if (old) return old.id;
        const p = profile(input.profile);
        const count = state.rows.filter(
          (r) => r.desired === "running" && r.state !== "deleted",
        ).length;
        if (count >= state.policy.maxRunning) throw new Error("Running sandbox limit reached");
        if (state.policy.monthlyBudget > 0) {
          if (!(p.hourlyRate > 0))
            throw new Error("Configure an hourly rate before using a budget");
          if (usage(state.rows, state.profiles, now()).monthCost >= state.policy.monthlyBudget)
            throw new Error("Monthly estimated budget reached");
        }
        if (
          typeof input.repository !== "string" ||
          !/^https:\/\/[^\s]+$|^git@[^\s]+:[^\s]+$/.test(input.repository)
        )
          throw new Error("Use a Git remote");
        if (typeof input.base !== "string" || !/^[a-f0-9]{40}$/.test(input.base))
          throw new Error("Use the full pushed commit SHA");
        const row = {
          id,
          name: `t3-${state.owner.slice(0, 8)}-${p.id}-${id}`,
          profile: p.id,
          title: String(input.title ?? "Sandbox").slice(0, 150),
          hourlyRate: p.hourlyRate,
          repository: input.repository,
          base: input.base,
          message: input.message ?? null,
          modelSelection: input.modelSelection ?? null,
          state: "starting",
          desired: "running",
          startedAt: now(),
          sessions: [{ start: now(), end: null }],
          lastActivity: now(),
          pinned: false,
        };
        state.rows.push(row);
        await save();
        return row.id;
      }),
    adopt: (input) =>
      serial(async () => {
        const row = state.rows.find((r) => r.id === input.id && r.orphan);
        if (!row) throw new Error("Unknown orphan");
        if (
          typeof input.repository !== "string" ||
          !/^https:\/\/[^\s]+$|^git@[^\s]+:[^\s]+$/.test(input.repository) ||
          !/^[a-f0-9]{40}$/.test(input.base)
        )
          throw new Error("Supply a Git remote and full pushed base SHA");
        row.repository = input.repository;
        row.base = input.base;
        row.orphan = false;
        row.title = input.title ?? "Adopted sandbox";
        row.desired = "stopped";
        await save();
      }),
    action: (id, action, input = {}) =>
      serial(async () => {
        const row = state.rows.find((r) => r.id === id);
        if (!row) throw new Error("Unknown sandbox");
        if (row.orphan && action === "running")
          throw new Error("Adopt the orphan with a repository and base before resuming.");
        if (action === "keep-awake") {
          if (!Number.isFinite(input.hours) || input.hours < 0 || input.hours > 24)
            throw new Error("Keep awake must be 0–24 hours");
          row.keepAwakeUntil = now() + input.hours * 3600000;
        } else if (action === "pin") row.pinned = input.pinned === true;
        else if (["running", "stopped", "deleted"].includes(action)) {
          if (action === "deleted" && input.confirm !== true)
            throw new Error("Confirm permanent loss of workspace and thread data");
          if (action === "running") {
            if (row.state === "deleted") throw new Error("Deleted sandboxes cannot be resumed.");
            if (
              state.rows.filter(
                (r) => r.id !== row.id && r.desired === "running" && r.state !== "deleted",
              ).length >= state.policy.maxRunning
            )
              throw new Error("Running sandbox limit reached");
            const p = profile(row.profile);
            if (
              state.policy.monthlyBudget > 0 &&
              (!(p.hourlyRate > 0) ||
                usage(state.rows, state.profiles, now()).monthCost >= state.policy.monthlyBudget)
            )
              throw new Error("Monthly estimated budget reached or rate missing");
            row.resumeRequested = true;
          }
          row.desired = action;
          row.confirmDelete = input.confirm === true;
        } else throw new Error("Unknown action");
        await save();
      }),
    pair: (id) =>
      serial(async () => {
        const row = state.rows.find((r) => r.id === id);
        if (!row || row.state !== "running") throw new Error("Resume sandbox first");
        const result = JSON.parse(
          await (await getDriver(row)).exec(row.ref, pairingScript(profile(row.profile), row.url)),
        );
        return result.pairUrl;
      }),
    tick: async () => {
      if (closed) return;
      for (const p of state.profiles) {
        try {
          const d = drivers({ ...p, token: await secrets.get(`provider-${p.id}`) });
          for (const observed of await d.list()) {
            if (
              !observed.name?.startsWith(`t3-${state.owner.slice(0, 8)}-${p.id}-`) ||
              state.rows.some((r) => r.ref === observed.ref || r.name === observed.name)
            )
              continue;
            state.rows.push({
              id: randomUUID(),
              name: observed.name,
              ref: observed.ref,
              profile: p.id,
              title: "Orphaned sandbox",
              state: observed.state,
              desired: "stopped",
              orphan: true,
              pinned: true,
              startedAt: now(),
              lastActivity: now(),
            });
            await save();
          }
        } catch {}
      }
      const pending = [];
      for (const row of state.rows) {
        if (
          row.state === "stopped" &&
          row.desired === "stopped" &&
          !row.pinned &&
          state.policy.deleteDays > 0 &&
          now() >= row.stoppedAt + state.policy.deleteDays * 86400000
        ) {
          if (row.retainedAt && (row.workSafe || row.rehomed)) {
            row.desired = "deleted";
            row.autoDeleteSafe = true;
            await save();
          } else
            row.error =
              "Automatic deletion blocked: work has not been verified as retained or pushed.";
        }
        if (row.state === "deleted" || (row.state === "stopped" && row.desired === "stopped"))
          continue;
        let worker = workers.get(row.id);
        if (!worker) {
          worker = reconcile(row).finally(() => workers.delete(row.id));
          workers.set(row.id, worker);
        }
        pending.push(worker);
      }
      await Promise.all(pending);
    },
    close: async () => {
      await tail;
      await Promise.all(workers.values());
      await saveTail;
      closed = true;
    },
  };
  return api;
}
