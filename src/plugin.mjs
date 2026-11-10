import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as PubSub from "effect/PubSub";
import * as Schedule from "effect/Schedule";
import { readFile, cp, mkdir } from "node:fs/promises";
import { watch } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { timingSafeEqual } from "node:crypto";
import { createController } from "./controller.mjs";
import { tone } from "./policy.mjs";
import { Guest } from "./mcp.mjs";
import { launchProject, launchSandbox } from "./launch.mjs";
class PluginError extends Schema.TaggedError()("PluginError", {
  operation: Schema.String,
  cause: Schema.Defect(),
}) {}
const call = (operation, run) =>
  Effect.tryPromise({ try: run, catch: (cause) => new PluginError({ operation, cause }) });
const action = (id, label, command, input = {}) => ({ id, label, command, input });
const field = (name, label, kind = "text", value) => ({
  name: name.replace(/[A-Z]/g, (letter) => "-" + letter.toLowerCase()),
  label,
  kind,
  ...(value === undefined ? {} : { value }),
});
const form = (id, label, command, fields, input = {}) => ({
  id,
  kind: "form",
  label,
  command,
  fields,
  input,
});
const descriptor = (
  id,
  title,
  run,
  { agent = false, permission = "environment:maintain" } = {},
) => ({
  id,
  title,
  description: title,
  permission,
  agent,
  run: (input) =>
    call(id, () =>
      run(
        Object.fromEntries(
          Object.entries(input).map(([key, value]) => [
            key.replace(/-([a-z])/g, (_, letter) => letter.toUpperCase()),
            value,
          ]),
        ),
      ),
    ),
});
function settings(controller) {
  const state = controller.snapshot();
  return {
    title: "Sandboxes",
    blocks: [
      {
        id: "intro",
        kind: "text",
        text: "Each sandbox runs its own T3 environment. Use an image or Boat snapshot containing this plugin host build and your chosen agent tools. Start a sandbox here, then pair to open its thread. Docker is reachable from this computer only; use a provider HTTPS URL or a tailnet URL for your phone.",
      },
      {
        id: "resources",
        kind: "table",
        columns: ["Thread", "ID", "Profile", "State", "Retention"],
        rows: state.rows.slice(-500).map((r) => ({
          id: r.id,
          cells: [
            r.title ?? r.name,
            r.id,
            r.profile,
            r.error ?? r.state,
            r.retainedAt ? "Transcript saved locally" : "Not exported yet",
          ],
          actions:
            r.state === "deleted"
              ? []
              : [
                  action("stop", "Stop", "state", { id: r.id, state: "stopped" }),
                  action("resume", "Resume", "state", { id: r.id, state: "running" }),
                  action("pair", "Pair client", "pair", { id: r.id }),
                  action("pin", r.pinned ? "Unpin" : "Pin", "pin", {
                    id: r.id,
                    pinned: !r.pinned,
                  }),
                ],
        })),
      },
      form("profile", "Save provider profile", "profile", [
        field("id", "Profile name"),
        {
          name: "driver",
          label: "Driver",
          kind: "select",
          options: ["docker", "boat", "external"].map((value) => ({ label: value, value })),
        },
        field("image", "Docker image or Boat snapshot"),
        field("environment", "Boat environment (blank injects no credentials)"),
        field("url", "Tailnet URL override ({id} is replaced)"),
        field("controllerUrl", "Reachable controller URL (optional, for guest controls)"),
        field("t3Command", "T3 executable in image", "text", "t3"),
        field("executable", "External driver executable"),
        field("rehomePath", "Optional Rehome plugin path inside guest image"),
        field("hourlyRate", "Hourly rate for estimated cost and budget", "number", 0),
        field("endpoint", "External driver HTTPS endpoint"),
        field("token", "External driver bearer key", "password"),
      ]),
      form("create", "Create sandbox and start thread", "create", [
        {
          name: "profile",
          label: "Provider profile",
          kind: "select",
          options: state.profiles.map((profile) => ({ label: profile.id, value: profile.id })),
        },
        field("repository", "Pushed Git remote"),
        field("base", "Full pushed commit SHA"),
        field("title", "Thread title"),
        field("message", "First message (optional)"),
      ]),
      form(
        "policy",
        "Save cost policy",
        "policy",
        Object.entries(state.policy).map(([key, value]) =>
          field(key, key, typeof value === "boolean" ? "boolean" : "number", value),
        ),
      ),
      form("adopt", "Adopt an orphaned sandbox", "adopt", [
        field("id", "Orphan sandbox ID"),
        field("repository", "Git remote"),
        field("base", "Full pushed base commit SHA"),
        field("title", "Thread title"),
      ]),
      form("awake", "Keep awake or clear", "awake", [
        field("id", "Sandbox ID"),
        field("hours", "Hours (0 clears, maximum 24)", "number", 1),
      ]),
      form("delete", "Permanently delete sandbox", "delete", [
        field("id", "Sandbox ID"),
        field(
          "confirm",
          "I understand this permanently deletes the checkout and guest thread data. Transcripts exclude workspace files.",
          "boolean",
          false,
        ),
      ]),
      { id: "stop-all", kind: "actions", actions: [action("stop-all", "Stop all", "stop-all")] },
      {
        id: "retention",
        kind: "text",
        text: "Before a controlled stop, all guest thread transcripts are exported to this plugin’s private data directory. Stop preserves provider disk; Delete permanently removes it. Keep the plugin enabled until every sandbox has stopped. Provider deadlines still apply if the controller is offline. Monthly budget caps refuse creation when spend cannot be verified. Docker has no independent provider deadline.",
      },
    ],
  };
}
export default {
  apiVersion: 1,
  id: "sandboxes",
  name: "Sandboxes",
  version: "0.1.1",
  description: "On-demand thread environments with bounded leases and retained history.",
  activate: (context) =>
    Effect.gen(function* () {
      const changes = yield* PubSub.unbounded();
      const guestConfig = yield* call("guest-config", async () => {
        try {
          return JSON.parse(
            await readFile(join(context.dataDirectory, "..", "..", "plugins-guest.json"), "utf8"),
          );
        } catch (error) {
          if (error.code === "ENOENT") return null;
          throw error;
        }
      });
      if (guestConfig?.mode === "guest") {
        const status = async () => {
          try {
            return JSON.parse(await readFile(join(context.dataDirectory, "status.json"), "utf8"));
          } catch {
            return {
              state: "running",
              detail: "Controller status unavailable; provider deadline remains in force.",
            };
          }
        };
        const remote = async (command, input) => {
          if (!guestConfig.controllerUrl)
            throw new Error("Manage this sandbox from its controller");
          const response = await fetch(
            `${guestConfig.controllerUrl}/api/plugins/sandboxes/control`,
            {
              method: "POST",
              headers: {
                Authorization: `Bearer ${guestConfig.key}`,
                "Content-Type": "application/json",
              },
              body: JSON.stringify({ id: guestConfig.sandboxId, command, input }),
              signal: AbortSignal.timeout(15000),
            },
          );
          if (!response.ok) throw new Error("Controller unavailable");
          return null;
        };
        yield* Effect.acquireRelease(
          Effect.sync(() =>
            watch(context.dataDirectory, () => {
              Effect.runFork(PubSub.publish(changes, undefined));
            }),
          ),
          (watcher) => Effect.sync(() => watcher.close()),
        );
        return {
          changes: Stream.fromPubSub(changes),
          commands: [
            descriptor(
              "prepare-stop",
              "Pause sandbox threads and capture checkpoints",
              async () => {
                if (!context.threads)
                  throw new Error("Thread portability host capability required");
                const tokenText = await readFile(
                  join(context.dataDirectory, "controller-token"),
                  "utf8",
                );
                const g = new Guest("http://127.0.0.1:3773", tokenText.trim());
                const threads = await g.threads();
                for (const thread of threads.filter((t) => t.relationshipToParent !== "subagent"))
                  await Effect.runPromise(
                    context.threads.export({
                      threadId: thread.threadId,
                      file: join(
                        context.dataDirectory,
                        `pause-${Buffer.from(thread.threadId).toString("hex")}.json`,
                      ),
                    }),
                  );
                return null;
              },
              { agent: true, permission: "orchestration:operate" },
            ),
            descriptor(
              "activity",
              "Read committed sandbox activity",
              async () => ({
                ...(await Effect.runPromise(context.activity)),
                ...(context.environmentId
                  ? { environmentId: await Effect.runPromise(context.environmentId) }
                  : {}),
              }),
              { agent: true, permission: "orchestration:read" },
            ),
            descriptor("stop", "Stop sandbox", () => remote("stopped", {})),
            descriptor("awake", "Keep sandbox awake", (input) => remote("keep-awake", input), {
              agent: true,
            }),
          ],
          views: [
            {
              id: "status",
              title: "Sandbox",
              location: "thread-details",
              permission: "orchestration:read",
              read: () =>
                call("status", async () => {
                  const s = await status();
                  return {
                    title: "Sandbox",
                    blocks: [
                      {
                        id: "sandbox",
                        kind: "status",
                        tone: tone(s.state),
                        label: s.state,
                        detail: s.detail ?? "Dedicated sandbox environment",
                        actions: guestConfig.controllerUrl
                          ? [
                              {
                                ...action("stop", "Stop sandbox", "stop"),
                                confirm:
                                  "Stop this sandbox? Its thread transcripts will be retained by the controller.",
                              },
                            ]
                          : [],
                      },
                    ],
                  };
                }),
            },
          ],
        };
      }
      const queue = yield* Queue.unbounded();
      const notify = () => {
        Effect.runFork(PubSub.publish(changes, undefined));
      };
      const secrets = Object.fromEntries(
        ["get", "set", "remove"].map((key) => [
          key,
          (...args) => Effect.runPromise(context.secrets[key](...args)),
        ]),
      );
      const controller = yield* call("activate", async () =>
        createController({
          directory: context.dataDirectory,
          secrets,
          source: await readFile(fileURLToPath(import.meta.url), "utf8"),
          changed: notify,
          preserve: async (row, threads) => {
            const peers = context.plugins ? await Effect.runPromise(context.plugins.list()) : [];
            if (!peers.some((p) => p.id === "rehome" && p.commands.some((c) => c.id === "pull")))
              return false;
            const profile = controller.snapshot().profiles.find((p) => p.id === row.profile);
            if (!profile?.rehomePath) return false;
            const key = await secrets.get(`reporter-${row.id}`);
            for (const thread of threads.filter((t) => t.relationshipToParent !== "subagent")) {
              let result = await Effect.runPromise(
                context.plugins.run({
                  pluginId: "rehome",
                  commandId: "pull",
                  input: {
                    threadId: thread.threadId,
                    source: { url: row.url, key },
                    waitMs: 20000,
                  },
                }),
              );
              while (result.ok && result.state === "running")
                result = await Effect.runPromise(
                  context.plugins.run({
                    pluginId: "rehome",
                    commandId: "status",
                    input: { moveId: result.moveId, waitMs: 20000 },
                  }),
                );
              if (!result.ok || result.state !== "done") return false;
            }
            return true;
          },
        }),
      );
      yield* Effect.addFinalizer(() => call("close", () => controller.close()).pipe(Effect.ignore));
      yield* Stream.fromQueue(queue).pipe(
        Stream.runForEach(() => call("reconcile", () => controller.tick()).pipe(Effect.ignore)),
        Effect.forkScoped,
      );
      yield* Queue.offer(queue, undefined).pipe(
        Effect.repeat(Schedule.spaced("30 seconds")),
        Effect.forkScoped,
      );
      const mutate = async (fn) => {
        const result = await fn();
        Effect.runFork(Queue.offer(queue, undefined));
        return result ?? null;
      };
      return {
        beforeRemove: () =>
          call("before-remove", async () => {
            if (controller.snapshot().rows.some((row) => row.state !== "deleted"))
              throw new Error(
                "Stop and delete managed sandboxes before disabling or removing this plugin.",
              );
            const archive = join(context.dataDirectory, "..", "..", "sandbox-archives");
            await mkdir(archive, { recursive: true, mode: 0o700 });
            try {
              await cp(join(context.dataDirectory, "transcripts"), archive, {
                recursive: true,
                force: false,
              });
            } catch (error) {
              if (error.code !== "ENOENT") throw error;
            }
          }),
        changes: Stream.fromPubSub(changes),
        commands: [
          descriptor("launch", "Start a sandboxed project thread", (input) =>
            mutate(() => launchSandbox(controller, input)),
          ),
          descriptor("cancel", "Cancel provisioning and delete sandbox", (input) =>
            mutate(() => controller.action(input.id, "deleted", { confirm: true })),
          ),
          descriptor("profile", "Configure provider profile", (input) =>
            controller.configure(input),
          ),
          descriptor("adopt", "Adopt orphaned sandbox", (input) => controller.adopt(input)),
          descriptor("policy", "Configure sandbox policy", (input) => controller.setPolicy(input)),
          descriptor("create", "Create sandbox", (input) => mutate(() => controller.create(input))),
          descriptor(
            "list",
            "List sandbox states",
            async () =>
              controller
                .snapshot()
                .rows.map(({ id, state, title, profile }) => ({ id, state, title, profile })),
            { agent: true, permission: "orchestration:read" },
          ),
          descriptor("state", "Stop or resume sandbox", (input) =>
            mutate(() => controller.action(input.id, input.state)),
          ),
          descriptor(
            "awake",
            "Keep sandbox awake or clear",
            (input) => mutate(() => controller.action(input.id, "keep-awake", input)),
            { agent: true },
          ),
          descriptor("pin", "Pin or unpin sandbox", (input) =>
            mutate(() => controller.action(input.id, "pin", input)),
          ),
          descriptor("delete", "Permanently delete sandbox", (input) =>
            mutate(() => controller.action(input.id, "deleted", input)),
          ),
          descriptor("pair", "Issue one-time sandbox pairing URL", async (input) => ({
            $t3: "secret",
            label: "Sandbox pairing URL",
            value: await controller.pair(input.id),
          })),
          descriptor("stop-all", "Stop all sandboxes", async () =>
            mutate(async () => {
              for (const r of controller.snapshot().rows)
                if (r.state !== "deleted") await controller.action(r.id, "stopped");
            }),
          ),
        ],
        views: [
          {
            id: "launcher",
            title: "New sandbox",
            location: "environment-launcher",
            permission: "environment:maintain",
            read: (input = {}) =>
              call("launcher", async () => {
                const state = controller.snapshot();
                const row = state.rows.find((row) => row.id === input.requestId);
                if (row)
                  return {
                    title: "New sandbox",
                    blocks: [
                      {
                        id: "progress",
                        kind: "text",
                        text: row.error ?? `${row.state}: ${row.stage ?? row.title}`,
                      },
                      {
                        id: "control",
                        kind: "actions",
                        actions:
                          row.state === "starting"
                            ? [
                                action(
                                  "cancel",
                                  "Cancel provisioning and delete sandbox",
                                  "cancel",
                                  { id: row.id },
                                ),
                              ]
                            : row.state === "error"
                              ? [
                                  action("retry", "Retry", "state", {
                                    id: row.id,
                                    state: "running",
                                  }),
                                ]
                              : [],
                      },
                    ],
                  };
                if (!state.profiles.length)
                  return {
                    title: "New sandbox",
                    blocks: [
                      {
                        id: "configure",
                        kind: "text",
                        text: "Configure a provider profile in Settings → Extensions → Sandboxes first.",
                      },
                    ],
                  };
                if (input.hasAttachments)
                  return {
                    title: "New sandbox",
                    blocks: [
                      {
                        id: "attachments",
                        kind: "text",
                        text: "Start with a text-only first message. Remove draft attachments and local context before choosing New sandbox.",
                      },
                    ],
                  };
                const project = await launchProject(input);
                return {
                  title: "New sandbox",
                  blocks: [
                    {
                      id: "scope",
                      kind: "text",
                      text: "The controller starts this thread from a pushed commit. Uncommitted local work stays here. After Send, provisioning continues if you lock your phone.",
                    },
                    form(
                      "launch",
                      "Send and start sandbox",
                      "launch",
                      [
                        {
                          name: "profile",
                          label: "Provider profile",
                          kind: "select",
                          required: true,
                          value: state.profiles[0].id,
                          options: state.profiles.map((p) => ({ label: p.id, value: p.id })),
                        },
                        field("title", "Thread title", "text", String(input.title ?? "Sandbox")),
                        field("base", "Pushed base commit", "text", project.base),
                        field("message", "First message", "text", String(input.message ?? "")),
                      ],
                      {
                        projectDirectory: project.directory,
                        requestId: input.requestId,
                        modelSelection: input.modelSelection ?? null,
                        runtimeMode: input.runtimeMode ?? null,
                      },
                    ),
                  ],
                };
              }),
          },
          {
            id: "environments",
            title: "Managed sandbox environments",
            location: "managed-environments",
            permission: "environment:maintain",
            read: () =>
              Effect.succeed({
                title: "Managed sandbox environments",
                blocks: controller
                  .snapshot()
                  .rows.filter((row) => row.environmentId && row.url)
                  .slice(-100)
                  .map((row) => ({
                    id: row.id.replace(/^[0-9]/, "s$&"),
                    kind: "environment",
                    environmentId: row.environmentId,
                    label: row.title ?? row.profile,
                    url: row.url,
                    state: row.state,
                    ...(row.threadId ? { threadId: row.threadId } : {}),
                    launchRequestId: row.id,
                    connect: action("connect", "Connect", "pair", { id: row.id }),
                  })),
              }),
          },
          {
            id: "managed-status",
            title: "Managed sandboxes",
            location: "thread-details",
            permission: "orchestration:read",
            read: () =>
              Effect.succeed({
                title: "Managed sandboxes",
                blocks: controller
                  .snapshot()
                  .rows.filter((row) => row.environmentId)
                  .slice(-100)
                  .map((row) => ({
                    id: row.id.replace(/^[0-9]/, "s$&"),
                    kind: "status",
                    environmentId: row.environmentId,
                    tone: tone(row.state),
                    label: `${row.profile}: ${row.state}`,
                    detail: row.error ?? "Managed sandbox",
                    actions:
                      row.state === "deleted"
                        ? []
                        : [
                            {
                              ...action(
                                "state",
                                row.state === "stopped" ? "Resume" : "Stop",
                                "state",
                                {
                                  id: row.id,
                                  state: row.state === "stopped" ? "running" : "stopped",
                                },
                              ),
                              ...(row.state === "stopped"
                                ? {}
                                : {
                                    confirm: "Stop the sandbox after retaining its thread history?",
                                  }),
                            },
                          ],
                  })),
              }),
          },
          {
            id: "settings",
            title: "Sandboxes",
            location: "settings-page",
            permission: "environment:maintain",
            read: () => Effect.succeed(settings(controller)),
          },
          {
            id: "usage",
            title: "Sandbox usage",
            location: "usage",
            permission: "orchestration:read",
            read: () =>
              Effect.succeed({
                title: "Sandbox usage",
                blocks: [
                  {
                    id: "cost",
                    kind: "text",
                    text: `Estimated month total: $${controller.usage().monthCost.toFixed(2)}. Rates are configured per profile; provider invoices are authoritative.`,
                  },
                  {
                    id: "resources",
                    kind: "table",
                    columns: ["Thread", "Runtime hours", "Estimated cost"],
                    rows: controller.usage().items.map((item) => ({
                      id: item.id,
                      cells: [
                        item.title ?? item.id,
                        (item.seconds / 3600).toFixed(2),
                        item.rateKnown ? "$" + item.cost.toFixed(2) : "Rate not configured",
                      ],
                      actions: [],
                    })),
                  },
                ],
              }),
          },
        ],
        handleHttp: (request) =>
          call("control", async () => {
            if (
              request.method !== "POST" ||
              new URL(request.url).pathname.split("/").at(-1) !== "control"
            )
              return new Response(null, { status: 404 });
            const text = await request.text();
            if (text.length > 8192) return new Response(null, { status: 413 });
            const body = JSON.parse(text);
            if (!/^[a-f0-9-]{36}$/.test(body.id)) return new Response(null, { status: 400 });
            const expected = await secrets.get(`reporter-${body.id}`);
            const supplied = request.headers.get("authorization")?.replace(/^Bearer /, "") ?? "";
            if (
              !expected ||
              Buffer.byteLength(expected) !== Buffer.byteLength(supplied) ||
              !timingSafeEqual(Buffer.from(expected), Buffer.from(supplied))
            )
              return new Response(null, { status: 401 });
            if (!["stopped", "keep-awake"].includes(body.command))
              return new Response(null, { status: 403 });
            await mutate(() => controller.action(body.id, body.command, body.input ?? {}));
            return Response.json({ accepted: true });
          }),
      };
    }),
};
