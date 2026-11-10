import { command, quote, safeUrl } from "./io.mjs";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
async function transfer(run, ref, path, content, kind) {
  const dir = await mkdtemp(join(tmpdir(), "sandbox-upload-"));
  try {
    const file = join(dir, "payload");
    await writeFile(file, content, { mode: 0o600 });
    await run(
      kind,
      kind === "boat"
        ? ["--no-update", "scp", file, `${ref}:${path}`]
        : ["cp", file, `${ref}:${path}`],
    );
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
}
const parse = JSON.parse;
function boatResult(text) {
  try {
    return JSON.parse(text);
  } catch {
    const frames = text
      .split("\n")
      .filter((x) => x.trim())
      .map((x) => JSON.parse(x));
    const last = frames.at(-1);
    return {
      ...last,
      ...(!last.id ? { id: frames.find((x) => x.id)?.id } : {}),
      ...(last.state ? {} : { state: last.event }),
    };
  }
}
const state = (value) =>
  ({
    archived: "stopped",
    idle: "running",
    busy: "running",
    ready: "running",
    pending: "starting",
    destroyed: "deleted",
  })[value] ?? value;
export function driver(profile, run = command) {
  if (profile.driver === "external") return external(profile, run);
  if (profile.driver === "boat") {
    const boat = async (...args) =>
      boatResult(await run("boat", ["--no-update", "--json", ...args]));
    const observe = (x) => ({
      ref: x.id,
      state: x.dataExpiredAt ? "deleted" : state(x.state),
      name: x.name,
      deadline: x.archiveAfter ? Date.parse(x.archiveAfter) : null,
    });
    return {
      capabilities: { deadline: true, stopKeepsDisk: true },
      async list() {
        const result = await boat("list", "--all");
        if (result.pageInfo?.hasMore)
          throw new Error("Boat listing is incomplete; refusing reconciliation");
        return result.sandboxes.map(observe);
      },
      async create(spec) {
        const x = await boat(
          "new",
          "--name",
          spec.name,
          "--from",
          profile.image,
          "--ttl",
          String(spec.ttl),
          ...(profile.environment ? ["--environment", profile.environment] : ["--no-env"]),
        );
        return observe(x.sandbox ?? x);
      },
      async get(ref) {
        const x = await boat("info", ref);
        return observe(x.sandbox ?? x);
      },
      async start(ref) {
        await boat("resume", ref);
      },
      async stop(ref) {
        await boat("stop", ref);
      },
      async remove(ref) {
        await boat("delete", ref, "--yes");
      },
      async setDeadline(ref, seconds) {
        await boat("extend", ref, "--ttl", String(Math.max(60, seconds)));
      },
      async exec(ref, script) {
        const x = await boat("exec", ref, "--timeout", "600", "--", "bash", "-lc", script);
        if (x.exitCode !== 0 && x.exitCode !== undefined) throw new Error("Boat command failed");
        return x.stdout ?? x.output ?? "";
      },
      async write(ref, path, content) {
        await transfer(run, ref, path, content, "boat");
      },
      async expose(ref) {
        const x = await boat("host", ref, "3773", "--public");
        return safeUrl(x.url ?? x.host?.url);
      },
    };
  }
  if (profile.driver !== "docker") throw new Error("Unknown driver");
  const docker = (...args) => run("docker", args);
  const observe = (x) => ({
    ref: x.Id,
    state: x.State.Running ? "running" : x.State.Status === "created" ? "starting" : "stopped",
    name: x.Name.replace(/^\//, ""),
  });
  return {
    capabilities: { deadline: false, stopKeepsDisk: true },
    async list() {
      const ids = (await docker("ps", "-aq", "--filter", "label=t3-sandboxes=true"))
        .split(/\s+/)
        .filter(Boolean);
      return ids.length ? parse(await docker("inspect", ...ids)).map(observe) : [];
    },
    async create(spec) {
      const ref = await docker(
        "run",
        "-d",
        "--name",
        spec.name,
        "--label",
        "t3-sandboxes=true",
        "--init",
        "-p",
        "127.0.0.1::3773",
        profile.image,
        "sleep",
        "infinity",
      );
      return { ref, state: "running", name: spec.name };
    },
    async get(ref) {
      return observe(parse(await docker("inspect", ref))[0]);
    },
    async start(ref) {
      await docker("start", ref);
    },
    async stop(ref) {
      await docker("stop", ref);
    },
    async remove(ref) {
      await docker("rm", ref);
    },
    async setDeadline() {},
    async exec(ref, script) {
      return docker("exec", ref, "bash", "-lc", script);
    },
    async write(ref, path, content) {
      await transfer(run, ref, path, content, "docker");
    },
    async expose(ref) {
      const x = parse(await docker("inspect", ref))[0];
      return `http://127.0.0.1:${x.NetworkSettings.Ports["3773/tcp"][0].HostPort}`;
    },
  };
}
function external(profile, run) {
  const call = async (operation, input) => {
    if (profile.executable)
      return JSON.parse(
        await run(profile.executable, [], { input: JSON.stringify({ operation, input }) }),
      );
    const response = await fetch(profile.endpoint, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        ...(profile.token ? { Authorization: `Bearer ${profile.token}` } : {}),
      },
      body: JSON.stringify({ operation, input }),
      signal: AbortSignal.timeout(120000),
    });
    if (!response.ok) throw new Error(`Driver operation failed (${response.status})`);
    const text = await response.text();
    if (text.length > 1048576) throw new Error("Driver result too large");
    return JSON.parse(text);
  };
  return {
    capabilities: profile.capabilities,
    ...Object.fromEntries(
      [
        "list",
        "get",
        "start",
        "stop",
        "remove",
        "setDeadline",
        "exec",
        "expose",
        "create",
        "write",
      ].map((op) => [op, (...args) => call(op, args)]),
    ),
  };
}
export function bootstrap(row, profile) {
  const root = "/home/user/t3-sandbox";
  const q = quote;
  return `set -eu
umask 077
mkdir -p ${root}/home/userdata ${root}/plugin
printf %s ${q(JSON.stringify({ plugins: [{ id: "sandboxes", path: root + "/plugin/plugin.mjs", enabled: true }, ...(profile.rehomePath ? [{ id: "rehome", path: profile.rehomePath, enabled: true }] : [])] }))} > ${root}/home/plugins.json
printf %s ${q(JSON.stringify({ mode: "guest", sandboxId: row.id, controllerUrl: profile.controllerUrl ?? null, key: row.reporterKey }))} > ${root}/home/plugins-guest.json
chmod 600 ${root}/home/plugins-guest.json
if [ ! -d ${root}/repo/.git ]; then
  git clone -- ${q(row.repository)} ${root}/repo
  cd ${root}/repo
  git checkout --detach ${q(row.base)}
fi
cd ${root}/repo
# A dedicated image supplies the same plugin host build as the controller.
${quote(profile.t3Command ?? "t3")} --version >/dev/null
if [ ! -f ${root}/server.pid ] || ! kill -0 "$(cat ${root}/server.pid)" 2>/dev/null; then
  T3_REHOME_TRANSFER_KEY="$(node -p 'JSON.parse(require("fs").readFileSync("/home/user/t3-sandbox/home/plugins-guest.json","utf8")).key')" nohup ${quote(profile.t3Command ?? "t3")} serve --base-dir ${root}/home --host 0.0.0.0 --port 3773 --no-browser > ${root}/server.log 2>&1 < /dev/null &
  echo $! > ${root}/server.pid
fi
mkdir -p ${root}/home/plugins/sandboxes
${quote(profile.t3Command ?? "t3")} auth mcp issue --base-dir ${root}/home --access full-access --token-only > ${root}/home/plugins/sandboxes/controller-token
chmod 600 ${root}/home/plugins/sandboxes/controller-token
node -e ${q("const fs=require('node:fs');const file='/home/user/t3-sandbox/server.log';const watcher=fs.watch('/home/user/t3-sandbox',check);const timer=setTimeout(()=>{watcher.close();process.exit(1)},60000);function check(){if(fs.existsSync(file)&&fs.readFileSync(file,'utf8').includes('T3 Code server is ready.')){clearTimeout(timer);watcher.close();}}check();")}
cat ${root}/home/plugins/sandboxes/controller-token`;
}
export function pairingScript(profile, url) {
  return `${quote(profile.t3Command ?? "t3")} auth pairing create --base-dir /home/user/t3-sandbox/home --json --base-url ${quote(url)}`;
}
