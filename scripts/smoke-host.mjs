import { spawn } from "node:child_process";
import { mkdtemp, mkdir, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { createServer } from "node:net";
import { once } from "node:events";
import assert from "node:assert/strict";
import { Guest } from "../src/mcp.mjs";
import { command } from "../src/io.mjs";
const checkout = process.env.T3_HOST_CHECKOUT;
if (!checkout) throw new Error("Set T3_HOST_CHECKOUT to the shared host checkout");
const directory = await mkdtemp(join(tmpdir(), "sandbox-host-smoke-"));
let child;
try {
  const userdata = join(directory, "userdata");
  await mkdir(userdata);
  await writeFile(
    join(directory, "plugins.json"),
    JSON.stringify({
      plugins: [{ id: "sandboxes", path: resolve("dist/plugin.mjs"), enabled: true }],
    }),
  );
  await writeFile(
    join(directory, "plugins-guest.json"),
    JSON.stringify({ mode: "guest", sandboxId: "test" }),
  );
  const bin = join(checkout, "apps/server/dist/bin.mjs");
  const token = await command(process.execPath, [
    bin,
    "auth",
    "mcp",
    "issue",
    "--access",
    "full-access",
    "--base-dir",
    directory,
    "--token-only",
  ]);
  const socket = createServer();
  socket.listen(0, "127.0.0.1");
  await once(socket, "listening");
  const port = socket.address().port;
  await new Promise((resolve) => socket.close(resolve));
  child = spawn(
    process.execPath,
    [
      bin,
      "serve",
      "--base-dir",
      directory,
      "--host",
      "127.0.0.1",
      "--port",
      String(port),
      "--no-browser",
    ],
    { stdio: ["ignore", "pipe", "pipe"] },
  );
  let output = "";
  const ready = new Promise((resolve, reject) => {
    const read = (chunk) => {
      output += chunk;
      if (output.includes("T3 Code server is ready.")) resolve();
    };
    child.stdout.on("data", read);
    child.stderr.on("data", read);
    child.on("exit", (code) => reject(new Error(`Isolated host exited (${code})`)));
  });
  const timer = setTimeout(() => {
    child.kill("SIGTERM");
  }, 30000);
  try {
    await ready;
  } finally {
    clearTimeout(timer);
  }
  const guest = new Guest(`http://127.0.0.1:${port}`, token);
  const catalog = await guest.call("t3_plugins_list");
  assert.ok(catalog.plugins.some((p) => p.id === "sandboxes" && p.status === "active"));
  const activity = await guest.activity();
  assert.equal(activity.active, false);
  assert.equal(activity.closed, true);
  assert.ok(activity.environmentId);

  console.log(
    "Packaged host: real isolated server, bearer-authenticated MCP, plugin worker, committed activity read verified.",
  );
} finally {
  if (child && child.exitCode === null) {
    child.kill("SIGTERM");
    await once(child, "exit");
  }
  await rm(directory, { recursive: true, force: true });
}
