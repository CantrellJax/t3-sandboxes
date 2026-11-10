import { driver } from "../src/drivers.mjs";
import { createController } from "../src/controller.mjs";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { command } from "../src/io.mjs";
import assert from "node:assert/strict";
const directory = await mkdtemp(join(tmpdir(), "t3-sandbox-e2e-"));
const values = new Map();
const controller = await createController({
  diagnostic: (stage, error) => console.log(stage, error.message),
  directory,
  source: await readFile("dist/plugin.mjs", "utf8"),
  secrets: {
    get: async (key) => values.get(key) ?? null,
    set: async (key, value) => {
      values.set(key, value);
    },
    remove: async (key) => {
      values.delete(key);
    },
  },
});
let id;
try {
  const [base] = (
    await command("git", ["ls-remote", "https://github.com/octocat/Hello-World.git", "HEAD"])
  ).split(/\s/);
  await controller.configure({ id: "smoke", driver: "docker", image: "localhost/t3-sandbox:dev" });
  id = await controller.create({
    profile: "smoke",
    repository: "https://github.com/octocat/Hello-World.git",
    base,
    title: "Sandbox verification",
    modelSelection: { instanceId: "codex", model: "gpt-6.1-sol" },
  });
  await controller.tick();
  let row = controller.snapshot().rows[0];
  if (row.state !== "running") {
    console.log(
      await driver({ driver: "docker" }).exec(
        row.ref,
        `node -e 'console.log(require("fs").readFileSync("/home/user/t3-sandbox/server.log","utf8").split("\\n").filter(x=>/Error|ERR_|Cannot/.test(x)).join("\\n"))'`,
      ),
    );
    console.log(JSON.stringify({ state: row.state, stage: row.stage, error: row.error }));
    throw new Error("Sandbox launch failed");
  }
  assert.ok(row.threadId);
  assert.ok(await controller.pair(id));
  await controller.action(id, "stopped");
  await controller.tick();
  row = controller.snapshot().rows[0];
  assert.equal(row.state, "stopped");
  assert.ok(row.retainedAt);
  await controller.action(id, "running");
  await controller.tick();
  row = controller.snapshot().rows[0];
  assert.equal(row.state, "running");
  console.log(
    "Full sandbox: controller provisioning, guest T3, idle thread creation, pairing, transcript retention, stop and resume verified.",
  );
} finally {
  if (id) {
    await controller.action(id, "deleted", { confirm: true });
    await controller.tick();
    const row = controller.snapshot().rows[0];
    if (row.state !== "deleted")
      console.log(JSON.stringify({ cleanupState: row.state, resource: row.ref, stage: row.stage }));
  }
  await controller.close();
  await rm(directory, { recursive: true, force: true });
}
