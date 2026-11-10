import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { workspaceSafetyScript } from "../src/safety.mjs";
import { command } from "../src/io.mjs";
import { launchProject, validatePushedProject, launchSandbox } from "../src/launch.mjs";

test("only commits reachable from a fetched remote branch can launch; local edits stay behind", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "sandbox-launch-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const remote = join(directory, "origin");
  const local = join(directory, "project");
  await command("git", ["init", "--bare", remote]);
  await command("git", ["clone", remote, local]);
  const git = (args) => command("git", ["-C", local, ...args]);
  await git(["config", "user.name", "Test"]);
  await git(["config", "user.email", "test@example.test"]);
  await writeFile(join(local, "file"), "pushed");
  await git(["add", "."]);
  await git(["commit", "-m", "base"]);
  await git(["push", "origin", "HEAD:main"]);
  const project = await launchProject({ projectDirectory: local });
  await validatePushedProject(project);
  const safe = () => command(process.execPath, ["-e", workspaceSafetyScript(local)]);
  assert.equal(await safe(), "safe");
  const worktree = join(directory, "other workspace");
  await git(["worktree", "add", "-b", "feature", worktree, project.base]);
  await writeFile(join(worktree, "untracked"), "work to retain");
  assert.equal(await safe(), "unsafe");
  await rm(join(worktree, "untracked"));
  assert.equal(await safe(), "safe");
  await writeFile(join(local, "file"), "uncommitted");
  assert.equal((await launchProject({ projectDirectory: local })).base, project.base);
  assert.equal(await safe(), "unsafe");
  await git(["commit", "-am", "unpushed"]);
  assert.equal(await safe(), "unsafe");
  await assert.rejects(
    validatePushedProject(await launchProject({ projectDirectory: local })),
    /Push this base/,
  );
  await assert.rejects(launchProject({ projectDirectory: "." }), /Select a Git project/);
  await git(["remote", "set-url", "origin", "https://user:secret@example.test/repo"]);
  await assert.rejects(launchProject({ projectDirectory: local }), /embedded credentials/);
});

test("retrying an accepted launch returns its receipt even if the source project is gone", async () => {
  const requestId = "11111111-1111-1111-1111-111111111111";
  const controller = {
    snapshot: () => ({ rows: [{ id: requestId }] }),
    create: () => assert.fail("Must not create twice"),
  };
  assert.deepEqual(await launchSandbox(controller, { requestId, projectDirectory: "/missing" }), {
    $t3: "environment-launch",
    requestId,
  });
  await assert.rejects(launchSandbox(controller, {}), /request id/);
});
