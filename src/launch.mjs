import { isAbsolute } from "node:path";
import { realpath } from "node:fs/promises";
import { command } from "./io.mjs";
export async function launchProject(input) {
  if (typeof input.projectDirectory !== "string" || !isAbsolute(input.projectDirectory))
    throw new Error("Select a Git project on the controller first.");
  const directory = await realpath(input.projectDirectory);
  const repository = await command("git", ["-C", directory, "remote", "get-url", "origin"]);
  if (repository.startsWith("https://")) {
    const url = new URL(repository);
    if (url.username || url.password)
      throw new Error("Use a Git remote without embedded credentials.");
  }
  const base =
    typeof input.base === "string" && input.base
      ? input.base
      : await command("git", ["-C", directory, "rev-parse", "--verify", "HEAD"]);
  if (!/^[a-f0-9]{40}$/.test(base)) throw new Error("Select a full pushed commit SHA.");
  return { directory, repository, base };
}
export async function validatePushedProject(project) {
  await command("git", ["-C", project.directory, "fetch", "origin"]);
  const refs = await command("git", [
    "-C",
    project.directory,
    "for-each-ref",
    "--contains",
    project.base,
    "--format=%(refname)",
    "refs/remotes/origin",
  ]);
  if (!refs) throw new Error("Push this base commit before starting a sandbox.");
}

export async function launchSandbox(controller, input) {
  if (!/^[a-f0-9-]{36}$/.test(input.requestId ?? "")) throw new Error("Invalid launch request id");
  if (!controller.snapshot().rows.some((row) => row.id === input.requestId)) {
    if (input.hasAttachments) throw new Error("Use a text-only first message.");
    const project = await launchProject(input);
    await validatePushedProject(project);
    const modelSelection =
      typeof input.modelSelection === "string" && input.modelSelection
        ? JSON.parse(input.modelSelection)
        : input.modelSelection;
    await controller.create({ ...input, ...project, modelSelection });
  }
  return { $t3: "environment-launch", requestId: input.requestId };
}
