export function workspaceSafetyScript(directory) {
  return `(${inspect.toString()})(${JSON.stringify(directory)})`;
}
function inspect(directory) {
  const { execFileSync: run } = require("node:child_process");
  const git = (path, args) =>
    run("git", ["-C", path, ...args], {
      encoding: "utf8",
      stdio: ["ignore", "pipe", "ignore"],
    }).trim();
  git(directory, ["fetch", "--all", "--prune"]);
  const paths = git(directory, ["worktree", "list", "--porcelain", "-z"])
    .split("\0")
    .filter((line) => line.startsWith("worktree "))
    .map((line) => line.slice(9));
  process.stdout.write(
    paths.length > 0 &&
      paths.every(
        (path) =>
          !git(path, ["status", "--porcelain"]) &&
          git(path, ["rev-list", "--count", "HEAD", "--not", "--remotes"]) === "0",
      )
      ? "safe"
      : "unsafe",
  );
}
