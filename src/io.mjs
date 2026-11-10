import { open, rename, mkdir } from "node:fs/promises";
import { dirname } from "node:path";
import { randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
export async function atomic(file, value) {
  await mkdir(dirname(file), { recursive: true, mode: 0o700 });
  const temp = `${file}.${randomUUID()}`;
  const handle = await open(temp, "wx", 0o600);
  try {
    await handle.writeFile(JSON.stringify(value));
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temp, file);
  const dir = await open(dirname(file), "r");
  try {
    await dir.sync();
  } finally {
    await dir.close();
  }
}
export function command(executable, args, { input, timeout = 120000, signal } = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(executable, args, {
      stdio: ["pipe", "pipe", "pipe"],
      signal,
      timeout,
      env: { ...process.env, BOAT_NO_UPDATE: "1" },
    });
    const chunks = [];
    let size = 0,
      overflow = false;
    child.stdout.on("data", (chunk) => {
      size += chunk.length;
      if (size <= 8 * 1024 * 1024) chunks.push(chunk);
      else {
        overflow = true;
        child.kill();
      }
    });
    child.stderr.resume();
    child.on("error", () => reject(new Error(`${executable} could not start`)));
    child.on("close", (code) =>
      code === 0 && !overflow
        ? resolve(Buffer.concat(chunks).toString().trim())
        : reject(new Error(`${executable} failed (${code ?? "terminated"})`)),
    );
    child.stdin.on("error", () => {});
    child.stdin.end(input);
  });
}
export const quote = (value) => "'" + String(value).replaceAll("'", "'\\''") + "'";
export function safeUrl(value) {
  const u = new URL(value);
  if (!["https:", "http:"].includes(u.protocol) || u.username || u.password || u.search || u.hash)
    throw new Error("Use an HTTP(S) origin without credentials");
  return u.origin;
}
