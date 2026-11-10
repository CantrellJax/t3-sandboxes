import { mkdtemp, cp, writeFile, rm, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { command } from "../src/io.mjs";
const checkout = process.env.T3_HOST_CHECKOUT;
if (!checkout) throw new Error("Set T3_HOST_CHECKOUT");
const image = process.env.T3_DOCKER_IMAGE ?? "localhost/t3-sandbox:dev";
const root = await mkdtemp(join(tmpdir(), "t3-sandbox-image-"));
try {
  const revision = await command("git", ["-C", checkout, "rev-parse", "HEAD"]);
  await cp(join(checkout, "apps/server/dist"), join(root, "host"), { recursive: true });
  await cp(join(checkout, "apps/web/dist"), join(root, "host", "client"), { recursive: true });
  const { selectCliRuntimeExternalDependencies } = await import(
    pathToFileURL(join(checkout, "scripts/lib/cli-external-packages.ts"))
  );
  const server = JSON.parse(await readFile(join(checkout, "apps/server/package.json"), "utf8"));
  const dependencies = { effect: "4.1.2" };
  for (const name of Object.keys(selectCliRuntimeExternalDependencies(server.dependencies)))
    dependencies[name] = JSON.parse(
      await readFile(join(checkout, "apps/server/node_modules", name, "package.json"), "utf8"),
    ).version;
  await writeFile(join(root, "package.json"), JSON.stringify({ private: true, dependencies }));
  await writeFile(
    join(root, "Dockerfile"),
    `FROM docker.io/library/node:24\nLABEL org.opencontainers.image.revision="${revision}"\nWORKDIR /opt/t3\nCOPY package.json ./package.json\nRUN npm install --omit=dev --no-audit --no-fund\nCOPY host ./dist\nRUN printf '#!/bin/sh\\nexec node /opt/t3/dist/bin.mjs "$@"\\n' > /usr/local/bin/t3 && chmod +x /usr/local/bin/t3\nCMD ["sleep","infinity"]\n`,
  );
  const build = spawn("docker", ["build", "-t", image, root], { stdio: "inherit" });
  const [code] = await once(build, "exit");
  if (code !== 0) throw new Error("Image build failed");
  console.log(`Built ${image} from host ${revision}.`);
} finally {
  await rm(root, { recursive: true, force: true });
}
