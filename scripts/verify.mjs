import assert from "node:assert/strict";
import * as Effect from "effect/Effect";
import { createRequire } from "node:module";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import plugin from "../dist/plugin.mjs";
const directory = await mkdtemp(join(tmpdir(), "t3-sandbox-package-"));
try {
  const store = new Map();
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const contributions = yield* plugin.activate({
          id: "sandboxes",
          dataDirectory: join(directory, "plugins", "sandboxes"),
          secrets: {
            get: (key) => Effect.succeed(store.get(key) ?? null),
            set: (key, value) =>
              Effect.sync(() => {
                store.set(key, value);
              }),
            remove: (key) =>
              Effect.sync(() => {
                store.delete(key);
              }),
          },
        });
        const settings = yield* contributions.views.find((x) => x.id === "settings").read();
        if (process.env.T3_HOST_CHECKOUT) {
          const { PluginView } = yield* Effect.promise(
            () => import(process.env.T3_HOST_CHECKOUT + "/packages/plugin-sdk/dist/index.js"),
          );
          const Schema = yield* Effect.promise(
            () =>
              import(
                createRequire(
                  process.env.T3_HOST_CHECKOUT + "/packages/plugin-sdk/package.json",
                ).resolve("effect/Schema")
              ),
          );
          Schema.decodeUnknownSync(PluginView)(settings);
        }
        assert.equal(settings.title, "Sandboxes");
        assert.ok(settings.blocks.some((x) => x.id === "create"));
        assert.equal(contributions.commands.find((x) => x.id === "delete").agent, false);
        console.log(
          "Packaged plugin: controller activation, settings, and agent permissions verified.",
        );
      }),
    ),
  );
} finally {
  await rm(directory, { recursive: true, force: true });
}
