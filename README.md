# Sandboxes for T3 Code

An external plugin for the host on [`t3/create-plugin-system`](https://github.com/djgilcrease/t3code/tree/t3/create-plugin-system). The controller lives on your always-on T3 environment. Each sandbox runs its own T3 environment and this plugin in reporter mode. Your clients connect directly to the sandbox.

## Install and start

Use Node 24 or later. Run `npm ci`, then `task verify`. Register `dist/plugin.mjs` as the `sandboxes` plugin in `plugins.json` under the controller’s T3 home. A tagged Git repository can also be installed through Plugins settings; the committed bundle is ready to run without executing a build during install. Do not point a development server at your live T3 home.

In Settings → Extensions → Sandboxes, save a provider profile. Docker profiles take an image name; Boat profiles take a saved snapshot and optionally a Boat environment that supplies Git and agent credentials. The image must include Bash, Git, Node 24, and the **same plugin host build** as your controller, with `t3` on PATH. No subscription credentials are copied from your home directory.

From a new thread in a Git project on the controller, choose **New sandbox** in the environment picker. Choose a provider profile, review the pushed base commit and first message, then send. The first message currently supports text only; remove attachments and local context first. Provisioning and the first send run on the controller, so locking your phone does not cancel them. The sandbox uses its own thread branch and runs the project's normal worktree setup.

Every connected client discovers the running environment and obtains its own one-time pairing token. The initiating client opens the new thread after registering the connection. A stopped sandbox stays in the connection list for cached reads; deletion removes only connections owned by this plugin. Manually added environments are never replaced. If automatic pairing fails, use Pair client in the extension page and open the one-time URL.

The extension page also accepts a Git remote, its full **pushed** base commit SHA, a title, and an optional first message. Docker's default loopback URL only works on the Docker host; remote clients skip that address. Boat exposes HTTPS guarded by T3 bearer authentication. Set a reachable URL override for a tailnet route.

The guest contributes a small status item to the thread info pane and sidebar on web, desktop, and mobile. For Stop controls in the guest, configure the controller's reachable origin. Otherwise manage it through the controller's settings page.

## Lifecycle and cost

The default idle stop is 15 minutes; pending questions get 60 minutes; unattended activity gets a two-hour provider lease; a continuous run stops at 12 hours. Connected clients do not count as activity. All foreground threads, subagents and pending background work count. Terminal input resets the idle clock. Archived threads remain in transcript exports. Keep awake has an explicit expiry; Pin prevents automatic deletion. Stop and Resume preserve the sandbox identity. Create retries reuse a persisted request id and provider name. A lost create response is reconciled before another create is considered. Failed operations back off; five failed provisioning attempts switch to stopping instead of provisioning forever.

Before a controlled stop, the guest attempts to pause turns and checkpoint them; the controller exports each complete thread transcript to `transcripts/<sandbox-id>/` in its private plugin data directory. Export failure blocks a controlled stop; the provider's independent lease can still expire. Stopping preserves provider disk. Deletion after seven days only proceeds when transcripts were retained and all Git worktrees were verified clean and pushed, or Rehome retained it. Deleting the last thread starts the same safety check immediately. Otherwise it stays stopped. Explicit Delete requires a separate data-loss checkbox. Provider-side retention still applies independently.

Boat keeps an independent stop deadline. Docker has no independent provider deadline, so its idle and lifetime limits require the controller to remain online. Usage shows runtime and estimated cost at each profile’s configured hourly rate. The monthly budget refuses creation and resume at the estimated cap; setting a budget without a rate refuses creation. Provider invoices remain authoritative.

Disable and Remove are guarded while managed resources remain. Stop and delete those resources first. On removal, transcripts are copied into `sandbox-archives` under the T3 home before the host clears plugin data. A crash or manual configuration edit still leaves the provider lease as the fallback for Boat.

## Optional Rehome

Rehome is a bonus, not a dependency. Install Rehome on the controller and include its packaged plugin in your guest image. Set the profile's `rehomePath` to that file's absolute guest path. Bootstrap enables it and supplies a per-sandbox transfer key. Before stopping, Sandboxes discovers Rehome's agent-enabled `pull` command, runs it on the controller, and waits through its `status` command. The source is `{url, key}`. The destination must contain a clone of the repository, as required by Rehome. If it is absent or fails, the controller retains transcripts and stops normally; workspace files remain in the stopped provider disk.

## Other providers

An external driver is either an executable receiving `{operation,input}` on stdin, or a JSON POST endpoint with a plugin-held bearer key. Operations are `list`, `create`, `get`, `start`, `stop`, `remove`, `setDeadline`, `exec`, `write`, and `expose`; `input` is an array of positional arguments. `list` returns observed records `{ref,name,state}`; create receives `{name,requestId,ttl}` and returns an observed record; get returns one; exec returns stdout; write takes resource id, absolute guest path, and UTF-8 contents; expose returns a credential-free origin. Create must be idempotent on requestId or name. Stop must keep disk. Do not advertise that guarantee for a provider whose stop destroys data.

## Build a local guest image

Build the shared host's server bundle and web client first. Then run:

```sh
T3_HOST_CHECKOUT=/path/to/t3code node scripts/build-image.mjs
```

This stages only built server and web assets and the host's declared runtime dependencies; it does not copy your T3 data, credentials, or checkout. The default image name is `localhost/t3-sandbox:dev`. Install agent executables and supply their credentials through your own derived image or Boat environment. Creating an idle thread does not run a paid agent. The verification scripts use temporary T3 homes and clean up their task-owned sandboxes.

The bundled Docker image and Boat snapshot must match the controller's host build. This first release uses prebuilt images rather than installing or upgrading T3 during bootstrap. Docker and Boat are built in; other providers use the external driver. T3 Connect reach, moving an existing local thread into a sandbox, and cloning a sandbox for a fork remain later features.
