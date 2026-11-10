# Sandboxes for T3 Code

An external plugin for the host on [`t3/create-plugin-system`](https://github.com/djgilcrease/t3code/tree/t3/create-plugin-system). The controller lives on your always-on T3 environment. Each sandbox runs its own T3 environment and this plugin in reporter mode. Your clients connect directly to the sandbox.

## Install and start

Use Node 24 or later. Run `npm ci`, then `task verify`. Register `dist/plugin.mjs` as the `sandboxes` plugin in `plugins.json` under the controller’s T3 home. A tagged Git repository can also be installed through Plugins settings; the committed bundle is ready to run without executing a build during install. Do not point a development server at your live T3 home.

In Settings → Extensions → Sandboxes, save a provider profile. Docker profiles take an image name; Boat profiles take a saved snapshot and optionally a Boat environment that supplies Git and agent credentials. The image must include Bash, Git, Node 24, and the **same plugin host build** as your controller, with `t3` on PATH. No subscription credentials are copied from your home directory.

Create a sandbox using a Git remote, its full **pushed** base commit SHA, a title, and optionally its first message. Provisioning and the first send run on the controller, so locking your phone does not cancel them. Once Running, choose Pair client and open the one-time URL. Each device needs its own token. Docker's default loopback URL only works on the Docker host; Boat exposes HTTPS guarded by T3 bearer authentication. Set a reachable URL override for a tailnet route.

The guest contributes a small status item to the thread info pane and sidebar on web, desktop, and mobile. For Stop controls in the guest, configure the controller's reachable origin. Otherwise manage it through the controller's settings page.

## Lifecycle and cost

The default idle stop is 15 minutes; pending questions get 60 minutes; unattended activity gets a two-hour provider lease; a continuous run stops at 12 hours. Connected clients do not count as activity. All foreground threads, subagents and pending background work count. Keep awake has an explicit expiry; Pin prevents automatic deletion. Stop and Resume preserve the sandbox identity. Create retries reuse a persisted request id and provider name. A lost create response is reconciled before another create is considered.

Before a controlled stop, the guest attempts to pause turns and checkpoint them; the controller exports each complete thread transcript to `transcripts/<sandbox-id>/` in its private plugin data directory. Export failure blocks a controlled stop; the provider's independent lease can still expire. Stopping preserves provider disk. Deletion after seven days only proceeds when transcripts were retained and the checkout was verified clean and pushed, or Rehome retained it. Otherwise it stays stopped. Explicit Delete requires a separate data-loss checkbox. Provider-side retention still applies independently.

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

Creation currently starts from the Sandboxes extension page. The new-thread environment picker's direct “New sandbox” entry and automatic client connection discovery are not implemented. Pair each client through the one-time URL.
