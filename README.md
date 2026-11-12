# Sandboxes for T3 Code

An external plugin for the host on [`t3/create-plugin-system`](https://github.com/djgilcrease/t3code/tree/t3/create-plugin-system). The controller lives on your always-on T3 environment. Each sandbox runs its own T3 environment and this plugin in reporter mode. Your clients connect directly to the sandbox.

## Install and start

Install `https://github.com/digitalxero/t3-sandboxes` through **Settings → Plugins** on the controller. The host selects the latest stable tag. This requires the [plugin-system host branch](https://github.com/djgilcrease/t3code/tree/t3/create-plugin-system), at least commit `e2d0e6ea87`; upstream T3 Code does not include this host yet.

Use Node 24 or later. Run `npm ci`, then `task verify`. Register `dist/plugin.mjs` as the `sandboxes` plugin in `plugins.json` under the controller’s T3 home. A tagged Git repository can also be installed through Plugins settings; the committed bundle is ready to run without executing a build during install. Do not point a development server at your live T3 home.

In Settings → Extensions → Sandboxes, save a provider profile. Docker profiles take an image name; Boat profiles take a saved snapshot and optionally a Boat environment that supplies Git and agent credentials. The image must include Bash, Git, Node 24, and the **same plugin host build** as your controller, with `t3` on PATH. No subscription credentials are copied from your home directory.

From a new thread in a Git project on the controller, choose **New sandbox** in the environment picker. Choose a provider profile, review the pushed base commit and first message, then send. The first message currently supports text only; remove attachments and local context first. Provisioning and the first send run on the controller, so locking your phone does not cancel them. The sandbox uses its own thread branch and runs the project's normal worktree setup.

Every connected client discovers the running environment and obtains its own one-time pairing token. The initiating client opens the new thread after registering the connection. A stopped sandbox stays in the connection list for cached reads; deletion removes only connections owned by this plugin. Manually added environments are never replaced. If automatic pairing fails, use Pair client in the extension page and open the one-time URL.

The extension page also accepts a Git remote, its full **pushed** base commit SHA, a title, and an optional first message. Docker's default loopback URL only works on the Docker host; remote clients skip that address. Boat exposes HTTPS guarded by T3 bearer authentication. Set a reachable URL override for a tailnet route.

The guest contributes a small status item to the thread info pane and sidebar on web, desktop, and mobile. For Stop controls in the guest, configure the controller's reachable origin. Otherwise manage it through the controller's settings page.

## Lifecycle and cost

The default idle stop is 15 minutes; pending questions get 60 minutes; unattended activity gets a two-hour provider lease; a continuous run stops at 12 hours. Connected clients do not count as activity. All foreground threads, subagents and pending background work count. Terminal input resets the idle clock. Archived threads remain in transcript exports. Keep awake has an explicit expiry; Pin prevents automatic deletion. Stop and Resume preserve the sandbox identity. Create retries reuse a persisted request id and provider name. A lost create response is reconciled before another create is considered. Failed operations back off; five failed provisioning attempts switch to stopping instead of provisioning forever.

Before a controlled stop, the guest attempts to pause turns and checkpoint them; the controller exports each complete thread transcript to `transcripts/<sandbox-id>/` in its private plugin data directory. Export failure blocks a controlled stop; the provider's independent lease can still expire. Stopping preserves provider disk. Deletion after seven days only proceeds when transcripts were retained and all Git worktrees were verified clean and pushed, or Rehome retained it. Deleting the last thread starts the same safety check immediately. Otherwise it stays stopped. Explicit Delete requires a separate data-loss checkbox. Provider-side retention still applies independently.

Boat keeps an independent stop deadline. Docker has no independent provider deadline, so its idle and lifetime limits require the controller to remain online. Usage shows runtime and estimated cost at the hourly rate, or the provider quote, each sandbox captured when it was created; replacing a profile never reprices existing rows. The monthly budget refuses creation and resume at the estimated cap; setting a budget without a rate or a fresh quote refuses creation. Provider invoices remain authoritative.

Disable and Remove are guarded while managed resources remain. Stop and delete those resources first. On removal, transcripts are copied into `sandbox-archives` under the T3 home before the host clears plugin data. A crash or manual configuration edit still leaves the provider lease as the fallback for Boat.

## Optional Rehome

Rehome is a bonus, not a dependency. Install Rehome on the controller and include its packaged plugin in your guest image. Set the profile's `rehomePath` to that file's absolute guest path. Bootstrap enables it and supplies a per-sandbox transfer key. Before stopping, Sandboxes discovers Rehome's agent-enabled `pull` command, runs it on the controller, and waits through its `status` command. The source is `{url, key}`. The destination must contain a clone of the repository, as required by Rehome. If it is absent or fails, the controller retains transcripts and stops normally; workspace files remain in the stopped provider disk.

## Other providers

An external driver is either an executable receiving `{operation,input}` on stdin, or a JSON POST endpoint with a plugin-held bearer key. Operations are `list`, `create`, `get`, `start`, `stop`, `remove`, `setDeadline`, `exec`, `write`, and `expose`; `input` is an array of positional arguments. `list` returns observed records `{ref,name,state}`; create receives `{name,requestId,ttl}` and returns an observed record; get returns one; exec returns stdout; write takes resource id, absolute guest path, and UTF-8 contents; expose returns a credential-free origin. Create must be idempotent on requestId or name. Stop must keep disk. Do not advertise that guarantee for a provider whose stop destroys data.

### Optional price quotes

An external profile can take its price from the driver instead of a typed hourly rate: set **Price source** to "Provider quote". Drivers that do not implement `quote` keep working unchanged with a typed rate; the plugin calls `quote` only for profiles that opt in.

`quote` takes no arguments (`{"operation":"quote","input":[]}`) and returns:

```json
{
  "currency": "USD",
  "unit": "minute",
  "rounding": "up",
  "validUntil": "2026-11-11T12:15:00Z",
  "choices": [
    { "id": "small", "label": "2 vCPU", "rate": 0.001 },
    { "id": "large", "label": "8 vCPU", "rate": 0.004 }
  ]
}
```

- `currency` must be `USD`, the unit of the monthly budget. Other currencies are rejected.
- `unit` is the billing unit for `rate`: `second`, `minute` or `hour`. `rounding` is `none` (prorated) or `up` (each session is rounded up to a whole unit; a session crossing a UTC month is rounded separately on each side, and the lifetime cost is the sum of those parts). Both fields are required; other values are rejected rather than assumed.
- `choices` lists 1 to 50 resource options. `id` is 1 to 64 letters, digits, `.`, `_` or `-`, unique within the quote; `label` is an optional string; `rate` must be a JSON number from 0 to 1000. `null`, strings, booleans, arrays and non-finite values are rejected. A rate of `0` is a valid price.
- `validUntil` is an optional ISO 8601 UTC timestamp of the form `YYYY-MM-DDTHH:MM:SS[.sss]Z`. Without it a quote is valid for 15 minutes after receipt. A quote that is already expired, or valid for more than 24 hours ahead, is rejected.
- Other fields are ignored. In particular, account grants, free allowances and credits are not applied: the plugin sees only its own sandboxes, not what an account has spent elsewhere, so estimates and budgets use the quoted rate for every unit.

The profile stores the choice it was saved with (blank selects the first listed). Saving the profile requests a quote and refuses a missing choice or an unusable quote. Afterwards the settings form offers the choices returned by saved profiles' quotes. The chosen id is passed to `create` as `{name,requestId,ttl,choice}`, so the driver provisions the size that was priced; profiles without a quote send no `choice`.

Each create and resume requests a fresh quote. A failed, expired or invalid quote refuses the request, with or without a monthly budget, and the last accepted quote is never reused for a new request. A refused request changes nothing: an earlier accepted resume keeps its pending price. Resume only acts on a stopped sandbox; on one already running or starting it does nothing. Provisioning and starting run later in the background, so just before the provider `create` or `start` the worker checks the accepted quote again. If it has expired, the worker requests a fresh quote for the same choice. It uses that quote only if its currency, billing unit and rounding are identical to the accepted quote, its per-unit rate is no higher, and the budget is not yet reached. A change of unit or rounding is never accepted automatically, because a larger rounded unit can raise the minimum charge even at a lower hourly rate; otherwise, or if the refresh fails, it does not call the provider: a pending create is marked deleted with no billed time, a pending resume returns the sandbox to stopped, and the reason is shown as the sandbox's error. Request again to accept a higher price. A create whose request was already sent is reconciled by name as before and is not quoted again. The accepted quote is copied onto the sandbox for its first session and onto each resumed session, and is never changed afterwards. A session opens at the time `start` was requested, so a start whose reply is lost is still billed once the provider reports the sandbox running, even if Stop was requested in the meantime. The usage page lists each quoted profile's last accepted price, its age and validity, and the last refresh failure.

## Build a local guest image

Build the shared host's server bundle and web client first. Then run:

```sh
T3_HOST_CHECKOUT=/path/to/t3code node scripts/build-image.mjs
```

This stages only built server and web assets and the host's declared runtime dependencies; it does not copy your T3 data, credentials, or checkout. The default image name is `localhost/t3-sandbox:dev`. Install agent executables and supply their credentials through your own derived image or Boat environment. Creating an idle thread does not run a paid agent. The verification scripts use temporary T3 homes and clean up their task-owned sandboxes.

The bundled Docker image and Boat snapshot must match the controller's host build. This first release uses prebuilt images rather than installing or upgrading T3 during bootstrap. Docker and Boat are built in; other providers use the external driver. T3 Connect reach, moving an existing local thread into a sandbox, and cloning a sandbox for a fork remain later features.
