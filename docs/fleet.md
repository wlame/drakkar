# Fleet registry

Workers of one deployment find each other through the **fleet registry**.
Every worker publishes a small **card** about itself and reads the cards of
the others. Two features use the cards:

- the **workers list** of the UI (the worker switcher, the Live page, every
  view that shows more than one worker), served at `GET /api/v1/workers`;
- the **cache exchange**, which takes each peer's cluster from the registry
  to decide whether to pull that peer's `cluster` rows or only its `global`
  rows.

The message pipeline does not use the registry. Sources, the executor,
sinks and offset commits run exactly the same with the registry switched
off, slow, or broken: no step of the pipeline waits for a registry call.

## What you get with the defaults

With nothing configured, a worker uses the **filesystem** provider and keeps
the cards in `<shared.dir>/fleet/workers/` (or `<storage.dir>/fleet/workers/`
when `shared.dir` is empty). Workers that see the same folder see each
other. The workers list shows each peer with its state, version and
liveness, and the cache exchange knows each peer's cluster.

The registry needs the UI server (`ui.enabled: true`), because peers and
browsers reach a worker through the URL on its card. With the UI off, the
registry is off and the worker logs `fleet_registry_disabled_no_ui` once.

## Providers

`fleet.registry.provider` selects where cards live:

| Provider | Where the cards are | Liveness |
|---|---|---|
| `filesystem` (default) | one JSON file per worker in a shared folder | from the file times on the file server |
| `static` | nowhere: a fixed peer list in the config | unknown |
| `none` | nowhere: no peers | — |
| a plugin name | wherever the plugin keeps them (see [Writing a provider](#writing-a-provider)) | the plugin's own clock |

With `none` the workers list shows only the current worker, and the cache
exchange treats every peer's cluster as unknown, so it pulls only `global`
rows.

### `filesystem`

```yaml
storage:
  dir: /var/lib/drakkar          # this worker's own files
shared:
  dir: /mnt/fleet                # seen by every worker of the fleet
fleet:
  registry:
    provider: filesystem         # the default
    # filesystem:
    #   dir: /mnt/fleet-registry # optional; default <shared.dir>/fleet
```

The folder is `fleet.registry.filesystem.dir` when you set it, else
`<shared.dir>/fleet`, else `<storage.dir>/fleet`. When none of these
resolves (memory-only `storage.dir: ''` and no `shared.dir`), the registry
is off and the worker logs `fleet_registry_disabled_no_dir` once.

The two ways of getting a folder fail differently:

- A folder you **set explicitly** must work: if the worker cannot create
  `<dir>/workers/` or cannot write it, **startup fails** with a message that
  names `fleet.registry.filesystem.dir`.
- A **derived** folder is best effort: if it cannot be written, the worker
  runs anyway, counts each failed publish in
  `drakkar_fleet_registry_errors_total{op="publish"}` and retries at the
  next heartbeat.

Layout and rules:

- One card per worker: `workers/<worker>.json`. Characters of the worker
  name outside `[a-zA-Z0-9_-]` become `_` in the file name, so `search.1`
  is stored as `search_1.json`. Two worker names that differ only in such
  characters share one file; give workers names that stay distinct.
- A card is written to `workers/.<worker>.json.<instance>.tmp` and renamed
  over the card, so a reader never sees half a card. A temp file that a
  crashed writer left behind is deleted once it is older than
  `prune_after_seconds`.
- The folder is created with mode 0700 and each card with mode 0600. A card
  holds no secrets; who may read the folder is decided by the folder's own
  permissions on the file server.
- The worker never follows a symbolic link in the folder. When `workers`
  itself is a link (or a file), publishing and listing fail and count as
  registry errors; nothing in the link's target is read or deleted. A link,
  folder or FIFO named like a card is skipped and logged with reason
  `not_a_file`.
- A card file that cannot be opened or read (for example its permissions
  deny this worker) is skipped with reason `unreadable`; one larger than
  4096 bytes with `too_large`; one that is not JSON or not a JSON object
  with `not_json` or `not_object`. The worker logs `fleet_card_invalid` once
  for that file name, and the other cards are still listed. Cards are
  written with mode 0600, so workers that run as different users and share
  the folder cannot read each other's cards; run them as one user.
- A card older than `prune_after_seconds` (default 86400, one day) is
  deleted by the next worker that lists the folder.
- At a clean stop the worker deletes its own card, but only while the card
  still holds this process's instance: if another process with the same
  worker name has written it since, the card stays.

### `static`

A fixed peer list, for deployments without a shared folder:

```yaml
fleet:
  registry:
    provider: static
    static:
      peers:
        - name: search-worker-1
          cluster: search
          url: http://10.0.3.17:8080/
        - name: search-worker-2
          cluster: search
          url: http://10.0.3.18:8080/
```

The worker publishes nothing. Its peers are always listed, and their
liveness is **unknown**: the workers list shows them as neither online nor
offline.

## The card

A card is a JSON object, at most 4096 bytes:

```json
{
  "backend": "python",
  "cluster": "search",
  "features": {},
  "heartbeat_at": "2026-10-13T09:40:11.000210Z",
  "instance": "9c41e0b27fd34a1e",
  "schema": 1,
  "started_at": "2026-10-13T09:12:01.123456Z",
  "state": "running",
  "url": "http://10.0.3.17:8080/",
  "version": "2.4.0",
  "worker": "search-worker-3"
}
```

| Field | Required | Meaning |
|---|---|---|
| `schema` | yes | Card format version, `1`. A card with another value is skipped and the worker logs `fleet_card_schema_unsupported` once for that worker. |
| `worker` | yes | Worker name. Not empty. |
| `cluster` | yes | The worker's cluster name, as configured (may be `""`). |
| `url` | yes | Where the worker's UI server answers: `ui.public_url` when set, else `http://<outbound ip>:<ui.port>/`. Not empty. |
| `instance` | no | 16 random hex characters, new at every process start. Tells two processes with one worker name apart. |
| `backend` | no | The implementation that runs the worker. |
| `version` | no | The worker's release, as `/api/v1/identity` reports it. |
| `state` | no | `starting`, `running` or `draining`. |
| `started_at`, `heartbeat_at` | no | Process start and the time of this publish, on the writer's clock. Informational only: liveness never uses them. |
| `features` | no | An object for optional capabilities. Unknown keys are ignored. |

A card whose required field is missing, empty (`worker`, `url`) or of the
wrong type, or whose optional field has the wrong type, is skipped; the
worker logs `fleet_card_invalid` with the reason once for that worker.
Unknown fields and unknown `state` values are kept and ignored.

The card is written with sorted keys, no spaces between tokens, non-ASCII
characters as raw UTF-8, and no trailing newline, so the same card has the
same bytes whichever worker wrote it.

## States

A worker publishes its card:

- with `starting` right after its UI server listens;
- with `running` once its sources have started;
- with `draining` as soon as shutdown begins;
- again every `heartbeat_seconds` (default 10) with the current state.

A state change is published at once, not at the next heartbeat. At the end
of a clean stop, after the recorder's last write and before the UI server
stops, the worker withdraws its card.

## Liveness

The workers list shows a peer as **online** when its card is no older than
`fleet.registry.offline_after_seconds` (default 30), and as **offline**
when it is older. `offline_after_seconds` must be at least twice
`heartbeat_seconds`, so one late heartbeat never marks a worker offline.

The age of a card is measured **on the provider's clock**, never by
comparing one host's wall clock with another's. For the filesystem provider
that clock is the file server's: right after a worker writes its own card,
it reads the card's modification time as a sample of the file server's
time, and from then on adds its own elapsed monotonic time. A peer card's
age is that estimate minus the card's modification time. Two consequences:

- Clock skew between worker hosts does not matter.
- Before a worker's first successful publish it has no sample, so it lists
  every peer with an unknown age: the workers list shows them as neither
  online nor offline until the first publish succeeds.

Each refresh (every `refresh_seconds`, default 15) keeps the age of every
card at the moment it was listed. The workers list adds the time since that
refresh, so a peer never looks younger because a refresh is slow or failed.

`last_seen_ts` in the workers list is the reading worker's current time
minus the card's age, in epoch seconds. It is `null` when the age is
unknown.

## The workers list

`GET /api/v1/workers` answers from memory, with no storage access: the
current worker plus the peers of the last refresh. The current worker is
always listed and always `online: true`. A peer row carries the fields of
its card that are present; see the UI contract for the full row shape.

## Failures

Every provider call runs under `fleet.registry.timeout_seconds` (default 5):

- A call that fails or times out keeps the last snapshot, counts one error
  in `drakkar_fleet_registry_errors_total{op}`, and logs
  `fleet_registry_publish_failed`, `fleet_registry_list_failed` or
  `fleet_registry_withdraw_failed`. Each of these logs at most once a
  minute; the line carries how many failures it stands for (`suppressed`).
- While refreshes fail, `drakkar_fleet_registry_snapshot_age_seconds` grows,
  so an alert can tell a stale workers list from an empty one.
- The filesystem provider runs its file calls on its own thread. A call
  stuck on an unresponsive network mount holds only that thread: the next
  registry call times out, and the rest of the worker does not notice.
- A provider that fails to start is logged (`fleet_registry_start_failed`)
  and replaced by `none` for the life of the process.

## Duplicate worker names

Two live processes that publish one worker name overwrite each other's
card. When a worker lists a card with its own name, another instance, and
an age within `offline_after_seconds`, it logs `fleet_duplicate_worker_name`
(with the other instance and URL) once for that instance and counts it in
`drakkar_fleet_duplicate_worker_name_total`. Give every worker a unique
name, for example from the pod name through `worker_name_env`.

## Writing a provider

A provider is a class with five async methods and a factory that builds it
from an options mapping:

```python
from drakkar.fleet.provider import PeerCard, PeerCardList, RegistryContext


class ConsulRegistry:
    def __init__(self, options):
        self._address = options['address']

    async def start(self, ctx: RegistryContext) -> None:
        """Called once, first. ctx carries the worker name, instance, cluster, logger, options."""

    async def publish(self, card: dict) -> None:
        """Store the card, replacing any card stored under the same worker name."""

    async def list(self) -> PeerCardList:
        """Every card you can read, this worker's own included, with its age
        on your own clock (None when you cannot tell)."""
        return [PeerCard(card={...}, age_seconds=12.5)]

    async def withdraw(self, worker: str, instance: str) -> None:
        """Delete the worker's card, but only while it still holds this instance."""

    async def stop(self) -> None:
        """Called once, last."""


def make_registry(options):
    return ConsulRegistry(options)
```

Register the factory as an entry point of your package:

```toml
[project.entry-points."drakkar.registry_providers"]
consul = "drakkar_consul:make_registry"
```

and select it:

```yaml
fleet:
  registry:
    provider: consul
    plugin_options:
      address: consul.service:8500
```

`plugin_options` is passed to the factory unchanged; the provider validates
it. A plugin name wins over a built-in name, so a package can replace
`filesystem`. A plugin that fails to load is logged
(`fleet_registry_entry_point_load_failed`) and skipped; startup then fails
only if the configured name is the missing one, with a message that lists
the providers that exist.

Provider methods are coroutines that run on the worker's event loop, the
same loop that runs the message pipeline. A method that blocks — file or
network I/O without `await`, a synchronous client library — stalls every
partition while it runs, and the time limit cannot interrupt it. Run
blocking work in a thread of your own (for example a one-thread
`concurrent.futures.ThreadPoolExecutor` driven with
`loop.run_in_executor`), so a hung call holds only that thread.

What the worker does, and what the provider does:

| The worker | The provider |
|---|---|
| loops, timeouts, the thread or task each call runs on | storing the cards |
| building and validating cards | the age of each card on its own clock (`None` when unknown) |
| online and offline, the snapshot, the duplicate warning | deleting old cards |

`list()` returns the caller's own card too; the worker leaves it out of the
snapshot and builds its own row from local state.

### The test kit

`drakkar.fleet.testkit` holds the checks every provider should pass. They
need no test framework and raise `AssertionError` with a plain message:

```python
import pytest

from drakkar.fleet import testkit


@pytest.mark.parametrize('check', testkit.PUBLISHING_CHECKS)
async def test_consul_registry(check, consul_store):
    await check(lambda: ConsulRegistry({'address': consul_store.address}), consul_store.advance, 600)
```

- `make_provider()` returns a new provider that shares storage with the
  others the same check builds (two of them stand for two processes).
- `advance(seconds)` makes every stored card that much older on your clock.
- The last argument is the age after which your provider deletes a card.

`testkit.check_list_shape(provider, ctx)` and
`testkit.check_age_is_none_or_non_negative(provider, ctx)` apply to every
provider, including read-only ones; `testkit.make_context(worker)` builds a
context for them.

## Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `drakkar_fleet_peers` | gauge | `state` (`online`, `offline`, `unknown`) | Peers in the last registry snapshot by liveness state (this worker not counted). |
| `drakkar_fleet_registry_errors_total` | counter | `op` (`publish`, `list`, `withdraw`) | Registry provider calls that failed or timed out. |
| `drakkar_fleet_registry_duration_seconds` | histogram | `op` | Duration of registry provider calls in seconds. |
| `drakkar_fleet_registry_snapshot_age_seconds` | gauge | — | Seconds since the last successful registry refresh; 0 right after one. |
| `drakkar_fleet_duplicate_worker_name_total` | counter | — | Other live instances seen publishing this worker's name. |

## Log events

| Event | Level | When |
|---|---|---|
| `fleet_registry_started` | info | The registry service started; names the provider, instance and URL. |
| `fleet_registry_disabled_no_ui` | warning | The UI is off, so the registry is off. |
| `fleet_registry_disabled_no_dir` | warning | The filesystem provider has no folder, so the registry is off. |
| `fleet_registry_start_failed` | error | The provider failed to start; the worker runs without peers. |
| `fleet_registry_publish_failed`, `fleet_registry_list_failed`, `fleet_registry_withdraw_failed` | warning | A provider call failed or timed out; at most once a minute per operation. |
| `fleet_registry_stop_failed` | warning | The provider failed to stop. |
| `fleet_registry_loop_failed` | error | A registry loop stopped on an unexpected error; logged at stop, before the card is withdrawn. |
| `fleet_card_invalid` | warning | A card was skipped; once per file name (a file that is unreadable, not a plain file, too large or not a JSON object) or per worker (field rules). |
| `fleet_card_schema_unsupported` | warning | A card of another format version was skipped; once per worker. |
| `fleet_duplicate_worker_name` | warning | Another live process publishes this worker's name; once per instance. |
| `fleet_registry_entry_point_scan_failed`, `fleet_registry_entry_point_load_failed`, `fleet_registry_entry_point_invalid` | warning | A provider plugin could not be found or loaded. |

## Settings

| Key | Default | Meaning |
|---|---|---|
| `fleet.registry.provider` | `filesystem` | `filesystem`, `static`, `none`, or a plugin name. |
| `fleet.registry.heartbeat_seconds` | `10` | Seconds between card publishes. |
| `fleet.registry.refresh_seconds` | `15` | Seconds between reads of the peer cards. |
| `fleet.registry.offline_after_seconds` | `30` | A card older than this is offline. At least `2 * heartbeat_seconds`. |
| `fleet.registry.prune_after_seconds` | `86400` | Cards older than this are deleted. At least `offline_after_seconds`. |
| `fleet.registry.timeout_seconds` | `5` | Time limit for one provider call. |
| `fleet.registry.filesystem.dir` | `""` | Card folder; empty means `<shared.dir>/fleet`. |
| `fleet.registry.static.peers` | `[]` | The `static` peer list: `name`, `cluster`, `url`. |
| `fleet.registry.plugin_options` | `{}` | Options for a provider plugin. |

Every key has a `DK_` environment override, for example
`DK_FLEET__REGISTRY__PROVIDER=static`. The full table with constraints is in
[Configuration](configuration.md#fleet-registry).

## What the registry does not do

- It does not authenticate anything. A card is a public statement of where
  a worker answers; anyone who can write the card folder can publish a
  card. Protect the folder like the rest of `shared.dir`.
- It is not used by the pipeline: a worker with no peers processes messages
  exactly like one in a large fleet.
- It does not carry cache data. The cache exchange takes only each peer's
  cluster from the registry (see [Cache](cache.md)).
