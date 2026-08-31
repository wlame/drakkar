# Operator Docs Site: Your Runbook Inside the UI

Your deployment already has documentation — a runbook, an architecture page,
a "what does this label mean" note. `ui.docs` lets the worker **serve that
documentation itself**, at `/docs/`, and link into it from the exact UI
surfaces it explains: a label chip, a sink row, a timeline event type, a
[declared page](ui-pages.md).

```yaml
ui:
  docs:
    site_dir: /srv/operator-docs      # any prebuilt static site
    title: "Ranking Pipeline Docs"    # nav-entry label
    anchors:
      - match: {label: module, value: vendor}
        path: architecture/scanners/#vendor-scanning
        title: "Vendor scanning"
```

Two things follow from that block. The site appears as a nav entry in the
operator UI, opening `/docs/` in a new tab. And every label chip whose
`module` is `vendor` grows a small book icon that opens
`architecture/scanners/#vendor-scanning` in a side drawer, next to the live
view rather than replacing it.

The worker serves the *built* site — it never builds anything. An mkdocs
`site/`, a Sphinx `_build/html/`, a directory of hand-written HTML: if a
static file server could serve it, so can the worker.

---

## Configuration

### `ui.docs`

| Field | Type | Default | Description |
|---|---|---|---|
| `site_dir` | `str` | `''` | Directory of a prebuilt static site served at `/docs/`. Empty disables the feature. |
| `title` | `str` | `Docs` | Nav-entry label, and the drawer heading for an anchor that declares no `title` of its own. |
| `anchors` | `list` | `[]` | Contextual links from UI surfaces into the site; at most 200. |

`site_dir` and `title` take the usual environment overrides —
`DK_UI__DOCS__SITE_DIR`, `DK_UI__DOCS__TITLE` — so one image can carry the
anchors while each environment points `site_dir` at its own staged site.
Declare `anchors` in the YAML file: like the other structured lists in
`ui`, it is not a value an environment variable can express.

### One anchor

| Field | Type | Default | Description |
|---|---|---|---|
| `match` | object | required | Which UI surface shows this anchor; **exactly one** selector. |
| `path` | `str` | required | Relative path into the site, with at most one `#fragment`. |
| `title` | `str` | `''` | Hover text on the icon and the drawer heading; falls back to `ui.docs.title`. |

`match` carries exactly one of:

| Selector | Names | Where the icon appears today |
|---|---|---|
| `label` | A task label **key** | The label chips on a task's detail page |
| `label` + `value` | One specific value of that key | The same chips, but only those carrying that value |
| `sink` | A configured sink instance name (`sinks.<type>.<name>`) | The sinks table on the dashboard |
| `event` | A declared [timeline event type](ui-timeline-events.md) name | The Live timeline's event-type list, and the hover strip of an instance |
| `page` | A [declared UI page](ui-pages.md) slug | The page's own heading |

The right-hand column describes where the current UI bundle draws the icon;
the backend only declares the binding, so a newer bundle may surface the
same anchor in more places.

**Most specific wins**: an anchor for `{label: module, value: vendor}` beats
the key-wide `{label: module}` on a vendored task, and the key-wide one
still covers every other value. The two are different matches, not
duplicates.

`path` is confined to the site: a URL scheme (`https:`, and single-colon
ones like `javascript:` too), a leading `/`, or a `..` segment all fail
config load, naming the rule they broke. So does a second anchor whose
`match` is identical to an earlier one — it could never be reached.

---

## Building the site with MkDocs

Nothing here is required — any static site works — but MkDocs is the common
case, and a few of its settings interact with the mount.

```yaml
# mkdocs.yml of your deployment's own docs
site_name: Ranking Pipeline Docs
theme:
  name: material
  font: false          # no webfont fetches; deployments are often firewalled
use_directory_urls: true   # the default
```

```bash
mkdocs build --site-dir /srv/operator-docs
```

```yaml
# drakkar.yaml
ui:
  docs:
    site_dir: /srv/operator-docs
```

- **`use_directory_urls: true`** (the default) writes each page as
  `architecture/scanners/index.html`, so anchor paths read
  `architecture/scanners/#vendor-scanning`. A directory request serves its
  `index.html`, so this works unchanged under `/docs/`. Set it to `false`
  and the same anchor becomes `architecture/scanners.html#vendor-scanning`.
- **Relative links only.** MkDocs writes relative links between pages by
  default, which survive the `/docs/` prefix. A hand-written absolute link
  to `/architecture/` does not — it leaves the mount.
- **Self-contained assets.** The site is served verbatim, so whatever it
  fetches from a CDN it will still try to fetch from the operator's browser.
  `font: false` (or an equivalent) keeps a firewalled deployment from
  rendering half-styled.
- **Headings are the anchor targets.** A `#fragment` must exist as a real
  `id` in the built HTML. With MkDocs that is the heading slug — check the
  built file, not the Markdown, when an anchor scrolls nowhere.
- **The drawer is narrow.** It opens over the live view at roughly 40% of
  the window, so a section an anchor points at reads better if it stands on
  its own rather than continuing an argument from three headings up.

---

## Anchor cookbook

**Explain a label your operators keep asking about.** One anchor for the
key covers every value:

```yaml
- match: {label: module}
  path: reference/labels/#module
  title: "What module means"
```

**Single out one value of it.** Narrower, so it wins wherever it applies:

```yaml
- match: {label: module, value: vendor}
  path: architecture/scanners/#vendor-scanning
  title: "Vendor scanning"
```

**Point a sink at its runbook** — what the database is for, who owns it,
what to do when its deliveries start failing:

```yaml
- match: {sink: archive_results_db}
  path: operations/#archive-database
```

**Document a timeline event type**, so an operator who sees an unfamiliar
band knows what produced it:

```yaml
- match: {event: scan_window_processing_range}
  path: architecture/pipeline/#scan-windows
```

**Document a declared page** you added with `ui_pages`:

```yaml
- match: {page: rankings}
  path: reference/pages/#rankings
```

### Linking into the site from your own templates

Whenever `site_dir` is set, the site is also available to every link
template as the built-in `{docs}` base — [probe-details](ui-enrichment.md#link-templates)
links, declared page widgets, and [timeline event](ui-timeline-events.md)
`link` templates alike:

```python
link_template='{docs}/reference/labels/#module'
```

`{docs}` resolves to the mount path this worker serves. It is reserved:
`ui.link_bases` may not define a base named `docs`, and config load rejects
one that tries — otherwise a deployment could silently redirect every
built-in docs link somewhere else.

---

## Serving rules

`GET /docs/` is registered **unconditionally**, whatever the configuration
says, so the path never falls through to the operator UI's catch-all and
answers with the UI shell.

| Request | Answer |
|---|---|
| A file inside the site | The file, verbatim |
| A directory (`/docs/`, `/docs/architecture/`) | That directory's `index.html` |
| A file that is not there | `404` — never the UI shell, so a wrong docs URL reads as wrong |
| A path escaping the site (`..`, an absolute path, a symlink pointing out) | `404` |
| Feature off, or the directory missing | `404` with a JSON hint naming `ui.docs.site_dir` |

`site_dir` is resolved to an absolute path once, at startup, so point it at
a path that exists by then. Whether the directory is *there* is re-checked
on every request: a site deleted or swapped out under a running worker
answers the hint-404 rather than an error page.

Symlinks are followed **and then checked**: a site whose `assets/` is a
symlink into a shared directory elsewhere on the host answers 404 for
everything under it. Stage real files inside `site_dir`.

---

## Auth and trust

The mount sits behind the same gate as the operator UI's own pages: open
when `ui.auth_token` is empty, token-required when it is set — including
the hint-404, which must not confirm the feature's state to an anonymous
caller.

!!! warning "A token-gated worker needs the token on the docs URL too"
    The operator UI opens the site as a plain same-origin URL and does not
    append its token to it, so on a worker with `ui.auth_token` set the
    drawer and the nav entry land on `401`. Reach the site with
    `/docs/?token=<token>`, or terminate auth at an ingress that injects
    the credential for the whole worker. The rest of the feature —
    anchors, icons, the drawer chrome — is unaffected.

**Treat the site's content as trusted code.** It is served from the
worker's own origin and framed inside the operator UI, so a script in it
runs with the same privileges as the UI itself, including access to the
browser storage the UI keeps its token in. This is the same posture
[`ui.custom_renderers_path`](ui-enrichment.md#custom-cell-renderers) has:
config points at deployment-owned files, and deployment-owned files are as
trusted as the config that named them. Do not point `site_dir` at a
directory anyone outside the deployment can write to.

---

## Degradation

The whole feature is optional at every step, and nothing about it can stop
a worker:

- **No `site_dir`** (the default): no nav entry, no icons, nothing on the
  identity payload. `/docs/` still answers — with the hint-404.
- **An anchor naming something that does not exist**: config load accepts
  it — the cross-check needs the whole deployment, so it runs at startup —
  and startup logs one warning per unresolved anchor — `docs_anchor_unknown_sink`, `docs_anchor_unknown_event`,
  `docs_anchor_unknown_page` — naming the target and where to declare it.
  The anchor renders nothing. **Label anchors are not checked**: label keys
  come from runtime task labels, not from config, so there is no declared
  set to check them against.
- **A `path` that names a file the site does not have**: the mount answers
  404 (never the UI shell), and the drawer shows that. A `#fragment` with no
  matching `id` is quieter still — the page opens, scrolled to the top — so
  verify fragments against the built HTML.
- **An older UI bundle** that predates the feature ignores the identity key
  and renders as it always did.

---

## Swagger moved to `/api-docs`

`/docs` used to serve the built-in Swagger UI over the worker's OpenAPI
document. That path now belongs to the operator docs site, and **Swagger
moved to `GET /api-docs`** (its assets to `/api-docs/swagger-ui-bundle.js`
and `/api-docs/swagger-ui.css`). The document itself is unchanged and still
served at `GET /api/v1/openapi.json`.

This is a breaking change for anything bookmarking or scripting `/docs` as
the API docs — update those to `/api-docs`. Nothing in the operator UI
linked the old path.

---

## In the integration harness

The [integration harness](integration.md) runs the whole feature: a
deliberately hand-written site under `integration/worker/docs-site/`
(three pages and a stylesheet — not an mkdocs build, to show that any
static directory works), wired up in `integration/worker/drakkar.yaml`'s
`ui.docs` block with four anchors covering a label key, one specific label
value, a sink instance, and a timeline event type. The repository root is
already mounted read-only inside the workers, so `site_dir` points straight
at it and `docker compose up` is the whole setup.

---

## See also

- [UI Enrichment](ui-enrichment.md#link-templates) for `ui.link_bases` and
  the link templates the built-in `{docs}` base plugs into.
- [Custom Timeline Events](ui-timeline-events.md) for the event types an
  `event` anchor can name.
- [Declared UI Pages](ui-pages.md) for the page slugs a `page` anchor can
  name.
- [Observability — Operator UI](observability.md#operator-ui) for the rest
  of the UI the anchors are scattered across.
- [Security Posture](security.md) for what the UI exposes and what
  `ui.auth_token` does and does not cover.
