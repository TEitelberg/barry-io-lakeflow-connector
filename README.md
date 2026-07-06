# barry-io-lakeflow — BARRY.IO Lakeflow Community Connector

A Databricks **Lakeflow Community Connector** (Beta, Python) that exposes a
**BARRY.IO Integration Runtime** transport group as a pull source. It runs inside
a Lakeflow ingestion pipeline in your Databricks workspace, calls the BARRY.IO
server's HTTP serving API, and yields rows into Unity Catalog.

- **Ingestion model:** snapshot / direct query only. Every read is a fresh full
  read. **No CDC, no delete stream.**
- **One token == one group == many tables.** A BARRY.IO admin issues a
  per-**group** access token; `list_tables()` returns every Lakeflow table in the
  group and one Databricks pipeline ingests them all.

> The Lakeflow Community Connectors framework is **Beta** — read `SOURCE.md` for
> the template revision this targets and the 2-3 spots to adjust if the upstream
> interface drifts.

---

## Layout

```
.
├── barry_io_lakeflow.py       # BarryClient (HTTP) + LakeflowConnect (glue)
├── connector_spec.yaml        # framework spec: connection params (static credential)
├── tools/
│   └── build_artifact.py      # build → dist/barry-io-lakeflow/barry_io_lakeflow.py
├── tests/
│   └── test_connector.py      # unit tests, HTTP fully mocked (no network)
├── .github/workflows/         # CI (tests) + Release (build + attach artifact on tag)
├── requirements.txt
├── pyproject.toml
├── LICENSE  ·  CODEOWNERS  ·  RELEASE_NOTES.md
├── README.md                  # this file
├── SOURCE.md                  # Beta-framework targeting note
└── .gitignore
```

The HTTP logic (`BarryClient`) is deliberately separated from the
`LakeflowConnect` glue so Beta-framework signature tweaks never touch the
networking code.

---

## Local development

Requires Python 3.10+.

```bash
python -m venv .venv
# Windows PowerShell:  .venv\Scripts\Activate.ps1
# Linux/macOS:         source .venv/bin/activate

pip install -r requirements.txt
```

### Run the tests

The tests mock the HTTP layer with `unittest.mock` (no live network) and build a
real Arrow IPC body with pyarrow for the read test. The Spark type-mapping tests
are skipped automatically if `pyspark` is not installed.

```bash
pytest
# or explicitly:
python -m pytest tests -v
```

### Syntax sanity check

```bash
python -m py_compile barry_io_lakeflow.py
```

---

## Connection parameters → Databricks Unity Catalog connection

The connector reads its parameters from the `options` dict the Lakeflow
framework passes to `LakeflowConnect.__init__`. These come from the **Unity
Catalog connection** you create in the pipeline wizard (UC holds the secret —
never hardcode the token):

| Option        | Required | Description                                                        |
|---------------|----------|--------------------------------------------------------------------|
| `base_url`    | yes      | BARRY.IO server origin, e.g. `https://barry.example.com`.           |
| `token`       | yes      | Per-**group** access token (sent as `Authorization: Bearer`). Scopes to one transport group == many tables. |
| `verify_ssl`  | no       | `true` (default) / `false`. Set `false` only for dev self-signed certs. Accepts bool or string. |

Ask your BARRY.IO administrator to issue the token from the group's **Lakeflow
access** card; they will give you the `base_url` and the `token`.

Create the connection in the Databricks UI (pipeline wizard → **+ Create
connection**, or Catalog → External data → Connections):

1. **Auth Type:** `USES_ANY_STATIC_CREDENTIAL`. BARRY.IO authenticates with a
   static, admin-issued bearer token — do **not** pick `USES_OAUTH_M2M`; the
   BARRY.IO server exposes no OAuth token endpoint, so the Client id / Client
   secret / Token endpoint fields cannot be filled.
2. **Connection name:** your choice, e.g. `barry_io_finance`.
3. **Options:** the parameters from the table above, with exactly those key
   names — `base_url`, `token`, and optionally `verify_ssl`. If the dialog shows
   named fields (rendered from `connector_spec.yaml`), fill those; otherwise add
   them as **Additional Options** key/value rows.

Alternatively, the framework CLI creates the same connection from
`connector_spec.yaml` (it reads the static-credential auth mode from the spec):

```bash
community-connector create_connection barry_io <CONNECTION_NAME> \
  -o '{"base_url": "https://barry.example.com", "token": "<group token>"}'
```

Unity Catalog stores the connection credentials encrypted; the token is never
logged or hardcoded into the pipeline definition.

---

## Build the deployment artifact

The connector is a single, self-contained module (only the standard library +
`requests` / `pyarrow` / `pyspark`, all provided by the Databricks runtime, and
**no local imports**). A reproducible build script emits the deployable file:

```bash
python tools/build_artifact.py
# -> dist/barry-io-lakeflow/barry_io_lakeflow.py
```

The output `dist/barry-io-lakeflow/barry_io_lakeflow.py` is the **single-file
deployment artifact** to upload to Databricks. It is deterministic (no
timestamps); `dist/` is git-ignored — re-run the script to regenerate. (Tagging a
`v*` release runs the same build in CI and attaches the artifact to the GitHub
release.)

**Upload to Databricks:** add it as a community connector in your workspace
(Lakeflow Connect → add a community connector), then create the Unity Catalog
connection (above) and an ingestion pipeline that uses it. Workspace-admin
approval is required to enable community connectors.

---

## Build / merge / deploy ritual (community-connector framework)

The Lakeflow Community Connectors framework expects a connector to be developed
from its GitHub template and shipped as a **single-file artifact**. The general
flow (consult the template's own README for the exact, current commands — it is
Beta and the tooling evolves):

1. **Clone the template.**
   ```bash
   git clone https://github.com/databrickslabs/lakeflow-community-connectors.git
   cd lakeflow-community-connectors
   ```

2. **Drop in this connector.** Copy `barry_io_lakeflow.py` (and the `tests/` for
   local verification) into the location the template designates for a custom
   connector (typically a per-connector folder). Keep `BarryClient` and
   `LakeflowConnect` as they are.

3. **Reconcile the interface.** Diff this connector's `LakeflowConnect` methods
   against the template revision's base class. If signatures/return shapes differ,
   fix only the 2-3 spots called out in `SOURCE.md`, then re-run the unit tests.

4. **Run the tests** — both this connector's unit tests and the template's
   generic/conformance suite.
   ```bash
   pytest                         # unit tests (mocked HTTP)
   # plus the template's own conformance/generic suite per its README
   ```

5. **Merge to the single-file artifact.** Use `python tools/build_artifact.py`
   (or the template's own merge step) to produce the deployable single file.

6. **Deploy to the workspace.** Upload/register the artifact in the target
   Databricks workspace (workspace-admin approval is required to enable community
   connectors), then create the UC connection (above) and a Lakeflow ingestion
   pipeline that uses it. Run the pipeline and verify rows land in the target
   Unity Catalog table.

7. **Version + release notes.** Bump `version` in `pyproject.toml` and
   `__version__` in `barry_io_lakeflow.py` together, and ship release notes with
   each artifact (tag `vX.Y.Z`).

---

## How it maps to the serving API

| `LakeflowConnect` method | Endpoint                                            |
|--------------------------|-----------------------------------------------------|
| `list_tables()`          | `GET /api/lakeflow/tables`                           |
| `get_table_schema()`     | `GET /api/lakeflow/tables/{table}/schema`            |
| `read_table_metadata()`  | `GET /api/lakeflow/tables/{table}/metadata`          |
| `read_table()`           | `GET /api/lakeflow/tables/{table}/read` (Arrow IPC)  |
| `read_table_deletes()`   | — (not supported; snapshot-only)                     |

All requests carry `Authorization: Bearer <token>`. A read can take a while (the
source is read on demand), so the connector uses a generous read timeout and
streams the response. **Any auth failure is reported as HTTP 404** and surfaced as
a `BarryApiError` hinting at a token/URL problem.

Note on memory: the full Arrow payload of one table read is buffered in memory
before decoding (Arrow IPC stream readers need the complete buffer for the
schema and dictionary messages), so a table snapshot must fit in the pipeline
worker's memory.
