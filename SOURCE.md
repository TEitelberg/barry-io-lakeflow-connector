# Beta-framework targeting note

This connector implements Databricks' **Lakeflow Community Connectors**
framework, which is currently **Beta**. Beta means the `LakeflowConnect`
interface — method names, signatures, and the exact return shapes — can change
between template revisions without a stability guarantee.

## What this targets

- **Framework / template:** `databrickslabs/lakeflow-community-connectors`
  (the public GitHub template referenced from
  <https://docs.databricks.com/aws/en/ingestion/community-build>).
- **Interface revision assumed:** `2025.x` (Beta) — the `LakeflowConnect` class
  with `list_tables`, `get_table_schema`, `read_table_metadata`, `read_table`,
  and `read_table_deletes`.
- **Connector ingestion model:** `snapshot` (direct query). No CDC, no delete
  stream.

## Why the HTTP code is isolated

All BARRY.IO networking lives in the `BarryClient` class in
`barry_io_lakeflow.py`. The `LakeflowConnect` subclass is a thin adapter
over it. **If an upstream signature changes, you edit only the adapter methods —
never `BarryClient`.** The BARRY.IO serving API contract is fixed and
independent of the Beta framework.

## The 2-3 spots to adjust if the upstream signatures differ

When you drop this connector into a newer template and the generic test suite or
the pipeline complains about a signature/return mismatch, these are the only
places to touch:

1. **`read_table_metadata(self, table) -> dict`** — the return *shape*.
   We return:
   ```python
   {
       "primary_keys": [...],
       "cursor_fields": [],          # snapshot -> no incremental cursor
       "ingestion_type": "snapshot",
       "supports_deletes": False,
   }
   ```
   Some template revisions expect different keys (e.g. `keys` instead of
   `primary_keys`, or a typed metadata object instead of a plain dict). Re-map
   the dict here to match the revision's documented contract. The underlying
   data comes from `self.client.get_metadata(table)` and does not change.

2. **`read_table(self, table, offset=None)`** — generator vs. return-an-offset.
   We implement it as a pure generator that yields row dicts (snapshot: a fresh
   full read every call; `offset` is accepted but ignored). If the running
   revision requires the method to **return** an end/cursor offset (e.g. a
   `(rows_iterator, next_offset)` tuple, or to set offset state on `self`),
   wrap the generator accordingly. Because this is snapshot ingestion, any
   returned offset can be a trivial/constant sentinel.

3. **`read_table_deletes(self, table, offset=None)`** — graceful-empty vs. raise.
   We return an empty iterator (`iter(())`) so a pipeline that probes for
   deletes degrades gracefully, and report `supports_deletes = False` in
   metadata. If the running revision instead expects an unsupported delete
   capture to **raise**, change the body to
   `raise NotImplementedError("BARRY.IO Lakeflow serving is snapshot-only; no delete stream.")`.

A possible 4th spot: **`__init__(self, options)`** — the options/parameter
container. We read a plain `dict` (`base_url`, `token`, optional `verify_ssl`).
If a revision passes a typed connection/options object instead of a `dict`,
adapt the attribute access at the top of `__init__`; everything downstream
(`BarryClient`) is unaffected.

## Verifying against a specific template revision

1. Clone the template at the revision you intend to deploy.
2. Diff its `LakeflowConnect` base/abstract class against the methods here.
3. If only names/return-shapes differ, fix the 2-3 spots above and re-run
   `pytest` (the unit tests mock HTTP, so they stay valid).
4. Run the template's own generic/conformance test suite against this connector.
