# Release notes — barry-io-lakeflow

## 0.1.0

Initial release of the BARRY.IO Databricks **Lakeflow Community Connector**.

- Implements the `LakeflowConnect` interface (Beta framework): `list_tables`,
  `get_table_schema`, `read_table_metadata`, `read_table`, `read_table_deletes`.
- **Snapshot / direct-query** ingestion — every read is a fresh full read; no CDC,
  no delete stream (`read_table_deletes` yields nothing).
- **Group-scoped access token**: one token serves every Lakeflow table in
  a BARRY.IO transport group; `list_tables()` returns them all and one Databricks
  pipeline ingests the group.
- Reads the BARRY.IO serving API over HTTPS with `Authorization: Bearer <token>`;
  decodes the Apache **Arrow IPC** stream to row dicts via pyarrow.
- Spark type mapping for the schema (incl. `decimal(p,s)`), `pyspark` imported
  lazily so the module unit-tests without Spark.

Connection options: `base_url`, `token`, optional `verify_ssl`.
