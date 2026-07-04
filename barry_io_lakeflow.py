"""BARRY.IO Integration Runtime — Databricks Lakeflow Community Connector.

This module implements Databricks' *Lakeflow Community Connectors* (Beta)
``LakeflowConnect`` interface so a Lakeflow ingestion pipeline running inside a
customer's Databricks workspace can pull data from the BARRY.IO Integration
Runtime's HTTP serving API and land it in Unity Catalog.

Ingestion model
---------------
**Direct query / snapshot only.** Every read performs a fresh full read of the
source table; there is no change-data-capture and no delete stream. The
connector exposes every table in a transport GROUP (the BARRY.IO access token is
scoped to a group: one token == one group == many tables). ``list_tables()``
returns the group's Lakeflow-served tables; ``read_table(table)`` fetches one.

Target template version
------------------------
This connector targets the ``databrickslabs/lakeflow-community-connectors``
template, **interface revision ``2025.x`` (Beta)**. Because the framework is
Beta, the exact method signatures and the expected return shape of
``read_table_metadata`` may drift between template revisions. See ``SOURCE.md``
for the 2-3 spots to adjust if the upstream signatures change. The BARRY.IO HTTP
logic is deliberately isolated in :class:`BarryClient` so any such glue tweaks
never touch the networking code.

BARRY.IO serving API (the contract this targets exactly)
--------------------------------------------------------
Base URL  : the BARRY.IO server origin (e.g. ``https://barry.example.com``).
Auth      : ``Authorization: Bearer <token>`` — a per-GROUP access token the
            BARRY.IO admin issues. The token scopes to one transport group (many
            tables). **Any auth failure returns HTTP 404** (the existence of a
            table is itself privileged information).

Endpoints (all relative to the base URL):
  * ``GET /api/lakeflow/tables``
        -> ``{"tables": [{"table": "Customers", "ingestion_type": "snapshot"}, ...]}``
        (every Lakeflow table in the token's group).
  * ``GET /api/lakeflow/tables/{table}/schema``
        -> ``{"table": "Customers",
               "columns": [{"name": "Id", "type": "integer", "nullable": true}, ...]}``
        where ``type`` is already a Spark type string (one of: boolean, byte,
        short, integer, long, float, double, decimal(p,s), date, timestamp,
        binary, string).
  * ``GET /api/lakeflow/tables/{table}/metadata``
        -> ``{"table": "Customers", "primary_keys": ["Id"],
               "ingestion_type": "snapshot", "supports_deletes": false}``.
  * ``GET /api/lakeflow/tables/{table}/read``
        -> an Apache Arrow IPC **stream** (Content-Type
        ``application/vnd.apache.arrow.stream``) of the table's rows. An empty
        body means zero rows. A read may be slow (the server reads the on-prem
        source live), so a generous timeout is used and the response is streamed.
"""

from __future__ import annotations

import logging
import re
from typing import Any, Dict, Iterator, List, Optional
from urllib.parse import quote

import pyarrow as pa
import pyarrow.ipc as ipc
import requests

__all__ = ["BarryClient", "LakeflowConnect", "BarryApiError"]

__version__ = "0.1.0"

logger = logging.getLogger(__name__)

# Content type the BARRY.IO ``/read`` endpoint streams.
ARROW_STREAM_CONTENT_TYPE = "application/vnd.apache.arrow.stream"

# A read can be slow: the server reads the on-prem source live. Generous, but
# bounded so a wedged pipeline does not hang forever. (connect, read) seconds.
_DEFAULT_CONNECT_TIMEOUT = 30.0
_DEFAULT_READ_TIMEOUT = 600.0  # 10 minutes


# ---------------------------------------------------------------------------
# BARRY.IO HTTP client — kept cleanly separated from the LakeflowConnect glue
# so Beta-framework signature changes never touch the networking code.
# ---------------------------------------------------------------------------


class BarryApiError(RuntimeError):
    """Raised when the BARRY.IO serving API returns an unexpected response.

    Note: per the contract an auth failure is reported by the server as HTTP
    404 (not 401/403). We surface that as a :class:`BarryApiError` with a hint
    that it may be an auth/token problem, since a 404 is otherwise unexpected
    for a token that is supposed to scope to a group of existing tables.
    """


class BarryClient:
    """Thin HTTP client for the BARRY.IO Lakeflow serving API.

    All Lakeflow-facing type mapping lives in :class:`LakeflowConnect`; this
    class only speaks HTTP + JSON + Arrow IPC. Construct it with the base URL
    and a per-group bearer token.
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        verify_ssl: bool = True,
        connect_timeout: float = _DEFAULT_CONNECT_TIMEOUT,
        read_timeout: float = _DEFAULT_READ_TIMEOUT,
        session: Optional[requests.Session] = None,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        if not token:
            raise ValueError("token is required")

        # Normalise: drop any trailing slash so we can join cleanly.
        self.base_url = base_url.rstrip("/")
        self._verify_ssl = verify_ssl
        self._timeout = (connect_timeout, read_timeout)

        self._session = session or requests.Session()
        self._session.headers.update(
            {
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "User-Agent": f"barry-io-lakeflow/{__version__}",
            }
        )
        self._session.verify = verify_ssl

    # -- low-level helpers --------------------------------------------------

    def _url(self, path: str) -> str:
        """Join ``path`` (must start with ``/``) onto the base URL."""
        return f"{self.base_url}{path}"

    @staticmethod
    def _encode_table(table: str) -> str:
        """URL-encode a table name for safe use in a path segment."""
        return quote(str(table), safe="")

    def _get_json(self, path: str) -> Dict[str, Any]:
        """GET a JSON document, translating transport/HTTP errors uniformly."""
        url = self._url(path)
        try:
            resp = self._session.get(url, timeout=self._timeout)
        except requests.RequestException as exc:  # pragma: no cover - network
            raise BarryApiError(f"GET {url} failed: {exc}") from exc

        self._raise_for_status(resp, url)
        try:
            return resp.json()
        except ValueError as exc:
            raise BarryApiError(f"GET {url} returned non-JSON body") from exc

    @staticmethod
    def _raise_for_status(resp: requests.Response, url: str) -> None:
        if resp.status_code == 404:
            # Per the contract, any auth failure is reported as 404. A scoped
            # token should always resolve to its group's tables, so a 404 almost
            # always means a bad/expired/revoked token (or a wrong base URL).
            raise BarryApiError(
                f"GET {url} returned 404 — the table was not found. "
                "This usually means the access token is invalid, expired, or "
                "revoked, or the base URL is wrong (BARRY.IO reports auth "
                "failures as 404)."
            )
        if resp.status_code >= 400:
            raise BarryApiError(
                f"GET {url} returned HTTP {resp.status_code}: "
                f"{resp.text[:500]!r}"
            )

    # -- API surface --------------------------------------------------------

    def list_tables(self) -> List[Dict[str, Any]]:
        """``GET /api/lakeflow/tables`` -> the list of table descriptors.

        One entry per Lakeflow-served table in the token's group, each
        ``{"table": str, "ingestion_type": str}``.
        """
        body = self._get_json("/api/lakeflow/tables")
        tables = body.get("tables", [])
        if not isinstance(tables, list):
            raise BarryApiError(
                "GET /api/lakeflow/tables: 'tables' was not a list"
            )
        return tables

    def get_schema(self, table: str) -> Dict[str, Any]:
        """``GET /api/lakeflow/tables/{table}/schema`` -> the schema document.

        ``{"table": str, "columns": [{"name", "type", "nullable"}, ...]}`` where
        ``type`` is already a Spark type string.
        """
        path = f"/api/lakeflow/tables/{self._encode_table(table)}/schema"
        return self._get_json(path)

    def get_metadata(self, table: str) -> Dict[str, Any]:
        """``GET /api/lakeflow/tables/{table}/metadata`` -> the metadata document.

        ``{"table", "primary_keys": [...], "ingestion_type", "supports_deletes"}``.
        """
        path = f"/api/lakeflow/tables/{self._encode_table(table)}/metadata"
        return self._get_json(path)

    def read_arrow_batches(self, table: str) -> Iterator[pa.RecordBatch]:
        """``GET /api/lakeflow/tables/{table}/read`` -> yield Arrow RecordBatches.

        The endpoint streams an Apache Arrow IPC stream. An empty body means
        zero rows (yields nothing). The HTTP response is streamed and the whole
        IPC payload is buffered before decoding — Arrow IPC stream readers need
        a seekable/whole buffer for the schema + dictionary messages, and a
        snapshot read is bounded by the source table size.
        """
        path = f"/api/lakeflow/tables/{self._encode_table(table)}/read"
        url = self._url(path)
        headers = {"Accept": ARROW_STREAM_CONTENT_TYPE}
        try:
            resp = self._session.get(
                url, timeout=self._timeout, stream=True, headers=headers
            )
        except requests.RequestException as exc:  # pragma: no cover - network
            raise BarryApiError(f"GET {url} failed: {exc}") from exc

        with resp:
            self._raise_for_status(resp, url)
            # Buffer the full IPC payload. ``resp.content`` consumes the stream.
            payload = resp.content

            if not payload:
                # Empty body == zero rows.
                logger.debug("read(%s): empty body, zero rows", table)
                return

            with ipc.open_stream(pa.BufferReader(pa.py_buffer(payload))) as reader:
                for batch in reader:
                    yield batch


# ---------------------------------------------------------------------------
# Spark type mapping
# ---------------------------------------------------------------------------

# Matches ``decimal(38,18)`` / ``decimal(19, 4)`` etc. (whitespace tolerant).
_DECIMAL_RE = re.compile(r"^\s*decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$", re.IGNORECASE)


def _spark_type_for(type_str: str) -> Any:
    """Map a BARRY.IO Spark type string to a ``pyspark.sql.types`` DataType.

    ``pyspark`` is imported lazily so this module can be imported (and unit
    tested) without a Spark installation; Databricks provides ``pyspark`` at
    pipeline runtime. Unknown / unrecognised types fall back to ``StringType``.
    """
    from pyspark.sql import types as T  # lazy import — Spark only at runtime

    # Scalar mapping. ``decimal(p,s)`` is handled separately below because it
    # carries parameters.
    scalar = {
        "boolean": T.BooleanType,
        "byte": T.ByteType,
        "short": T.ShortType,
        "integer": T.IntegerType,
        "long": T.LongType,
        "float": T.FloatType,
        "double": T.DoubleType,
        "date": T.DateType,
        "timestamp": T.TimestampType,
        "binary": T.BinaryType,
        "string": T.StringType,
    }

    if type_str is None:
        return T.StringType()

    key = type_str.strip().lower()

    decimal_match = _DECIMAL_RE.match(key)
    if decimal_match:
        precision = int(decimal_match.group(1))
        scale = int(decimal_match.group(2))
        return T.DecimalType(precision, scale)

    factory = scalar.get(key)
    if factory is not None:
        return factory()

    logger.warning("Unknown Spark type %r — falling back to StringType", type_str)
    return T.StringType()


# ---------------------------------------------------------------------------
# LakeflowConnect — the Beta-framework glue.
# ---------------------------------------------------------------------------


class LakeflowConnect:
    """BARRY.IO implementation of Databricks' ``LakeflowConnect`` interface.

    The Lakeflow Community Connectors framework (Beta) instantiates this class
    with the Unity Catalog connection options and drives ingestion through the
    methods below. Each method is a thin adapter over :class:`BarryClient`.

    Connection ``options`` (passed to :meth:`__init__`):
      * ``base_url`` (required) — the BARRY.IO server origin.
      * ``token`` (required) — a per-group BARRY.IO access token.
      * ``verify_ssl`` (optional, default ``True``) — set ``False`` only for
        dev servers with self-signed certs. Accepts a bool or the strings
        ``"true"``/``"false"`` (UC connection options are often strings).
    """

    def __init__(self, options: Dict[str, Any]) -> None:
        """Read connection params from ``options`` and build the HTTP client.

        Raises ``ValueError`` if ``base_url`` or ``token`` is missing.
        """
        if options is None:
            raise ValueError("options is required")

        base_url = options.get("base_url")
        token = options.get("token")
        if not base_url:
            raise ValueError("Missing required connection option: 'base_url'")
        if not token:
            raise ValueError("Missing required connection option: 'token'")

        verify_ssl = _coerce_bool(options.get("verify_ssl", True))

        self._options = dict(options)
        self.client = BarryClient(base_url, token, verify_ssl=verify_ssl)

    # -- discovery ----------------------------------------------------------

    def list_tables(self) -> List[str]:
        """Return the table names the connector exposes.

        BARRY.IO returns every Lakeflow-served table in the token's transport
        group — one Databricks pipeline ingests them all.
        """
        descriptors = self.client.list_tables()
        return [d["table"] for d in descriptors if "table" in d]

    def get_table_schema(self, table: str) -> Any:
        """Return a ``pyspark.sql.types.StructType`` for ``table``.

        Fetches the schema document and maps each column's Spark type string to
        a concrete ``DataType`` via :func:`_spark_type_for`.
        """
        from pyspark.sql import types as T  # lazy import — Spark only at runtime

        doc = self.client.get_schema(table)
        fields = []
        for col in doc.get("columns", []):
            name = col["name"]
            data_type = _spark_type_for(col.get("type"))
            nullable = bool(col.get("nullable", True))
            fields.append(T.StructField(name, data_type, nullable))
        return T.StructType(fields)

    def read_table_metadata(self, table: str) -> Dict[str, Any]:
        """Return table metadata in the framework's expected shape.

        BARRY.IO is snapshot-only, so there are no cursor/sequence fields.
        Returned shape (documented assumption — adjust if the upstream Beta
        template expects different keys; see ``SOURCE.md``)::

            {
                "primary_keys": [...],     # from the source's key columns
                "cursor_fields": [],       # snapshot ingestion -> none
                "ingestion_type": "snapshot",
                "supports_deletes": False,
            }
        """
        doc = self.client.get_metadata(table)
        return {
            "primary_keys": doc.get("primary_keys", []),
            "cursor_fields": [],  # snapshot: no incremental cursor
            "ingestion_type": doc.get("ingestion_type", "snapshot"),
            "supports_deletes": bool(doc.get("supports_deletes", False)),
        }

    # -- read ---------------------------------------------------------------

    def read_table(
        self, table: str, offset: Optional[Any] = None
    ) -> Iterator[Dict[str, Any]]:
        """Yield rows of ``table`` as plain Python dicts (snapshot read).

        Snapshot semantics: every call performs a **fresh full read** of the
        source, regardless of ``offset`` (there is no incremental cursor). The
        Arrow IPC stream is decoded with pyarrow and each batch is converted to
        dicts via ``RecordBatch.to_pylist()``. An empty body yields nothing.

        The framework tracks progress via an opaque offset; since the read is
        always a full snapshot, the offset is trivial. If the running template
        revision expects this method to *return* an end offset rather than be a
        pure generator, see ``SOURCE.md`` for the one-line adjustment.
        """
        for batch in self.client.read_arrow_batches(table):
            for row in batch.to_pylist():
                yield row

    def read_table_deletes(
        self, table: str, offset: Optional[Any] = None
    ) -> Iterator[Dict[str, Any]]:
        """Delete stream — **not supported** (snapshot-only, no CDC).

        BARRY.IO's Lakeflow serving does direct queries only; it does not track
        deletes. Per the framework convention for a source that cannot produce a
        delete stream, this yields nothing (an empty iterator) rather than
        raising, so a pipeline that probes for deletes degrades gracefully. The
        accompanying metadata reports ``supports_deletes = False``.

        If the running template revision instead requires this method to raise
        for unsupported delete capture, replace the ``return`` with
        ``raise NotImplementedError(...)`` (see ``SOURCE.md``).
        """
        return iter(())  # no deletes in snapshot mode


# ---------------------------------------------------------------------------
# small helpers
# ---------------------------------------------------------------------------


def _coerce_bool(value: Any) -> bool:
    """Coerce a UC connection option (often a string) to a bool.

    ``"false"``/``"0"``/``"no"`` (case-insensitive) -> ``False``; everything
    else truthy -> ``True``.
    """
    if isinstance(value, bool):
        return value
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() not in {"false", "0", "no", "off", ""}
    return bool(value)
