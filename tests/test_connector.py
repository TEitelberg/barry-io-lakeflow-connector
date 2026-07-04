"""Unit tests for the BARRY.IO Lakeflow connector.

The HTTP layer is fully mocked (``unittest.mock`` patching of
``requests.Session.get``) — **no live network**. The Arrow read test builds a
real Arrow IPC stream body with pyarrow and asserts it decodes to dicts.

Spark type-mapping assertions are guarded with ``pytest.importorskip("pyspark")``
so the rest of the suite still runs on a machine without a Spark install
(Databricks provides ``pyspark`` at pipeline runtime).
"""

from __future__ import annotations

import io
import json
from typing import Any, Dict, Optional
from unittest import mock

import pyarrow as pa
import pyarrow.ipc as ipc
import pytest

from barry_io_lakeflow import (
    LakeflowConnect,
    BarryApiError,
    BarryClient,
    _coerce_bool,
    _spark_type_for,
)

BASE_URL = "https://barry.example.com"
TOKEN = "test-token-123"


# ---------------------------------------------------------------------------
# A tiny fake ``requests.Response`` good enough for the client.
# ---------------------------------------------------------------------------


class FakeResponse:
    def __init__(
        self,
        *,
        status_code: int = 200,
        json_body: Optional[Dict[str, Any]] = None,
        content: bytes = b"",
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._json_body = json_body
        self.content = content
        self.text = text or (json.dumps(json_body) if json_body is not None else "")

    def json(self) -> Dict[str, Any]:
        if self._json_body is None:
            raise ValueError("no json body")
        return self._json_body

    # support ``with resp:`` used by the streaming read path
    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


def _make_router(routes: Dict[str, FakeResponse]):
    """Return a side_effect fn that maps a request URL suffix -> FakeResponse.

    Matching is by ``url.endswith(suffix)`` so tests can register short paths.
    """

    def _side_effect(url: str, *args: Any, **kwargs: Any) -> FakeResponse:
        for suffix, resp in routes.items():
            if url.endswith(suffix):
                return resp
        raise AssertionError(f"unexpected request URL: {url}")

    return _side_effect


def _arrow_ipc_bytes(table: pa.Table) -> bytes:
    """Serialise a pyarrow Table to an Arrow IPC *stream* byte payload."""
    sink = io.BytesIO()
    with ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue()


# ---------------------------------------------------------------------------
# BarryClient / LakeflowConnect construction
# ---------------------------------------------------------------------------


def test_client_requires_base_url_and_token():
    with pytest.raises(ValueError):
        BarryClient("", TOKEN)
    with pytest.raises(ValueError):
        BarryClient(BASE_URL, "")


def test_client_sets_bearer_header_and_strips_trailing_slash():
    client = BarryClient(BASE_URL + "/", TOKEN)
    assert client.base_url == BASE_URL  # trailing slash stripped
    assert client._session.headers["Authorization"] == f"Bearer {TOKEN}"


def test_lakeflowconnect_requires_options():
    with pytest.raises(ValueError):
        LakeflowConnect({"token": TOKEN})  # missing base_url
    with pytest.raises(ValueError):
        LakeflowConnect({"base_url": BASE_URL})  # missing token


# ---------------------------------------------------------------------------
# list_tables
# ---------------------------------------------------------------------------


def test_list_tables_returns_single_name():
    routes = {
        "/api/lakeflow/tables": FakeResponse(
            json_body={"tables": [{"table": "Customers", "ingestion_type": "snapshot"}]}
        )
    }
    conn = LakeflowConnect({"base_url": BASE_URL, "token": TOKEN})
    with mock.patch.object(
        conn.client._session, "get", side_effect=_make_router(routes)
    ):
        assert conn.list_tables() == ["Customers"]


# ---------------------------------------------------------------------------
# get_table_schema (needs pyspark)
# ---------------------------------------------------------------------------


def test_get_table_schema_maps_types():
    T = pytest.importorskip("pyspark.sql.types")

    schema_doc = {
        "table": "Customers",
        "columns": [
            {"name": "Id", "type": "integer", "nullable": False},
            {"name": "Name", "type": "string", "nullable": True},
            {"name": "Balance", "type": "decimal(19,4)", "nullable": True},
            {"name": "CreatedAt", "type": "timestamp", "nullable": True},
            {"name": "Active", "type": "boolean", "nullable": True},
            {"name": "Mystery", "type": "geography", "nullable": True},  # unknown
        ],
    }
    routes = {
        "/api/lakeflow/tables/Customers/schema": FakeResponse(json_body=schema_doc)
    }

    conn = LakeflowConnect({"base_url": BASE_URL, "token": TOKEN})
    with mock.patch.object(
        conn.client._session, "get", side_effect=_make_router(routes)
    ):
        struct = conn.get_table_schema("Customers")

    by_name = {f.name: f for f in struct.fields}
    assert isinstance(by_name["Id"].dataType, T.IntegerType)
    assert by_name["Id"].nullable is False
    assert isinstance(by_name["Name"].dataType, T.StringType)
    assert isinstance(by_name["Balance"].dataType, T.DecimalType)
    assert by_name["Balance"].dataType.precision == 19
    assert by_name["Balance"].dataType.scale == 4
    assert isinstance(by_name["CreatedAt"].dataType, T.TimestampType)
    assert isinstance(by_name["Active"].dataType, T.BooleanType)
    # unknown type falls back to StringType
    assert isinstance(by_name["Mystery"].dataType, T.StringType)


def test_spark_type_mapping_table():
    T = pytest.importorskip("pyspark.sql.types")
    cases = {
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
    for type_str, expected in cases.items():
        assert isinstance(_spark_type_for(type_str), expected)

    # decimal with whitespace + different params
    d = _spark_type_for("decimal(38, 18)")
    assert isinstance(d, T.DecimalType)
    assert (d.precision, d.scale) == (38, 18)

    # None / unknown -> StringType
    assert isinstance(_spark_type_for(None), T.StringType)
    assert isinstance(_spark_type_for("nonsense"), T.StringType)


# ---------------------------------------------------------------------------
# read_table_metadata
# ---------------------------------------------------------------------------


def test_read_table_metadata_shape():
    meta_doc = {
        "table": "Customers",
        "primary_keys": ["Id"],
        "ingestion_type": "snapshot",
        "supports_deletes": False,
    }
    routes = {
        "/api/lakeflow/tables/Customers/metadata": FakeResponse(json_body=meta_doc)
    }

    conn = LakeflowConnect({"base_url": BASE_URL, "token": TOKEN})
    with mock.patch.object(
        conn.client._session, "get", side_effect=_make_router(routes)
    ):
        meta = conn.read_table_metadata("Customers")

    assert meta["primary_keys"] == ["Id"]
    assert meta["cursor_fields"] == []
    assert meta["ingestion_type"] == "snapshot"
    assert meta["supports_deletes"] is False


# ---------------------------------------------------------------------------
# read_table -> dicts (Arrow IPC decode)
# ---------------------------------------------------------------------------


def test_read_table_decodes_arrow_to_dicts():
    table = pa.table(
        {
            "Id": pa.array([1, 2, 3], type=pa.int32()),
            "Name": pa.array(["Ada", "Linus", "Grace"], type=pa.string()),
        }
    )
    payload = _arrow_ipc_bytes(table)
    routes = {
        "/api/lakeflow/tables/Customers/read": FakeResponse(content=payload)
    }

    conn = LakeflowConnect({"base_url": BASE_URL, "token": TOKEN})
    with mock.patch.object(
        conn.client._session, "get", side_effect=_make_router(routes)
    ):
        rows = list(conn.read_table("Customers"))

    assert rows == [
        {"Id": 1, "Name": "Ada"},
        {"Id": 2, "Name": "Linus"},
        {"Id": 3, "Name": "Grace"},
    ]


def test_read_table_empty_body_yields_no_rows():
    routes = {
        "/api/lakeflow/tables/Customers/read": FakeResponse(content=b"")
    }
    conn = LakeflowConnect({"base_url": BASE_URL, "token": TOKEN})
    with mock.patch.object(
        conn.client._session, "get", side_effect=_make_router(routes)
    ):
        rows = list(conn.read_table("Customers"))
    assert rows == []


def test_read_table_offset_ignored_full_snapshot():
    """An offset argument is accepted but ignored — every read is a full read."""
    table = pa.table({"Id": pa.array([7], type=pa.int32())})
    payload = _arrow_ipc_bytes(table)
    routes = {"/api/lakeflow/tables/Customers/read": FakeResponse(content=payload)}

    conn = LakeflowConnect({"base_url": BASE_URL, "token": TOKEN})
    with mock.patch.object(
        conn.client._session, "get", side_effect=_make_router(routes)
    ):
        rows = list(conn.read_table("Customers", offset={"anything": "here"}))
    assert rows == [{"Id": 7}]


# ---------------------------------------------------------------------------
# read_table_deletes — not supported (snapshot-only)
# ---------------------------------------------------------------------------


def test_read_table_deletes_yields_nothing():
    conn = LakeflowConnect({"base_url": BASE_URL, "token": TOKEN})
    # no HTTP call should happen; if it did, the (unpatched) session would fail
    assert list(conn.read_table_deletes("Customers")) == []


# ---------------------------------------------------------------------------
# auth failure -> 404 surfaced as BarryApiError
# ---------------------------------------------------------------------------


def test_auth_failure_404_raises_barry_api_error():
    routes = {
        "/api/lakeflow/tables": FakeResponse(status_code=404, text="not found")
    }
    conn = LakeflowConnect({"base_url": BASE_URL, "token": TOKEN})
    with mock.patch.object(
        conn.client._session, "get", side_effect=_make_router(routes)
    ):
        with pytest.raises(BarryApiError) as exc:
            conn.list_tables()
    assert "404" in str(exc.value)
    assert "token" in str(exc.value).lower()


def test_server_error_raises_barry_api_error():
    routes = {
        "/api/lakeflow/tables": FakeResponse(status_code=500, text="boom")
    }
    conn = LakeflowConnect({"base_url": BASE_URL, "token": TOKEN})
    with mock.patch.object(
        conn.client._session, "get", side_effect=_make_router(routes)
    ):
        with pytest.raises(BarryApiError) as exc:
            conn.list_tables()
    assert "500" in str(exc.value)


# ---------------------------------------------------------------------------
# _coerce_bool helper
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "value,expected",
    [
        (True, True),
        (False, False),
        (None, True),
        ("true", True),
        ("True", True),
        ("false", False),
        ("FALSE", False),
        ("0", False),
        ("no", False),
        ("off", False),
        ("", False),
        ("yes", True),
        (1, True),
        (0, False),
    ],
)
def test_coerce_bool(value, expected):
    assert _coerce_bool(value) is expected


def test_verify_ssl_option_flows_to_session():
    conn = LakeflowConnect(
        {"base_url": BASE_URL, "token": TOKEN, "verify_ssl": "false"}
    )
    assert conn.client._session.verify is False
