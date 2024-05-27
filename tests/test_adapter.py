import sys

import pytest
from harlequin.adapter import HarlequinAdapter, HarlequinConnection
from harlequin.catalog import Catalog, CatalogItem
from harlequin_databend.adapter import (
    HarlequinDatabendAdapter,
    HarlequinDatabendConnection,
)
from textual_fastdatatable.backend import create_backend  # noqa: F401

if sys.version_info < (3, 10):
    from importlib_metadata import entry_points
else:
    from importlib.metadata import entry_points


def test_plugin_discovery() -> None:
    PLUGIN_NAME = "databend-adapter"
    eps = entry_points(group="harlequin.adapter")
    assert eps[PLUGIN_NAME]
    adapter_cls = eps[PLUGIN_NAME].load()
    assert issubclass(adapter_cls, HarlequinAdapter)
    assert adapter_cls == HarlequinDatabendAdapter


def test_connect() -> None:
    conn = HarlequinDatabendAdapter(
        port=8000, host="127.0.0.1", dbname="default"
    ).connect()
    assert isinstance(conn, HarlequinConnection)


@pytest.fixture
def connection() -> HarlequinDatabendConnection:
    return HarlequinDatabendAdapter(
        conn_str=None, port=8000, host="127.0.0.1", dbname="default"
    ).connect()


def test_get_catalog(conn: HarlequinDatabendConnection) -> None:
    catalog = conn.get_catalog()
    assert isinstance(catalog, Catalog)
    assert catalog.items
    assert isinstance(catalog.items[0], CatalogItem)
