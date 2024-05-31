from __future__ import annotations

from typing import Any, List, Sequence, Tuple

import pandas as pd
import pyarrow
from databend_py import Client
from harlequin import (
    HarlequinAdapter,
    HarlequinCompletion,
    HarlequinConnection,
    HarlequinCursor,
)
from harlequin.catalog import Catalog, CatalogItem
from harlequin.exception import HarlequinConnectionError
from textual_fastdatatable.backend import AutoBackendType

from harlequin_databend.cli_options import DATABEND_OPTIONS
from harlequin_databend.completions import _get_completions


class HarlequinDatabendCursor(HarlequinCursor):
    def __init__(self, conn: HarlequinDatabendConnection, query: str) -> None:
        self.results = conn.execute(query, with_column_types=True)
        self._limit: int | None = None

    def columns(self) -> list[tuple[str, str]]:
        return self.results[0]

    def set_limit(self, limit: int) -> HarlequinDatabendCursor:
        self._limit = limit
        return self

    def fetchall(self) -> AutoBackendType | None:
        return pyarrow.Table.from_pandas(
            pd.DataFrame(
                data=[list(row) for row in self.results[1]],
                columns=[col[0] for col in self.results[0]],
            )
        )


class HarlequinDatabendConnection(HarlequinConnection):
    def __init__(
        self,
        conn_str: Sequence[str],
        *_: Any,
        init_message: str = "",
        options: dict[str, Any],
    ) -> None:
        self.init_message = "Hello from Databend!" if not init_message else init_message
        try:
            if conn_str and conn_str[0]:
                self.conn = Client(conn_str[0])
            else:
                hostname = options.get("host", "127.0.0.1")
                database = options.get("dbname", "default")
                port = options.get("port", "8000")
                username = options.get("user", None)
                password = options.get("password", None)
                self.conn = Client(
                    host=hostname,
                    port=int(port) if port is not None else 8000,
                    database=database,
                    user=username if username else "",
                    password=password if password else "",
                )

        except Exception as e:
            raise HarlequinConnectionError(
                msg=str(e),
                title="Harlequin could not connect to databend.",
            ) from e

    def execute(self, query: str) -> HarlequinCursor:
        return HarlequinDatabendCursor(self.conn, query)

    def get_catalog(self) -> Catalog:
        databases = self._get_databases()
        db_items: List[CatalogItem] = []

        for (db,) in databases:
            schemas = self._get_schemas(db)
            schema_items: List[CatalogItem] = []

            for (schema,) in schemas:
                schema_items.append(
                    CatalogItem(
                        qualified_identifier=f"{db}.{schema}",
                        query_name=f"{db}.{schema}",
                        label=schema,
                        type_label="s",
                        children=[
                            *self._get_table(db, schema, "BASE TABLE", "t"),
                        ],
                    )
                )

            db_items.append(
                CatalogItem(
                    qualified_identifier=db,
                    query_name=db,
                    label=db,
                    type_label="db",
                    children=schema_items,
                )
            )

        return Catalog(items=db_items)

    def _get_databases(self) -> List[Tuple[str]]:
        _, res = self.conn.execute("SHOW DATABASES;")
        return res

    def _get_schemas(self, dbname: str) -> List[Tuple[str]]:
        _, res = self.conn.execute(
            f"""
            select schema_name
            from information_schema.schemata
            where
                catalog_name = '{dbname}'
                and schema_name != 'information_schema'
            order by schema_name asc
            ;"""
        )
        return res

    def _get_table(
        self,
        dbname: str,
        schema: str,
        table_type: str,
        type_label: str,
    ) -> List[CatalogItem]:
        _, res = self.conn.execute(
            f"""
            select table_name, table_type
            from information_schema.tables
            where
                table_catalog = '{dbname}'
                and table_schema = '{schema}'
            order by table_name asc
            ;"""
        )
        tables = [
            CatalogItem(
                qualified_identifier=f"{dbname}.{schema}.{table_name}",
                query_name=f"{dbname}.{schema}.{table_name}",
                label=table_name,
                type_label=type_label,
                children=[
                    CatalogItem(
                        qualified_identifier=f"{dbname}.{schema}.{table_name}.{col}",
                        query_name=col,
                        label=col,
                        type_label=self._get_short_type(col_type),
                    )
                    for (col, col_type) in self._get_columns(dbname, schema, table_name)
                ],
            )
            for (table_name, _) in res
        ]
        return tables

    def _get_columns(
        self, dbname: str, schema: str, relation: str
    ) -> List[Tuple[str, str]]:
        _, res = self.conn.execute(
            f"""
            select column_name, data_type
            from information_schema.columns
            where
                table_catalog = '{dbname}'
                and table_schema = '{schema}'
                and table_name = '{relation}'
            order by ordinal_position asc
            ;"""
        )
        return res

    @staticmethod
    def _get_short_type(type_name: str) -> str:
        MAPPING = {
            "bigint": "##",
            "bigserial": "##",
            "bit": "010",
            "boolean": "t/f",
            "box": "□",
            "bytea": "b",
            "character": "s",
            "cidr": "ip",
            "circle": "○",
            "date": "d",
            "double": "#.#",
            "inet": "ip",
            "integer": "#",
            "interval": "|-|",
            "json": "{}",
            "jsonb": "b{}",
            "line": "—",
            "lseg": "-",
            "macaddr": "mac",
            "macaddr8": "mac",
            "money": "$$",
            "numeric": "#.#",
            "path": "╭",
            "pg_lsn": "lsn",
            "pg_snapshot": "snp",
            "point": "•",
            "polygon": "▽",
            "real": "#.#",
            "smallint": "#",
            "smallserial": "#",
            "serial": "#",
            "text": "s",
            "time": "t",
            "timestamp": "ts",
            "tsquery": "tsq",
            "tsvector": "tsv",
            "txid_snapshot": "snp",
            "uuid": "uid",
            "xml": "xml",
            "array": "[]",
        }
        return MAPPING.get(type_name.split("(")[0].split(" ")[0], "?")


class HarlequinDatabendAdapter(HarlequinAdapter):
    ADAPTER_OPTIONS = DATABEND_OPTIONS

    def __init__(
        self,
        conn_str: Sequence[str],
        host: str | None = None,
        port: str | None = None,
        dbname: str | None = None,
        user: str | None = None,
        password: str | None = None,
        **_: Any,
    ) -> None:
        self.conn_str = conn_str
        self.options = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }

    def connect(self) -> HarlequinDatabendConnection:
        if len(self.conn_str) > 1:
            raise HarlequinConnectionError(
                "Cannot provide multiple connection strings to the Databend adapter."
                f"{self.conn_str}"
            )
        conn = HarlequinDatabendConnection(self.conn_str, options=self.options)
        return conn

    def get_completions(
        self, catalog: Catalog, item: CatalogItem
    ) -> Sequence[HarlequinCompletion]:
        return _get_completions(catalog, item)
