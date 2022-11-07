import contextlib
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List

import pymssql
from odd_collector_sdk.errors import DataSourceAuthorizationError
from pydantic import SecretStr

from .models import Column, Table, View, ViewDependency


class BaseConnector(ABC):
    @abstractmethod
    def get_tables(self, connection) -> List[Table]:
        ...

    @abstractmethod
    def get_columns(self, connection) -> List[Column]:
        ...


TABLES_QUERY: str = """
SELECT
       table_catalog,
       table_schema,
       table_name,
       table_type
FROM information_schema.tables
WHERE table_type != 'VIEW'
ORDER BY table_catalog, table_schema, table_name;"""

COLUMNS_QUERY: str = """
SELECT
    table_catalog,
    table_schema,
    table_name,
    column_name,
    ordinal_position,
    column_default,
    is_nullable,
    data_type,
    character_maximum_length,
    character_octet_length,
    numeric_precision,
    numeric_precision_radix,
    numeric_scale,
    datetime_precision,
    character_set_catalog,
    character_set_schema,
    character_set_name,
    collation_catalog,
    collation_schema,
    collation_name,
    domain_catalog,
    domain_schema,
    domain_name
FROM information_schema.columns
ORDER BY table_catalog, table_schema, table_name, ordinal_position
"""

VIEWS_QUERY: str = """
    select 
        tvu.view_catalog as view_catalog, 
        tvu.view_schema as view_schema, 
        tvu.view_name as view_name, 
        tvu.table_catalog as table_catalog, 
        tvu.table_schema as table_schema, 
        tvu.table_name as table_name, 
        tb.table_type as table_type
    from information_schema.view_table_usage tvu
    inner join information_schema.tables tb
            on tvu.table_catalog = tb.table_catalog 
            and tvu.table_schema = tb.table_schema 
            and tvu.table_name = tb.table_name
    order by view_catalog, view_schema, view_name, table_catalog, table_schema, table_name
"""


@dataclass
class ConnectionConfig:
    server: str
    database: str
    user: str
    password: SecretStr
    port: str


class DefaultConnector(contextlib.ContextDecorator):
    def __init__(self, config: ConnectionConfig):
        self._config = config
        self._conn = None

    def __enter__(self) -> "DefaultConnector":
        try:
            self.conn = pymssql.connect(
                server=self._config.server,
                user=self._config.user,
                password=self._config.password.get_secret_value(),
                database=self._config.database,
                port=self._config.port
            )
            return self
        except pymssql._pymssql.OperationalError as e:
            raise DataSourceAuthorizationError("Could'n connect to database") from e

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def get_tables(self) -> List[Table]:
        return [Table(**obj) for obj in self.__exec(TABLES_QUERY)]

    def get_columns(self) -> List[Column]:
        return [Column(**obj) for obj in self.__exec(COLUMNS_QUERY)]

    def get_views(self) -> List[View]:
        result: Dict[str, View] = {}
        for obj in self.__exec(VIEWS_QUERY):
            view_name = obj.get("view_name")

            if view_name not in result:
                result[view_name] = View(
                    view_catalog=obj.get("view_catalog"),
                    view_schema=obj.get("view_schema"),
                    view_name=view_name,
                    view_dependencies=[],
                )

            result[view_name].view_dependencies.append(
                ViewDependency(
                    table_catalog=obj.get("table_catalog"),
                    table_schema=obj.get("table_schema"),
                    table_name=obj.get("table_name"),
                    table_type=obj.get("table_type"),
                )
            )

        return list(result.values())

    def __exec(self, query: str) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor(as_dict=True)
        cursor.execute(query)
        return cursor.fetchall()
