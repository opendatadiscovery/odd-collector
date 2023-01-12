from abc import ABC, abstractmethod
from collections import UserList, defaultdict
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

import pymssql
from odd_collector_sdk.errors import DataSourceConnectionError
from pydantic import SecretStr

from .models import Column, Table, View, ViewDependency


class Columns(UserList):
    _by_table: Dict[str, List[Column]] = defaultdict(list)

    def __init__(self, columns: Iterable[Any]):
        super().__init__([Column(*r) for r in columns])
        self._by_table = group_by(
            lambda c: f"{c.table_catalog}.{c.table_schema}.{c.table_name}", self
        )

    def get_columns_for(self, database: str, schema: str, name: str) -> List[Column]:
        return self._by_table.get(f"{database}.{schema}.{name}", [])


class BaseConnector(ABC):
    @abstractmethod
    def get_tables(self, connection) -> Iterable[Table]:
        ...

    @abstractmethod
    def get_columns(self, connection) -> Columns:
        ...

    @abstractmethod
    def get_views(self, connection) -> Iterable[View]:
        ...


from contextlib import contextmanager

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
WITH primary_keys as (
    SELECT KU.table_catalog, KU.table_schema, KU.table_name, KU.column_name 
    FROM information_schema.table_constraints AS TC 
    INNER JOIN information_schema.key_column_usage AS KU
    ON TC.constraint_name = KU.constraint_name
    AND TC.constraint_type  = 'PRIMARY KEY'
)
SELECT
    C.table_catalog,
    C.table_schema,
    C.table_name,
    C.column_name,
    C.ordinal_position,
    C.column_default,
    C.is_nullable,
    CASE
        WHEN PK.column_name IS NOT NULL THEN 1
    ELSE 0
    END AS is_primary_key,
    C.data_type,
    C.character_maximum_length,
    C.character_octet_length,
    C.numeric_precision,
    C.numeric_precision_radix,
    C.numeric_scale,
    C.datetime_precision,
    C.character_set_catalog,
    C.character_set_schema,
    C.character_set_name,
    C.collation_catalog,
    C.collation_schema,
    C.collation_name,
    C.domain_catalog,
    C.domain_schema,
    C.domain_name
FROM information_schema.columns as C
LEFT JOIN primary_keys as PK
ON  C.table_catalog = PK.table_catalog
AND C.table_schema = PK.table_schema
AND C.table_name = PK.table_name
AND C.column_name = PK.column_name
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

from funcy import group_by


@dataclass
class ConnectionConfig:
    server: str
    database: str
    user: str
    password: SecretStr
    port: str


class DefaultConnector:
    def __init__(self, config: ConnectionConfig):
        self._config = config
        self._connect_config = config

    def get_tables(self) -> Iterable[Table]:
        with get_cursor(self._connect_config) as cursor:
            for row in get_tables(cursor):
                yield Table(*row)

    def get_columns(self) -> Columns:
        with get_cursor(self._connect_config) as cursor:
            return Columns(get_columns(cursor))

    def get_views(self) -> Iterable[View]:
        result: Dict[str, View] = {}
        with get_cursor(self._connect_config, as_dict=True) as cursor:
            for obj in get_views(cursor):
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


@contextmanager
def get_cursor(connect_config: ConnectionConfig, as_dict=False):
    connection = None
    cursor = None
    try:
        connection = pymssql.connect(
            server=connect_config.server,
            database=connect_config.database,
            user=connect_config.user,
            password=connect_config.password.get_secret_value(),
            port=connect_config.port,
        )
        cursor = connection.cursor(as_dict=as_dict)
        yield cursor
    except pymssql._pymssql.OperationalError as e:
        raise DataSourceConnectionError(e) from e
    finally:
        if cursor:
            cursor.close()
        if connection:
            connection.close()


def get_tables(cursor):
    cursor.execute(TABLES_QUERY)
    return cursor.fetchall()


def get_views(cursor):
    cursor.execute(VIEWS_QUERY)
    return cursor.fetchall()


def get_columns(cursor):
    cursor.execute(COLUMNS_QUERY)
    return cursor.fetchall()
