from collections import UserList, defaultdict
from dataclasses import dataclass
from typing import Any, Iterable

import pymssql
from funcy import group_by
from pydantic import SecretStr

from .logger import logger
from .models import Column, Table, View, ViewDependency


class Columns(UserList):
    _by_table: dict[str, list[Column]] = defaultdict(list)

    def __init__(self, columns: Iterable[Any]):
        super().__init__([Column(*r) for r in columns])
        self._by_table = group_by(
            lambda c: f"{c.table_catalog}.{c.table_schema}.{c.table_name}", self
        )

    def get_columns_for(self, database: str, schema: str, name: str) -> list[Column]:
        return self._by_table.get(f"{database}.{schema}.{name}", [])


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


@dataclass
class ConnectionConfig:
    server: str
    database: str
    user: str
    password: SecretStr
    port: int


class MssqlRepository:
    def __init__(self, config: ConnectionConfig):
        self.config = config
        self.connection = None

    def __enter__(self) -> "MssqlRepository":
        self.connection = pymssql.connect(
            server=self.config.server,
            database=self.config.database,
            user=self.config.user,
            password=self.config.password.get_secret_value(),
            port=self.config.port,
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug("Closing connection")
        if self.connection:
            self.connection.close()

    def get_tables(self) -> Iterable[Table]:
        with self.connection.cursor() as cursor:
            cursor.execute(TABLES_QUERY)
            for row in cursor.fetchall():
                yield Table(*row)

    def get_columns(self) -> Columns:
        with self.connection.cursor() as cursor:
            cursor.execute(COLUMNS_QUERY)
            return Columns(cursor.fetchall())

    def get_views(self) -> Iterable[View]:
        result: dict[str, View] = {}
        with self.connection.cursor(as_dict=True) as cursor:
            cursor.execute(VIEWS_QUERY)
            for row in cursor.fetchall():
                view_name = row.get("view_name")

                if view_name not in result:
                    result[view_name] = View(
                        view_catalog=row.get("view_catalog"),
                        view_schema=row.get("view_schema"),
                        view_name=view_name,
                        view_dependencies=[],
                    )

                result[view_name].view_dependencies.append(
                    ViewDependency(
                        table_catalog=row.get("table_catalog"),
                        table_schema=row.get("table_schema"),
                        table_name=row.get("table_name"),
                        table_type=row.get("table_type"),
                    )
                )

            return list(result.values())
