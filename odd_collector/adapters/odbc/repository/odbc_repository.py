from contextlib import contextmanager
from enum import Enum
from typing import Iterable, Optional, Type, TypeVar

import pyodbc
from funcy import zipdict
from odd_collector_sdk.errors import DataSourceError
from pydantic import SecretStr
from pyodbc import Cursor

from odd_collector.adapters.odbc.domain import BaseTable, Table, View
from odd_collector.adapters.odbc.domain.column import Column
from odd_collector.adapters.odbc.logger import logger
from odd_collector.adapters.odbc.repository.repository_base import RepositoryBase
from odd_collector.domain.plugin import OdbcPlugin
from odd_collector.helpers.logging_exception import logging_exception

T = TypeVar("T", bound=BaseTable)


class TableType(Enum):
    TABLE = "TABLE"
    VIEW = "VIEW"


def create_connection_str(config: OdbcPlugin) -> SecretStr:
    return SecretStr(
        f"DRIVER={config.driver};"
        f"SERVER={config.host},{config.port};"
        f"DATABASE={config.database};"
        f"UID={config.user};"
        f"PWD={config.password.get_secret_value()}"
    )


class Repository(RepositoryBase):
    def __init__(self, config: OdbcPlugin):
        super().__init__(config)
        self._connection_str: SecretStr = create_connection_str(config)

    def get_data(self) -> Iterable[BaseTable]:
        yield from self._get_tables()
        yield from self._get_views()

    def _get_tables(self) -> Iterable[Table]:
        yield from self._fetch_tables_by_type(TableType.TABLE, Table)

    def _get_views(self) -> Iterable[View]:
        yield from self._fetch_tables_by_type(TableType.VIEW, View)

    def _fetch_tables_by_type(self, table_type: TableType, target_class: Type[T]) -> T:
        db = self._config.database
        with self.connect_odbc() as cursor:
            base_tables: Cursor = cursor.tables(catalog=db, tableType=table_type.value)
            for row in base_tables.fetchall():
                base_table = target_class.from_response(row)
                base_table.columns = list(self._get_columns_for(base_table, cursor))

                yield base_table

    def _get_columns_for(self, table: BaseTable, cursor: Cursor) -> Iterable[Column]:
        columns: Cursor = cursor.columns(
            table.table_name, table.table_catalog, table.table_schema
        )

        for row in columns.fetchall():
            description = row.cursor_description
            response = zipdict(keys=[desc[0] for desc in description], vals=row)
            yield Column.from_response(response)

    @contextmanager
    def connect_odbc(self) -> pyodbc.Cursor:
        connection: Optional[pyodbc.Connection] = None
        cursor: Optional[pyodbc.Cursor] = None

        try:
            connection = pyodbc.connect(self._connection_str.get_secret_value())
            cursor = connection.cursor()
            yield cursor
        except Exception as e:
            raise DataSourceError(f"Error with ({self._config.database})") from e
        finally:
            if cursor:
                with logging_exception("closing cursor", logger):
                    cursor.close()
            if connection:
                with logging_exception("closing connection", logger):
                    connection.close()
