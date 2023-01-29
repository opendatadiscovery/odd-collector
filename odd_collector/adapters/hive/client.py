import json
from contextlib import contextmanager
from typing import Protocol, Optional

from hive_metastore_client import HiveMetastoreClient
from hive_metastore_client.builders.table_builder import Table as HiveTable
from thrift_files.libraries.thrift_hive_metastore_client.ttypes import (
    PrimaryKeysRequest,
    StorageDescriptor,
    FieldSchema,
    ColumnStatistics,
)

from odd_collector.adapters.hive.grammar_parser.parser import parser
from odd_collector.adapters.hive.grammar_parser.transformer import transformer
from odd_collector.adapters.hive.logger import logger
from odd_collector.adapters.hive.models.base_table import BaseTable
from odd_collector.adapters.hive.models.column import Column
from odd_collector.adapters.hive.models.column_types import ColumnType, UnionColumnType
from odd_collector.adapters.hive.models.table import Table
from odd_collector.adapters.hive.models.view import View


class Client(Protocol):
    def get_databases(self) -> list[str]:
        ...

    def get_tables(self) -> list[BaseTable]:
        ...


def _get_column_name_with_stats(table: HiveTable) -> set[str]:
    params = table.parameters
    columns_with_stats: set[str] = set()

    try:
        if (statistics := params.get("COLUMN_STATS_ACCURATE")) is not None:
            statistics = json.loads(statistics)

            fields_has_statistics = statistics.get("COLUMN_STATS")

            if fields_has_statistics:
                columns_with_stats = {
                    field
                    for field, has_statistic in fields_has_statistics.items()
                    if has_statistic == "true"
                }
    except Exception as e:
        logger.warning(
            f"Could not get column names whach has statistics {table.tableName}. {e}"
        )
        return set()

    return columns_with_stats


class HiveClient:
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    @contextmanager
    def connect(self) -> HiveMetastoreClient:
        with HiveMetastoreClient(self.host, self.port) as conn:
            yield conn

    def get_all_tables(
        self, database_name: str, client: HiveMetastoreClient
    ) -> list[BaseTable]:
        result = []
        for table_name in client.get_all_tables(database_name):
            try:
                hive_table: HiveTable = client.get_table(database_name, table_name)
                table: BaseTable = create_table(hive_table)

                primary_keys = self.get_table_primary_keys(hive_table, client)
                table.primary_keys = primary_keys
                table.columns = self.get_table_columns(hive_table, primary_keys, client)
                result.append(table)
            except Exception as e:
                logger.warning(e)

        return result

    def get_table_primary_keys(
        self, table: HiveTable, client: HiveMetastoreClient
    ) -> list[str]:
        """Fetch tables primary keys"""
        try:
            request = PrimaryKeysRequest(table.dbName, table.tableName, table.catName)
            response = client.get_primary_keys(request)

            return [key.column_name for key in response.primaryKeys]
        except Exception as e:
            logger.warning(f"Couldn't get primary keys for {table.tableName}. {e}")
            return []

    def get_table_columns(
        self, table: HiveTable, primary_keys: list[str], client: HiveMetastoreClient
    ) -> list[Column]:
        result = []

        storage_descriptor: StorageDescriptor = table.sd
        fields: list[FieldSchema] = storage_descriptor.cols
        columns_with_stats: set[str] = _get_column_name_with_stats(table)

        for field in fields:
            column_type = parse_field_schema(field)
            column = Column(
                col_name=field.name,
                col_type=column_type,
                comment=field.comment,
            )

            if field.name in primary_keys:
                column.is_primary = True

            if field.name in columns_with_stats:
                column.statistic = self.get_column_statistic(
                    table.dbName, table.tableName, column.col_name, client
                )

            result.append(column)

        return result

    def get_column_statistic(
        self, db_name: str, table_name: str, col_name: str, client: HiveMetastoreClient
    ) -> Optional[ColumnStatistics]:
        try:
            return client.get_table_column_statistics(db_name, table_name, col_name)
        except Exception as e:
            logger.warning(
                f"Couldn't fetch statistics for {db_name=} {table_name=} {col_name=}, {e}"
            )
            return None


def parse_field_schema(field: FieldSchema) -> ColumnType:
    try:
        parsed = parser.parse(field.type)
        column_type: ColumnType = transformer.transform(parsed)
        column_type.logical_type = field.type

        return column_type
    except Exception as e:
        logger.error(f"Could not parse type {field.type}. {e}")
        return UnionColumnType(logical_type=field.type)


def create_table(hive_table: HiveTable) -> BaseTable:
    if hive_table.tableType == "MANAGED_TABLE":
        return Table.from_hive(hive_table)
    elif hive_table.tableType == "VIRTUAL_VIEW":
        return View.from_hive(hive_table)
    else:
        raise ValueError(f"Unsupported table: {hive_table!r}")
