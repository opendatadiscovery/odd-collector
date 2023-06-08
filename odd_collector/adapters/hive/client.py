import json
import traceback

from pyhive import hive

from odd_collector.adapters.hive.logger import logger
from odd_collector.adapters.hive.models import View

from ...domain.plugin import HiveConnectionParams
from .models.column_statistics import ColumnStatistics
from .models.table import Table
from .models.table_description import Initial, TableDescription


class HiveClient:
    def __init__(self, connection_params: HiveConnectionParams):
        self.connection_params = connection_params

    def __enter__(self) -> "HiveClient":
        c_params = self.connection_params.dict()
        self.conn = hive.connect(**c_params)
        self.cursor = self.conn.cursor()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.conn.close()

    def get_tables(self, count_statistic: bool = False) -> list[Table]:
        tables = []
        table_names = self.get_table_names()
        view_names = self.get_views_names()

        for name in table_names:
            try:
                description = self.get_table_description(name)

                cls = View if name in view_names else Table
                table = cls(name=name, description=description)
                tables.append(table)

                if count_statistic:
                    self.collect_table_columns_statistics(table)

            except Exception as e:
                logger.warning(f"Could not get table description {name}. {e}")
                logger.debug(traceback.format_exc())
                continue

        return tables

    def get_table_names(self) -> list[str]:
        """Returns a list of table names. Note that views are also included."""
        with self.conn.cursor() as cursor:
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            return [table[0] for table in tables]

    def get_views_names(self) -> list[str]:
        """Returns a list of view names. Need that method to separate views from tables."""
        with self.conn.cursor() as cursor:
            cursor.execute("SHOW VIEWS")
            return [view[0] for view in cursor.fetchall()]

    def get_table_description(self, table_name: str) -> TableDescription:
        """
        Returns a TableDescription object for the given table name. From the table description
        we get columns, statistics, detailed properties and more useful information.
        """
        with self.conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE FORMATTED {table_name}")
            context = TableDescription()
            state = Initial(context)
            rows = cursor.fetchall()

            for row in rows:
                state = state.next(row)

            return context

    def get_column_statistics(
        self, table_name: str, column_name: str
    ) -> ColumnStatistics:
        with self.conn.cursor() as cursor:
            cursor.execute(f"DESCRIBE FORMATTED {table_name} {column_name}")
            rows = iter(cursor.fetchall())

            header = next(rows)
            empty_lines = next(rows)
            statistics = ColumnStatistics(*next(rows))

            return statistics

    def collect_table_columns_statistics(self, table: Table) -> None:
        try:
            logger.debug(f"Getting count statistic for {table.name}")
            if raw_statistics := table.description.table_parameters.get(
                "COLUMN_STATS_ACCURATE"
            ):
                statistics = json.loads(raw_statistics.replace("\\", ""))
                logger.debug(f"Getting count statistic for {table.name}")

                if statistics.get("BASIC_STATS") and statistics.get("COLUMN_STATS"):
                    column_names = statistics["COLUMN_STATS"].keys()
                    for column in table.columns:
                        try:
                            if column.name in column_names:
                                column.statistics = self.get_column_statistics(
                                    table.name, column.name
                                )
                        except Exception as e:
                            logger.warning(
                                f"Could not get column statistics for {column.name} in {table.name}. {e}"
                            )
                            logger.debug(traceback.format_exc())
                            continue
        except Exception as e:
            logger.warning(f"Could not get table statistics for {table.name}. {e}")
            logger.debug(traceback.format_exc())
            return
