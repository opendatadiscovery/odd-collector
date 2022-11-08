import asyncio
from abc import ABC, abstractmethod
from typing import List

from aiohttp import ClientSession
from odd_collector_sdk.errors import DataSourceError

from odd_collector.adapters.druid.domain.column import Column
from odd_collector.adapters.druid.domain.column_stats import ColumnStats
from odd_collector.adapters.druid.domain.column_type import ColumnType
from odd_collector.adapters.druid.domain.table import Table
from odd_collector.adapters.druid.logger import logger
from odd_collector.domain.plugin import DruidPlugin


class DruidBaseClient(ABC):
    @abstractmethod
    async def get_resources(self):
        raise NotImplementedError

    @abstractmethod
    async def get_tables(self, session: ClientSession) -> List[Table]:
        raise NotImplementedError

    @abstractmethod
    async def get_columns(self, session: ClientSession) -> List[Column]:
        raise NotImplementedError

    @abstractmethod
    async def get_tables_nr_of_rows(self, session: ClientSession) -> dict:
        raise NotImplementedError

    @abstractmethod
    async def get_column_stats(self, table_names: List[str]) -> dict:
        raise NotImplementedError

    @abstractmethod
    async def get_segment_metadata(
        self, session: ClientSession, table_name: str
    ) -> dict:
        raise NotImplementedError


class DruidClient(DruidBaseClient):
    def __init__(self, config: DruidPlugin) -> None:
        self.__config = config

    async def get_resources(self):
        # Prepare
        async with ClientSession() as session:
            tasks = [
                asyncio.create_task(self.get_tables(session)),
                asyncio.create_task(self.get_columns(session)),
                asyncio.create_task(self.get_tables_nr_of_rows(session)),
            ]

            # Return
            return await asyncio.gather(*tasks)

    async def get_column_stats(self, table_names: List[str]):
        """
        Fetch column stats for all the provided table names.
        :param table_names: a list of table names
        :return:
        """
        # Prepare
        async with ClientSession() as session:
            # Prepare
            result = {}
            tasks = [
                asyncio.create_task(self.get_segment_metadata(session, table_name))
                for table_name in table_names
            ]

            # Fetch
            responses = await asyncio.gather(*tasks)

            # Transform
            for response in responses:
                result[response["table_name"]] = response["column_stats"]

            # Return
            return result

    async def get_tables(self, session: ClientSession) -> List[Table]:
        """
        Get tables from druid.

        Note: It excludes the INFORMATION_SCHEMA that keep druid metadata related
        to tables persisted.

        Note: It excludes system tables related to druid operations.

        :return: a list of tables
        """
        # Prepare
        url = f"{self.__config.host}:{self.__config.port}/druid/v2/sql/"
        sql_query = """
            SELECT 
              "TABLE_CATALOG", 
              "TABLE_SCHEMA", 
              "TABLE_NAME", 
              "TABLE_TYPE", 
              "IS_JOINABLE", 
              "IS_BROADCAST"
            FROM INFORMATION_SCHEMA.TABLES
            WHERE LOWER("TABLE_SCHEMA") != 'information_schema'
              AND LOWER("TABLE_SCHEMA") != 'sys'
        """

        # Execute
        try:
            async with session.post(url, json={"query": sql_query}) as response:
                # Log it
                logger.debug("Get tables")

                records = await response.json()

                # Return
                return [Table.from_response(record) for record in records]
        except Exception as e:
            # Throw
            raise DataSourceError(f"Couldn't execute Druid query: {sql_query}") from e

    async def get_columns(self, session: ClientSession) -> List[Column]:
        # Prepare
        url = f"{self.__config.host}:{self.__config.port}/druid/v2/sql/"
        sql_query = """
            SELECT
              TABLE_CATALOG,
              TABLE_SCHEMA,
              TABLE_NAME,
              COLUMN_NAME,
              ORDINAL_POSITION,
              COLUMN_DEFAULT,
              IS_NULLABLE,
              DATA_TYPE,
              CHARACTER_MAXIMUM_LENGTH,
              CHARACTER_OCTET_LENGTH,
              NUMERIC_PRECISION,
              NUMERIC_PRECISION_RADIX,
              NUMERIC_SCALE,
              DATETIME_PRECISION,
              CHARACTER_SET_NAME,
              COLLATION_NAME,
              JDBC_TYPE
            FROM INFORMATION_SCHEMA.COLUMNS
        """

        # Execute
        try:
            async with session.post(url, json={"query": sql_query}) as response:
                # Log it
                logger.debug("Get columns")

                records = await response.json()

                # Return
                return [Column.from_response(record) for record in records]
        except Exception as e:
            # Throw
            raise DataSourceError(f"Couldn't execute Druid query: {sql_query}") from e

    async def get_tables_nr_of_rows(self, session: ClientSession) -> dict:
        # Prepare
        url = f"{self.__config.host}:{self.__config.port}/druid/v2/sql/"
        sql_query = f"""
        SELECT
            datasource,
            COUNT(*) FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AS num_segments,
            COUNT(*) FILTER (WHERE is_published = 1 AND is_overshadowed = 0 AND is_available = 0) AS num_segments_to_load,
            COUNT(*) FILTER (WHERE is_available = 1 AND NOT ((is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1)) AS num_segments_to_drop,
            SUM("size") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS total_data_size,
            MIN("num_rows") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS min_segment_rows,
            AVG("num_rows") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS avg_segment_rows,
            MAX("num_rows") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS max_segment_rows,
            SUM("num_rows") FILTER (WHERE (is_published = 1 AND is_overshadowed = 0) OR is_realtime = 1) AS total_rows,
            CASE WHEN SUM("num_rows") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) <> 0 THEN (SUM("size") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) / SUM("num_rows") FILTER (WHERE is_published = 1 AND is_overshadowed = 0)) ELSE 0 END AS avg_row_size,
            SUM("size" * "num_replicas") FILTER (WHERE is_published = 1 AND is_overshadowed = 0) AS replicated_size
        FROM sys.segments
        GROUP BY 1
        ORDER BY 1
        """

        # Execute
        try:
            async with session.post(url, json={"query": sql_query}) as response:
                # Log it
                logger.debug("Get number of rows for tables")

                # Fetch
                records = await response.json()

                # Return
                return dict(
                    (record["datasource"], record["total_rows"]) for record in records
                )

        except Exception as e:
            # Throw
            raise DataSourceError(f"Couldn't execute Druid query: {sql_query}") from e

    async def get_segment_metadata(
        self, session: ClientSession, table_name: str
    ) -> dict:
        """
        Fetch table metadata from segments
        :param session: the client session
        :param table_name: the table name to fetch data for
        :return: a list of dimensions as array of string
        """
        # Prepare
        url = f"{self.__config.host}:{self.__config.port}/druid/v2/"
        query_payload = {
            "queryType": "segmentMetadata",
            "dataSource": f"{table_name}",
            "merge": "true",
            "analysisTypes": ["cardinality", "minmax", "size", "aggregators", "rollup"],
        }

        # Execute
        try:
            async with session.post(url, json=query_payload) as response:
                # Log it
                logger.debug(
                    f"Get segment metadata for table: {query_payload['dataSource']}"
                )

                # Fetch
                records = await response.json()

                # Transform
                columns = records[0]["columns"]
                columns_stats = [
                    ColumnStats.from_response(column_name, columns[column_name])
                    for column_name in columns
                ]

                # Add column type
                aggregators = records[0]["aggregators"] or []
                for column_stats in columns_stats:
                    if column_stats.name in aggregators:
                        column_stats.type = ColumnType.metric

                # Return
                return dict(table_name=table_name, column_stats=columns_stats)

        except Exception as e:
            # Throw
            raise DataSourceError(f"Couldn't execute Druid query") from e
