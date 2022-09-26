from abc import ABC, abstractmethod
from typing import List

from aiohttp import ClientSession
from odd_collector_sdk.errors import DataSourceError

from odd_collector.adapters.druid.domain.column import Column
from odd_collector.adapters.druid.domain.table import Table
from odd_collector.domain.plugin import DruidPlugin


class DruidBaseClient(ABC):
    @abstractmethod
    async def get_tables(self) -> List[Table]:
        raise NotImplementedError

    @abstractmethod
    async def get_columns(self) -> List[Column]:
        raise NotImplementedError

    async def get_tables_nr_of_rows(self) -> dict:
        raise NotImplementedError


class DruidClient(DruidBaseClient):
    def __init__(self, config: DruidPlugin) -> None:
        self.__config = config

    async def get_tables(self) -> List[Table]:
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
            async with ClientSession() as session:
                async with session.post(url, json={"query": sql_query}) as response:
                    records = await response.json()
                    return [Table.from_response(record) for record in records]
        except Exception as e:
            # Throw
            raise DataSourceError(f"Couldn't execute Druid query: {sql_query}") from e

    async def get_columns(self) -> List[Column]:
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
            async with ClientSession() as session:
                async with session.post(url, json={"query": sql_query}) as response:
                    records = await response.json()
                    return [Column.from_response(record) for record in records]
        except Exception as e:
            # Throw
            raise DataSourceError(f"Couldn't execute Druid query: {sql_query}") from e

    async def get_tables_nr_of_rows(self) -> dict:
        # Prepare
        url = f"{self.__config.host}:{self.__config.port}/druid/v2/sql/"
        sql_query = f"""
        SELECT 
            datasource,
            CASE WHEN SUM(num_rows) = 0 THEN 0 ELSE SUM(num_rows) / (COUNT(*) FILTER(WHERE num_rows > 0)) END AS avg_num_rows
        FROM sys.segments
--      todo: is_active column shall be considered for newest druid versions   
        WHERE is_available = 1
        GROUP BY 1
        """

        # Execute
        try:
            async with ClientSession() as session:
                async with session.post(url, json={"query": sql_query}) as response:
                    # Fetch
                    records = await response.json()

                    # Return
                    return dict((record["datasource"], record["avg_num_rows"]) for record in records)

        except Exception as e:
            # Throw
            raise DataSourceError(f"Couldn't execute Druid query: {sql_query}") from e
