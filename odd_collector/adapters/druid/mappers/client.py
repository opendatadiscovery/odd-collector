import json
from abc import ABC, abstractmethod
from typing import List

import requests
from odd_collector_sdk.errors import MappingDataError, DataSourceError

from odd_collector.adapters.druid.mappers.column import Column
from odd_collector.adapters.druid.mappers.table import Table
from odd_collector.domain.plugin import DruidPlugin


class DruidBaseClient(ABC):
    @abstractmethod
    def get_tables(self) -> List[Table]:
        raise NotImplementedError

    @abstractmethod
    def get_columns(self) -> List[Column]:
        raise NotImplementedError


class DruidClient(DruidBaseClient):
    def __init__(self, config: DruidPlugin) -> None:
        self.__config = config

    def get_tables(self) -> List[Table]:
        """
        Get tables from druid.

        Note: It excludes the INFORMATION_SCHEMA that keep druid metadata related
        to tables persisted.

        Note: It excludes system tables related to druid operations.

        :return: a list of tables
        """
        # Prepare
        tables: List[Table] = []
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
            WHERE 
                "TABLE_SCHEMA" != 'INFORMATION_SCHEMA'
            AND "TABLE_SCHEMA" != 'sys'
        """

        # Execute
        try:
            result = requests.post(url, json={"query": sql_query})
            result_dict = json.loads(result.text)
        except Exception as e:
            # Throw
            raise DataSourceError(f"Couldn't execute Druid query: {sql_query}") from e

        # Transform
        try:
            for row in result_dict:
                tables.append(Table(
                    row["TABLE_CATALOG"],
                    row["TABLE_SCHEMA"],
                    row["TABLE_NAME"],
                    row["TABLE_TYPE"],
                    row["IS_JOINABLE"],
                    row["IS_BROADCAST"]))
        except Exception as e:
            # Throw
            raise MappingDataError("Couldn't transform Druid result to Table model") from e

        # Return
        return tables

    def get_columns(self) -> List[Column]:
        # Prepare
        table_columns: List[Column] = []
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
            result = requests.post(url, json={"query": sql_query})
            result_dict = json.loads(result.text)
        except Exception as e:
            # Throw
            raise DataSourceError(f"Couldn't execute Druid query: {sql_query}") from e

        # Transform
        try:
            for row in result_dict:
                column = Column(row["TABLE_CATALOG"],
                                row["TABLE_SCHEMA"],
                                row["TABLE_NAME"],
                                row["COLUMN_NAME"],
                                row["DATA_TYPE"],
                                True if row["IS_NULLABLE"] == 'YES' else False)
                table_columns.append(column)
        except Exception as e:
            # Throw
            raise MappingDataError("Couldn't transform Druid result to Column model") from e

        # Return
        return table_columns
