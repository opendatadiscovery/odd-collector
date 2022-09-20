from abc import ABC, abstractmethod
from typing import List

from odd_collector_sdk.errors import MappingDataError, DataSourceError, DataSourceConnectionError
from pydruid.db import connect

from .domain.table import Table
from .logger import logger
from ...domain.plugin import DruidPlugin


class DruidBaseClient(ABC):
    @abstractmethod
    def get_tables(self) -> List[Table]:
        raise NotImplementedError


class DruidClient(DruidBaseClient):
    def __init__(self, config: DruidPlugin) -> None:
        self.__config = config

    @property
    def get_tables(self) -> List[Table]:
        # Prepare
        tables = []

        # Connect
        connection = self._connect()

        # Execute
        sql_query = """
            SELECT "TABLE_CATALOG", "TABLE_SCHEMA", "TABLE_NAME", "TABLE_TYPE", "IS_JOINABLE", "IS_BROADCAST"
            FROM INFORMATION_SCHEMA.TABLES
        """
        try:
            cursor = connection.cursor()
            cursor.execute(sql_query)
        except Exception as e:
            raise DataSourceError(f"Couldn't execute Druid query: {sql_query}") from e

        # Transform
        try:
            for row in cursor:
                tables.append(Table(row[0], row[1], row[2], row[3], row[4], row[5]))
        except Exception as e:
            raise MappingDataError("Couldn't transform Druid result to Table model") from e

        # Disconnect
        self._disconnect(connection)

        # Return
        return tables

    def _connect(self):
        """
        Initialise the connection to a druid cluster.
        :return: a druid connection
        """
        # Log it
        logger.debug(f"Initialise connection to Druid cluster: {self.__config.host}")

        # !!
        try:
            # Connect
            connection = connect(host=self.__config.host, port=self.__config.port, path='/druid/v2/sql/', scheme='http')
        except Exception as e:
            # Throw
            raise DataSourceConnectionError("Couldn't connect to Druid") from e

        # Return
        return connection

    def _disconnect(self, connection):
        """
        Close all connections to the druid cluster.
        :param connection: The connection to be closed
        :return:
        """
        # Log it
        logger.debug(f"Initialise disconnect from Druid cluster: {self.__config.host}")

        # Disconnect
        connection.close()
