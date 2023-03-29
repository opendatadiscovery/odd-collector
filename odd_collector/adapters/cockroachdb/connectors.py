import contextlib
from abc import ABC, abstractmethod

import psycopg2
from odd_collector_sdk.errors import DataSourceConnectionError, DataSourceError

from odd_collector.adapters.cockroachdb.logger import logger
from odd_collector.domain.plugin import CockroachDBPlugin


class AbstractConnector(ABC):  # TODO: Create one abstract connector for all adapters
    @abstractmethod
    def connection(self):
        pass


class CockroachDbSQLConnector(AbstractConnector):
    __connection = None
    __cursor = None

    def __init__(self, config: CockroachDBPlugin) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password

    @contextlib.contextmanager
    def connection(self):
        self.__connect()
        yield self.__cursor
        self.__disconnect()

    def __connect(self):
        try:
            logger.debug("Connecting to CockroachDb")
            self.__connection = psycopg2.connect(
                dbname=self.__database,
                user=self.__user,
                password=self.__password.get_secret_value(),
                host=self.__host,
                port=self.__port,
            )
            self.__cursor = self.__connection.cursor()
        except psycopg2.Error as e:
            raise DataSourceConnectionError(f"CockroachDb connection error {e}") from e

    def __disconnect(self) -> None:
        try:
            logger.debug("Disconnecting from CockroachDb")
            if self.__cursor:
                self.__cursor.close()
            if self.__connection:
                self.__connection.close()
        except (psycopg2.OperationalError, psycopg2.InternalError) as e:
            raise DataSourceError("Error in disconnecting from database") from e
