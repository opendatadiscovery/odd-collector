import contextlib
import logging
from abc import ABC, abstractmethod

import psycopg2
from odd_collector.adapters.postgresql.exceptions import DbPostgreSQLException


class AbstractConnector(ABC):  # TODO: Create one abstract connector for all adapters
    @abstractmethod
    def connection(self):
        pass


class PostgreSQLConnector(AbstractConnector):
    __connection = None
    __cursor = None

    def __init__(self, config) -> None:
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
            self.__connection = psycopg2.connect(
                dbname=self.__database,
                user=self.__user,
                password=self.__password,
                host=self.__host,
                port=self.__port,
            )
            self.__cursor = self.__connection.cursor()
        except psycopg2.Error as e:
            logging.error("Error in __connect method", exc_info=True)
            raise DbPostgreSQLException(
                "Database error. Troubles with connecting"
            ) from e

    def __disconnect(self) -> None:
        try:
            if self.__cursor:
                self.__cursor.close()
            if self.__connection:
                self.__connection.close()
        except (psycopg2.OperationalError, psycopg2.InternalError) as e:
            logging.error("Error in disconnecting from database", exc_info=True)
            raise DbPostgreSQLException(
                "Database error. Troubles with disconnecting"
            ) from e
