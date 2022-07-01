import contextlib
import logging
from abc import ABC, abstractmethod
from typing import List, Union

import psycopg2
from psycopg2 import sql

from odd_collector.adapters.postgresql.mappers.exceptions import DbPostgreSQLException


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

    def execute(self, query: Union[str, sql.Composed]) -> List[tuple]:
        self.__cursor.execute(query)
        records = self.__cursor.fetchall()
        return records

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

        except psycopg2.Error as err:
            logging.error(err)
            raise DbPostgreSQLException("Database error. Troubles with connecting")

    def __disconnect(self) -> None:
        try:
            if self.__cursor:
                self.__cursor.close()
            if self.__connection:
                self.__connection.close()
        except (psycopg2.OperationalError, psycopg2.InternalError) as err:
            logging.error(f'Error in disconnecting from database = {err}')
            raise DbPostgreSQLException("Database error. Troubles with disconnecting")
