import contextlib
import logging
from abc import ABC, abstractmethod
from typing import List, Union

import psycopg2
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import PostgresqlGenerator
from psycopg2 import sql

from .mappers.config import _table_select, _column_select
from .mappers.tables import map_table


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
            raise DBException("Database error. Troubles with connecting")

    def __disconnect(self) -> None:
        try:
            if self.__cursor:
                self.__cursor.close()
            if self.__connection:
                self.__connection.close()
        except (psycopg2.OperationalError, psycopg2.InternalError) as err:
            logging.error(f'Error in disconnecting from database = {err}')
            raise DBException("Database error. Troubles with disconnecting")


class Adapter(AbstractAdapter):

    def __init__(self, config) -> None:
        self.__database = config.database
        self.__postgresql_cursor = PostgreSQLConnector(config)
        self.__oddrn_generator = PostgresqlGenerator(
            host_settings=f"{config.host}", databases=self.__database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:

            with self.__postgresql_cursor.connection():
                tables = self.__postgresql_cursor.execute(_table_select)
                columns = self.__postgresql_cursor.execute(_column_select)

            return map_table(self.__oddrn_generator, tables, columns, self.__database)
        except Exception as e:
            logging.error("Failed to load metadata for tables")
            logging.exception(e)
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )


class DBException(Exception):
    pass
