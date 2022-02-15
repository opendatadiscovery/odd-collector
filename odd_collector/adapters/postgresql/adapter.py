import logging

import psycopg2
from odd_models.models import DataEntity, DataEntityType, DataEntityList
from oddrn_generator import PostgresqlGenerator
from psycopg2 import sql


from odd_collector_sdk.domain.adapter import AbstractAdapter


from .mappers.tables import map_table
from .mappers import _table_select, _column_select

from typing import List


class Adapter(AbstractAdapter):
    __connection = None
    __cursor = None

    def __init__(self, config) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__oddrn_generator = PostgresqlGenerator(host_settings=f"{self.__host}", databases=self.__database)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:
            self.__connect()

            tables = self.__execute(_table_select)
            columns = self.__execute(_column_select)

            return map_table(self.__oddrn_generator, tables, columns, self.__database)
        except Exception as e:
            logging.error('Failed to load metadata for tables')
            logging.exception(e)
        finally:
            self.__disconnect()
        return []

    def get_data_entity_list(self) -> DataEntityList:
        print(self.get_data_entities())
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )

    def __execute(self, query: str) -> List[tuple]:
        self.__cursor.execute(query)
        records = self.__cursor.fetchall()
        return records

    def __execute_sql(self, query: sql.Composed) -> List[tuple]:
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
                port=self.__port
            )
            self.__cursor = self.__connection.cursor()

        except psycopg2.Error as err:
            logging.error(err)
            raise DBException('Database error')
        return

    def __disconnect(self):
        try:
            if self.__cursor:
                self.__cursor.close()
        except Exception:
            pass
        try:
            if self.__connection:
                self.__connection.close()
        except Exception:
            pass
        return


class DBException(Exception):
    pass
