import logging

import mysql.connector
from mysql.connector import errorcode
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import MysqlGenerator

from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from .mappers import _column_metadata, _column_table, _column_order_by, _table_select
from .mappers.tables import map_tables


class Adapter(AbstractAdapter):
    __connection = None
    __cursor = None

    def __init__(self, config) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__ssl_disabled = config.ssl_disabled
        self.__oddrn_generator = MysqlGenerator(host_settings=f"{self.__host}", databases=self.__database)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:
            self.__connect()

            tables = self.__execute(_table_select)
            columns = self.__query(_column_metadata, _column_table, _column_order_by)
            self.__disconnect()
            logging.info(f'Load {len(tables)} Datasets DataEntities from database')
            return map_tables(self.__oddrn_generator, tables, columns, self.__database)
        except Exception:
            logging.error('Failed to load metadata for tables')
            logging.exception(Exception)
            self.__disconnect()
        return []

    def get_data_entity_list(self) -> DataEntityList:
        print(self.get_data_entities())
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )

    def __query(self, columns: str, table: str, order_by: str) -> List[tuple]:
        return self.__execute(f'select {columns} from {table} order by {order_by}')

    def __execute(self, query: str) -> List[tuple]:
        self.__cursor.execute(query)
        records = self.__cursor.fetchall()
        return records

    # replace
    def __connect(self):
        try:
            self.__connection = mysql.connector.connect(
                host=self.__host, port=self.__port, database=self.__database,
                user=self.__user, password=self.__password, ssl_disabled=self.__ssl_disabled)
            self.__cursor = self.__connection.cursor()

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logging.error('Something is wrong with your user name or password')
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logging.error('Database does not exist')
            else:
                logging.error(err)
            raise DBException('Database error')
        return

    # replace
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
