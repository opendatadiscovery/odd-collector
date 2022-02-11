import logging
from typing import List

from clickhouse_driver import connect
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import ClickHouseGenerator

from odd_generic_collector.domain.adapter import AbstractAdapter
from odd_generic_collector.domain.plugin import ClickhousePlugin

from .mappers import _table_select, _column_select, _integration_engines_select
from .mappers.tables import map_table


class Adapter(AbstractAdapter):
    __connection = None
    __cursor = None

    def __init__(self, config: ClickhousePlugin) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__oddrn_generator = ClickHouseGenerator(host_settings=f"{self.__host}", databases=self.__database)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:
            self.__connect()
            params = {
                "database": self.__database
            }
            tables = self.__execute(_table_select, params)
            columns = self.__execute(_column_select, params)
            integration_engines = self.__execute(_integration_engines_select, params)
            return map_table(self.__oddrn_generator, tables, columns, integration_engines, self.__database)
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

    def __execute(self, query: str, params: dict = None) -> List[tuple]:
        self.__cursor.execute(query, params)
        records = self.__cursor.fetchall()
        return records

    def __connect(self):
        try:
            self.__connection = connect(
                database=self.__database,
                user=self.__user,
                password=self.__password,
                host=self.__host,
                port=self.__port
            )

            self.__cursor = self.__connection.cursor()

        except Exception as err:
            logging.error(err)
            raise DBException('Database error')

    def __disconnect(self):
        self.__close_cursor()
        self.__close_connection()

    def __close_cursor(self):
        try:
            if self.__cursor:
                self.__cursor.close()
        except Exception:
            pass

    def __close_connection(self):
        try:
            if self.__connection:
                self.__connection.close()
        except Exception:
            pass


class DBException(Exception):
    pass
