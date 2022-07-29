import logging

import pyodbc
from oddrn_generator import OdbcGenerator
from pyodbc import Connection, Cursor

from .connection import connect_odbc
from .exception import DBException
from .mappers.tables import map_table
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList, DataEntity

from .odbc_repository import OdbcRepository


class Adapter(AbstractAdapter):

    def __init__(self, config) -> None:
        # https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
        # https://github.com/mkleehammer/pyodbc/wiki/Install
        # https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Linux
        # cat /etc/odbcinst.ini
        self.__oddrn_generator = OdbcGenerator(host_settings=f"{config.host}", databases=config.database)
        self.__odbc_repository = OdbcRepository(config)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        entities = self.get_data_entities()
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=entities,
        )

    def get_data_entities(self) -> list[DataEntity]:
        try:
            tables = self.__odbc_repository.get_tables()
            columns = self.__odbc_repository.get_columns()
            entities = map_table(self.__oddrn_generator, tables, columns)
            return entities
        except Exception as e:
            logging.error("Failed to load metadata for tables")
            logging.exception(e)

            return []
