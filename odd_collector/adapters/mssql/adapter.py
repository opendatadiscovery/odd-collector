import contextlib
from typing import List

import pyodbc
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import MssqlGenerator

from odd_collector.domain.plugin import MSSQLPlugin

from .logger import logger
from .mappers import column_query, table_query
from odd_collector.domain.database_enitites_extractor import extract_schemas_entities_from_tables, map_db_service
from .mappers.tables import map_table


class Adapter(AbstractAdapter):
    __connection = None
    __cursor = None

    def __init__(self, config: MSSQLPlugin) -> None:
        # https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
        # https://github.com/mkleehammer/pyodbc/wiki/Install
        # https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Linux
        # cat /etc/odbcinst.ini

        # TODO: Encrypt=YES;TrustServerCertificate=YES for 18 ODBC Driver
        self.__data_source: str = (
            f"DRIVER={config.driver};SERVER={config.host};DATABASE={config.database};"
            f"UID={config.user};PWD={config.password}"
        )
        self.__oddrn_generator = MssqlGenerator(
            host_settings=f"{config.host}", databases=config.database
        )

    def get_data_entity_list(self) -> DataEntityList:
        items = self.get_data_entities()
        schemas_entities = extract_schemas_entities_from_tables(
            items, self.__oddrn_generator
        )
        dbs_entity = map_db_service(
            items[0].metadata[0].metadata["table_catalog"],
            [schema_entity.oddrn for schema_entity in schemas_entities],
            "databases",
            self.__oddrn_generator,
        )

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*items, *schemas_entities, dbs_entity],
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:
            self.__connect()

            tables = self.__execute(table_query)
            columns = self.__execute(column_query)

            return map_table(self.__oddrn_generator, tables, columns)
        except Exception:
            logger.error("Failed to load metadata for tables", exc_info=True)
        finally:
            self.__disconnect()
        return []

    def __execute(self, query: str) -> List[tuple]:
        self.__cursor.execute(query)
        return self.__cursor.fetchall()

    def __connect(self) -> None:
        try:
            self.__connection = pyodbc.connect(self.__data_source)
            self.__cursor = self.__connection.cursor()
        except Exception as e:
            raise DBException("Database error") from e
        return

    def __disconnect(self) -> None:
        with contextlib.suppress(Exception):
            if self.__cursor:
                self.__cursor.close()
        with contextlib.suppress(Exception):
            if self.__connection:
                self.__connection.close()
        return


class DBException(Exception):
    pass
