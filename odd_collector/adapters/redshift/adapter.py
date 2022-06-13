import logging

import psycopg2
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import RedshiftGenerator

from .mappers import (
    MetadataNamedtuple_QUERY,
    MetadataNamedtupleAll_QUERY,
    MetadataNamedtupleRedshift_QUERY,
    MetadataNamedtupleExternal_QUERY,
    MetadataNamedtupleInfo_QUERY,
    ColumnMetadataNamedtuple_QUERY,
    ColumnMetadataNamedtupleRedshift_QUERY,
    ColumnMetadataNamedtupleExternal_QUERY,
)
from .mappers.metadata import MetadataTables, MetadataColumns
from .mappers.tables import map_table


from odd_collector_sdk.domain.adapter import AbstractAdapter

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
        self.__schemas = config.schemas

        self._data_source = f"postgresql://{self.__user}:{self.__password}@{self.__host}:{self.__port}/{self.__database}?connect_timeout=10"
        self.__oddrn_generator = RedshiftGenerator(
            host_settings=f"{self.__host}", databases=self.__database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:
            self.__connect()

            mtables: MetadataTables = MetadataTables(
                self.__execute(MetadataNamedtuple_QUERY),
                self.__execute(MetadataNamedtupleAll_QUERY),
                self.__execute(MetadataNamedtupleRedshift_QUERY),
                self.__execute(MetadataNamedtupleExternal_QUERY),
                self.__execute(MetadataNamedtupleInfo_QUERY),
            )

            mcolumns: MetadataColumns = MetadataColumns(
                self.__execute(ColumnMetadataNamedtuple_QUERY),
                self.__execute(ColumnMetadataNamedtupleRedshift_QUERY),
                self.__execute(ColumnMetadataNamedtupleExternal_QUERY),
            )

            self.__disconnect()
            logging.info(
                f"Load {len(mtables.items)} Datasets DataEntities from database"
            )

            return map_table(self.__oddrn_generator, mtables, mcolumns, self.__database, self.__schemas)
        except Exception as e:
            logging.error("Failed to load metadata for tables")
            logging.exception(e)
            self.__disconnect()
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )

    def __execute(self, query: str) -> List[tuple]:
        self.__cursor.execute(query)
        records = self.__cursor.fetchall()
        return records

    def __connect(self):
        try:
            self.__connection = psycopg2.connect(self._data_source)
            self.__cursor = self.__connection.cursor()
        except psycopg2.Error as err:
            logging.error(err)
            raise DBException("Database error")
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
