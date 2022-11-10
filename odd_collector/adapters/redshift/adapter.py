import logging
import traceback
from typing import List

import psycopg2
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import RedshiftGenerator

from .mappers import (
    ColumnMetadataNamedtuple_QUERY,
    ColumnMetadataNamedtupleExternal_QUERY,
    ColumnMetadataNamedtupleRedshift_QUERY,
    MetadataNamedtuple_QUERY,
    MetadataNamedtupleAll_QUERY,
    MetadataNamedtupleExternal_QUERY,
    MetadataNamedtupleInfo_QUERY,
    MetadataNamedtupleRedshift_QUERY,
    PrimaryKeys_QUERY,
)
from .mappers.metadata import MetadataColumns, MetadataTables
from .mappers.tables import map_table
from ...domain.plugin import RedshiftPlugin
from .logger import logger


class Adapter(AbstractAdapter):
    __connection = None
    __cursor = None

    def __init__(self, config: RedshiftPlugin) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__schemas = config.schemas

        self._data_source = f"postgresql://{self.__user}:{self.__password.get_secret_value()}@{self.__host}:{self.__port}/{self.__database}?connect_timeout=10"
        self.__oddrn_generator = RedshiftGenerator(
            host_settings=f"{self.__host}", databases=self.__database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        logger.debug("start fetching data")
        logger.debug(f'Schemas for filter: {self.__schemas or "Were not set"}')
        try:
            self.__connect()

            mtables: MetadataTables = MetadataTables(
                self.__execute(MetadataNamedtuple_QUERY(self.__schemas)),
                self.__execute(MetadataNamedtupleAll_QUERY(self.__schemas)),
                self.__execute(MetadataNamedtupleRedshift_QUERY(self.__schemas)),
                self.__execute(MetadataNamedtupleExternal_QUERY(self.__schemas)),
                self.__execute(MetadataNamedtupleInfo_QUERY(self.__schemas)),
            )

            mcolumns: MetadataColumns = MetadataColumns(
                self.__execute(ColumnMetadataNamedtuple_QUERY(self.__schemas)),
                self.__execute(ColumnMetadataNamedtupleRedshift_QUERY(self.__schemas)),
                self.__execute(ColumnMetadataNamedtupleExternal_QUERY(self.__schemas)),
            )

            primary_keys = self.__execute(PrimaryKeys_QUERY)

            self.__disconnect()
            logger.debug(
                f"Load {len(mtables.items)} Datasets DataEntities from database"
            )

            return map_table(
                self.__oddrn_generator, mtables, mcolumns, primary_keys, self.__database
            )
        except Exception as e:
            logger.error("Failed to load metadata for tables", exc_info=True)
            self.__disconnect()
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )

    def __execute(self, query: str) -> List[tuple]:
        self.__cursor.execute(query)
        return self.__cursor.fetchall()

    def __connect(self):
        try:
            self.__connection = psycopg2.connect(self._data_source)
            self.__cursor = self.__connection.cursor()
        except psycopg2.Error as err:
            logger.error(err)
            logger.debug(traceback.format_exc())
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
