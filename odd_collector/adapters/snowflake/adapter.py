import logging

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import SnowflakeGenerator
from snowflake import connector

from .mappers import _columns_select, _tables_select
from .mappers.tables import map_table
from odd_collector.domain.database_enitites_extractor import (
    extract_schemas_entities_from_tables,
    map_db_service,
)


class Adapter(AbstractAdapter):
    __connection = None
    __cursor = None

    def __init__(self, config):
        self.__database = config.database
        self.__user = config.user
        self.__password = config.password
        self.__account = config.host.split(".snowflakecomputing.com")[0]
        self.__warehouse = config.warehouse
        self.__oddrn_generator = SnowflakeGenerator(
            host_settings=config.host,
            databases=self.__database,
        )

    def get_data_entity_list(self) -> DataEntityList:

        tables_entities = self.get_data_entities()
        schemas_entities = extract_schemas_entities_from_tables(
            tables_entities, self.__oddrn_generator
        )

        dbs_entity = map_db_service(
            tables_entities[0].metadata[0].metadata["table_catalog"],
            [schema_entity.oddrn for schema_entity in schemas_entities],
            "databases",
            self.__oddrn_generator,
        )
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*tables_entities, *schemas_entities, dbs_entity],
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> list[DataEntity]:
        try:
            self.__connect()

            tables = self.__execute(_tables_select)
            columns = self.__execute(_columns_select)

            return map_table(self.__oddrn_generator, tables, columns)
        except Exception as e:
            logging.error("Failed to load metadata for tables")
            logging.exception(e)
        finally:
            self.__disconnect()
        return []

    def __execute(self, query: str) -> list[tuple]:
        self.__cursor.execute(query)
        return self.__cursor.fetchall()

    def __connect(self):
        self.__connection = connector.connect(
            user=self.__user, password=self.__password, account=self.__account
        )
        self.__cursor = self.__connection.cursor().execute(
            f"USE DATABASE {self.__database}"
        )

    def __disconnect(self):
        try:
            if self.__cursor:
                self.__cursor.close()
        except Exception as e:
            logging.exception(e)
        try:
            if self.__connection:
                self.__connection.close()
        except Exception as e:
            logging.exception(e)
