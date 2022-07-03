import logging
from typing import List

from odd_models.models import DataEntity, DataEntityList
from odd_collector_sdk.domain.adapter import AbstractAdapter
from .vertica_generator import VerticaGenerator
from .vertica_db import VerticaDB


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__db = VerticaDB(config)
        self.__schema = None  # TODO

        # For testing, remove later
        self.db_querying()

        self.__oddrn_generator = VerticaGenerator(
            host_settings=config.host, databases=config.database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        try:
            self.__db.connect() # TODO use context manager 

            metadata_query = f"""            
                SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                OWNER_NAME,
                CREATE_TIME FROM v_catalog.tables WHERE table_schema = '{self.__schema}';
            """

            metadata = self.__db.execute(metadata_query)
            data_entities = []

            for table in metadata:
                data_entities.append(
                    DataEntity(
                        oddrn=None,
                        name=table[1],
                        type=None,
                        owner=table[2],
                        description=None,
                        metadata=[],
                    )
                )

            return data_entities

        except Exception as e:
            logging.error(f"Failed to load metadata for tables. {e}")

        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )

    def db_querying(self) -> None:  # TODO remove later
        """
        This method is just for debugging, remove it later
        """
        self.__db.connect()
        schemas_metadata = self.__db.execute("SELECT * FROM SCHEMATA;")

        logging.debug("SCHEMA METADATA:")
        for i in schemas_metadata:
            logging.debug(i)

        tables_metadata = self.__db.execute(
            """
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                OWNER_NAME,
                CREATE_TIME FROM v_catalog.tables WHERE table_schema = 'test';
        """
        )

        metadata = []

        for row in tables_metadata:
            metadata.append(
                {
                    "table_schema": row[0],
                    "table_name": row[1],
                    "owner_name": row[2],
                    "creation_time": row[3],
                }
            )

        logging.debug("METADATA:")
        logging.debug(metadata)
