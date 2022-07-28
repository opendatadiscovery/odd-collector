import logging
from odd_models.models import DataEntity, DataEntityList
from typing import List
from .db_exception import DBException
from .mappers.schemas import map_collection
from oddrn_generator import MongoGenerator
from odd_collector_sdk.domain.adapter import AbstractAdapter

from .mongo_repository import MongoRepository


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__database = config.database
        self.__mongo_repository = MongoRepository(config)
        self.__oddrn_generator = MongoGenerator(
            host_settings=f"{config.host}", databases=self.__database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> List[DataEntity]:
        """
        This function will convert a dictionary schema into
        an odd list of data entities
        """
        try:
            schemas = self.__mongo_repository.retrieve_schemas()
            return map_collection(self.__oddrn_generator, schemas, self.__database)
        except DBException as db_e:
            logging.error("Failed retrieve data from Database")
        except Exception as e:
            logging.error("Failed to load metadata for tables")
            logging.exception(e)
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )
