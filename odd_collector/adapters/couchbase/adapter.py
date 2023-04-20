import logging

from odd_collector_sdk.domain.adapter import AsyncAbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator import CouchbaseGenerator
from .mappers.collections import map_collection

from .repository import CouchbaseRepository
from ...domain.plugin import CouchbasePlugin


class Adapter(AsyncAbstractAdapter):
    def __init__(self, config: CouchbasePlugin) -> None:
        self.__bucket = config.bucket
        self.__repository = CouchbaseRepository(config)
        self.__oddrn_generator = CouchbaseGenerator(
            host_settings=f"{config.host}", buckets=self.__bucket
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        items = []
        try:
            collections = await self.__repository.get_collections()
            items = map_collection(self.__oddrn_generator, collections, self.__bucket)
        except Exception as e:
            logging.error("Failed to load metadata for collections", exc_info=True)
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=items
        )
