from funcy import lpluck_attr
from odd_collector_sdk.domain.adapter import AsyncAbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator import CouchbaseGenerator

from .mappers.bucket import map_bucket
from .mappers.collections import map_collections

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
        collections = await self.__repository.get_collections()
        collection_entities = map_collections(self.__oddrn_generator, collections)
        bucket_entity = map_bucket(
            self.__oddrn_generator,
            self.config.bucket,
            lpluck_attr("oddrn", collection_entities),
        )
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*collection_entities, bucket_entity],
        )
