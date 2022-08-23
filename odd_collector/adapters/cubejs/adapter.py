from typing import Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList

from odd_collector.domain.plugin import CubeJSPlugin
from odd_collector.domain.predefined_data_source import create_predefined_datasource

from .generator import CubeJsGenerator
from .logger import logger
from .mappers.cube import map_cube
from .repository import CubeJsRepository, CubeJsRepositoryBase


class Adapter(AbstractAdapter):
    def __init__(
        self,
        config: CubeJSPlugin,
        client: Type[CubeJsRepositoryBase] = CubeJsRepository,
    ):
        self.config = config
        self.repo = client(config)
        self.datasource = create_predefined_datasource(config.predefined_datasource)
        self.generator = CubeJsGenerator(host_settings=config.host)

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        logger.debug("Start collecting")
        cubes = await self.repo.get_cubes()
        logger.debug("End collecting")
        entities = [map_cube(self.generator, self.datasource, cube) for cube in cubes]

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=entities
        )
