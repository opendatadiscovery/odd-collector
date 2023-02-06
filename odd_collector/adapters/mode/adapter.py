from typing import Type
from urllib.parse import urlparse

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList

from odd_collector.domain.plugin import ModePlugin

from .generator import ModeGenerator
from .logger import logger
from .mappers.report import map_report
from .repository import ModeRepository, ModeRepositoryBase


class Adapter(AbstractAdapter):
    def __init__(
        self, config: ModePlugin, repo: Type[ModeRepositoryBase] = ModeRepository
    ):
        self.config = config
        self.repo = repo(config)
        host = urlparse(config.host).hostname
        self.generator = ModeGenerator(host_settings=host)

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        # mode collections(spaces) are not used in the current version of the adapter
        # collections = await self.repo.get_collections()

        logger.debug("Start collecting mode data")
        data_sources = await self.repo.get_data_sources()
        reports = await self.repo.get_reports_for_data_sources(data_sources)
        logger.debug("End collecting of mode data")
        entities = [map_report(self.generator, report) for report in reports]
        result = DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=entities
        )
        return result
