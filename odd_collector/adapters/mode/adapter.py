import re

from typing import Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList

from odd_collector.domain.plugin import ModePlugin

from .repository import ModeRepositoryBase, ModeRepository
from .generator import ModeGenerator
from .mappers.report import map_report


class Adapter(AbstractAdapter):
    def __init__(
        self, config: ModePlugin, repo: Type[ModeRepositoryBase] = ModeRepository
    ):
        self.config = config
        self.repo = repo(config)
        re_host = re.sub(r"https?://", "", config.host)
        self.generator = ModeGenerator(host_settings=re_host)

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        # mode collections(spaces) are not used in the current version of the adapter
        # collections = await self.repo.get_collections()

        data_sources = await self.repo.get_data_sources()
        reports = await self.repo.get_reports_for_data_sources(data_sources)
        entities = [map_report(self.generator, report) for report in reports]
        result = DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=entities
        )
        return result
