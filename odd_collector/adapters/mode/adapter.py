from typing import Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList

from odd_collector.domain.plugin import ModePlugin

from .repository import ModeRepositoryBase, ModeRepository
from .generator import ModeGenerator
from .mappers.report import map_report


class Adapter(AbstractAdapter):
    def __init__(self,
                 config: ModePlugin,
                 repo: Type[ModeRepositoryBase] = ModeRepository):
        self.config = config
        self.repo = repo(config)
        self.generator = ModeGenerator(host_settings=config.host)

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    # TODO: complete this function
    async def get_data_entity_list(self) -> DataEntityList:
        reports = await self.repo.get_reports()

        entities = [map_report(self.generator, report) for report in reports]
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(), items=entities
        )
