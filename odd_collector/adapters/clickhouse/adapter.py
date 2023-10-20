from typing import List

from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import ClickHouseGenerator, Generator

from ...domain.plugin import ClickhousePlugin
from .mappers.tables import map_table
from .repository import ClickHouseRepository


class Adapter(BaseAdapter):
    generator: ClickHouseGenerator
    config: ClickhousePlugin

    def __init__(self, config: ClickhousePlugin) -> None:
        super().__init__(config)
        self.db = config.database
        self.repository = ClickHouseRepository(config)

    def create_generator(self) -> Generator:
        return ClickHouseGenerator(
            host_settings=f"{self.config.host}", databases=self.config.database
        )

    def get_data_entities(self) -> List[DataEntity]:
        records = self.repository.get_records()
        return map_table(
            self.generator,
            records.tables,
            records.columns,
            records.integration_engines,
            self.db,
        )

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )
