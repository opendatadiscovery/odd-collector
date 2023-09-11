from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import RedshiftGenerator, Generator

from ...domain.plugin import RedshiftPlugin
from .logger import logger
from .mappers.metadata import MetadataColumns, MetadataTables
from .mappers.tables import map_table
from .repository import RedshiftRepository


class Adapter(BaseAdapter):
    def __init__(self, config: RedshiftPlugin) -> None:
        super().__init__(config)
        self.database = config.database
        self.repository = RedshiftRepository(config)

    def create_generator(self) -> Generator:
        return RedshiftGenerator(
            host_settings=self.config.host,
            databases=self.config.database,
        )

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    def get_data_entities(self) -> list[DataEntity]:
        try:
            mtables: MetadataTables = self.repository.get_tables()
            mcolumns: MetadataColumns = self.repository.get_columns()
            primary_keys = self.repository.get_primary_keys()

            logger.debug(
                f"Load {len(mtables.items)} Datasets DataEntities from database"
            )

            return map_table(
                self.generator, mtables, mcolumns, primary_keys, self.database
            )
        except Exception as e:
            logger.error(f"Failed to load metadata for tables: {e}")
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )
