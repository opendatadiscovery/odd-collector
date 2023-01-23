from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import RedshiftGenerator

from .repository import RedshiftRepository
from ...domain.plugin import RedshiftPlugin
from .logger import logger
from .mappers.metadata import MetadataTables, MetadataColumns
from .mappers.tables import map_table


class Adapter(AbstractAdapter):
    __connection = None
    __cursor = None

    def __init__(self, config: RedshiftPlugin) -> None:
        self.__database = config.database
        self.__repository = RedshiftRepository(config)
        self.__oddrn_generator = RedshiftGenerator(
            host_settings=config.host, databases=self.__database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> list[DataEntity]:
        try:
            mtables: MetadataTables = self.__repository.get_tables()
            mcolumns: MetadataColumns = self.__repository.get_columns()
            primary_keys = self.__repository.get_primary_keys()

            logger.debug(
                f"Load {len(mtables.items)} Datasets DataEntities from database"
            )

            return map_table(
                self.__oddrn_generator, mtables, mcolumns, primary_keys, self.__database
            )
        except Exception as e:
            logger.error("Failed to load metadata for tables", exc_info=True)
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=self.get_data_entities(),
        )

