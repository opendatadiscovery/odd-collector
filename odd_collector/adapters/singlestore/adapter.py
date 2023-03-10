from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import (
    DataEntity,
    DataEntityGroup,
    DataEntityList,
    DataEntityType,
)
from oddrn_generator import SingleStoreGenerator

from .logger import logger
from .mappers.tables import map_tables
from .mappers.views import map_views
from .singlestore_repository import SingleStoreRepository


class Adapter(AbstractAdapter):
    def __init__(self, config) -> None:
        self.__config = config
        self.__singlestore_repository = SingleStoreRepository(config)
        self.__oddrn_generator = SingleStoreGenerator(
            host_settings=f"{self.__config.host}", databases=self.__config.database
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entities(self) -> list[DataEntity]:
        try:
            tables = self.__singlestore_repository.get_tables()
            views = self.__singlestore_repository.get_views()
            columns = self.__singlestore_repository.get_columns()
            logger.info(
                f"Load {len(tables) + len(views)} Datasets DataEntities from database"
            )
            return self._map_entities(tables, views, columns)
        except Exception:
            logger.error("Failed to load metadata for tables")
            logger.exception(Exception)
        return []

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )

    def _map_entities(
        self, tables: list[tuple], views: list[tuple], columns: list[tuple]
    ) -> list[DataEntity]:
        data_entities: list[DataEntity] = []

        data_entities.extend(
            map_tables(self.__oddrn_generator, tables, columns, self.__config.database)
        )
        data_entities.extend(
            map_views(self.__oddrn_generator, views, columns, self.__config.database)
        )
        data_entities.append(
            DataEntity(
                oddrn=self.__oddrn_generator.get_oddrn_by_path("databases"),
                name=self.__config.database,
                type=DataEntityType.DATABASE_SERVICE,
                metadata=[],
                data_entity_group=DataEntityGroup(
                    entities_list=[de.oddrn for de in data_entities]
                ),
            )
        )
        return data_entities
