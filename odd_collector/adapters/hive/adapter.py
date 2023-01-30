from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator import HiveGenerator

from odd_collector.adapters.hive.client import HiveClient
from odd_collector.adapters.hive.logger import logger
from odd_collector.adapters.hive.mappers.base_table import map_base_table
from odd_collector.adapters.hive.mappers.connect_views import connect_views
from odd_collector.adapters.hive.mappers.database import map_database
from odd_collector.adapters.hive.models.view import View
from odd_collector.domain.plugin import HivePlugin


class Adapter(AbstractAdapter):
    def __init__(self, config: HivePlugin) -> None:
        self.client = HiveClient(config.host, config.port)
        self.database = config.database

        # TODO: netloc
        self._generator = HiveGenerator(
            host_settings=f"{config.host}", databases=config.database
        )

    def get_data_source_oddrn(self) -> str:
        return self._generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        logger.info(f"Start collecting metadata for {self.database} database")
        tables = []

        with self.client.connect() as client:
            tables.extend(self.client.get_all_tables(self.database, client))

        table_entities = [map_base_table(table, self._generator) for table in tables]
        database_entity = map_database(
            self.database, self._generator, [tbl.oddrn for tbl in table_entities]
        )

        views = [view for view in tables if isinstance(view, View)]
        all_entities = [database_entity, *table_entities]

        connect_views(views, table_entities)

        logger.success(f"Collected {len(all_entities)} entities.")

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=all_entities,
        )
