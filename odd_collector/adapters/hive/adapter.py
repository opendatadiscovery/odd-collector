from funcy import lcat, lpluck_attr
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator import HiveGenerator

from odd_collector.adapters.hive.models.view import View
from odd_collector.domain.plugin import HivePlugin

from .client import HiveClient
from .logger import logger
from .mappers.connect_views import connect_views
from .mappers.database import map_database
from .mappers.table import map_table
from .mappers.view import map_view


class Adapter(BaseAdapter):
    config: HivePlugin
    client: HiveClient
    generator: HiveGenerator

    def __init__(self, config: HivePlugin):
        super().__init__(config)
        self.client = HiveClient(config.connection_params)

    def create_generator(self) -> HiveGenerator:
        return HiveGenerator(
            host_settings=f"{self.config.connection_params.host}",
            databases=self.config.connection_params.database,
        )

    def get_data_entity_list(self) -> DataEntityList:
        database = self.config.connection_params.database

        logger.info(f"Start collecting metadata for {database=}")
        tables = []
        views = []

        with self.client as client:
            for table in client.get_tables(
                count_statistic=self.config.count_statistics
            ):
                if isinstance(table, View):
                    views.append(table)
                else:
                    tables.append(table)

        table_entities = [map_table(table, self.generator) for table in tables]
        view_entities = [map_view(view, self.generator) for view in views]

        database_entity = map_database(
            database,
            self.generator,
            lpluck_attr("oddrn", lcat([table_entities, view_entities])),
        )

        connect_views(views, lcat([table_entities, view_entities]))

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[database_entity, *table_entities, *view_entities],
        )
