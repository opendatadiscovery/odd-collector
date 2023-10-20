from collections import defaultdict

from funcy import lpluck_attr
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntityList
from oddrn_generator import Generator, MssqlGenerator

from odd_collector.domain.plugin import MSSQLPlugin

from .mappers.database import map_database
from .mappers.schemas import map_schemas
from .mappers.tables import map_table
from .mappers.views import map_view
from .repository import ConnectionConfig, MssqlRepository


class Adapter(BaseAdapter):
    config: MSSQLPlugin
    generator: MssqlGenerator
    repository: MssqlRepository

    def __init__(self, config: MSSQLPlugin) -> None:
        super().__init__(config)
        self.repository = MssqlRepository(
            ConnectionConfig(
                server=config.host,
                database=config.database,
                user=config.user,
                password=config.password,
                port=config.port,
            )
        )

    def create_generator(self) -> Generator:
        return MssqlGenerator(
            host_settings=f"{self.config.host}", databases=self.config.database
        )

    def get_data_entity_list(self) -> DataEntityList:
        data_source_oddrn = self.get_data_source_oddrn()
        generator = self.generator
        database = self.config.database

        with self.repository as repository:
            columns = repository.get_columns()
            schemas: dict[str, set[str]] = defaultdict(set)

            tables_data_entities = []
            for table in repository.get_tables():
                table.columns = columns.get_columns_for(
                    table.table_catalog, table.table_schema, table.table_name
                )
                table_data_entity = map_table(table, generator)

                tables_data_entities.append(table_data_entity)
                schemas[table.table_schema].add(table_data_entity.oddrn)

            views_data_entities = []
            for view in repository.get_views():
                view.columns = columns.get_columns_for(
                    view.view_catalog, view.view_schema, view.view_name
                )
                view_data_entity = map_view(view, generator)

                views_data_entities.append(view_data_entity)
                schemas[view.view_schema].add(view_data_entity.oddrn)

            schema_entities = list(map_schemas(schemas, generator))
            database_entity = map_database(
                database, lpluck_attr("oddrn", schema_entities), generator
            )

            return DataEntityList(
                data_source_oddrn=data_source_oddrn,
                items=[
                    *tables_data_entities,
                    *views_data_entities,
                    *schema_entities,
                    database_entity,
                ],
            )
