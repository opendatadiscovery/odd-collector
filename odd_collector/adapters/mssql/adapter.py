from collections import defaultdict
from typing import Dict, Generator, Set

from funcy import lpluck_attr
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator import MssqlGenerator

from odd_collector.adapters.mssql.mappers.views import map_view
from odd_collector.domain.plugin import MSSQLPlugin

from .mappers.database import map_database
from .mappers.schemas import map_schemas
from .mappers.tables import map_table
from .mappers.views import map_view
from .repository import ConnectionConfig, DefaultConnector


class Adapter(AbstractAdapter):
    def __init__(self, config: MSSQLPlugin) -> None:
        self._generator = MssqlGenerator(
            host_settings=f"{config.host}", databases=config.database
        )
        self._cfg = config
        self._repo = DefaultConnector(
            ConnectionConfig(
                server=self._cfg.host,
                database=self._cfg.database,
                user=self._cfg.user,
                password=self._cfg.password,
                port=self._cfg.port,
            )
        )

    def get_data_entity_list(self) -> Generator[DataEntityList, None, None]:
        data_source_oddrn = self.get_data_source_oddrn()
        generator = self._generator

        columns = self._repo.get_columns()
        schemas: Dict[str, Set[str]] = defaultdict(set)

        tables_data_entities = []
        for table in self._repo.get_tables():
            table.columns = columns.get_columns_for(
                table.table_catalog, table.table_schema, table.table_name
            )
            table_data_entity = map_table(table, generator)

            tables_data_entities.append(table_data_entity)
            schemas[table.table_schema].add(table_data_entity.oddrn)

        views_data_entities = []
        for view in self._repo.get_views():
            view.columns = columns.get_columns_for(
                view.view_catalog, view.view_schema, view.view_name
            )
            view_data_entity = map_view(view, generator)

            views_data_entities.append(view_data_entity)
            schemas[view.view_schema].add(view_data_entity.oddrn)

        schema_entities = list(map_schemas(schemas, generator))
        database_entity = map_database(
            self._cfg.database, lpluck_attr("oddrn", schema_entities), generator
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

    def get_data_source_oddrn(self) -> str:
        return self._generator.get_data_source_oddrn()
