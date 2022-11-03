import traceback
from typing import List

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import MssqlGenerator

from odd_collector.domain.plugin import MSSQLPlugin

from .connector import ConnectionConfig, DefaultConnector
from .logger import logger
from .mappers.database import map_database
from .mappers.schemas import map_schemas
from .mappers.tables import map_tables
from .mappers.views import map_views
from .models import Column, Table, View


class Adapter(AbstractAdapter):
    def __init__(self, config: MSSQLPlugin) -> None:
        self._generator = MssqlGenerator(
            host_settings=f"{config.host}", databases=config.database
        )
        self._cfg = config

    def get_data_entity_list(self) -> DataEntityList:
        try:
            tables, columns, views = [], [], []

            connection_cfg = ConnectionConfig(
                server=self._cfg.host,
                database=self._cfg.database,
                user=self._cfg.user,
                password=self._cfg.password,
            )

            with DefaultConnector(connection_cfg) as conn:
                tables = conn.get_tables()
                columns = conn.get_columns()
                views = conn.get_views()

            tables_entities = self.get_table_entities(tables, columns)
            views_entities = self.get_views_entities(views, columns)
            schemas_entities = self.get_schemas_entities(tables, views)
            database_entities = self.get_database_entities(schemas_entities)

            de = DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=[
                    *tables_entities,
                    *views_entities,
                    *schemas_entities,
                    *database_entities,
                ],
            )

            return de
        except Exception as err:
            logger.debug(traceback.format_exc())
            raise err

    def get_data_source_oddrn(self) -> str:
        return self._generator.get_data_source_oddrn()

    def get_views_entities(self, views, columns) -> List[DataEntity]:
        return list(map_views(views, columns, self._generator))

    def get_table_entities(
        self, tables: List[Table], columns: List[Column]
    ) -> List[DataEntity]:
        return list(map_tables(tables, columns, self._generator))

    def get_schemas_entities(
        self, tables: List[Table], views: List[View]
    ) -> List[DataEntity]:
        return list(map_schemas(tables, views, self._generator))

    def get_database_entities(
        self, schema_entities: List[DataEntity]
    ) -> List[DataEntity]:
        return [
            map_database(
                self._cfg.database,
                [de.oddrn for de in schema_entities],
                self._generator,
            )
        ]
