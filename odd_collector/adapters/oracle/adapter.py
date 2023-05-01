from typing import Optional

from funcy import concat, lpluck_attr
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList
from oddrn_generator import OracleGenerator

from odd_collector.domain.plugin import OraclePlugin

from .mappers.database import map_database
from .mappers.table import map_table
from .mappers.view import map_view
from .repository.base_repository import Repository
from .repository.sqlalchemy_repository import SqlAlchemyRepository


class Adapter(AbstractAdapter):
    def __init__(self, config: OraclePlugin, repo: Optional[Repository] = None):
        self._generator = OracleGenerator(
            host_settings=config.host, databases=config.service, schemas=config.user
        )
        self._repo = repo or SqlAlchemyRepository(config)
        self._service = config.service

    def get_data_source_oddrn(self) -> str:
        return self._generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        tables_entities = [
            map_table(self._generator, table) for table in self._repo.get_tables()
        ]

        views_entities = [
            map_view(self._generator, view) for view in self._repo.get_views()
        ]

        oddrns = lpluck_attr("oddrn", concat(tables_entities, views_entities))
        database_entity = map_database(self._generator, self._service, oddrns)

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=tables_entities + views_entities + [database_entity],
        )
