from funcy import lpluck_attr
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList

from odd_collector.domain.plugin import FivetranPlugin

from oddrn_generator import FivetranGenerator
from .mappers.database import map_database
from .mappers.tables import map_tables
from .repository import FivetranRepository


class Adapter(AbstractAdapter):
    def __init__(self, config: FivetranPlugin):
        self._repo = FivetranRepository(config)
        self._generator = FivetranGenerator(
            host_settings=config.base_url,
            databases=self._repo.get_db_name(),
            schemas=config.schema_name,
        )

    def get_data_source_oddrn(self) -> str:
        return self._generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        tables_entities = map_tables(
            self._generator, self._repo.get_tables(), self._repo.get_columns()
        )

        oddrns = lpluck_attr("oddrn", tables_entities)
        database_entity = map_database(
            self._generator, self._repo.get_db_name(), oddrns
        )

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=tables_entities + [database_entity],
        )
