from typing import Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import OdbcGenerator

from odd_collector.domain.plugin import OdbcPlugin

from .mappers.database import map_database
from .mappers.tables import map_base_table
from .repository.odbc_repository import Repository
from .repository.repository_base import RepositoryBase


class Adapter(AbstractAdapter):
    def __init__(
        self, config: OdbcPlugin, repository: Type[RepositoryBase] = Repository
    ) -> None:
        # https://docs.microsoft.com/en-us/sql/connect/odbc/linux-mac/installing-the-microsoft-odbc-driver-for-sql-server?view=sql-server-ver15
        # https://github.com/mkleehammer/pyodbc/wiki/Install
        # https://github.com/mkleehammer/pyodbc/wiki/Connecting-to-SQL-Server-from-Linux
        # cat /etc/odbcinst.ini

        self._config = config
        self._generator = OdbcGenerator(
            host_settings=f"{config.host}", databases=config.database
        )
        self._repository = repository(config)

    def get_data_source_oddrn(self) -> str:
        return self._generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        tables_entities = list(self._get_tables_entity())

        db_entity = self._get_database_entity()
        db_entity.data_entity_group.entities_list.extend(
            e.oddrn for e in tables_entities
        )

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=tables_entities + [db_entity],
        )

    def _get_database_entity(self):
        return map_database(self._generator, self._config.database)

    def _get_tables_entity(self) -> list[DataEntity]:
        base_tables = self._repository.get_data()

        try:
            for table in base_tables:
                yield map_base_table(self._generator, table)
        except Exception as e:
            raise MappingDataError(f"Could not map table") from e
