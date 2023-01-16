from typing import List, Optional, Tuple, Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityList

from odd_collector.domain.plugin import SnowflakePlugin

from .client import SnowflakeClient, SnowflakeClientBase
from .domain import Table
from .generator import SnowflakeGenerator
from .map import map_database, map_schemas, map_table, map_view


class Adapter(AbstractAdapter):
    def __init__(
        self,
        config: SnowflakePlugin,
        client: Optional[Type[SnowflakeClientBase]] = SnowflakeClient,
    ):
        self._client = client(config)
        self._config = config
        self._database_name = config.database.upper()
        self._generator = SnowflakeGenerator(
            host_settings=config.host,
            databases=self._database_name,
        )

    def get_data_source_oddrn(self) -> str:
        return self._generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        tables = self._client.get_tables()

        tables_with_data_entities: List[
            Tuple[Table, DataEntity]
        ] = self._get_tables_entities(tables)

        try:
            tables_entities = [
                table_with_entity[1] for table_with_entity in tables_with_data_entities
            ]
            schemas_entities = self._get_schemas_entities(tables_with_data_entities)
            database_entity: DataEntity = self._get_database_entity(schemas_entities)

            all_entities = [*tables_entities, *schemas_entities, database_entity]

            dels = DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(), items=all_entities
            )
            return dels

        except Exception as e:
            raise MappingDataError("Error during mapping") from e

    def _get_tables_entities(
        self, tables: List[Table]
    ) -> List[Tuple[Table, DataEntity]]:
        result = []

        for table in tables:
            if table.table_type == "VIEW":
                result.append((table, map_view(table, self._generator)))
            else:
                result.append((table, map_table(table, self._generator)))

        return result

    def _get_schemas_entities(
        self, tables_with_entities: List[Tuple[Table, DataEntity]]
    ) -> List[DataEntity]:
        return map_schemas(tables_with_entities, self._generator)

    def _get_database_entity(self, schemas: List[DataEntity]) -> DataEntity:
        return map_database(self._database_name, schemas, self._generator)
