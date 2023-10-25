from typing import List, Optional, Tuple, Type

from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import Generator, SnowflakeGenerator

from odd_collector.domain.plugin import SnowflakePlugin

from .client import SnowflakeClient, SnowflakeClientBase
from .domain import Pipe, Table, View
from .logger import logger
from .map import map_database, map_pipe, map_schemas, map_table, map_view


class Adapter(BaseAdapter):
    config: SnowflakePlugin
    generator: SnowflakeGenerator

    def __init__(
        self,
        config: SnowflakePlugin,
        client: Optional[Type[SnowflakeClientBase]] = SnowflakeClient,
    ):
        self._database_name = config.database.upper()
        self._client = client(config)

        super().__init__(config)

    def create_generator(self) -> Generator:
        return SnowflakeGenerator(
            host_settings=self.config.host,
            databases=self._database_name,
        )

    def get_data_entity_list(self) -> DataEntityList:
        pipes_entities = []

        # TODO: Create more user-friendly error messages and handle them
        try:
            raw_pipes = self._client.get_raw_pipes()
            raw_stages = self._client.get_raw_stages()
            pipes: list[Pipe] = []
            for raw_pipe in raw_pipes:
                pipes.extend(
                    Pipe(
                        name=raw_pipe.pipe_name,
                        definition=raw_pipe.definition,
                        stage_url=raw_stage.stage_url,
                        stage_type=raw_stage.stage_type,
                        downstream=raw_pipe.downstream,
                    )
                    for raw_stage in raw_stages
                    if raw_pipe.stage_full_name == raw_stage.stage_full_name
                )
            pipes_entities = [map_pipe(pipe, self.generator) for pipe in pipes]
        except Exception as e:
            logger.warning(f"Can't get pipes and stages. {e}")

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

            all_entities = [
                *tables_entities,
                *schemas_entities,
                database_entity,
                *pipes_entities,
            ]

            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(), items=all_entities
            )
        except Exception as e:
            raise MappingDataError("Error during mapping") from e

    def _get_tables_entities(
        self, tables: List[Table]
    ) -> List[Tuple[Table, DataEntity]]:
        result = []

        for table in tables:
            if isinstance(table, View):
                result.append((table, map_view(table, self.generator)))
            else:
                result.append((table, map_table(table, self.generator)))

        return result

    def _get_schemas_entities(
        self, tables_with_entities: List[Tuple[Table, DataEntity]]
    ) -> List[DataEntity]:
        return map_schemas(tables_with_entities, self.generator)

    def _get_database_entity(self, schemas: List[DataEntity]) -> DataEntity:
        return map_database(self._database_name, schemas, self.generator)
