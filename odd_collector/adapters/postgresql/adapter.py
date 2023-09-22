from collections import defaultdict

from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models import DataEntity
from odd_models.models import DataEntityList
from oddrn_generator import PostgresqlGenerator

from odd_collector.domain.plugin import PostgreSQLPlugin

from .mappers.database import map_database
from .mappers.tables import map_tables
from .mappers.schemas import map_schema
from .repository import ConnectionParams, PostgreSQLRepository


class Adapter(BaseAdapter):
    config: PostgreSQLPlugin
    generator: PostgresqlGenerator

    def __init__(self, config: PostgreSQLPlugin) -> None:
        super().__init__(config)

    def create_generator(self) -> PostgresqlGenerator:
        return PostgresqlGenerator(
            host_settings=self.config.host, databases=self.config.database
        )

    def get_data_entity_list(self) -> DataEntityList:
        with PostgreSQLRepository(
            ConnectionParams.from_config(self.config), self.config.schemas_filter
        ) as repo:
            table_entities: list[DataEntity] = []
            schema_entities: list[DataEntity] = []
            tables = repo.get_tables()
            schemas = repo.get_schemas()
            self.generator.set_oddrn_paths(**{"databases": self.config.database})

            tables_by_schema = defaultdict(list)
            for table in tables:
                tables_by_schema[table.table_schema].append(table)

            for schema in schemas:
                tables_per_schema = tables_by_schema.get(schema.schema_name, [])
                table_entities_tmp = map_tables(self.generator, tables_per_schema)
                schema_entities.append(
                    map_schema(self.generator, schema, table_entities_tmp)
                )
                table_entities.extend(table_entities_tmp)

            database_entity = map_database(
                self.generator, self.config.database, schema_entities
            )

            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=[*table_entities, *schema_entities, database_entity],
            )
