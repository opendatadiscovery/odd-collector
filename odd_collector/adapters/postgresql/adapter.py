from collections import defaultdict

from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models import DataEntity
from odd_models.models import DataEntityList
from oddrn_generator import PostgresqlGenerator

from odd_collector.adapters.postgresql.models import Table
from odd_collector.domain.plugin import PostgreSQLPlugin

from .logger import logger
from .mappers.database import map_database
from .mappers.schemas import map_schema
from .mappers.tables import map_tables
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
            schema_entities: list[DataEntity] = []

            all_table_entities: dict[str, DataEntity] = {}

            tables = repo.get_tables()
            schemas = repo.get_schemas()

            self.generator.set_oddrn_paths(**{"databases": self.config.database})

            tables_by_schema = defaultdict(list)
            for table in tables:
                tables_by_schema[table.table_schema].append(table)

            for schema in schemas:
                tables_per_schema: list[Table] = tables_by_schema.get(
                    schema.schema_name, []
                )
                table_entities_tmp = map_tables(self.generator, tables_per_schema)
                schema_entities.append(
                    map_schema(
                        self.generator, schema, list(table_entities_tmp.values())
                    )
                )
                all_table_entities |= table_entities_tmp

            database_entity = map_database(
                self.generator, self.config.database, schema_entities
            )

            create_lineage(tables, all_table_entities)

            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=[*all_table_entities.values(), *schema_entities, database_entity],
            )


def create_lineage(tables: list[Table], data_entities: dict[str, DataEntity]) -> None:
    views = [table for table in tables if table.table_type in ("v", "m")]

    for view in views:
        try:
            depending_entity = data_entities.get(view.as_dependency.uid)

            if depending_entity.data_transformer is None:
                continue

            for dependency in view.dependencies:
                if dependency_entity := data_entities.get(dependency.uid):
                    if (
                        dependency_entity.oddrn
                        not in depending_entity.data_transformer.inputs
                    ):
                        depending_entity.data_transformer.inputs.append(
                            dependency_entity.oddrn
                        )
        except Exception as e:
            logger.warning(f"Error creating lineage for {view.table_name} {e=}")
