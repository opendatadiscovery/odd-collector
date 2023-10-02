from collections import defaultdict
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import RedshiftGenerator, Generator

from .mappers.database import map_database
from .mappers.schema import map_schema
from ...domain.plugin import RedshiftPlugin
from .logger import logger
from .mappers.metadata import MetadataSchemas, MetadataTables, MetadataColumns
from .mappers.tables import map_tables
from .repository import RedshiftRepository


class Adapter(BaseAdapter):
    def __init__(self, config: RedshiftPlugin) -> None:
        super().__init__(config)
        self.database = config.database
        self.repository = RedshiftRepository(config)

    def create_generator(self) -> Generator:
        return RedshiftGenerator(
            host_settings=self.config.host,
            databases=self.config.database,
        )

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        try:
            mschemas: MetadataSchemas = self.repository.get_schemas()
            mtables: MetadataTables = self.repository.get_tables()
            mcolumns: MetadataColumns = self.repository.get_columns()
            primary_keys = self.repository.get_primary_keys()
            self.append_columns(mtables, mcolumns)
            self.append_primary_keys(mtables, primary_keys)

            table_entities: list[DataEntity] = []
            schema_entities: list[DataEntity] = []
            database_entities: list[DataEntity] = []

            self.generator.set_oddrn_paths(**{"databases": self.database})

            tables_by_schema = defaultdict(list)
            for mtable in mtables.items:
                tables_by_schema[mtable.schema_name].append(mtable)

            for schema in mschemas.items:
                tables = tables_by_schema.get(schema.schema_name, [])
                table_entities_tmp = map_tables(self.generator, tables)
                schema_entities.append(
                    map_schema(self.generator, schema, table_entities_tmp)
                )
                table_entities.extend(table_entities_tmp)

            database_entities.append(
                map_database(self.generator, self.database, schema_entities)
            )
            return DataEntityList(
                data_source_oddrn=self.get_data_source_oddrn(),
                items=[
                    *table_entities,
                    *schema_entities,
                    *database_entities,
                ],
            )
        except Exception as e:
            logger.error(f"Failed to load metadata for tables: {e}", exc_info=True)

    @staticmethod
    def append_columns(mtables: MetadataTables, mcolumns: MetadataColumns):
        columns_by_table = defaultdict(list)
        for column in mcolumns.items:
            columns_by_table[(column.schema_name, column.table_name)].append(column)

        for table in mtables.items:
            table.columns = columns_by_table.get(
                (table.schema_name, table.table_name), []
            )

    @staticmethod
    def append_primary_keys(mtables: MetadataTables, primary_keys: list[tuple]):
        grouped_pks = defaultdict(list)
        for pk in primary_keys:
            schema_name, table_name, column_name = pk
            grouped_pks[(schema_name, table_name)].append(column_name)

        for table in mtables.items:
            table.primary_keys = grouped_pks.get(
                (table.schema_name, table.table_name), []
            )
