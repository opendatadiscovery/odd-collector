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
            for schema in mschemas.items:
                tables = [
                    mtable
                    for mtable in mtables.items
                    if mtable.schema_name == schema.schema_name
                ]
                # self.generator.set_oddrn_paths(**{"schemas": schema.schema_name})
                table_entities_tmp = (
                    map_tables(self.generator, tables) if tables else []
                )
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
            logger.error(f"Failed to load metadata for tables: {e}")

    @staticmethod
    def append_columns(mtables: MetadataTables, mcolumns: MetadataColumns):
        columns_by_table = {}
        for column in mcolumns.items:
            if column.table_name not in columns_by_table:
                columns_by_table[column.table_name] = []
            columns_by_table[column.table_name].append(column)

        for table in mtables.items:
            table.columns = columns_by_table.get(table.table_name, [])

    @staticmethod
    def append_primary_keys(mtables: MetadataTables, primary_keys: list[tuple]):
        grouped_pks = {}
        for pk in primary_keys:
            table_name, column_name = pk
            if table_name not in grouped_pks:
                grouped_pks[table_name] = []
            grouped_pks[table_name].append(column_name)

        for table in mtables.items:
            table.primary_keys = grouped_pks.get(table.table_name, [])
