from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import RedshiftGenerator, Generator

from .mappers.database import map_database
from .mappers.schema import map_schema
from ...domain.plugin import RedshiftPlugin
from .logger import logger
from .mappers.metadata import MetadataSchemas
from .mappers.tables import map_table
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
            primary_keys = self.repository.get_primary_keys()

            table_entities: list[DataEntity] = []
            schema_entities: list[DataEntity] = []
            database_entities: list[DataEntity] = []

            self.generator.set_oddrn_paths(**{"databases": self.config.database})

            for schema in mschemas.items:
                table_entities_tmp: list[DataEntity] = []
                schema_name = schema.schema_name
                self.generator.set_oddrn_paths(**{"schemas": schema_name})
                mtables = self.repository.get_tables(schema_name)
                for table in mtables.items:
                    mcolumns = self.repository.get_columns(
                        schema_name, table.table_name
                    )
                    table_entities_tmp.append(
                        map_table(self.generator, table, mcolumns, primary_keys)
                    )
                schema_entities.append(
                    map_schema(self.generator, schema, table_entities_tmp)
                )
                table_entities.extend(table_entities_tmp)

            database_entities.append(
                map_database(self.generator, self.config.database, schema_entities)
            )
            logger.info(database_entities)
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
