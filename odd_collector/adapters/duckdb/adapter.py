from duckdb import IOException
from odd_collector_sdk.domain.adapter import BaseAdapter
from odd_collector_sdk.errors import MappingDataError
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import DuckDBGenerator, Generator

from odd_collector.domain.plugin import DuckDBPlugin

from .client import DuckDBClient
from .logger import logger
from .mappers.catalog import map_catalog
from .mappers.schema import map_schema
from .mappers.table import map_table


class Adapter(BaseAdapter):
    def __init__(self, config: DuckDBPlugin) -> None:
        super().__init__(config)
        self.paths = config.paths
        self.client = DuckDBClient(config.paths)

    def create_generator(self) -> Generator:
        return DuckDBGenerator(host_settings=self.config.host)

    def get_data_source_oddrn(self) -> str:
        return self.generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        client = self.client
        catalog_entities: list[DataEntity] = []
        schema_entities: list[DataEntity] = []
        tables_entities: list[DataEntity] = []

        try:
            for catalog in client.db_files:
                schema_entities_tmp = []
                connection = client.get_connection(catalog)
                schemas = client.get_schemas(connection, catalog)
                for schema in schemas:
                    tables_entities_tmp = []
                    tables = client.get_tables_metadata(connection, catalog, schema)
                    self.generator.set_oddrn_paths(
                        catalogs=catalog,
                        schemas=schema,
                    )
                    for table in tables:
                        columns = client.get_columns_metadata(
                            connection, catalog, schema, table.name
                        )
                        tables_entities_tmp.append(
                            map_table(self.generator, table, columns)
                        )
                    schema_entities_tmp.append(
                        map_schema(self.generator, schema, tables_entities_tmp)
                    )
                    tables_entities.extend(tables_entities_tmp)
                catalog_entities.append(
                    map_catalog(
                        self.generator,
                        catalog,
                        schema_entities_tmp,
                        client.db_files[catalog],
                    )
                )
                schema_entities.extend(schema_entities_tmp)
        except IOException as e:
            logger.error(f"Error during connection: {e}")
        except Exception as e:
            raise MappingDataError(f"Error during mapping: {e}") from e

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*tables_entities, *schema_entities, *catalog_entities],
        )
