from odd_collector_sdk.errors import MappingDataError
from odd_collector.domain.plugin import DuckDBPlugin
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import DuckDBGenerator
from .mappers.schema import map_schema
from .mappers.table import map_table
from .mappers.catalog import map_catalog
from .client import DuckDBClient


class Adapter(AbstractAdapter):
    def __init__(self, config: DuckDBPlugin) -> None:
        self.oddrn_generator = DuckDBGenerator(host_settings=config.host)
        self.paths = config.paths
        self.client = DuckDBClient(config.paths)

    def get_data_source_oddrn(self) -> str:
        return self.oddrn_generator.get_data_source_oddrn()

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
                    self.oddrn_generator.set_oddrn_paths(
                        catalogs=catalog,
                        schemas=schema,
                    )
                    for table in tables:
                        columns = client.get_columns_metadata(
                            connection, catalog, schema, table["table_name"]
                        )
                        tables_entities_tmp.append(
                            map_table(self.oddrn_generator, table, columns)
                        )
                    schema_entities_tmp.append(
                        map_schema(self.oddrn_generator, schema, tables_entities_tmp)
                    )
                    tables_entities.extend(tables_entities_tmp)
                catalog_entities.append(
                    map_catalog(
                        self.oddrn_generator,
                        catalog,
                        schema_entities_tmp,
                        client.db_files[catalog],
                    )
                )
                schema_entities.extend(schema_entities_tmp)
        except Exception as e:
            raise MappingDataError(f"Error during mapping: {e}") from e

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*tables_entities, *schema_entities, *catalog_entities],
        )
