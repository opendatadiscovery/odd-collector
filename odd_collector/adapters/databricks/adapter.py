from odd_collector_sdk.errors import MappingDataError
from odd_collector.domain.plugin import DatabricksPlugin
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import DatabricksUnityCatalogGenerator
from .mappers.schema import map_schema
from .mappers.table import map_table
from .mappers.catalog import map_catalog
from .client import DatabricksRestClient
from itertools import groupby


class Adapter(AbstractAdapter):
    def __init__(self, config: DatabricksPlugin) -> None:
        self.__oddrn_generator = DatabricksUnityCatalogGenerator(
            host_settings=config.workspace
        )
        self.__client = DatabricksRestClient(config)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        catalogs = await self.__client.get_catalogs()
        schemas_per_catalog = await self._get_schemas_per_catalog_list(catalogs)
        metadata_raw = await self._get_tables_metadata(schemas_per_catalog)
        metadata = self._group_metadata(metadata_raw)

        catalog_entities: list[DataEntity] = []
        schema_entities: list[DataEntity] = []
        tables_entities: list[DataEntity] = []

        try:
            for catalog_name in metadata:
                catalog = metadata[catalog_name]
                schema_entities_tmp: list[DataEntity] = []
                for schema_name in catalog:
                    schema = catalog[schema_name]
                    tables_entities_tmp: list[DataEntity] = []
                    tables_entities_tmp.extend(
                        [map_table(self.__oddrn_generator, table) for table in schema]
                    )
                    schema_entities_tmp.append(
                        map_schema(
                            self.__oddrn_generator, schema_name, tables_entities_tmp
                        )
                    )
                    tables_entities.extend(tables_entities_tmp)
                schema_entities.extend(schema_entities_tmp)
                catalog_entities.append(
                    map_catalog(
                        self.__oddrn_generator, catalog_name, schema_entities_tmp
                    )
                )
        except Exception as e:
            raise MappingDataError("Error during mapping") from e

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*tables_entities, *schema_entities, *catalog_entities],
        )

    async def _get_schemas_per_catalog_list(self, catalogs: list) -> list[tuple]:
        schemas = [
            (catalog, schema)
            for catalog in catalogs
            for schema in await self.__client.get_schemas(catalog)
        ]
        return schemas

    async def _get_tables_metadata(self, schemas_per_catalog: list) -> list[dict]:
        tables = [
            await self.__client.get_tables(catalog, schema)
            for catalog, schema in schemas_per_catalog
        ]
        tables = [table for schemas in tables for table in schemas]

        return tables

    @staticmethod
    def _group_metadata(data: list) -> dict:
        # Sort the data by catalog_name and schema_name
        sorted_data = sorted(data, key=lambda x: (x["catalog_name"], x["schema_name"]))

        # Group the sorted data by catalog_name and schema_name
        grouped_data = {}
        for key, group in groupby(
            sorted_data, key=lambda x: (x["catalog_name"], x["schema_name"])
        ):
            catalog_name, schema_name = key
            grouped_data.setdefault(catalog_name, {}).setdefault(
                schema_name, []
            ).extend(group)
        return grouped_data
