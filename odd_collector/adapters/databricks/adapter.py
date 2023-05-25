from odd_collector_sdk.errors import MappingDataError
from odd_collector.domain.plugin import DatabricksPlugin
from odd_collector_sdk.domain.adapter import AsyncAbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator import DatabricksUnityCatalogGenerator
from .mappers.schema import map_schema
from .mappers.table import map_table
from .mappers.catalog import map_catalog
from .client import DatabricksRestClient
from funcy import group_by
from collections import defaultdict
import asyncio


class Adapter(AsyncAbstractAdapter):
    def __init__(self, config: DatabricksPlugin) -> None:
        self.oddrn_generator = DatabricksUnityCatalogGenerator(
            host_settings=config.workspace
        )
        self.client = DatabricksRestClient(config)
        self.catalogs = config.catalogs

    def get_data_source_oddrn(self) -> str:
        return self.oddrn_generator.get_data_source_oddrn()

    async def get_data_entity_list(self) -> DataEntityList:
        catalogs = await self.client.get_catalogs()
        schemas_per_catalog = await self._get_schemas_per_catalog(catalogs)
        metadata = await self._get_tables_metadata(schemas_per_catalog)
        catalog_entities: list[DataEntity] = []
        schema_entities: list[DataEntity] = []
        tables_entities: list[DataEntity] = []

        try:
            for catalog_name in metadata:
                if not self.catalogs or catalog_name in self.catalogs:
                    catalog = metadata[catalog_name]
                    schema_entities_tmp: list[DataEntity] = []
                    for schema_name in catalog:
                        schema = catalog[schema_name]
                        tables_entities_tmp: list[DataEntity] = []
                        tables_entities_tmp.extend(
                            [map_table(self.oddrn_generator, table) for table in schema]
                        )
                        schema_entities_tmp.append(
                            map_schema(
                                self.oddrn_generator, schema_name, tables_entities_tmp
                            )
                        )
                        tables_entities.extend(tables_entities_tmp)
                    schema_entities.extend(schema_entities_tmp)
                    catalog_entities.append(
                        map_catalog(
                            self.oddrn_generator, catalog_name, schema_entities_tmp
                        )
                    )
        except Exception as e:
            raise MappingDataError(f"Error during mapping: {e}") from e

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*tables_entities, *schema_entities, *catalog_entities],
        )

    async def _get_schemas_per_catalog(self, catalogs: list[str]) -> list[tuple]:
        response = await asyncio.gather(
            *[self.client.get_schemas(catalog) for catalog in catalogs]
        )
        schemas = [item for catalog in response for item in catalog if item[1] not in ["information_schema"]]
        return schemas

    async def _get_tables_metadata(
        self, schemas_per_catalog: list[tuple]
    ) -> defaultdict[str, defaultdict]:
        tables = []
        for catalog, schema in schemas_per_catalog:
            tables.extend(await self.client.get_tables(catalog, schema))
        metadata = self._group_metadata(tables)
        return metadata

    @staticmethod
    def _group_metadata(data: list[dict]) -> defaultdict[str, defaultdict]:
        grouped_metadata = defaultdict(lambda: defaultdict(list))
        for catalog, items in group_by(lambda x: x["catalog_name"], data).items():
            for item in items:
                grouped_metadata[catalog][item["schema_name"]].append(item)
        return grouped_metadata
