from odd_collector.domain.plugin import PrestoPlugin
from odd_collector_sdk.domain.adapter import AbstractAdapter
from .presto_repository import PrestoRepository
from typing import Type, List
from pandas import DataFrame
from .mappers.models import ColumnMetadata
from .mappers.catalogs import map_catalog
from .mappers.schemas import map_schema
from .mappers.tables import map_table
from oddrn_generator.generators import PrestoGenerator
from odd_models.models import (
    DataEntity,
    DataEntityList
)


class Adapter(AbstractAdapter):
    def __init__(
            self, config: PrestoPlugin, repository: Type[PrestoRepository] = None
    ) -> None:
        repository = repository or PrestoRepository
        self.repository = repository(config)

        self.__oddrn_generator = PrestoGenerator(
            host_settings=self.repository.server_url
        )

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        columns_nodes = self.repository.get_columns()
        df = DataFrame(columns_nodes)
        df.columns = ColumnMetadata._fields
        nested_nodes = {
            tb_catalog_name: {tb_schema_name: {tb_name: tb[['column_name', 'type_name']].to_dict('records') for
                                               tb_name, tb in tb_schema.groupby('table_name')} for
                              tb_schema_name, tb_schema in tb_catalog.groupby('table_schem')} for
            tb_catalog_name, tb_catalog in df.groupby('table_cat')}

        cats_entities: List[DataEntity] = []
        schemas_entities: List[DataEntity] = []
        tables_entities: List[DataEntity] = []

        for catalog_node_name, schemas_node in nested_nodes.items():
            cats_entities.append(map_catalog(self.__oddrn_generator, catalog_node_name, schemas_node))

            self.__oddrn_generator.set_oddrn_paths(catalogs=catalog_node_name)

            for schema_node_name, tables_node in schemas_node.items():
                schemas_entities.append(map_schema(self.__oddrn_generator, schema_node_name, tables_node))

                self.__oddrn_generator.set_oddrn_paths(catalogs=catalog_node_name, schemas=schema_node_name)

                for table_node_name, columns_nodes in tables_node.items():
                    tables_entities.append(map_table(self.__oddrn_generator, table_node_name, columns_nodes))

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*cats_entities, *schemas_entities, *tables_entities],
        )
