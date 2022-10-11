from typing import List, Type, Union

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from oddrn_generator.generators import PrestoGenerator, TrinoGenerator
from pandas import DataFrame

from odd_collector.domain.plugin import PrestoPlugin, TrinoPlugin

from .mappers.catalogs import map_catalog
from .mappers.models import ColumnMetadata, TableMetadata
from .mappers.schemas import map_schema
from .mappers.tables import map_table
from .presto_repository import PrestoRepository
from .presto_repository_base import PrestoRepositoryBase
from .trino_repository import TrinoRepository


class Adapter(AbstractAdapter):
    def __init__(
        self,
        config: Union[PrestoPlugin, TrinoPlugin],
        repository: Type[PrestoRepositoryBase] = None,
    ) -> None:
        if config.type == "presto":
            repository = repository or PrestoRepository
            generator = PrestoGenerator
        else:
            repository = repository or TrinoRepository
            generator = TrinoGenerator
        self.repository = repository(config)

        self.__oddrn_generator = generator(host_settings=self.repository.server_url)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_tables_df(self) -> DataFrame:
        tables_nodes = self.repository.get_tables()
        df_tables = DataFrame(tables_nodes)
        df_tables.columns = TableMetadata._fields
        return df_tables

    def get_columns_df(self) -> DataFrame:
        columns_nodes = self.repository.get_columns()
        df_column = DataFrame(columns_nodes)
        df_column.columns = ColumnMetadata._fields
        return df_column

    def get_nested_columns_nodes(self) -> dict:
        df_columns = self.get_columns_df()
        return {
            tb_catalog_name: {
                tb_schema_name: {
                    tb_name: tb[["column_name", "type_name"]].to_dict("records")
                    for tb_name, tb in tb_schema.groupby("table_name")
                }
                for tb_schema_name, tb_schema in tb_catalog.groupby("table_schem")
            }
            for tb_catalog_name, tb_catalog in df_columns.groupby("table_cat")
        }

    def get_data_entity_list(self) -> DataEntityList:
        nested_nodes = self.get_nested_columns_nodes()
        df_tables = self.get_tables_df()

        catalogs_entities: List[DataEntity] = []
        schemas_entities: List[DataEntity] = []
        tables_entities: List[DataEntity] = []

        for catalog_node_name, schemas_node in nested_nodes.items():
            catalogs_entities.append(
                map_catalog(self.__oddrn_generator, catalog_node_name, schemas_node)
            )

            self.__oddrn_generator.set_oddrn_paths(catalogs=catalog_node_name)

            for schema_node_name, tables_node in schemas_node.items():
                schemas_entities.append(
                    map_schema(self.__oddrn_generator, schema_node_name, tables_node)
                )

                self.__oddrn_generator.set_oddrn_paths(
                    catalogs=catalog_node_name, schemas=schema_node_name
                )

                for table_node_name, columns_nodes in tables_node.items():
                    tables_entities.append(
                        map_table(
                            self.__oddrn_generator,
                            table_node_name,
                            columns_nodes,
                            df_tables,
                            catalog_node_name,
                            schema_node_name,
                        )
                    )

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*catalogs_entities, *schemas_entities, *tables_entities],
        )
