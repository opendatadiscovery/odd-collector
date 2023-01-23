from typing import List, Type
from odd_collector.domain.plugin import DatabricksLakehousePlugin
from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntity, DataEntityList
from pandas import DataFrame
from oddrn_generator import DatabricksLakehouseGenerator
from .mappers.database import map_database
from .mappers.table import map_table
from .client import DatabricksRestClient


class Adapter(AbstractAdapter):
    def __init__(
            self,
            config: DatabricksLakehousePlugin, client: Type[DatabricksRestClient] = None
    ) -> None:
        client = client or DatabricksRestClient
        self.client = client(config)

        self.__oddrn_generator = DatabricksLakehouseGenerator(
            host_settings=self.client.get_server_host()
        )

    def get_tables_df(self):
        return DataFrame({'databaseName': ['test_database', 'test_database'], 'tableName': ['test_table', 'test_table'],
                          'columnName': ['id', 'value'], 'columnDataType': ['int', 'string']})

    @staticmethod
    def get_nested_columns_nodes(tables_df: DataFrame):
        return {
            tb_schema_name: {
                tb_name: tb[["columnName", "columnDataType"]].to_dict("records")
                for tb_name, tb in tb_schema.groupby("tableName")
            }
            for tb_schema_name, tb_schema in tables_df.groupby("databaseName")
        }

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        df_tables = self.get_tables_df()
        nested_nodes = self.get_nested_columns_nodes(df_tables)

        databases_entities: List[DataEntity] = []
        tables_entities: List[DataEntity] = []

        for database_node_name, tables_node in nested_nodes.items():
            databases_entities.append(
                map_database(self.__oddrn_generator, database_node_name, tables_node)
            )

            self.__oddrn_generator.set_oddrn_paths(
                databases=database_node_name
            )

            for table_node_name, columns_nodes in tables_node.items():
                tables_entities.append(
                    map_table(
                        self.__oddrn_generator,
                        table_node_name,
                        columns_nodes
                    )
                )

        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=[*databases_entities, *tables_entities],
        )
