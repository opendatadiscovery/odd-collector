from typing import List, Type

from odd_collector_sdk.domain.adapter import AbstractAdapter
from odd_models.models import DataEntityList, DataEntity, DataEntityType, DataSet, DataSetField, DataSetFieldType

from odd_collector.adapters.druid.mappers.client import DruidBaseClient, DruidClient
from odd_collector.adapters.druid.mappers.column import Column
from odd_collector.adapters.druid.mappers.generator import DruidGenerator
from odd_collector.adapters.druid.mappers.table import Table
from odd_collector.adapters.druid.mappers.types import TYPES_DRUID_TO_ODD
from odd_collector.domain.plugin import DruidPlugin


class Adapter(AbstractAdapter):
    def __init__(self, config: DruidPlugin, client: Type[DruidBaseClient] = None) -> None:
        self.__host = config.host
        self.__port = config.port
        self.__oddrn_generator = DruidGenerator(
            host_settings=f"{self.__host}"
        )
        client = client or DruidClient
        self.client = client(config)

    def get_data_source_oddrn(self) -> str:
        return self.__oddrn_generator.get_data_source_oddrn()

    def get_data_entity_list(self) -> DataEntityList:
        return DataEntityList(
            data_source_oddrn=self.get_data_source_oddrn(),
            items=(self.get_data_entities()),
        )

    def get_data_entities(self) -> List[DataEntity]:
        # Fetch
        tables: List[Table] = self.client.get_tables()
        columns: List[Column] = self.client.get_columns()

        # Set oddrn
        [self.set_table_oddrns(table) for table in tables]
        [self.set_column_oddrns(column) for column in columns]

        # Transform
        data_entities = [
            self.table_to_data_entity(table, list(filter(lambda column: column.table == table.name, columns)))
            for table in tables
        ]

        # Return
        return data_entities

    def table_to_data_entity(self, table: Table, columns: List[Column]) -> DataEntity:
        # Return
        return DataEntity(
            oddrn=self.__oddrn_generator.get_oddrn_by_path("tables", table.name),
            name=table.name,
            type=DataEntityType.DATABASE_SERVICE,
            metadata=[],
            dataset=DataSet(
                field_list=list(map(self.column_to_data_set_field, columns))
            )
        )

    def column_to_data_set_field(self, column: Column) -> DataSetField:
        from odd_models import models
        return DataSetField(
            oddrn=self.__oddrn_generator.get_oddrn_by_path("columns", column.name),
            name=column.name,
            type=DataSetFieldType(
                type=TYPES_DRUID_TO_ODD.get(column.type, models.Type.TYPE_UNKNOWN),
                logical_type=column.type,
                is_nullable=column.is_nullable
            )
        )

    def set_table_oddrns(self, table: Table):
        self.__oddrn_generator.set_oddrn_paths(**{"catalogs": table.catalog, "schemas": table.schema, "tables": table.name})

    def set_column_oddrns(self, column: Column):
        self.__oddrn_generator.set_oddrn_paths(**{"catalogs": column.catalog, "schemas": column.schema, "tables": column.table, "columns": column.name})
