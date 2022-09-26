from typing import List

from odd_models.models import DataEntity, DataEntityType, DataSet

from odd_collector.adapters.druid.domain.column import Column
from odd_collector.adapters.druid.domain.table import Table
from odd_collector.adapters.druid.generator import DruidGenerator
from odd_collector.adapters.druid.mappers.columns import column_to_data_set_field


def table_to_data_entity(oddrn_generator: DruidGenerator,
                         table: Table,
                         columns: List[Column],
                         rows_number: int) -> DataEntity:
    # Set
    oddrn_generator.set_oddrn_paths(**{"schemas": table.schema, "catalogs": table.catalog})

    # Return
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tables", table.name),
        name=table.name,
        type=DataEntityType.DATABASE_SERVICE,
        metadata=[],
        dataset=DataSet(
            rows_number=rows_number,
            field_list=[column_to_data_set_field(oddrn_generator, column) for column in columns]
        )
    )
