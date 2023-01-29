from funcy import lflatten
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import HiveGenerator

from odd_collector.adapters.hive.mappers.column import TABLE_CONTEXT, map_column
from odd_collector.adapters.hive.mappers.metadata import map_metadata
from odd_collector.adapters.hive.models.table import Table


def map_table(table: Table, generator: HiveGenerator) -> DataEntity:
    generator.set_oddrn_paths(tables=table.table_name)

    dataset_fields = lflatten(
        [map_column(column, generator, TABLE_CONTEXT) for column in table.columns]
    )

    data_entity = DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.table_name,
        type=DataEntityType.TABLE,
        metadata=map_metadata(table),
        dataset=DataSet(field_list=dataset_fields, rows_number=table.rows_number),
        created_at=table.create_time.isoformat() if table.create_time else None,
    )

    return data_entity
