from funcy import lflatten
from odd_collector_sdk.utils.metadata import DefinitionType, extract_metadata
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import HiveGenerator

from odd_collector.adapters.hive.mappers.column import TABLE_CONTEXT, map_column
from odd_collector.adapters.hive.models.table import Table


def map_table(table: Table, generator: HiveGenerator) -> DataEntity:
    """
    Map Hive table to DataEntity

    :param table: Hive table
    :param generator: HiveGenerator
    :return: DataEntity
    """
    generator.set_oddrn_paths(tables=table.name)

    dataset_fields = lflatten(
        [map_column(column, generator, TABLE_CONTEXT) for column in table.columns]
    )

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.name,
        type=DataEntityType.TABLE,
        metadata=[extract_metadata("hive", table, DefinitionType.DATASET)],
        dataset=DataSet(field_list=dataset_fields, rows_number=table.rows_number),
        owner=None,
    )
