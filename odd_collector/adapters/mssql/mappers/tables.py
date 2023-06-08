from functools import partial

from funcy import lmap
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import MssqlGenerator

from ..models import Table
from .columns import map_column
from .metadata import dataset_metadata


def map_table(table: Table, generator: MssqlGenerator):
    """Map Table to DataEntity"""
    schema: str = table.table_schema
    name: str = table.table_name
    generator.set_oddrn_paths(**{"schemas": schema, "tables": name})

    oddrn = generator.get_oddrn_by_path("tables")

    map_col = partial(map_column, oddrn_generator=generator, parent_oddrn_path="tables")
    dataset = DataSet(field_list=lmap(map_col, table.columns))

    return DataEntity(
        oddrn=oddrn,
        name=name,
        owner=None,
        type=DataEntityType.TABLE,
        metadata=[dataset_metadata(entity=table)],
        dataset=dataset,
    )
