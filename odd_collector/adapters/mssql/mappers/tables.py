from functools import partial
from typing import List

from funcy import group_by, lmap
from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import MssqlGenerator

from ..models import Column, Table
from .columns import map_column
from .metadata import map_metadata


def map_tables(tables: List[Table], columns: List[Column], generator: MssqlGenerator):
    """Map list of Table to DataEntities"""
    cols_by_table = group_by(lambda col: col.table_name, columns)
    for table in tables:
        cols = cols_by_table.get(table.table_name)
        yield map_table(table, cols, generator)


def map_table(table: Table, generator: MssqlGenerator):
    """Map Table to DataEntity"""
    schema: str = table.table_schema
    name: str = table.table_name
    generator.set_oddrn_paths(**{"schemas": schema, "tables": name})

    oddrn = generator.get_oddrn_by_path("tables")

    map_col = partial(map_column, oddrn_generator=generator, parent_oddrn_path="tables")
    dataset = DataSet(field_list=lmap(map_col, table.columns))

    metadata = map_metadata(table)

    return DataEntity(
        oddrn=oddrn,
        name=name,
        type=DataEntityType.TABLE,
        metadata=metadata,
        dataset=dataset,
    )
