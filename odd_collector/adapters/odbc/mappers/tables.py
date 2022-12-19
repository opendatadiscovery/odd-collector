from odd_models.models import DataEntity, DataEntityType, DataSet
from oddrn_generator import OdbcGenerator

from ..domain import BaseTable, Table, View
from .columns import map_column
from .metadata import map_metadata


def map_base_table(generator: OdbcGenerator, base_table: BaseTable) -> DataEntity:
    if isinstance(base_table, Table):
        return map_table(generator, base_table)
    elif isinstance(base_table, View):
        return map_view(generator, base_table)

    raise ValueError("Unknown base table type.")


def map_table(generator: OdbcGenerator, table: Table) -> DataEntity:
    params = dict(schemas=table.table_schema, tables=table.table_name)
    generator.set_oddrn_paths(**params)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("tables"),
        name=table.table_name,
        type=DataEntityType.TABLE,
        metadata=[map_metadata(table)],
        dataset=DataSet(
            field_list=[map_column(generator, "tables", col) for col in table.columns]
        ),
    )


def map_view(generator: OdbcGenerator, view: View):
    params = dict(schemas=view.table_schema, views=view.table_name)
    generator.set_oddrn_paths(**params)

    return DataEntity(
        oddrn=generator.get_oddrn_by_path("views"),
        name=view.table_name,
        type=DataEntityType.VIEW,
        metadata=[map_metadata(view)],
        dataset=DataSet(
            field_list=[map_column(generator, "views", col) for col in view.columns]
        ),
    )
