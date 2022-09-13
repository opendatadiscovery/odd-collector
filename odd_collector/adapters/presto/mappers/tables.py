from typing import Dict, List, Any
from oddrn_generator.generators import PrestoGenerator
from .columns import map_column
from pandas import DataFrame
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataSet,
)


def map_table(oddrn_generator: PrestoGenerator, table_node_name: str,
              columns_nodes: List[Dict[str, Any]],
              tables_df: DataFrame, catalog_name: str, schema_name: str
              )\
        -> DataEntity:
    tb_type = tables_df[(tables_df.table_cat == catalog_name)
                        & (tables_df.table_schem == schema_name)
                        ]['table_type'].iloc[0]
    if tb_type == 'VIEW':
        _type = DataEntityType.VIEW
    else:
        _type = DataEntityType.TABLE
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tables", table_node_name),
        name=table_node_name,
        type=_type,
        metadata=[],
        dataset=DataSet(
            parent_oddrn=oddrn_generator.get_oddrn_by_path("tables"),
            field_list=[map_column(oddrn_generator, column_node)
                        for column_node in columns_nodes],
        )
    )
