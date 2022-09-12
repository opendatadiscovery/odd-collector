from typing import Dict, List, Any
from oddrn_generator.generators import Generator
from odd_models.models import (
    DataEntity,
    DataEntityType,
    DataSetField,
    DataSet,
    DataSetFieldType,
    Type as ColumnType,
)


def map_table(oddrn_generator: Generator, table_node_name: str, columns_nodes: List[Dict[str, Any]]):
    return DataEntity(
        oddrn=oddrn_generator.get_oddrn_by_path("tables", table_node_name),
        name=table_node_name,
        type=DataEntityType.TABLE,
        metadata=[],
        dataset=DataSet(
            parent_oddrn=oddrn_generator.get_oddrn_by_path("tables"),
            field_list=[DataSetField(oddrn=oddrn_generator
                                     .get_oddrn_by_path("columns",
                                                        column.get('column_name')),
                                     name=column.get('column_name'),
                                     type=DataSetFieldType(
                                         type=ColumnType.TYPE_UNKNOWN,
                                         logical_type=column.get('type_name'),
                                         is_nullable=False
                                     )
                                     )
                        for column in columns_nodes],
        )
    )
