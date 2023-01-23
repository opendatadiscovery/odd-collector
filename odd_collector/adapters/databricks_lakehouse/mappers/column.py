from oddrn_generator import DatabricksLakehouseGenerator
from typing import Dict, Any
from odd_models.models import DataSetField, DataSetFieldType, Type


def map_column(
    oddrn_generator: DatabricksLakehouseGenerator, column_node: Dict[str, Any]
):
    name = column_node.get("columnName")
    _type = column_node.get("columnDataType")
    return DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path("columns", name),
        name=name,
        type=DataSetFieldType(
            type=Type.TYPE_UNKNOWN,
            logical_type=_type,
            is_nullable=False,
        ),
    )
