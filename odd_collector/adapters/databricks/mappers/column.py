from oddrn_generator import DatabricksUnityCatalogGenerator
from typing import Dict, Any
from odd_models.models import DataSetField, DataSetFieldType, Type
from .types import TYPES_SQL_TO_ODD


def split_by_braces(value: str) -> str:
    if isinstance(value, str):
        return value.split("(")[0].split("<")[0]
    return value


def map_column(
    oddrn_generator: DatabricksUnityCatalogGenerator, column_node: Dict[str, Any]
):
    name = column_node.get("name")
    _type = column_node.get("type_text")
    return DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path("columns", name),
        name=name,
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(split_by_braces(_type), Type.TYPE_UNKNOWN),
            logical_type=_type,
            is_nullable=False,
        ),
    )
