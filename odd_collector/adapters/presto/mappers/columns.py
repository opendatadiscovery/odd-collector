from typing import Any, Dict

from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator.generators import PrestoGenerator

from .types import TYPES_SQL_TO_ODD


def split_by_braces(value: str) -> str:
    if isinstance(value, str):
        return value.split("(")[0]
    return value


def map_column(oddrn_generator: PrestoGenerator, column_node: Dict[str, Any]):
    name = column_node.get("column_name")
    _type = column_node.get("type_name")
    return DataSetField(
        oddrn=oddrn_generator.get_oddrn_by_path("columns", name),
        name=name,
        type=DataSetFieldType(
            type=TYPES_SQL_TO_ODD.get(split_by_braces(_type), Type.TYPE_UNKNOWN),
            logical_type=_type,
            is_nullable=False,
        ),
    )
