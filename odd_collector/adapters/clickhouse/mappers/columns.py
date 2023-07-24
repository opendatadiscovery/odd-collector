from typing import List

from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import ClickHouseGenerator

from ..domain import Column
from ..grammar_parser.column_type import (
    Array,
    BasicType,
    Map,
    NamedTuple,
    Nested,
    ParseType,
    Tuple,
)
from ..grammar_parser.parser import parser, traverse_tree
from ..logger import logger
from .types import TYPES_SQL_TO_ODD


def build_dataset_fields(
    columns: List[Column], oddrn_generator: ClickHouseGenerator, table_oddrn_path: str
) -> List[DataSetField]:
    generated_dataset_fields = []
    ds_fields_oddrn = {}

    def _build_dataset_fields(
        column_name: str, column_type: ParseType, parent_oddrn=None
    ):
        logger.debug(
            f"Process {column_name} with type {column_type} and parent {parent_oddrn}"
        )
        column_names = column_name.split(".")
        if parent_oddrn is None:
            oddrn = oddrn_generator.get_oddrn_by_path(
                f"{table_oddrn_path}_columns", column_names[0]
            )
        else:
            oddrn = f"{parent_oddrn}/keys/{column_names[0]}"
        logger.debug(f"Column {column_names[0]} has oddrn {oddrn}")

        if len(column_names) != 1:
            logger.debug(f"Process complex column {column_name}")

            if not column_names[0] in ds_fields_oddrn:
                ds_fields_oddrn[column_names[0]] = oddrn
                generated_dataset_fields.append(
                    DataSetField(
                        oddrn=oddrn,
                        name=column_names[0],
                        type=DataSetFieldType(
                            type=type_to_oddrn_type(column_type),
                            logical_type="Array",
                            is_nullable=False,
                        ),
                        owner=None,
                    )
                )
            else:
                logger.debug(f"Alrady processed {column_names[0]}")
            if isinstance(column_type, Array):
                _build_dataset_fields(
                    column_names[1], column_type.type, ds_fields_oddrn[column_names[0]]
                )
            else:
                logger.debug(
                    f"Expect that in complex structure type starts with 'Array'. Started from {column_type}"
                )
        else:
            if isinstance(column_type, Map):
                generated_dataset_fields.append(
                    DataSetField(
                        oddrn=oddrn,
                        name=column_name,
                        type=DataSetFieldType(
                            type=Type.TYPE_MAP,
                            logical_type=column_type.to_clickhouse_type(),
                            is_nullable=False,
                        ),
                        parent_field_oddrn=parent_oddrn,
                        owner=None,
                    )
                )
                _build_dataset_fields("Key", column_type.key_type, oddrn)
                _build_dataset_fields("Value", column_type.value_type, oddrn)
            elif isinstance(column_type, Nested) or isinstance(column_type, NamedTuple):
                generated_dataset_fields.append(
                    DataSetField(
                        oddrn=oddrn,
                        name=column_name,
                        type=DataSetFieldType(
                            type=Type.TYPE_STRUCT,
                            logical_type=column_type.to_clickhouse_type(),
                            is_nullable=False,
                        ),
                        owner=None,
                        parent_field_oddrn=parent_oddrn,
                    ),
                )
                for field_name, field_type in column_type.fields.items():
                    _build_dataset_fields(field_name, field_type, oddrn)
            elif isinstance(column_type, Tuple):
                generated_dataset_fields.append(
                    DataSetField(
                        oddrn=oddrn,
                        name=column_name,
                        type=DataSetFieldType(
                            type=Type.TYPE_STRUCT,
                            logical_type=column_type.to_clickhouse_type(),
                            is_nullable=False,
                        ),
                        owner=None,
                        parent_field_oddrn=parent_oddrn,
                    ),
                )
                for count, subtype in enumerate(column_type.types):
                    _build_dataset_fields(str(count), subtype, oddrn)
            else:
                odd_type = type_to_oddrn_type(column_type)
                logger.debug(
                    f"Column {column_name} has ODD type {odd_type} and logical type {column_type.to_clickhouse_type()}"
                )
                generated_dataset_fields.append(
                    DataSetField(
                        oddrn=oddrn,
                        name=column_name,
                        type=DataSetFieldType(
                            type=odd_type,
                            logical_type=column_type.to_clickhouse_type(),
                            is_nullable=False,
                        ),
                        owner=None,
                        parent_field_oddrn=parent_oddrn,
                    )
                )

    for column in columns:
        type_tree = parser.parse(column.type)
        column_type = traverse_tree(type_tree)

        _build_dataset_fields(column.name, column_type)

    return generated_dataset_fields


def type_to_oddrn_type(column_type):
    if isinstance(column_type, Array):
        return Type.TYPE_LIST
    elif isinstance(column_type, Nested):
        return Type.TYPE_STRUCT
    elif isinstance(column_type, Map):
        return Type.TYPE_MAP
    elif isinstance(column_type, BasicType):
        return TYPES_SQL_TO_ODD.get(column_type.type_name, Type.TYPE_UNKNOWN)
    elif isinstance(column_type, str):
        return TYPES_SQL_TO_ODD.get(column_type, Type.TYPE_UNKNOWN)
    elif isinstance(column_type, Tuple):
        return Type.TYPE_STRUCT
    elif isinstance(column_type, NamedTuple):
        return Type.TYPE_STRUCT
    else:
        return Type.TYPE_UNKNOWN
