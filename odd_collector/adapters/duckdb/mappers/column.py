from typing import Union
from odd_collector_sdk.utils.metadata import extract_metadata, DefinitionType
from oddrn_generator import DuckDBGenerator
from odd_models.models import DataSetField, DataSetFieldType, Type
from .models import DuckDBColumn
from .types import TYPES_SQL_TO_ODD
from ..logger import logger
from ..grammar_parser.parser import parser, traverse_tree
from ..grammar_parser.column_type import (
    Struct,
    ListType,
    Map,
    BasicType,
    ParseType,
    DUnion,
)


def build_dataset_field(
    column: DuckDBColumn, oddrn_generator: DuckDBGenerator
) -> list[DataSetField]:
    logger.debug(f"Build dataset field for {column.name} with type {column.type}")
    type_tree = parser.parse(column.type)
    column_type = traverse_tree(type_tree)

    generated_dataset_fields = []

    def _build_ds_field_from_type(
        column_name: str, column_type: Union[ParseType, str], parent_oddrn=None
    ):

        if parent_oddrn is None:
            oddrn = oddrn_generator.get_oddrn_by_path("columns", column_name)
        else:
            oddrn = f"{parent_oddrn}/keys/{column_name}"

        if isinstance(column_type, Struct):
            generated_dataset_fields.append(
                DataSetField(
                    oddrn=oddrn,
                    name=column_name,
                    metadata=[
                        extract_metadata("duckdb", column, DefinitionType.DATASET_FIELD)
                    ],
                    type=DataSetFieldType(
                        type=Type.TYPE_STRUCT,
                        logical_type=get_logical_type(column_type),
                        is_nullable=column.is_nullable,
                    ),
                    owner=None,
                    parent_field_oddrn=parent_oddrn,
                )
            )
            for field_name, field_type in column_type.fields.items():
                _build_ds_field_from_type(field_name, field_type, oddrn)
        elif isinstance(column_type, Map):
            generated_dataset_fields.append(
                DataSetField(
                    oddrn=oddrn,
                    name=column_name,
                    metadata=[
                        extract_metadata("duckdb", column, DefinitionType.DATASET_FIELD)
                    ],
                    type=DataSetFieldType(
                        type=Type.TYPE_MAP,
                        logical_type=get_logical_type(column_type),
                        is_nullable=column.is_nullable,
                    ),
                    owner=None,
                    parent_field_oddrn=parent_oddrn,
                )
            )
            _build_ds_field_from_type("Key", column_type.key_type, oddrn)
            _build_ds_field_from_type("Value", column_type.value_type, oddrn)

        else:
            odd_type = get_odd_type(column_type)
            logical_type = get_logical_type(column_type)
            logger.debug(
                f"Column {column_name} has ODD type {odd_type} and logical type {logical_type}"
            )
            generated_dataset_fields.append(
                DataSetField(
                    oddrn=oddrn,
                    name=column_name,
                    metadata=[
                        extract_metadata(
                            "databricks", column, DefinitionType.DATASET_FIELD
                        )
                    ],
                    type=DataSetFieldType(
                        type=odd_type,
                        logical_type=logical_type,
                        is_nullable=column.is_nullable,
                    ),
                    owner=None,
                    parent_field_oddrn=parent_oddrn,
                )
            )

    _build_ds_field_from_type(column.name, column_type)
    return generated_dataset_fields


def get_logical_type(type_field: Union[ParseType, str]) -> str:
    if isinstance(type_field, BasicType):
        return type_field.type_name
    elif isinstance(type_field, ListType):
        return f"Array({get_logical_type(type_field.type)})"
    elif isinstance(type_field, Map):
        return f"Map({get_logical_type(type_field.key_type)}, {get_logical_type(type_field.value_type)})"
    elif isinstance(type_field, str):
        return type_field
    elif isinstance(type_field, Struct):
        return (
            "Struct("
            + ", ".join(
                f"{name}: {get_logical_type(type)}"
                for name, type in type_field.fields.items()
            )
            + ")"
        )
    elif isinstance(type_field, DUnion):
        return (
            "Union("
            + ", ".join(
                f"{name}: {get_logical_type(type)}"
                for name, type in type_field.fields.items()
            )
            + ")"
        )
    else:
        return "Unknown"


def get_odd_type(type_field: Union[ParseType, str]) -> Type:
    if isinstance(type_field, BasicType):
        return TYPES_SQL_TO_ODD.get(type_field.type_name, Type.TYPE_UNKNOWN)
    elif isinstance(type_field, ListType):
        return Type.TYPE_LIST
    elif isinstance(type_field, Map):
        return Type.TYPE_MAP
    elif isinstance(type_field, DUnion):
        return Type.TYPE_UNION
    elif isinstance(type_field, str):
        return TYPES_SQL_TO_ODD.get(type_field, Type.TYPE_UNKNOWN)
    elif isinstance(type_field, Struct):
        return Type.TYPE_STRUCT
    else:
        return Type.TYPE_UNKNOWN


def map_column(
    oddrn_generator: DuckDBGenerator, column: DuckDBColumn
) -> list[DataSetField]:
    return build_dataset_field(column, oddrn_generator)
