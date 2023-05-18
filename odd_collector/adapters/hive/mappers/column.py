from dataclasses import dataclass

from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import HiveGenerator

from odd_collector.adapters.hive.logger import logger
from odd_collector.adapters.hive.mappers.columm_type import map_column_type
from odd_collector.adapters.hive.models.column import Column
from odd_collector.adapters.hive.models.column_types import (
    ArrayColumnType,
    ColumnType,
    MapColumnType,
    PrimitiveColumnType,
    StructColumnType,
    UnionColumnType,
)

from .statistics import map_statistic


@dataclass
class MapContext:
    table_path: str
    columns_path: str


TABLE_CONTEXT = MapContext(table_path="tables", columns_path="tables_columns")
VIEW_CONTEXT = MapContext(table_path="views", columns_path="views_columns")


def map_column(
    column: Column, generator: HiveGenerator, ctx: MapContext = TABLE_CONTEXT
) -> list[DataSetField]:
    if isinstance(column.type, PrimitiveColumnType):
        return map_primitive_column(column, generator, ctx)
    elif isinstance(
        column.type,
        (MapColumnType, StructColumnType, ArrayColumnType, UnionColumnType),
    ):
        return map_complex_column(column, generator, ctx)
    else:
        return map_unknown_dataset_field(column, generator, ctx)


def map_primitive_column(
    column: Column, generator: HiveGenerator, ctx: MapContext
) -> list[DataSetField]:
    col_name = column.name
    col_type = column.type

    generator.set_oddrn_paths(**{ctx.columns_path: col_name})
    dataset_field = DataSetField(
        oddrn=generator.get_oddrn_by_path(ctx.columns_path),
        name=col_name,
        owner=None,
        type=DataSetFieldType(
            type=map_column_type(col_type),
            is_nullable=True,
            logical_type=col_type.logical_type,
        ),
    )

    if column.statistics:
        dataset_field.stats = map_statistic(column.statistics)

    return [dataset_field]


def map_complex_column(
    column: Column, generator: HiveGenerator, ctx: MapContext
) -> list[DataSetField]:
    prefix = f"{generator.get_oddrn_by_path(ctx.table_path)}/columns"
    return map_complex_type(prefix, column.name, column.type)


def map_unknown_dataset_field(
    column: Column, generator: HiveGenerator, ctx: MapContext
) -> list[DataSetField]:
    logger.warning(f"Unknown column type {column.type=}. Return Unknown dataset field")
    generator.set_oddrn_paths(tables_columns=column.name)
    return [
        DataSetField(
            oddrn=generator.get_oddrn_by_path(ctx.columns_path),
            name=column.name,
            owner=None,
            # is_primary_key=column.is_primary,
            type=DataSetFieldType(
                type=Type.TYPE_UNKNOWN,
                is_nullable=False,
                logical_type=column.type.logical_type,
            ),
        )
    ]


def map_complex_type(
    prefix: str, col_name: str, col_type: ColumnType
) -> list[DataSetField]:
    if isinstance(col_type, MapColumnType):
        return map_map_type(prefix, col_name, col_type)
    if isinstance(col_type, StructColumnType):
        return map_struct_type(prefix, col_name, col_type)
    if isinstance(col_type, ArrayColumnType):
        return map_array_type(prefix, col_name, col_type)
    if isinstance(col_type, UnionColumnType):
        return map_union_type(prefix, col_name, col_type)
    else:
        return map_primitive_type(prefix, col_name, col_type)


def map_primitive_type(
    prefix: str, col_name: str, col_type: ColumnType
) -> list[DataSetField]:
    dataset_field = DataSetField(
        oddrn=f"{prefix}/{col_name}",
        owner=None,
        name=col_name,
        type=DataSetFieldType(
            type=map_column_type(col_type),
            is_nullable=False,
            logical_type=col_type.logical_type,
        ),
    )
    return [dataset_field]


def map_union_type(prefix: str, col_name: str, col_type: UnionColumnType):
    dataset_field = DataSetField(
        oddrn=f"{prefix}/{col_name}",
        owner=None,
        name=col_name,
        type=DataSetFieldType(
            type=Type.TYPE_UNION, is_nullable=False, logical_type=col_type.logical_type
        ),
    )
    return [dataset_field]


def map_array_type(prefix: str, col_name: str, col_type: ArrayColumnType):
    dataset_field = DataSetField(
        oddrn=f"{prefix}/{col_name}",
        owner=None,
        name=col_name,
        type=DataSetFieldType(
            type=Type.TYPE_LIST, is_nullable=False, logical_type=col_type.logical_type
        ),
    )
    return [dataset_field]


def map_struct_type(
    prefix: str, col_name: str, col_type: StructColumnType
) -> list[DataSetField]:
    result = []

    field = DataSetField(
        oddrn=f"{prefix}/{col_name}",
        owner=None,
        name=col_name,
        type=DataSetFieldType(
            type=Type.TYPE_STRUCT, is_nullable=False, logical_type=col_type.logical_type
        ),
    )

    result.append(field)

    for key, value in col_type.fields.items():
        k = DataSetField(
            oddrn=f"{field.oddrn}/keys/{key}",
            owner=None,
            name=key,
            type=DataSetFieldType(
                type=Type.TYPE_STRING,
                is_nullable=False,
                logical_type="string",
            ),
            is_key=True,
            parent_field_oddrn=field.oddrn,
        )
        result.append(k)

        values = map_complex_type(f"{field.oddrn}/keys/{key}", "value", value)
        main_type = values[0]
        main_type.parent_field_oddrn = field.oddrn
        main_type.is_value = True

        result.extend(values)
    return result


def map_map_type(
    prefix: str, col_name: str, col_type: MapColumnType
) -> list[DataSetField]:
    field = DataSetField(
        oddrn=f"{prefix}/{col_name}",
        owner=None,
        name=col_name,
        type=DataSetFieldType(
            type=Type.TYPE_MAP, is_nullable=False, logical_type=col_type.logical_type
        ),
    )

    key = map_primitive_type(f"{prefix}/{col_name}", "key", col_type.key_type)[0]
    key.parent_field_oddrn = field.oddrn
    key.is_key = True

    values = map_complex_type(f"{prefix}/{col_name}", "value", col_type.value_type)
    main_type = values[0]
    main_type.parent_field_oddrn = field.oddrn
    main_type.is_value = True

    return [field, key, *values]
