import re
from typing import Optional, List, Union
from collections import OrderedDict


from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import ClickHouseGenerator
from ..domain import Column, NestedColumn
from ..logger import logger
from .types import TYPES_SQL_TO_ODD
from ..grammar_parser.parser import parser, traverse_tree
from ..grammar_parser.column_type import ParseType, Array, Nested
from ..grammar_parser.exceptions import UnexpectedTypeError


def build_nested_columns(columns: List[Column]) -> List[NestedColumn]:

    parent_columns = OrderedDict()

    for column in columns:
        column_names = column.name.split(".")

        if len(column_names) == 1:
            logger.debug(f"Processing non-nested column {column_names[0]}")

            nested_column = NestedColumn.from_column(column)
            parent_columns[column_names[0]] = nested_column

        elif len(column_names) == 2:
            parent, child = column_names

            logger.debug(f"Processing nested columns {parent}: {child}")

            if parent not in parent_columns:
                logger.debug(f"Creating parent column {parent}")
                parent_column = Column.from_related(column, parent, "Nested")
                parent_nested_column = NestedColumn.from_column(parent_column)
                parent_columns[parent] = parent_nested_column

            logger.debug(f"Creating child column {child}")

            grammar_tree = parser.parse(column.type)
            logger.debug(f"Parsed grammar tree: {grammar_tree}")

            type_tree = traverse_tree(grammar_tree)
            logger.debug(f"Parsed type tree: {type_tree}")
            if not isinstance(type_tree, Array):
                raise UnexpectedTypeError(
                    f"Invalid _tranverse_tree result type "
                    f"(should always be Array): {type(type_tree)}"
                )

            type_tree = type_tree.type

            def build_complex_nested_columns(
                node: ParseType,
                new_name: str,
            ) -> NestedColumn:
                items = []
                if isinstance(node, Nested):
                    for key, value in node.fields.items():
                        item_column = build_complex_nested_columns(value, key)
                        items.append(item_column)

                return NestedColumn.from_column(
                    column,
                    items=items,
                    new_name=new_name,
                    new_type=node.to_clickhouse_type(),
                )

            child_nested_column = build_complex_nested_columns(type_tree, child)
            parent_columns[parent].items.append(child_nested_column)

        else:
            raise ValueError(f"Unexpected column name format: {column.name}")

    return list(parent_columns.values())


def to_dataset_fields(
    oddrn_generator: ClickHouseGenerator,
    table_oddrn_path: str,
    columns: List[NestedColumn],
    owner: Optional[str] = None,
) -> List[DataSetField]:
    def process_nested_column_items(
        column: NestedColumn,
        parent_oddrn: Optional[str],
        res: List,
        is_parent_column: bool = False,
    ) -> Union(List[DataSetField], DataSetField):

        # Unique oddrn for nested column
        oddrn = f"{parent_oddrn}/keys/{column.name}"

        logger.debug(f"Column {column.name} has original clickhouse type {column.type}")
        column_type = _get_column_type(column.type)

        if is_parent_column:
            oddrn = oddrn_generator.get_oddrn_by_path(
                f"{table_oddrn_path}_columns", column.name
            )
            logger.debug(f"Parse first order column {column.name} with oddrn {oddrn}")

        dataset_field = DataSetField(
            oddrn=oddrn,
            name=column.name,
            type=DataSetFieldType(
                type=column_type, is_nullable=False, logical_type=column.type
            ),
            is_key=False,
            parent_field_oddrn=parent_oddrn,
            owner=owner,
        )

        logger.debug(
            f"Dataset field {column.name} has type {column_type} and oddrn {oddrn}"
        )

        res.append(dataset_field)

        if not column.items:
            return dataset_field
        else:
            for item in column.items:
                process_nested_column_items(column=item, parent_oddrn=oddrn, res=res)
        return res

    dataset_fields = []

    for column in columns:
        oddrn = oddrn_generator.get_oddrn_by_path(
            f"{table_oddrn_path}_columns", column.name
        )
        if column.items:
            logger.debug(f"Column {column.name} has nested structure")
            nested_columns = process_nested_column_items(
                column=column, parent_oddrn=None, res=[], is_parent_column=True
            )
            dataset_fields.extend(nested_columns)

        else:
            logger.debug(f"Column {column.name} is basic column with oddrn {oddrn}")
            dataset_field = DataSetField(
                oddrn=oddrn,
                name=column.name,
                owner=owner,
                type=DataSetFieldType(
                    type=_get_column_type(column.type),
                    is_nullable=column.type.startswith("Nullable"),
                    logical_type=column.type,
                ),
            )
            dataset_fields.append(dataset_field)

    return dataset_fields


def _get_column_type(data_type: str) -> Type:
    # trim Nullable
    trimmed = re.search("Nullable\((.+?)\)", data_type)

    if trimmed:
        data_type = trimmed.group(1)

    # trim LowCardinality
    trimmed = re.search("LowCardinality\((.+?)\)", data_type)
    if trimmed:
        data_type = trimmed.group(1)

    if data_type.startswith("Array") or data_type.startswith("Nested"):
        data_type = "Array"

    elif data_type.startswith("Tuple"):
        data_type = "Tuple"

    elif data_type.startswith("Enum8"):
        data_type = "Enum8"

    elif type := re.search("SimpleAggregateFunction\(\w+,\s(\w+)\)", data_type):
        data_type = type.group(1)

    logger.debug(f"Data type after parsing: {data_type}")
    return TYPES_SQL_TO_ODD.get(data_type, Type.TYPE_UNKNOWN)
