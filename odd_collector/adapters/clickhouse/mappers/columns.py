import re
from typing import Optional, List, Union
from collections import defaultdict

from lark import Tree, Token

from odd_models.models import DataSetField, DataSetFieldType, Type
from oddrn_generator import ClickHouseGenerator
from ..domain import Column, NestedColumn
from ..logger import logger
from . import (
    _data_set_field_metadata_excluded_keys,
    _data_set_field_metadata_schema_url,
)
from .metadata import extract_metadata
from .types import TYPES_SQL_TO_ODD
from ..grammar_parser.parser import parser
from ..grammar_parser.column_type import Field, ParseType, BasicType, Array, Nested 


class NestedColumnsTransformer:
    
    def __init__(
        self,
        owner: Optional[str] = None
    ):
        self.owner = owner

    def build_nested_columns(self, columns: List[Column]):

        def build_hierarchy():
            hierarchy = defaultdict(list)
            for column in columns:
                column_names = column.name.split('.')
                parent = column_names[0]
                
                # Rename nested column
                if len(column_names) == 2:
                    column.name = column_names[1]

                hierarchy[parent].append(column)

            return hierarchy

        res: List[NestedColumn] = []

        hierarchy = build_hierarchy()
        logger.info(hierarchy)

        for key, columns in hierarchy.items():

          # Crate first order columns
            if len(columns) == 1 and columns[0].name == key:
                new_column = NestedColumn.from_column(
                    column=columns[0],
                    items=[],
                )
                res.append(new_column)
            else:
                # Create nested columns
                first_order_column = NestedColumn.from_column(
                    column=columns[0],
                    new_name=key,
                    items=[]
                )
                for column in columns:
                    nested_column = NestedColumn.from_column(
                        column=column,
                        items=[]
                    )
                    if 'Nested' in nested_column.type:
                        parsed = parser.parse(nested_column.type)
                        array_type = self._traverse_tree(parsed)
                        self.build_complex_nested_columns(
                            parent_column=nested_column,
                            node=array_type
                        )
                    first_order_column.items.append(nested_column)
                res.append(first_order_column)
        return res


    def build_complex_nested_columns(
        self,
        parent_column: NestedColumn,
        node: Union[ParseType, str, Field, None]  
    ):
        if isinstance(node, Array):
            self.build_complex_nested_columns(parent_column, node.type)
        elif isinstance(node, Nested):
            for key, value in node.fields.items():
                child_column = NestedColumn.from_column(parent_column, key)
                parent_column.items.append(child_column)
                self.build_complex_nested_columns(child_column, value)
        elif isinstance(node, BasicType):
            parent_column.type = node.type_name

    def to_dataset_fields(
        self,
        oddrn_generator: ClickHouseGenerator,
        table_oddrn_path: str,
        columns: List[NestedColumn],
    ):
        def process_nested_column_items(
                column: NestedColumn,
                parent_oddrn: Optional[str],
                res: List,
                first_time: bool=False
        ):

            oddrn = f"{parent_oddrn}/keys/{column.name}"
            column_type = self._get_nested_column_type(column.type)
            if first_time:
                logger.info("Apllied first time")
                oddrn = oddrn_generator.get_oddrn_by_path(
                    f"{table_oddrn_path}_columns", column.name
                ) 
                column_type = self._get_column_type(column.type)

            dataset_field = DataSetField(
                oddrn=oddrn,
                name=column.name,
                type=DataSetFieldType(
                    type=column_type,
                    is_nullable=False,
                    logical_type=str(column_type)
                ),
                is_key=False,
                parent_field_oddrn=parent_oddrn,
                owner=self.owner
            )
            res.append(dataset_field)
            if not column.items:
                return dataset_field
            else:
                for item in column.items:
                    process_nested_column_items(column=item, parent_oddrn=oddrn, res=res)
            return res

        result = []
        # Oddrn of first-order column
        for column in columns:
            oddrn = oddrn_generator.get_oddrn_by_path(
                f"{table_oddrn_path}_columns", column.name
            ) 
            if column.items:
                nested_columns = process_nested_column_items(
                    column=column,
                    parent_oddrn=None,
                    res=[],
                    first_time=True
                    )
                result.extend(nested_columns)
            else:
                dataset_field = DataSetField(
                    oddrn=oddrn,
                    name=column.name,
                    owner=self.owner,
                    type=DataSetFieldType(
                        type=self._get_nested_column_type(column.type),
                        is_nullable=column.type.startswith("Nullable"),
                        logical_type=column.type,
                    )
                )
                result.append(dataset_field)

        return result


    def _get_nested_column_type(self, data_type: str) -> Type:

        if 'Nested' in data_type:
            logger.info("Convert complex nested data type")
            nested_data_tree = parser.parse(data_type)
            nested_data_type = self._traverse_tree(nested_data_tree)

            if isinstance(nested_data_type, Array) or isinstance(nested_data_type, Nested):
                return Type.TYPE_LIST

        data_type = data_type.replace('Array', '').replace('(', '').replace(')', '')

        return self._get_column_type(data_type)

    def _get_column_type(self, data_type: str) -> Type:
        # trim Nullable
        trimmed = re.search("Nullable\((.+?)\)", data_type)
        if trimmed:
            data_type = trimmed.group(1)

        # trim LowCardinality
        trimmed = re.search("LowCardinality\((.+?)\)", data_type)
        if trimmed:
            data_type = trimmed.group(1)

        if data_type.startswith("Array"):
            data_type = "Array"
        elif data_type.startswith("Enum8"):
            data_type = "Enum8"
        elif type := re.search("SimpleAggregateFunction\(\w+,\s(\w+)\)", data_type):
            data_type = type.group(1)

        logger.debug(f"Data type after parsing: {data_type}")
        return TYPES_SQL_TO_ODD.get(data_type, Type.TYPE_UNKNOWN)

    @classmethod
    def _traverse_tree(cls, node) -> Union[ParseType, str, Field, None]:
        if isinstance(node, Tree):
            if node.data == "array":
                if len(node.children) != 1:
                    raise Exception(f"Invalid array structure: expected 1 child, got: {len(node.children)}")
                child = node.children[0]
                child_value = cls._traverse_tree(child)
                if not isinstance(child_value, ParseType):
                    raise Exception(f"Array got no type: {node}")
                return Array(child_value)

            elif node.data == "nested":
                fields = {}
                for child in node.children:
                    value = cls._traverse_tree(child)
                    if value is None:
                        continue
                    elif isinstance(value, Field):
                        fields[value.name] = value.value
                    else:
                        raise Exception(f"Got an unexpected nested child: {value}")
                return Nested(fields)

            elif node.data == "field":
                if len(node.children) != 3:
                    raise Exception(f"Unexpected field structure: {node}")
                field_name_node, _, field_type_node = node.children
                field_name = cls._traverse_tree(field_name_node)
                if not isinstance(field_name, str):
                    raise Exception(f"Unexpected field name type: {type(field_name)}")
                field_type = cls._traverse_tree(field_type_node)
                if not isinstance(field_type, ParseType):
                    raise Exception(f"Unexpected field type type: {type(field_type)}")
                return Field(field_name, field_type)

        elif isinstance(node, Token):
            if node.type == "BASIC_TYPE":
                return BasicType(node.value)
            elif node.type == "FIELD_NAME":
                return node.value
            elif node.type == "WS":
                return None
            else:
                raise Exception(f"Unexpected token type: {node.type}")

        else:
            raise Exception(f"Unexpected node type: {type(node)}")

