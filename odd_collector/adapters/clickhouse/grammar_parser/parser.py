from typing import Union

from lark import Lark, Token, Tree

from ..logger import logger
from .column_type import (
    Array,
    BasicType,
    DateTime,
    DateTime64,
    Field,
    Map,
    NamedTuple,
    Nested,
    ParseType,
    Tuple,
)
from .exceptions import *

"""
This parser uses earley type instead of larl type. We need to support different types of Tuple in Clickhouse
named Tuples as example Tuple(a String, b Boolean), and we need to supplort classic Tuples as example
Tuple(String, Boolean).

LARL does not support the different types of Tuples presented in filed_types.lark
"""

parser = Lark.open("filed_types.lark", rel_to=__file__, parser="earley", start="type")


def traverse_tree(node) -> Union[ParseType, str, Field, None]:
    logger.debug(f"Node: {node}")

    if isinstance(node, Tree):
        if node.data == "date":
            return BasicType("Date")
        if node.data == "date32":
            return BasicType("Date32")
        if node.data == "datetime":
            return DateTime("DateTime", node.children[0].value)
        if node.data == "datetime64":
            return DateTime64(
                "DateTime64", node.children[0].value, node.children[2].value
            )
        if node.data == "array":
            if len(node.children) != 1:
                raise StructureError(
                    f"Invalid array structure: expected 1 child, got: {len(node.children)}"
                )
            child = node.children[0]
            child_value = traverse_tree(child)
            if not isinstance(child_value, ParseType):
                raise NonTypeObjectError(f"Array got a non-type object: {child}")
            return Array(child_value)

        elif node.data == "tuple":
            logger.debug(f"Get tuple node {node}")
            subtypes = []
            for child in node.children:
                child_value = traverse_tree(child)
                if child_value is None:
                    continue
                if not isinstance(child_value, ParseType):
                    raise NonTypeObjectError(f"Tuple got a non-type object: {child}")
                subtypes.append(child_value)
            return Tuple(subtypes)

        elif node.data == "named_tuple":
            logger.debug(f"Get named tuple node {node}")
            fields = {}
            for child in node.children:
                value = traverse_tree(child)
                if value is None:
                    continue
                elif isinstance(value, Field):
                    fields[value.name] = value.value
                else:
                    raise StructureError(f"Got an unexpected nested child: {value}")
            return NamedTuple(fields)

        elif node.data == "map":
            subtypes = []
            for child in node.children:
                child_value = traverse_tree(child)
                if child_value is None:
                    continue
                if not isinstance(child_value, ParseType):
                    raise NonTypeObjectError(f"Tuple got a non-type object: {child}")
                subtypes.append(child_value)
            return Map(subtypes[0], subtypes[1])

        elif node.data == "nested":
            fields = {}
            for child in node.children:
                value = traverse_tree(child)
                if value is None:
                    continue
                elif isinstance(value, Field):
                    fields[value.name] = value.value
                else:
                    raise StructureError(f"Got an unexpected nested child: {value}")
            return Nested(fields)

        elif node.data == "field":
            if len(node.children) != 3:
                raise StructureError(f"Unexpected field structure: {node}")
            field_name_node, _, field_type_node = node.children
            field_name = traverse_tree(field_name_node)
            if not isinstance(field_name, str):
                raise UnexpectedTypeError(
                    f"Unexpected field name type: {type(field_name)}"
                )
            field_type = traverse_tree(field_type_node)
            if not isinstance(field_type, ParseType):
                raise UnexpectedTypeError(
                    f"Unexpected field type type: {type(field_type)}"
                )
            return Field(field_name, field_type)

        else:
            raise UnexpectedTypeError(f"Unexpected tree type: {node.data}")

    elif isinstance(node, Token):
        if node.type == "BASIC_TYPE":
            return BasicType(node.value)
        elif node.type == "FIELD_NAME":
            return node.value
        elif node.type == "WS":
            return None
        else:
            raise UnexpectedTypeError(f"Unexpected token type: {node.type}")

    else:
        raise UnexpectedTypeError(f"Unexpected node type: {type(node)}")
