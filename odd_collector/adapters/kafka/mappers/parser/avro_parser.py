import logging as log
from copy import copy
from typing import Dict, List

from odd_models.models import DataSetField, DataSetFieldType
from oddrn_generator import KafkaGenerator

from . import AbstractParser
from .nodes import (
    ArrayNode,
    AvroPrimitiveNode,
    EnumNode,
    Inherited,
    MapNode,
    Node,
    ObjectNode,
    UnionNode,
)
from .types import Field, RawSchema


class AvroParser(AbstractParser):
    def __init__(self, oddrn_generator: KafkaGenerator) -> None:
        self.__cached_records: Dict[str, Node] = {}
        super().__init__(oddrn_generator)

    def map_schema(
        self, schema: RawSchema, references: List[RawSchema] = None
    ) -> List[Field]:
        for r in references or []:
            self.__parse_schema(r)

        fields = self.__parse_schema(schema).to_odd_fields()

        # post processing with setting of base_oddrn
        for field in fields:
            field["oddrn"] = f"{self.base_oddrn}/{field['oddrn']}"
            field["parent_field_oddrn"] = (
                f"{self.base_oddrn}/{field['parent_field_oddrn']}"
                if field.get("parent_field_oddrn") is not None
                else None
            )

        return [
            DataSetField(
                oddrn=field["oddrn"],
                name=field["name"],
                parent_field_oddrn=field["parent_field_oddrn"],
                type=DataSetFieldType(
                    type=field["type"]["type"],
                    logical_type=field["type"]["logical_type"],
                    is_nullable=field["type"]["is_nullable"],
                ),
                is_key=field["is_key"],
                is_value=field["is_value"],
                default_value=field["default_value"],
                stats=field["stats"],
            )
            for field in fields
        ]

    def __parse_schema(
        self,
        node_schema: RawSchema,
        parent_node=None,
        inherited: Inherited = Inherited(),
    ):
        if isinstance(node_schema, str):
            name = inherited.name or node_schema

            if node_schema in self.__cached_records:
                if node_schema in AvroPrimitiveNode.get_primitives():
                    log.warning(
                        "Node definition found both in cached records and primitives. "
                        "Taking one from cached records"
                    )

                node = self.__cached_records[node_schema]
                node.parent_node = parent_node
                return node

            return AvroPrimitiveNode(
                name=name,
                type=node_schema,
                is_nullable=inherited.is_nullable,
                parent_node=parent_node,
            )

        if isinstance(node_schema, List):
            name = inherited.name or "union"
            union_node = UnionNode(
                name=name, is_nullable="null" in node_schema, parent_node=parent_node
            )

            union_node.inner_nodes = [
                self.__parse_schema(node_schema=s, parent_node=union_node)
                for s in node_schema
                if s != "null"
            ]

            return union_node

        node_type = node_schema.get("type")
        node_name = node_schema.get("name")
        description = node_schema.get("doc")
        default = node_schema.get("default")

        if isinstance(node_type, List):
            name = node_name or inherited.name or "union"
            union_node = UnionNode(
                name=name,
                description=description,
                default=default,
                is_nullable="null" in node_type,
                parent_node=parent_node,
            )

            union_node.inner_nodes = [
                self.__parse_schema(
                    node_schema=s,
                    parent_node=union_node,
                    inherited=Inherited(is_nullable=True),
                )
                for s in node_type
                if s != "null"
            ]

            return union_node

        if node_type == "record":
            name = (
                f"{node_schema['namespace']}.{node_schema['name']}"
                if "namespace" in node_schema
                else node_schema["name"]
            )

            object_node = ObjectNode(
                name=name,
                description=description,
                required=node_schema.get("required"),
                default=default,
                parent_node=parent_node,
                is_nullable=inherited.is_nullable,
            )

            object_node.properties = [
                node
                for node in [
                    self.__parse_schema(
                        node_schema=s | s["type"] if isinstance(s["type"], dict) else s,
                        parent_node=object_node,
                    )
                    for s in node_schema.get("fields", [])
                ]
                if node is not None
            ]

            self.__cached_records[node_schema["name"]] = copy(object_node)

            return object_node

        if node_type == "array":
            name = node_name or inherited.name or "array"

            array_node = ArrayNode(
                name=name,
                description=description,
                default=default,
                parent_node=parent_node,
                is_nullable=inherited.is_nullable,
            )

            array_node.items = self.__parse_schema(
                node_schema=node_schema.get("items"), parent_node=array_node
            )

            return array_node

        if node_type == "map":
            name = node_name or inherited.name or "map"

            map_node = MapNode(
                name=name,
                description=description,
                default=default,
                parent_node=parent_node,
                is_nullable=inherited.is_nullable,
            )

            map_value_node = self.__parse_schema(
                node_schema=node_schema["values"], parent_node=map_node
            )
            map_value_node.is_value = True

            map_node.key = AvroPrimitiveNode(
                type="string", is_key=True, parent_node=map_node
            )
            map_node.value = map_value_node

            return map_node

        if node_type == "enum":
            name = node_name or inherited.name or "enum"

            return EnumNode(
                name=name,
                description=description,
                default=default,
                parent_node=parent_node,
                is_nullable=inherited.is_nullable,
            )

        if node_type in self.__cached_records:
            if node_type in AvroPrimitiveNode.get_primitives():
                log.warning(
                    "Node definition found both in cached records and primitives. "
                    "Taking one from cached records"
                )

            node = self.__cached_records[node_type]
            node.parent_node = parent_node
            return node

        if node_type in AvroPrimitiveNode.get_primitives():
            name = node_name or inherited.name or node_type

            return AvroPrimitiveNode(
                name=name,
                type=node_type,
                description=description,
                default=default,
                parent_node=parent_node,
                is_nullable=inherited.is_nullable,
            )

        log.warning(f"Avro parser could not handle the schema: {node_schema}")
