import logging as log
from typing import Any, Dict, List

from odd_models.models import DataSetField, DataSetFieldType
from oddrn_generator import KafkaGenerator

from . import AbstractParser
from .nodes import ArrayNode, Inherited, JsonPrimitiveNode, Node, ObjectNode, UnionNode
from .types import Field, RawSchema


class JsonParser(AbstractParser):
    def __init__(self, oddrn_generator: KafkaGenerator) -> None:
        self.__definitions: Dict[str, Node] = {}
        super().__init__(oddrn_generator)

    def map_schema(
        self, schema: RawSchema, references: List[RawSchema] = None
    ) -> List[Field]:
        # parsing definitions
        for def_name, def_schema in (
            schema.get("definitions") or schema.get("$defs", {})
        ).items():
            self.__definitions[def_name] = self.__parse_schema(
                node_schema=def_schema, inherited=Inherited(name=def_name)
            )

        # parsing external definitions
        for r in references:
            for def_name, def_schema in (
                r.get("definitions") or r.get("$defs", {})
            ).items():
                self.__definitions[def_name] = self.__parse_schema(
                    node_schema=def_schema, inherited=Inherited(name=def_name)
                )

        fields = self.__parse_schema(schema).to_odd_fields()

        # post processing with setting of base_oddrn
        for field in fields:
            field["oddrn"] = f"{self.base_oddrn}/{field['oddrn']}"
            field["parentFieldOddrn"] = (
                f"{self.base_oddrn}/{field['parentFieldOddrn']}"
                if field.get("parentFieldOddrn") is not None
                else None
            )

        return [
            DataSetField(
                oddrn=field["oddrn"],
                name=field["name"],
                parent_field_oddrn=field["parentFieldOddrn"],
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
        node_schema: Any,
        parent_node: Node = None,
        inherited: Inherited = Inherited(),
    ) -> Node:
        if isinstance(node_schema, str):
            name = inherited.name or node_schema
            return JsonPrimitiveNode(
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

        ref: str = node_schema.get("$ref")
        if ref:
            node = self.__definitions[ref.split("/")[-1]]
            node.parent_node = parent_node
            return node

        node_type = node_schema.get("type")
        description = node_schema.get("description")
        default = node_schema.get("default")

        if isinstance(node_type, List):
            name = inherited.name or "union"
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

        if node_type == "object" or "properties" in node_schema:
            name = node_schema.get("title") or inherited.name or "struct"
            required = node_schema.get("required") or []

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
                        node_schema=s,
                        parent_node=object_node,
                        inherited=Inherited(name=n, is_nullable=n not in required),
                    )
                    for n, s in node_schema.get("properties", {}).items()
                ]
                if node is not None
            ]

            return object_node

        if node_type == "array":
            name = inherited.name or "array"

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

        # don't need this in statement if all the other options are processed above
        if node_type in JsonPrimitiveNode.get_primitives():
            name = inherited.name or node_type

            return JsonPrimitiveNode(
                name=name,
                type=node_type,
                enum_types=[
                    self.__parse_schema(node_schema=s) for s in node_schema.get("enum")
                ]
                if "enum" in node_schema
                else None,
                description=description,
                default=default,
                parent_node=parent_node,
                is_nullable=inherited.is_nullable,
            )

        log.warning(f"Json parser could not handle the schema: {node_schema}")
