from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List

from more_itertools import flatten

from .types import Field


def flatten_list(_list: List):
    return list(flatten(_list))


@dataclass
class Node(ABC):
    _oddrn: str = None

    name: str = None
    description: str = None
    default: Any = None
    is_nullable: bool = True
    is_key: bool = False
    is_value: bool = False
    parent_node: 'Node' = None

    @abstractmethod
    def to_odd_fields(self) -> List[Field]:
        raise NotImplementedError

    @abstractmethod
    def resource_prefix(self) -> str:
        raise NotImplementedError

    def _get_oddrn(self) -> str:
        if self._oddrn is None:
            if self.parent_node is not None:
                prefix = 'keys' if self.is_key \
                    else 'values' if self.is_value \
                    else self.parent_node.resource_prefix()

                self._oddrn = f'{self.parent_node._get_oddrn()}/{prefix}/{self.name}'
            else:
                self._oddrn = self.name

        return self._oddrn

    def _get_parent_oddrn(self) -> str:
        return self.parent_node._get_oddrn() if self.parent_node else None

    def _create_base_field(self) -> Field:
        return {
            'oddrn': self._get_oddrn(),
            'parent_field_oddrn': self._get_parent_oddrn(),
            'name': self.name,
            'default_value': self.default,
            'description': self.description,
            'is_key': self.is_key,
            'is_value': self.is_value,
            'stats': {}
        }


@dataclass
class ObjectNode(Node):
    properties: List[Node] = field(default_factory=list)
    required: List[str] = field(default_factory=list)

    def to_odd_fields(self) -> List[Field]:
        struct_field = self._create_base_field() | {'type': {
            'type': 'TYPE_STRUCT',
            'logical_type': None,
            'is_nullable': self.is_nullable
        }}

        return [struct_field] + flatten_list([
            p.to_odd_fields()
            for p in self.properties
        ])

    def resource_prefix(self) -> str:
        return 'fields'


@dataclass
class PrimitiveNode(Node, ABC):
    type: str = 'any'

    @abstractmethod
    def get_primitives(self):
        raise NotImplementedError

    def resource_prefix(self) -> str:
        raise RuntimeError('PrimitiveNode does not have resource prefix')

    def _create_base_field(self) -> Field:
        if self.name is None:
            self.name = self.type

        return super()._create_base_field()


@dataclass
class JsonPrimitiveNode(PrimitiveNode):
    __primitives = {
        'string': 'TYPE_STRING',
        'integer': 'TYPE_INTEGER',
        'int': 'TYPE_INTEGER',
        'number': 'TYPE_NUMBER',
        'boolean': 'TYPE_BOOLEAN',
    }

    enum_types: List[Node] = None

    @classmethod
    def get_primitives(cls):
        return list(cls.__primitives.keys())

    def to_odd_fields(self) -> List[Field]:
        return [self._create_base_field() | {'type': {
            'type': self.__primitives[self.type] if self.enum_types is None else "TYPE_ENUM",
            'logical_type': [s.name for s in self.enum_types] if self.enum_types is not None else None,
            'is_nullable': self.is_nullable
        }}]


@dataclass
class AvroPrimitiveNode(PrimitiveNode):
    __primitives = {
        'string': 'TYPE_STRING',
        'float': 'TYPE_NUMBER',
        'double': 'TYPE_NUMBER',
        'int': 'TYPE_INTEGER',
        'long': 'TYPE_INTEGER',
        'boolean': 'TYPE_BOOLEAN',
        'bytes': 'TYPE_BINARY'
    }

    @classmethod
    def get_primitives(cls):
        return list(cls.__primitives.keys())

    def to_odd_fields(self) -> List[Field]:
        return [self._create_base_field() | {'type': {
            'type': self.__primitives[self.type],
            # TODO: add logical types mapping
            'logical_type': None,
            'is_nullable': self.is_nullable
        }}]


@dataclass
class ArrayNode(Node):
    items: Node = None

    def to_odd_fields(self) -> List[Field]:
        return [self._create_base_field() | {'type': {
            'type': 'TYPE_LIST',
            'logical_type': None,
            'is_nullable': self.is_nullable
        }}] + self.items.to_odd_fields()

    def resource_prefix(self) -> str:
        return 'items'


@dataclass
class UnionNode(Node):
    inner_nodes: List[Node] = field(default_factory=list)

    def to_odd_fields(self) -> List[Field]:
        if len(self.inner_nodes) == 1:
            node = self.inner_nodes[0]
            for attr in ['parent_node', 'name', 'description', 'default', 'is_key', 'is_value']:
                setattr(node, attr, self.__getattribute__(attr))

            return node.to_odd_fields()

        return [self._create_base_field() | {'type': {
            'type': 'TYPE_UNION',
            'logical_type': None,
            'is_nullable': self.is_nullable
        }}] + flatten_list([
            inner_schema.to_odd_fields()
            for inner_schema in self.inner_nodes
        ])

    def resource_prefix(self) -> str:
        return 'types'


@dataclass
class MapNode(Node):
    key: Node = None
    value: Node = None

    def to_odd_fields(self) -> List[Field]:
        map_field = self._create_base_field() | {'type': {
            'type': 'TYPE_MAP',
            'logical_type': None,
            'is_nullable': self.is_nullable
        }}

        return [map_field] + self.key.to_odd_fields() + self.value.to_odd_fields()

    def resource_prefix(self) -> str:
        raise RuntimeError('MapNode does not have single resource prefix. '
                           'Key/value node should identify prefix by is_key/is_value property')


@dataclass
class EnumNode(Node):
    values: List = field(default_factory=list)

    def to_odd_fields(self) -> List[Field]:
        return [self._create_base_field() | {'type': {
            'type': 'TYPE_ENUM',
            'logical_type': None,
            'is_nullable': self.is_nullable
        }}]

    def resource_prefix(self) -> str:
        raise RuntimeError('EnumNode does not have resource prefix')


@dataclass
class Inherited:
    is_nullable: bool = False
    name: str = None
