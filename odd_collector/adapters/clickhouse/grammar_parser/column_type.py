from abc import ABC, abstractmethod
from typing import List


class ParseType(ABC):
    @abstractmethod
    def to_clickhouse_type(self) -> str:
        pass


class BasicType(ParseType):
    def __init__(self, type_name: str):
        self.type_name = type_name

    def to_clickhouse_type(self) -> str:
        return self.type_name

    def __repr__(self) -> str:
        return f"BasicType({self.type_name})"


class Array(ParseType):
    def __init__(self, type: ParseType):
        self.type = type

    def to_clickhouse_type(self) -> str:
        return f"Array({self.type.to_clickhouse_type()})"

    def __repr__(self) -> str:
        return f"Array({self.type})"


class Tuple(ParseType):
    def __init__(self, types: List[ParseType]):
        self.types = types

    def to_clickhouse_type(self) -> str:
        subtypes = ", ".join(t.to_clickhouse_type() for t in self.types)
        return f"Tuple({subtypes})"

    def __repr__(self) -> str:
        return f"Tuple({self.types})"


class Nested(ParseType):
    def __init__(self, fields: dict):
        self.fields = fields

    def to_clickhouse_type(self) -> str:
        fields_str = ", ".join(
            f"{name} {type.to_clickhouse_type()}"
            for (name, type) in self.fields.items()
        )
        return f"Nested({fields_str})"

    def __repr__(self) -> str:
        return f"Nested({self.fields})"


class Field:
    def __init__(self, name: str, value: ParseType):
        self.name = name
        self.value = value
