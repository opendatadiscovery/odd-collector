from abc import ABC, abstractmethod

from funcy import join


class ParseType(ABC):
    @abstractmethod
    def to_databricks_type(self) -> str:
        pass


class BasicType(ParseType):
    def __init__(self, type_name: str):
        self.type_name = type_name

    def to_databricks_type(self) -> str:
        return self.type_name

    def __repr__(self) -> str:
        return f"BasicType<{self.type_name}>"


class ArrayType(ParseType):
    def __init__(self, type: ParseType):
        self.type = type

    def to_databricks_type(self) -> str:
        return f"Array<{self.type.to_databricks_type()}>"

    def __repr__(self) -> str:
        return f"Array<{self.type}>"


class Map(ParseType):
    def __init__(self, key_type: ParseType, value_type: ParseType):
        self.key_type = key_type
        self.value_type = value_type

    def to_databricks_type(self) -> str:
        return f"Map<{self.key_type.to_databricks_type()}, {self.value_type.to_databricks_type()}>"

    def __repr__(self) -> str:
        return f"Map<{self.key_type}, {self.value_type}>"


class Field:
    def __init__(self, name: str, type_value: ParseType):
        self.name = name
        self.type_value = type_value


class Struct(ParseType):
    def __init__(self, fields: dict):
        self.fields = fields

    def to_databricks_type(self) -> str:
        fields_str = ", ",join(
                f"{name}: {type_val.to_databricks_type()}"
                for (name, type_val) in self.fields.items()
        )

        return f"Struct<{fields_str}>"

    def __repr__(self) -> str:
        return f"Struct<{self.fields}>"
