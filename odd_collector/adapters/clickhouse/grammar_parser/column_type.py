from abc import ABC


class ParseType(ABC):
    pass

class BasicType(ParseType):
    def __init__(self, type_name: str):
        self.type_name = type_name

    def __repr__(self):
        return f"BasicType({self.type_name})"

class Array(ParseType):
    def __init__(self, type: ParseType):
        self.type = type

    def __repr__(self):
        return f"Array({self.type})"

class Nested(ParseType):
    def __init__(self, fields: dict):
        self.fields = fields

    def __repr__(self):
        return f"Nested({self.fields})"

class Field:
    def __init__(self, name: str, value: ParseType):
        self.name = name
        self.value = value
