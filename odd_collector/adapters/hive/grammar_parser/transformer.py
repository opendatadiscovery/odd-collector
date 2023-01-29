from lark import Token, Transformer

from odd_collector.adapters.hive.models.column_types import (
    ColumnType,
    ArrayColumnType,
    MapColumnType,
    StructColumnType,
    PrimitiveColumnType,
    UnionColumnType,
)


class HiveFieldTypeTransformer(Transformer):
    def field_definition(self, items) -> (str, ColumnType):
        token, field_type = items

        return token.lower(), field_type

    def type(self, items) -> ColumnType:
        token = items[0]

        if isinstance(token, Token) and token.type == "SIMPLE_TYPE":
            return PrimitiveColumnType(
                field_type=token.lower(), logical_type=token.lower()
            )
        else:
            return token

    def iterable_type(self, items) -> ArrayColumnType:
        assert len(items) == 1
        return ArrayColumnType(specific_type=items[0])

    def map_type(seflf, items) -> MapColumnType:
        assert len(items) == 2
        return MapColumnType(key_type=items[0], value_type=items[1])

    def struct_type(self, items) -> StructColumnType:
        return StructColumnType(fields=dict(items))

    def union_type(self, items):
        return UnionColumnType(types=items)

    def decimal_type(self, items):
        return PrimitiveColumnType(field_type="decimal")

    def char_with_length_type(self, items):
        return PrimitiveColumnType(field_type=items[0].lower())


transformer = HiveFieldTypeTransformer()
