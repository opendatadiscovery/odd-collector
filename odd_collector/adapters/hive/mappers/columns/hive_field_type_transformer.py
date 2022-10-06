from lark import Token, Transformer


class HiveFieldTypeTransformer(Transformer):
    def field_definition(self, items):
        return items[1] or {"field_name": str(items[0])}

    def type(self, items):
        obj = items[0]
        return (
            {"type": str(obj).lower()}
            if isinstance(obj, Token) and obj.type == "SIMPLE_TYPE"
            else obj
        )

    def iterable_type(self, items):
        return {"type": "array", "children": items}

    def struct_type(self, items):
        return {"type": "struct", "children": items}

    def map_type(self, items):
        return {
            "type": "map",
            "key_type": items[0],
            "value_type": items[1],
        }

    def union_type(self, items):
        return {"type": "union", "children": items}
