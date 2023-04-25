from lark import Lark


parser = Lark.open(
    "filed_types.lark", rel_to=__file__, parser="lalr", start="type"
)
