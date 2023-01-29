from lark import Lark

parser = Lark.open(
    "hive_field_type_grammar.lark", rel_to=__file__, parser="lalr", start="type"
)
