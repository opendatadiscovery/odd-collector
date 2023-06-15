from unittest import TestCase

from odd_models.models import Type
from odd_collector.adapters.databricks.mappers.column import get_logical_type, get_databricks_type
from odd_collector.adapters.databricks.grammar_parser.parser import parser, traverse_tree


class TestCastTypes(TestCase):
    def test_logical_nested_type(self):
        column_type_str = "struct<a:int,b:struct<c:string,d:array<int>>>"
        column_type_tree = parser.parse(column_type_str)
        column_type = traverse_tree(column_type_tree)

        logical_type = get_logical_type(column_type)
        self.assertEqual(logical_type, "Struct(a: int, b: Struct(c: string, d: Array(int)))")

    def test_logical_map_type(self):
        column_type_str = "map<string,int>"
        column_type_tree = parser.parse(column_type_str)
        column_type = traverse_tree(column_type_tree)

        logical_type = get_logical_type(column_type)
        self.assertEqual(logical_type, "Map(string, int)")

    def test_odd_type(self):
        column_type_str = "array<int>"
        column_type_tree = parser.parse(column_type_str)
        column_type = traverse_tree(column_type_tree)

        odd_type = get_databricks_type(column_type)
        self.assertIsInstance(odd_type, Type)
        self.assertEqual(odd_type, Type.TYPE_LIST)

        column_type_str = "map<int,string>"
        column_type_tree = parser.parse(column_type_str)
        column_type = traverse_tree(column_type_tree)

        odd_type = get_databricks_type(column_type)
        self.assertIsInstance(odd_type, Type)
        self.assertEqual(odd_type, Type.TYPE_MAP)
