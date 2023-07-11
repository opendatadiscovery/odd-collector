from unittest import TestCase

from odd_collector.adapters.databricks.grammar_parser.parser import parser, traverse_tree
from odd_collector.adapters.databricks.grammar_parser.column_type import *


class TestGrammarParser(TestCase):

    def test_array_type(self):
        test_type = "array<int>"
        tree = parser.parse(test_type)
        result = traverse_tree(tree)

        self.assertIsInstance(result, ArrayType)
        self.assertIsInstance(result.type, BasicType)

        self.assertEqual(result.type.type_name, "int")

    def test_complex_structure(self):
        test_type = "struct<a:int,b:struct<c:map<string,int>>>"
        tree = parser.parse(test_type)
        result = traverse_tree(tree)

        self.assertIsInstance(result, Struct)

        fields = result.fields

        self.assertIn("a", fields)
        self.assertIsInstance(fields["a"], BasicType)
        self.assertEqual(fields["a"].type_name, "int")


        self.assertIn("b", fields)
        self.assertIsInstance(fields["b"], Struct)

        fields_b = fields["b"].fields

        self.assertIn("c", fields_b)
        self.assertIsInstance(fields_b["c"], Map)
        self.assertIsInstance(fields_b["c"].key_type, BasicType)
        self.assertIsInstance(fields_b["c"].value_type, BasicType)
        self.assertEqual(fields_b["c"].key_type.type_name, "string")
        self.assertEqual(fields_b["c"].value_type.type_name, "int")
