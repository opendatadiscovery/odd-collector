import unittest

from odd_collector.adapters.clickhouse.grammar_parser.parser import (
    parser,
    traverse_tree,
)
from odd_collector.adapters.clickhouse.grammar_parser.column_type import (
    BasicType,
    Array,
    Nested,
)


class TestTypeParser(unittest.TestCase):
    def test_array_type(self):
        test_type = "Array(Nested(g String))"
        expected = Array(type=Nested(fields={"g": BasicType(type_name="String")}))
        tree = parser.parse(test_type)
        result = traverse_tree(tree)

        self.assertIsInstance(result, Array)
        self.assertIsInstance(result.type, Nested)
        self.assertEqual(
            expected.type.fields["g"].type_name, result.type.fields["g"].type_name
        )
        result_length = len(result.type.fields)
        self.assertEqual(result_length, 1)

    def test_complex_structure(self):
        test_type = (
            "Array(Nested(UserId UInt64, Username Nested(Name String, Surname String)))"
        )
        expected = Array(
            type=Nested(
                fields={
                    "UserId": BasicType(type_name="UInt64"),
                    "Username": Nested(
                        fields={
                            "Name": BasicType(type_name="String"),
                            "Surname": BasicType(type_name="String"),
                        }
                    ),
                }
            )
        )
        tree = parser.parse(test_type)
        result = traverse_tree(tree)
        self.assertIsInstance(result, Array)
        self.assertIsInstance(result.type, Nested)
        self.assertIsInstance(result.type.fields["Username"], Nested)
        self.assertEqual(
            result.type.fields["UserId"].type_name,
            expected.type.fields["UserId"].type_name,
        )
        self.assertEqual(
            result.type.fields["Username"].fields["Name"].type_name,
            expected.type.fields["Username"].fields["Name"].type_name,
        )
        self.assertEqual(
            result.type.fields["Username"].fields["Surname"].type_name,
            expected.type.fields["Username"].fields["Surname"].type_name,
        )
        result_length = len(result.type.fields)
        self.assertEqual(result_length, 2)

        result_nested_length = len(result.type.fields["Username"].fields)
        self.assertEqual(result_nested_length, 2)
