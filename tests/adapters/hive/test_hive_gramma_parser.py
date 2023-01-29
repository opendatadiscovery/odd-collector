import unittest

from odd_collector.adapters.hive.grammar_parser.parser import parser
from odd_collector.adapters.hive.grammar_parser.transformer import transformer
from odd_collector.adapters.hive.models.column_types import (
    ArrayColumnType,
    ColumnType,
    MapColumnType,
    StructColumnType,
)


class TestFieldTypeParser(unittest.TestCase):
    def test_common_type(self):
        parsed = parser.parse("int")
        transformed = transformer.transform(parsed)

        self.assertIsInstance(transformed, ColumnType)

    def test_aray_type(self):
        parsed = parser.parse("array<string>")
        transformed = transformer.transform(parsed)

        self.assertIsInstance(transformed, ArrayColumnType)
        self.assertIsInstance(transformed.specific_type, ColumnType)
        self.assertEqual(transformed.specific_type.field_type, "string")

    def test_map_type(self):
        parsed = parser.parse("map<int,int>")
        transformed = transformer.transform(parsed)

        self.assertIsInstance(transformed, MapColumnType)
        self.assertEqual(transformed.key_type.field_type, "int")
        self.assertEqual(transformed.value_type.field_type, "int")

    def test_struct_type(self):
        parsed = parser.parse("struct<a:int,b:string>")
        transformed = transformer.transform(parsed)

        self.assertIsInstance(transformed, StructColumnType)
        self.assertIn("a", transformed.fields)
        self.assertIn("b", transformed.fields)

        self.assertEqual(transformed.fields["a"].field_type, "int")
        self.assertEqual(transformed.fields["b"].field_type, "string")

    def test_decimal_type(self):
        parsed = parser.parse("decimal(10,1)")
        transformed = transformer.transform(parsed)

        self.assertIsInstance(transformed, ColumnType)
        self.assertEqual(transformed.field_type, "decimal")


if __name__ == "__main__":
    unittest.main()
