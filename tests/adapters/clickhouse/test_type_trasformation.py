import unittest

from odd_collector.adapters.clickhouse.mappers.columns import NestedColumnsTransformer
from odd_models.models import Type


class TestTypeTransformation(unittest.TestCase):
    def test_simple_type(self):
        transformer = NestedColumnsTransformer()
        simple_type = "Array"
        another_simple_type = "UInt64"

        result = transformer._get_column_type(simple_type)
        another_result = transformer._get_column_type(another_simple_type)

        self.assertEqual(result, Type.TYPE_LIST)
        self.assertEqual(another_result, Type.TYPE_INTEGER)

    def test_complex_type(self):
        transformer = NestedColumnsTransformer()
        list_type = "Nested"

        list_result = transformer._get_column_type(list_type)
        self.assertEqual(list_result, Type.TYPE_LIST)
