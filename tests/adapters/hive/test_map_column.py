import unittest

from oddrn_generator import HiveGenerator

from odd_collector.adapters.hive.mappers.column import map_column
from odd_collector.adapters.hive.models.column import Column
from odd_collector.adapters.hive.models.column_types import (
    MapColumnType,
    PrimitiveColumnType,
    StructColumnType,
)


class TestFieldTypeParser(unittest.TestCase):
    def setUp(self) -> None:
        self.generator = HiveGenerator(
            host_settings="test", databases="test", tables="test"
        )

    def test_map_simple_column(self):
        column = Column(
            col_name="name",
            col_type=PrimitiveColumnType(field_type="string"),
            comment=None,
            statistic=None,
        )
        entities = map_column(column, self.generator)

        self.assertEqual(len(entities), 1)

        entity = entities[0]

        self.assertEqual(
            entity.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/name",
        )

    def test_map_map_column(self):
        column = Column(
            col_name="name",
            col_type=MapColumnType(
                key_type=PrimitiveColumnType(field_type="string"),
                value_type=PrimitiveColumnType(field_type="int"),
            ),
            comment=None,
            statistic=None,
        )
        entities = map_column(column, self.generator)

        self.assertEqual(len(entities), 3)

        parent_field = entities[0]

        self.assertEqual(
            parent_field.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/name",
        )

        key_field = entities[1]
        self.assertEqual(
            key_field.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/name/key",
        )

        value_field = entities[2]
        self.assertEqual(
            value_field.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/name/value",
        )

    def test_map_nested_map_column(self):
        column = Column(
            col_name="name",
            col_type=MapColumnType(
                key_type=PrimitiveColumnType(field_type="string"),
                value_type=MapColumnType(
                    key_type=PrimitiveColumnType(field_type="string"),
                    value_type=PrimitiveColumnType(field_type="int"),
                ),
            ),
            comment=None,
            statistic=None,
        )
        entities = map_column(column, self.generator)

        self.assertEqual(len(entities), 5)

        parent_field = entities[0]

        self.assertEqual(
            parent_field.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/name",
        )

        key_field = entities[1]
        self.assertEqual(
            key_field.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/name/key",
        )

        value_field = entities[2]
        self.assertEqual(
            value_field.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/name/value",
        )

        nested_key_field = entities[3]
        self.assertEqual(
            nested_key_field.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/name/value/key",
        )

        nested_value_field = entities[4]
        self.assertEqual(
            nested_value_field.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/name/value/value",
        )

    def test_map_stuct_column(self):
        column = Column(
            col_name="address",
            col_type=StructColumnType(
                fields={"city": PrimitiveColumnType(field_type="string")}
            ),
        )

        entities = map_column(column, self.generator)
        self.assertEqual(len(entities), 3)
        field = entities[0]
        self.assertEqual(
            field.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/address",
        )

        city_key = entities[1]
        self.assertEqual(
            city_key.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/address/keys/city",
        )

        city_value = entities[2]
        self.assertEqual(
            city_value.oddrn,
            "//hive/host/test/databases/test/tables/test/columns/address/keys/city/value",
        )
