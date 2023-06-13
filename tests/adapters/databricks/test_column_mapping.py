from unittest import TestCase

from oddrn_generator import DatabricksUnityCatalogGenerator
from odd_collector.adapters.databricks.mappers.column import build_dataset_field
from odd_collector.adapters.databricks.mappers.models import DatabricksColumn
from odd_models.models import Type


class TestColumnMapping(TestCase):
    def setUp(self) -> None:
        self.oddrn_generator = DatabricksUnityCatalogGenerator(
            host_settings="test"
        )
        self.oddrn_generator.set_oddrn_paths(
        catalogs="test",
        schemas="test",
        tables="test",
    )

    def test_build_simple_dataset(self):
        column = DatabricksColumn(
            name="test",
            type="map<string,boolean>",
            table_catalog="test",
            table_schema="test",
            table_name="test",
            is_nullable=False
        )

        dataset_fields = build_dataset_field(column, self.oddrn_generator)
        self.assertEqual(len(dataset_fields), 1)

        self.assertEqual(dataset_fields[0].parent_field_oddrn, None)
        self.assertEqual(dataset_fields[0].type.type, Type.TYPE_MAP)
        self.assertEqual(dataset_fields[0].type.logical_type, "Map(string, boolean)")


    def test_build_nested_dataset(self):
        column = DatabricksColumn(
            name="test",
            type="struct<a:struct<b:int,c:array<int>>>",
            table_catalog="test",
            table_schema="test",
            table_name="test",
            is_nullable=False
        )

        dataset_fields = build_dataset_field(column, self.oddrn_generator)
        self.assertEqual(len(dataset_fields), 4)

        self.assertEqual(dataset_fields[0].parent_field_oddrn, None)
        self.assertEqual(dataset_fields[0].type.type, Type.TYPE_LIST)
        self.assertEqual(dataset_fields[0].type.logical_type, "Struct(a: Struct(b: int, c: Array(int)))")

        self.assertEqual(dataset_fields[1].parent_field_oddrn, dataset_fields[0].oddrn)
        self.assertEqual(dataset_fields[1].oddrn, dataset_fields[0].oddrn + "/keys/a")
        self.assertEqual(dataset_fields[1].type.type, Type.TYPE_LIST)
        self.assertEqual(dataset_fields[1].type.logical_type, "Struct(b: int, c: Array(int))")

        self.assertEqual(dataset_fields[2].parent_field_oddrn, dataset_fields[1].oddrn)
        self.assertEqual(dataset_fields[2].oddrn, dataset_fields[1].oddrn + "/keys/b")
        self.assertEqual(dataset_fields[2].type.type, Type.TYPE_INTEGER)
        self.assertEqual(dataset_fields[2].type.logical_type, "int")

        self.assertEqual(dataset_fields[3].parent_field_oddrn, dataset_fields[1].oddrn)
        self.assertEqual(dataset_fields[3].oddrn, dataset_fields[1].oddrn + "/keys/c")
        self.assertEqual(dataset_fields[3].type.type, Type.TYPE_LIST)
        self.assertEqual(dataset_fields[3].type.logical_type, "Array(int)")
